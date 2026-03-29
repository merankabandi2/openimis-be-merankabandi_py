import json
import logging
from datetime import datetime, timezone

from django.db.models import Q, F, Subquery, OuterRef

from grievance_social_protection.models import Ticket, Comment
from individual.models import Group, GroupIndividual, Individual
from location.models import Location, UserDistrict
from merankabandi.models import SensitizationTraining, BehaviorChangePromotion, MicroProject
from payroll.models import BenefitConsumption, PayrollBenefitConsumption
from social_protection.models import GroupBeneficiary

logger = logging.getLogger(__name__)


def _sid(val):
    """Return str(uuid) or None."""
    return str(val) if val is not None else None


def _epoch_ms_now():
    """Current UTC time as epoch-milliseconds integer."""
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _parse_since(last_sync_timestamp):
    """Convert epoch-ms integer (or None) to a tz-aware datetime, or None."""
    if last_sync_timestamp is None:
        return None
    try:
        ts = int(last_sync_timestamp)
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def _split_changes(qs, since, id_field="id", deleted_qs=None):
    """
    Split a queryset into created / updated / deleted lists.

    For initial sync (since is None): everything goes into created.
    For delta sync: date_created >= since -> created, else -> updated.
    deleted_qs (if provided) supplies deleted IDs.
    """
    if since is None:
        return list(qs), [], []

    created_ids = set(
        qs.filter(date_created__gte=since).values_list(id_field, flat=True)
    )
    records = list(qs)
    created = [r for r in records if getattr(r, id_field) in created_ids]
    updated = [r for r in records if getattr(r, id_field) not in created_ids]

    deleted_ids = []
    if deleted_qs is not None:
        deleted_ids = [_sid(pk) for pk in deleted_qs.values_list(id_field, flat=True)]

    return created, updated, deleted_ids


class SyncService:
    """
    WatermelonDB-compatible sync service.

    Table names and field names match the mobile schema exactly.
    Timestamps are epoch-ms integers.
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_user_province_ids(user):
        """Return province Location IDs for the user. None for superusers."""
        interactive_user = user._u if hasattr(user, "_u") else user
        if interactive_user.is_superuser:
            return None
        districts = UserDistrict.get_user_districts(user)
        return [d.location_id for d in districts]

    # ------------------------------------------------------------------
    # LOCATIONS
    # ------------------------------------------------------------------

    @classmethod
    def _pull_locations(cls, province_ids, since):
        """
        Return Province (D), Commune (W), Colline (V) locations.
        Location uses VersionedModel (validity_to, no is_deleted/date_created).
        For delta sync we cannot reliably detect created-vs-updated, so
        everything goes into 'created' on initial and 'updated' on delta.
        """
        qs = Location.objects.filter(
            type__in=["D", "W", "V"],
            validity_to__isnull=True,
        ).select_related("parent")

        if province_ids is not None:
            # Include the provinces themselves, their communes, and collines
            qs = qs.filter(
                Q(id__in=province_ids)
                | Q(parent_id__in=province_ids)
                | Q(parent__parent_id__in=province_ids)
            )

        def _serialize(loc):
            return {
                "id": str(loc.uuid),
                "server_id": str(loc.uuid),
                "name": loc.name or "",
                "code": loc.code or "",
                "loc_type": loc.type,
                "parent_id": str(loc.parent.uuid) if loc.parent_id else None,
                "synced_at": _epoch_ms_now(),
            }

        records = list(qs)
        if since is None:
            return {
                "created": [_serialize(r) for r in records],
                "updated": [],
                "deleted": [],
            }
        else:
            # Location has no date_created; treat all as updated on delta
            return {
                "created": [],
                "updated": [_serialize(r) for r in records],
                "deleted": [],
            }

    # ------------------------------------------------------------------
    # HOUSEHOLDS (Group model with head info denormalized)
    # ------------------------------------------------------------------

    @classmethod
    def _pull_households(cls, province_ids, since):
        qs = Group.objects.filter(is_deleted=False).select_related("location")
        if province_ids is not None:
            qs = qs.filter(location__parent__parent__id__in=province_ids)

        if since is not None:
            qs = qs.filter(date_updated__gte=since)

        # Prefetch HEAD individuals in bulk to avoid N+1
        group_ids = list(qs.values_list("id", flat=True))
        head_gi_qs = GroupIndividual.objects.filter(
            group_id__in=group_ids,
            is_deleted=False,
            role=GroupIndividual.Role.HEAD,
        ).select_related("individual")
        head_map = {}
        for gi in head_gi_qs:
            head_map[gi.group_id] = gi.individual

        def _serialize(g):
            head = head_map.get(g.id)
            json_ext = g.json_ext or {}
            return {
                "id": _sid(g.id),
                "server_id": _sid(g.id),
                "code": g.code or "",
                "head_first_name": head.first_name if head else "",
                "head_last_name": head.last_name if head else "",
                "location_id": str(g.location.uuid) if g.location_id else None,
                "selection_status": json_ext.get("selection_status"),
                "pmt_score": json_ext.get("pmt_score"),
                "json_ext": json.dumps(json_ext) if json_ext else None,
                "synced_at": _epoch_ms_now(),
            }

        deleted_qs = None
        if since is not None:
            deleted_qs = Group.objects.filter(
                is_deleted=True, date_updated__gte=since
            )
            if province_ids is not None:
                deleted_qs = deleted_qs.filter(
                    location__parent__parent__id__in=province_ids
                )

        created, updated, deleted_ids = _split_changes(qs, since)
        return {
            "created": [_serialize(r) for r in created],
            "updated": [_serialize(r) for r in updated],
            "deleted": deleted_ids if deleted_ids else (
                [_sid(pk) for pk in deleted_qs.values_list("id", flat=True)]
                if deleted_qs is not None else []
            ),
        }

    # ------------------------------------------------------------------
    # INDIVIDUALS
    # ------------------------------------------------------------------

    @classmethod
    def _pull_individuals(cls, province_ids, since):
        qs = Individual.objects.filter(is_deleted=False)
        if province_ids is not None:
            qs = qs.filter(location__parent__parent__id__in=province_ids)

        if since is not None:
            qs = qs.filter(date_updated__gte=since)

        # Build individual -> (group_id, role) mapping via GroupIndividual
        ind_ids = list(qs.values_list("id", flat=True))
        gi_qs = GroupIndividual.objects.filter(
            individual_id__in=ind_ids,
            is_deleted=False,
        ).values("individual_id", "group_id", "role")
        gi_map = {}
        for gi in gi_qs:
            gi_map[gi["individual_id"]] = (gi["group_id"], gi["role"])

        def _serialize(ind):
            group_id, role = gi_map.get(ind.id, (None, None))
            json_ext = ind.json_ext or {}
            social_id = json_ext.get("social_id", "")
            gender = json_ext.get("gender") or json_ext.get("sexe") or ""
            return {
                "id": _sid(ind.id),
                "server_id": _sid(ind.id),
                "household_id": _sid(group_id),
                "first_name": ind.first_name or "",
                "last_name": ind.last_name or "",
                "dob": ind.dob.isoformat() if ind.dob else None,
                "gender": gender,
                "social_id": social_id,
                "role": role,
                "json_ext": json.dumps(json_ext) if json_ext else None,
                "synced_at": _epoch_ms_now(),
            }

        deleted_qs = None
        if since is not None:
            deleted_qs = Individual.objects.filter(
                is_deleted=True, date_updated__gte=since
            )
            if province_ids is not None:
                deleted_qs = deleted_qs.filter(
                    location__parent__parent__id__in=province_ids
                )

        created, updated, deleted_ids = _split_changes(qs, since)
        return {
            "created": [_serialize(r) for r in created],
            "updated": [_serialize(r) for r in updated],
            "deleted": deleted_ids if deleted_ids else (
                [_sid(pk) for pk in deleted_qs.values_list("id", flat=True)]
                if deleted_qs is not None else []
            ),
        }

    # ------------------------------------------------------------------
    # BENEFICIARIES (GroupBeneficiary)
    # ------------------------------------------------------------------

    @classmethod
    def _pull_beneficiaries(cls, province_ids, since):
        qs = GroupBeneficiary.objects.filter(
            is_deleted=False
        ).select_related("group", "benefit_plan")
        if province_ids is not None:
            qs = qs.filter(group__location__parent__parent__id__in=province_ids)

        if since is not None:
            qs = qs.filter(date_updated__gte=since)

        def _serialize(gb):
            json_ext = gb.json_ext or {}
            return {
                "id": _sid(gb.id),
                "server_id": _sid(gb.id),
                "household_id": _sid(gb.group_id),
                "benefit_plan_code": gb.benefit_plan.code if gb.benefit_plan else "",
                "benefit_plan_name": gb.benefit_plan.name if gb.benefit_plan else "",
                "status": gb.status or "",
                "json_ext": json.dumps(json_ext) if json_ext else None,
                "synced_at": _epoch_ms_now(),
            }

        deleted_qs = None
        if since is not None:
            deleted_qs = GroupBeneficiary.objects.filter(
                is_deleted=True, date_updated__gte=since
            )
            if province_ids is not None:
                deleted_qs = deleted_qs.filter(
                    group__location__parent__parent__id__in=province_ids
                )

        created, updated, deleted_ids = _split_changes(qs, since)
        return {
            "created": [_serialize(r) for r in created],
            "updated": [_serialize(r) for r in updated],
            "deleted": deleted_ids if deleted_ids else (
                [_sid(pk) for pk in deleted_qs.values_list("id", flat=True)]
                if deleted_qs is not None else []
            ),
        }

    # ------------------------------------------------------------------
    # ACTIVITIES (combined: MicroProject, SensitizationTraining,
    #             BehaviorChangePromotion)
    # ------------------------------------------------------------------

    @classmethod
    def _pull_activities(cls, province_ids, since):
        """
        These are plain Django models (no is_deleted, no date_created/date_updated).
        For delta sync we cannot reliably detect changes, so we return all
        matching records as 'updated'. For initial sync, all as 'created'.
        """

        def _loc_filter(qs):
            if province_ids is not None:
                qs = qs.filter(location__parent__parent__id__in=province_ids)
            return qs

        def _serialize_mp(mp):
            return {
                "id": _sid(mp.id),
                "server_id": _sid(mp.id),
                "activity_type": "micro_project",
                "report_date": mp.report_date.isoformat() if mp.report_date else None,
                "location_id": str(mp.location.uuid) if mp.location_id else None,
                "location_name": mp.location.name if mp.location_id else "",
                "male_participants": mp.male_participants,
                "female_participants": mp.female_participants,
                "twa_participants": mp.twa_participants,
                "validation_status": mp.validation_status or "",
                "json_ext": None,
                "synced_at": _epoch_ms_now(),
            }

        def _serialize_st(st):
            return {
                "id": _sid(st.id),
                "server_id": _sid(st.id),
                "activity_type": "sensitization",
                "report_date": st.sensitization_date.isoformat() if st.sensitization_date else None,
                "location_id": str(st.location.uuid) if st.location_id else None,
                "location_name": st.location.name if st.location_id else "",
                "male_participants": st.male_participants,
                "female_participants": st.female_participants,
                "twa_participants": st.twa_participants,
                "validation_status": st.validation_status or "",
                "json_ext": None,
                "synced_at": _epoch_ms_now(),
            }

        def _serialize_bc(bc):
            return {
                "id": _sid(bc.id),
                "server_id": _sid(bc.id),
                "activity_type": "behavior_change",
                "report_date": bc.report_date.isoformat() if bc.report_date else None,
                "location_id": str(bc.location.uuid) if bc.location_id else None,
                "location_name": bc.location.name if bc.location_id else "",
                "male_participants": bc.male_participants,
                "female_participants": bc.female_participants,
                "twa_participants": bc.twa_participants,
                "validation_status": bc.validation_status or "",
                "json_ext": None,
                "synced_at": _epoch_ms_now(),
            }

        mp_qs = _loc_filter(
            MicroProject.objects.select_related("location")
        )
        st_qs = _loc_filter(
            SensitizationTraining.objects.select_related("location")
        )
        bc_qs = _loc_filter(
            BehaviorChangePromotion.objects.select_related("location")
        )

        all_records = (
            [_serialize_mp(r) for r in mp_qs]
            + [_serialize_st(r) for r in st_qs]
            + [_serialize_bc(r) for r in bc_qs]
        )

        if since is None:
            return {"created": all_records, "updated": [], "deleted": []}
        else:
            return {"created": [], "updated": all_records, "deleted": []}

    # ------------------------------------------------------------------
    # GRIEVANCES (Ticket)
    # ------------------------------------------------------------------

    @classmethod
    def _pull_grievances(cls, province_ids, since):
        qs = Ticket.objects.filter(is_deleted=False)

        # Filter by location through the ticket's province/commune/colline text fields
        # or through the reporter individual's location
        if province_ids is not None:
            province_names = list(
                Location.objects.filter(
                    id__in=province_ids, validity_to__isnull=True
                ).values_list("name", flat=True)
            )
            if province_names:
                qs = qs.filter(province__in=province_names)

        if since is not None:
            qs = qs.filter(date_updated__gte=since)

        def _serialize(t):
            resolved_at = None
            if t.status in ("RESOLVED", "CLOSED") and t.date_updated:
                resolved_at = t.date_updated.isoformat() if hasattr(t.date_updated, "isoformat") else str(t.date_updated)

            created_at = None
            if t.date_created:
                created_at = t.date_created.isoformat() if hasattr(t.date_created, "isoformat") else str(t.date_created)

            return {
                "id": _sid(t.id),
                "server_id": _sid(t.id),
                "code": t.code or "",
                "title": t.title or "",
                "description": t.description or "",
                "status": t.status or "",
                "category": t.category or "",
                "channel": t.channel or "",
                "beneficiary_id": t.reporter_id or None,
                "created_at": created_at,
                "resolved_at": resolved_at,
                "is_local": False,
                "synced_at": _epoch_ms_now(),
            }

        deleted_qs = None
        if since is not None:
            deleted_qs = Ticket.objects.filter(
                is_deleted=True, date_updated__gte=since
            )
            if province_ids is not None:
                province_names = list(
                    Location.objects.filter(
                        id__in=province_ids, validity_to__isnull=True
                    ).values_list("name", flat=True)
                )
                if province_names:
                    deleted_qs = deleted_qs.filter(province__in=province_names)

        created, updated, deleted_ids = _split_changes(qs, since)
        return {
            "created": [_serialize(r) for r in created],
            "updated": [_serialize(r) for r in updated],
            "deleted": deleted_ids if deleted_ids else (
                [_sid(pk) for pk in deleted_qs.values_list("id", flat=True)]
                if deleted_qs is not None else []
            ),
        }

    # ------------------------------------------------------------------
    # GRIEVANCE COMMENTS (Comment)
    # ------------------------------------------------------------------

    @classmethod
    def _pull_grievance_comments(cls, province_ids, since):
        qs = Comment.objects.filter(is_deleted=False).select_related("ticket")

        if since is not None:
            qs = qs.filter(date_updated__gte=since)

        def _serialize(c):
            created_at = None
            if c.date_created:
                created_at = c.date_created.isoformat() if hasattr(c.date_created, "isoformat") else str(c.date_created)

            # Determine author from commenter if available
            author = ""
            if c.commenter_id:
                author = str(c.commenter_id)

            return {
                "id": _sid(c.id),
                "server_id": _sid(c.id),
                "grievance_id": _sid(c.ticket_id),
                "text": c.comment or "",
                "author": author,
                "created_at": created_at,
                "is_local": False,
                "synced_at": _epoch_ms_now(),
            }

        deleted_qs = None
        if since is not None:
            deleted_qs = Comment.objects.filter(
                is_deleted=True, date_updated__gte=since
            )

        created, updated, deleted_ids = _split_changes(qs, since)
        return {
            "created": [_serialize(r) for r in created],
            "updated": [_serialize(r) for r in updated],
            "deleted": deleted_ids if deleted_ids else (
                [_sid(pk) for pk in deleted_qs.values_list("id", flat=True)]
                if deleted_qs is not None else []
            ),
        }

    # ------------------------------------------------------------------
    # PAYMENTS (BenefitConsumption)
    # ------------------------------------------------------------------

    @classmethod
    def _pull_payments(cls, province_ids, since):
        qs = BenefitConsumption.objects.filter(is_deleted=False).select_related(
            "individual"
        )

        if province_ids is not None:
            qs = qs.filter(
                individual__location__parent__parent__id__in=province_ids
            )

        if since is not None:
            qs = qs.filter(date_updated__gte=since)

        # Build a map of benefit_consumption_id -> payment_method via PayrollBenefitConsumption
        bc_ids = list(qs.values_list("id", flat=True))
        pbc_qs = PayrollBenefitConsumption.objects.filter(
            benefit_id__in=bc_ids
        ).select_related("payroll").values("benefit_id", "payroll__payment_method")
        pm_map = {}
        for pbc in pbc_qs:
            pm_map[pbc["benefit_id"]] = pbc["payroll__payment_method"] or ""

        # Build individual -> group mapping
        ind_ids = list(qs.values_list("individual_id", flat=True).distinct())
        gi_qs = GroupIndividual.objects.filter(
            individual_id__in=ind_ids,
            is_deleted=False,
        ).values("individual_id", "group_id")
        ind_group_map = {}
        for gi in gi_qs:
            ind_group_map[gi["individual_id"]] = gi["group_id"]

        # Build group -> beneficiary mapping
        group_ids = list(set(ind_group_map.values()))
        gb_qs = GroupBeneficiary.objects.filter(
            group_id__in=group_ids,
            is_deleted=False,
        ).values("group_id", "id")
        group_ben_map = {}
        for gb in gb_qs:
            group_ben_map[gb["group_id"]] = gb["id"]

        def _serialize(bc):
            group_id = ind_group_map.get(bc.individual_id)
            beneficiary_id = group_ben_map.get(group_id) if group_id else None

            return {
                "id": _sid(bc.id),
                "server_id": _sid(bc.id),
                "beneficiary_id": _sid(beneficiary_id),
                "amount": str(bc.amount) if bc.amount is not None else None,
                "date_due": bc.date_due.isoformat() if bc.date_due else None,
                "receipt": bc.receipt or "",
                "status": bc.status or "",
                "payment_method": pm_map.get(bc.id, ""),
                "synced_at": _epoch_ms_now(),
            }

        deleted_qs = None
        if since is not None:
            deleted_qs = BenefitConsumption.objects.filter(
                is_deleted=True, date_updated__gte=since
            )
            if province_ids is not None:
                deleted_qs = deleted_qs.filter(
                    individual__location__parent__parent__id__in=province_ids
                )

        created, updated, deleted_ids = _split_changes(qs, since)
        return {
            "created": [_serialize(r) for r in created],
            "updated": [_serialize(r) for r in updated],
            "deleted": deleted_ids if deleted_ids else (
                [_sid(pk) for pk in deleted_qs.values_list("id", flat=True)]
                if deleted_qs is not None else []
            ),
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def pull(cls, user, last_sync_timestamp=None, tables=None):
        """
        Return WatermelonDB-compatible sync payload.

        Parameters:
            user: Django user
            last_sync_timestamp: epoch-ms integer or None
            tables: optional list of table names to include

        Returns:
            {
              "changes": { "<table>": { "created": [...], "updated": [...], "deleted": [...] } },
              "timestamp": <epoch-ms integer>
            }
        """
        province_ids = cls._get_user_province_ids(user)
        since = _parse_since(last_sync_timestamp)

        all_tables = {
            "locations": cls._pull_locations,
            "households": cls._pull_households,
            "individuals": cls._pull_individuals,
            "beneficiaries": cls._pull_beneficiaries,
            "activities": cls._pull_activities,
            "grievances": cls._pull_grievances,
            "grievance_comments": cls._pull_grievance_comments,
            "payments": cls._pull_payments,
        }

        changes = {}
        for table_name, pull_fn in all_tables.items():
            if tables is not None and table_name not in tables:
                changes[table_name] = {"created": [], "updated": [], "deleted": []}
            else:
                try:
                    changes[table_name] = pull_fn(province_ids, since)
                except Exception:
                    logger.exception("sync pull error for table %s", table_name)
                    changes[table_name] = {"created": [], "updated": [], "deleted": []}

        return {
            "changes": changes,
            "timestamp": _epoch_ms_now(),
        }

    @classmethod
    def push(cls, user, changes):
        """
        Apply offline changes from mobile client.

        Pushable tables:
        - grievances / grievance_comments: create + update
        - households (updated): community validation status changes
        - beneficiaries (updated): contact info / json_ext merges
        - activities (updated): validation_status changes

        Field names from mobile are snake_case matching the mobile schema.
        Returns: { "success": true|false, "errors": [...] }
        """
        errors = []

        grievance_changes = changes.get("grievances", {})
        comment_changes = changes.get("grievance_comments", {})

        # --- Grievances created offline ---
        for record in grievance_changes.get("created", []):
            try:
                Ticket.objects.create(
                    id=record.get("id"),
                    title=record.get("title", ""),
                    description=record.get("description", ""),
                    status=record.get("status", Ticket.TicketStatus.RECEIVED),
                    category=record.get("category", ""),
                    channel=record.get("channel", ""),
                    code=record.get("code"),
                )
            except Exception as exc:
                logger.exception("sync push: error creating grievance %s", record.get("id"))
                errors.append({"table": "grievances", "id": record.get("id"), "error": str(exc)})

        # --- Grievances updated offline ---
        for record in grievance_changes.get("updated", []):
            try:
                ticket = Ticket.objects.get(id=record["id"])
                for mobile_field, model_field in [
                    ("title", "title"),
                    ("description", "description"),
                    ("status", "status"),
                    ("category", "category"),
                    ("channel", "channel"),
                ]:
                    if mobile_field in record:
                        setattr(ticket, model_field, record[mobile_field])
                ticket.save(user=user)
            except Ticket.DoesNotExist:
                errors.append({"table": "grievances", "id": record.get("id"), "error": "not found"})
            except Exception as exc:
                logger.exception("sync push: error updating grievance %s", record.get("id"))
                errors.append({"table": "grievances", "id": record.get("id"), "error": str(exc)})

        # --- Grievance comments created offline ---
        for record in comment_changes.get("created", []):
            try:
                ticket = Ticket.objects.get(id=record.get("grievance_id"))
                Comment.objects.create(
                    id=record.get("id"),
                    ticket=ticket,
                    comment=record.get("text", ""),
                    is_resolution=False,
                )
            except Ticket.DoesNotExist:
                errors.append({"table": "grievance_comments", "id": record.get("id"), "error": "ticket not found"})
            except Exception as exc:
                logger.exception("sync push: error creating comment %s", record.get("id"))
                errors.append({"table": "grievance_comments", "id": record.get("id"), "error": str(exc)})

        # --- Grievance comments updated offline ---
        for record in comment_changes.get("updated", []):
            try:
                comment = Comment.objects.get(id=record["id"])
                if "text" in record:
                    comment.comment = record["text"]
                comment.save(user=user)
            except Comment.DoesNotExist:
                errors.append({"table": "grievance_comments", "id": record.get("id"), "error": "not found"})
            except Exception as exc:
                logger.exception("sync push: error updating comment %s", record.get("id"))
                errors.append({"table": "grievance_comments", "id": record.get("id"), "error": str(exc)})

        # --- Households updated offline (community validation) ---
        for record in changes.get("households", {}).get("updated", []):
            try:
                group = Group.objects.get(id=record["server_id"])
                json_ext = group.json_ext or {}
                if "selection_status" in record:
                    new_status = record["selection_status"]
                    json_ext["selection_status"] = new_status

                    # Track community validation metadata
                    if new_status in ("COMMUNITY_VALIDATED", "COMMUNITY_REJECTED"):
                        json_ext["community_validation"] = {
                            "status": "VALIDATED" if new_status == "COMMUNITY_VALIDATED" else "REJECTED",
                            "date": record.get(
                                "community_validation_date",
                                datetime.now(tz=timezone.utc).isoformat(),
                            ),
                        }

                    group.json_ext = json_ext
                    group.save()

                    # If rejected, trigger waiting list promotion
                    if new_status == "COMMUNITY_REJECTED" and group.location_id:
                        from merankabandi.selection_service import SelectionService
                        try:
                            gb = GroupBeneficiary.objects.filter(
                                group=group, is_deleted=False
                            ).first()
                            if gb and gb.benefit_plan_id:
                                SelectionService.promote_from_waiting_list(
                                    benefit_plan_id=str(gb.benefit_plan_id),
                                    colline_id=str(group.location_id),
                                    count=1,
                                )
                        except Exception as e:
                            logger.warning(
                                "Waiting list promotion failed for group %s: %s",
                                group.id, e,
                            )

            except Group.DoesNotExist:
                errors.append({
                    "table": "households",
                    "id": record.get("server_id"),
                    "error": "not found",
                })
            except Exception as e:
                logger.exception(
                    "sync push: error updating household %s", record.get("server_id")
                )
                errors.append({
                    "table": "households",
                    "id": record.get("server_id"),
                    "error": str(e),
                })

        # --- Beneficiaries updated offline (contact info / json_ext) ---
        for record in changes.get("beneficiaries", {}).get("updated", []):
            try:
                gb = GroupBeneficiary.objects.get(id=record["server_id"])
                json_ext = gb.json_ext or {}
                if "json_ext" in record:
                    import json as json_module
                    mobile_ext = record["json_ext"]
                    if isinstance(mobile_ext, str):
                        mobile_ext = json_module.loads(mobile_ext)
                    json_ext.update(mobile_ext)
                    gb.json_ext = json_ext
                    gb.save()
            except GroupBeneficiary.DoesNotExist:
                errors.append({
                    "table": "beneficiaries",
                    "id": record.get("server_id"),
                    "error": "not found",
                })
            except Exception as e:
                logger.exception(
                    "sync push: error updating beneficiary %s", record.get("server_id")
                )
                errors.append({
                    "table": "beneficiaries",
                    "id": record.get("server_id"),
                    "error": str(e),
                })

        # --- Activities updated offline (validation status) ---
        for record in changes.get("activities", {}).get("updated", []):
            try:
                server_id = record["server_id"]
                new_status = record.get("validation_status")
                if not new_status:
                    continue

                updated = False
                for Model in [SensitizationTraining, MicroProject, BehaviorChangePromotion]:
                    try:
                        obj = Model.objects.get(id=server_id)
                        obj.validation_status = new_status
                        obj.save()
                        updated = True
                        break
                    except Model.DoesNotExist:
                        continue

                if not updated:
                    errors.append({
                        "table": "activities",
                        "id": server_id,
                        "error": "not found in any activity model",
                    })
            except Exception as e:
                logger.exception(
                    "sync push: error updating activity %s", record.get("server_id")
                )
                errors.append({
                    "table": "activities",
                    "id": record.get("server_id"),
                    "error": str(e),
                })

        return {"success": len(errors) == 0, "errors": errors}
