import logging
from datetime import datetime, timezone

from django.db.models import Q

from grievance_social_protection.models import Ticket, Comment
from individual.models import Group, GroupIndividual, Individual
from location.models import UserDistrict
from merankabandi.models import SensitizationTraining, BehaviorChangePromotion, MicroProject
from social_protection.models import GroupBeneficiary

logger = logging.getLogger(__name__)


def _str_id(val):
    return str(val) if val is not None else None


def _dt(dt):
    """Return ISO-8601 string from a datetime, or None."""
    if dt is None:
        return None
    if hasattr(dt, 'isoformat'):
        if getattr(dt, 'tzinfo', None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return str(dt)


def _now_ts():
    return datetime.now(tz=timezone.utc).isoformat()


class SyncService:

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_user_province_ids(user):
        """Return province location IDs assigned to the user via UserDistrict.
        Returns None for superusers (no filtering)."""
        interactive_user = user._u if hasattr(user, '_u') else user
        if interactive_user.is_superuser:
            return None
        districts = UserDistrict.get_user_districts(user)
        return [d.location_id for d in districts]

    @staticmethod
    def _since_filter(qs, field, last_sync_timestamp):
        if last_sync_timestamp:
            try:
                since = datetime.fromisoformat(last_sync_timestamp)
            except (ValueError, TypeError):
                try:
                    since = datetime.utcfromtimestamp(float(last_sync_timestamp) / 1000).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    since = None
            if since:
                return qs.filter(**{f"{field}__gte": since})
        return qs

    # ------------------------------------------------------------------
    # Serializers for each table
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize_beneficiary(gb):
        group = gb.group
        location = group.location
        commune = location.parent if location else None
        province = commune.parent if commune else None
        return {
            "id": _str_id(gb.id),
            "groupId": _str_id(group.id),
            "groupCode": group.code,
            "benefitPlanId": _str_id(gb.benefit_plan_id),
            "status": gb.status,
            "locationId": _str_id(location.id) if location else None,
            "locationName": location.name if location else None,
            "communeId": _str_id(commune.id) if commune else None,
            "communeName": commune.name if commune else None,
            "provinceId": _str_id(province.id) if province else None,
            "provinceName": province.name if province else None,
            "selectionStatus": (group.json_ext or {}).get("selection_status"),
            "pmtScore": (group.json_ext or {}).get("pmt_score"),
            "jsonExt": gb.json_ext,
            "isDeleted": gb.is_deleted,
            "dateCreated": _dt(gb.date_created),
            "dateUpdated": _dt(gb.date_updated),
        }

    @staticmethod
    def _serialize_individual(ind):
        return {
            "id": _str_id(ind.id),
            "firstName": ind.first_name,
            "lastName": ind.last_name,
            "dob": ind.dob.isoformat() if ind.dob else None,
            "locationId": _str_id(ind.location_id),
            "jsonExt": ind.json_ext,
            "isDeleted": ind.is_deleted,
            "dateCreated": _dt(ind.date_created),
            "dateUpdated": _dt(ind.date_updated),
        }

    @staticmethod
    def _serialize_group_individual(gi):
        return {
            "id": _str_id(gi.id),
            "groupId": _str_id(gi.group_id),
            "individualId": _str_id(gi.individual_id),
            "role": gi.role,
            "recipientType": gi.recipient_type,
            "jsonExt": gi.json_ext,
            "isDeleted": gi.is_deleted,
            "dateCreated": _dt(gi.date_created),
            "dateUpdated": _dt(gi.date_updated),
        }

    @staticmethod
    def _serialize_ticket(ticket):
        return {
            "id": _str_id(ticket.id),
            "key": ticket.key,
            "title": ticket.title,
            "description": ticket.description,
            "code": ticket.code,
            "status": ticket.status,
            "priority": ticket.priority,
            "category": ticket.category,
            "channel": ticket.channel,
            "resolution": ticket.resolution,
            "flags": ticket.flags,
            "dateOfIncident": ticket.date_of_incident.isoformat() if ticket.date_of_incident else None,
            "dueDate": ticket.due_date.isoformat() if ticket.due_date else None,
            "attendingStaffId": _str_id(ticket.attending_staff_id),
            "isDeleted": ticket.is_deleted,
            "dateCreated": _dt(ticket.date_created),
            "dateUpdated": _dt(ticket.date_updated),
        }

    @staticmethod
    def _serialize_comment(comment):
        return {
            "id": _str_id(comment.id),
            "ticketId": _str_id(comment.ticket_id),
            "comment": comment.comment,
            "isResolution": comment.is_resolution,
            "isDeleted": comment.is_deleted,
            "dateCreated": _dt(comment.date_created),
            "dateUpdated": _dt(comment.date_updated),
        }

    @staticmethod
    def _serialize_sensitization(st):
        return {
            "id": _str_id(st.id),
            "sensitizationDate": st.sensitization_date.isoformat() if st.sensitization_date else None,
            "locationId": _str_id(st.location_id),
            "category": st.category,
            "modules": st.modules,
            "facilitator": st.facilitator,
            "maleParticipants": st.male_participants,
            "femaleParticipants": st.female_participants,
            "twaParticipants": st.twa_participants,
            "observations": st.observations,
            "validationStatus": st.validation_status,
            "koboSubmissionId": st.kobo_submission_id,
        }

    # ------------------------------------------------------------------
    # Table-level pull queries
    # ------------------------------------------------------------------

    @classmethod
    def _pull_beneficiaries(cls, province_ids, last_sync_timestamp, tables):
        if tables is not None and "beneficiaries" not in tables:
            return {"created": [], "updated": [], "deleted": []}

        qs = GroupBeneficiary.objects.select_related(
            "group__location__parent__parent", "benefit_plan"
        )
        if province_ids is not None:
            qs = qs.filter(group__location__parent__parent__id__in=province_ids)

        active_qs = cls._since_filter(qs.filter(is_deleted=False), "date_updated", last_sync_timestamp)
        deleted_qs = cls._since_filter(qs.filter(is_deleted=True), "date_updated", last_sync_timestamp)

        if last_sync_timestamp:
            created_qs = active_qs.filter(date_created=active_qs.filter(id=active_qs.values("id")).values("date_created"))
            # Simpler: use date_created >= since for created, date_updated >= since for all active
            try:
                since = datetime.fromisoformat(last_sync_timestamp)
            except (ValueError, TypeError):
                try:
                    since = datetime.utcfromtimestamp(float(last_sync_timestamp) / 1000).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    since = None
            if since:
                created_ids = set(qs.filter(date_created__gte=since, is_deleted=False).values_list("id", flat=True))
                updated_records = [r for r in active_qs if r.id not in created_ids]
                created_records = [r for r in active_qs if r.id in created_ids]
            else:
                created_records = list(active_qs)
                updated_records = []
        else:
            created_records = list(active_qs)
            updated_records = []

        return {
            "created": [cls._serialize_beneficiary(r) for r in created_records],
            "updated": [cls._serialize_beneficiary(r) for r in updated_records],
            "deleted": [_str_id(r.id) for r in deleted_qs],
        }

    @classmethod
    def _pull_individuals(cls, province_ids, last_sync_timestamp, tables):
        if tables is not None and "individuals" not in tables:
            return {"created": [], "updated": [], "deleted": []}

        qs = Individual.objects.select_related("location")
        if province_ids is not None:
            # Individual location is at colline level: colline -> commune -> province
            qs = qs.filter(location__parent__parent__id__in=province_ids)

        active_qs = cls._since_filter(qs.filter(is_deleted=False), "date_updated", last_sync_timestamp)
        deleted_qs = cls._since_filter(qs.filter(is_deleted=True), "date_updated", last_sync_timestamp)

        if last_sync_timestamp:
            try:
                since = datetime.fromisoformat(last_sync_timestamp)
            except (ValueError, TypeError):
                try:
                    since = datetime.utcfromtimestamp(float(last_sync_timestamp) / 1000).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    since = None
            if since:
                created_ids = set(qs.filter(date_created__gte=since, is_deleted=False).values_list("id", flat=True))
                updated_records = [r for r in active_qs if r.id not in created_ids]
                created_records = [r for r in active_qs if r.id in created_ids]
            else:
                created_records = list(active_qs)
                updated_records = []
        else:
            created_records = list(active_qs)
            updated_records = []

        return {
            "created": [cls._serialize_individual(r) for r in created_records],
            "updated": [cls._serialize_individual(r) for r in updated_records],
            "deleted": [_str_id(r.id) for r in deleted_qs],
        }

    @classmethod
    def _pull_group_individuals(cls, province_ids, last_sync_timestamp, tables):
        if tables is not None and "groupIndividuals" not in tables:
            return {"created": [], "updated": [], "deleted": []}

        qs = GroupIndividual.objects.select_related("group__location")
        if province_ids is not None:
            qs = qs.filter(group__location__parent__parent__id__in=province_ids)

        active_qs = cls._since_filter(qs.filter(is_deleted=False), "date_updated", last_sync_timestamp)
        deleted_qs = cls._since_filter(qs.filter(is_deleted=True), "date_updated", last_sync_timestamp)

        if last_sync_timestamp:
            try:
                since = datetime.fromisoformat(last_sync_timestamp)
            except (ValueError, TypeError):
                try:
                    since = datetime.utcfromtimestamp(float(last_sync_timestamp) / 1000).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    since = None
            if since:
                created_ids = set(qs.filter(date_created__gte=since, is_deleted=False).values_list("id", flat=True))
                updated_records = [r for r in active_qs if r.id not in created_ids]
                created_records = [r for r in active_qs if r.id in created_ids]
            else:
                created_records = list(active_qs)
                updated_records = []
        else:
            created_records = list(active_qs)
            updated_records = []

        return {
            "created": [cls._serialize_group_individual(r) for r in created_records],
            "updated": [cls._serialize_group_individual(r) for r in updated_records],
            "deleted": [_str_id(r.id) for r in deleted_qs],
        }

    @classmethod
    def _pull_tickets(cls, province_ids, last_sync_timestamp, tables):
        if tables is not None and "tickets" not in tables:
            return {"created": [], "updated": [], "deleted": []}

        # Ticket does not have a direct location FK; filter through reporter (Individual) or
        # include all non-deleted tickets visible to the user.
        # For now, pull all active tickets (row-security is handled in Ticket.get_queryset).
        qs = Ticket.objects.all()

        active_qs = qs.filter(is_deleted=False)
        deleted_qs = qs.filter(is_deleted=True)
        active_qs = cls._since_filter(active_qs, "date_updated", last_sync_timestamp)
        deleted_qs = cls._since_filter(deleted_qs, "date_updated", last_sync_timestamp)

        if last_sync_timestamp:
            try:
                since = datetime.fromisoformat(last_sync_timestamp)
            except (ValueError, TypeError):
                try:
                    since = datetime.utcfromtimestamp(float(last_sync_timestamp) / 1000).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    since = None
            if since:
                created_ids = set(qs.filter(date_created__gte=since, is_deleted=False).values_list("id", flat=True))
                updated_records = [r for r in active_qs if r.id not in created_ids]
                created_records = [r for r in active_qs if r.id in created_ids]
            else:
                created_records = list(active_qs)
                updated_records = []
        else:
            created_records = list(active_qs)
            updated_records = []

        return {
            "created": [cls._serialize_ticket(r) for r in created_records],
            "updated": [cls._serialize_ticket(r) for r in updated_records],
            "deleted": [_str_id(r.id) for r in deleted_qs],
        }

    @classmethod
    def _pull_comments(cls, province_ids, last_sync_timestamp, tables):
        if tables is not None and "comments" not in tables:
            return {"created": [], "updated": [], "deleted": []}

        qs = Comment.objects.select_related("ticket")
        active_qs = cls._since_filter(qs.filter(is_deleted=False), "date_updated", last_sync_timestamp)
        deleted_qs = cls._since_filter(qs.filter(is_deleted=True), "date_updated", last_sync_timestamp)

        if last_sync_timestamp:
            try:
                since = datetime.fromisoformat(last_sync_timestamp)
            except (ValueError, TypeError):
                try:
                    since = datetime.utcfromtimestamp(float(last_sync_timestamp) / 1000).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    since = None
            if since:
                created_ids = set(qs.filter(date_created__gte=since, is_deleted=False).values_list("id", flat=True))
                updated_records = [r for r in active_qs if r.id not in created_ids]
                created_records = [r for r in active_qs if r.id in created_ids]
            else:
                created_records = list(active_qs)
                updated_records = []
        else:
            created_records = list(active_qs)
            updated_records = []

        return {
            "created": [cls._serialize_comment(r) for r in created_records],
            "updated": [cls._serialize_comment(r) for r in updated_records],
            "deleted": [_str_id(r.id) for r in deleted_qs],
        }

    @classmethod
    def _pull_sensitizations(cls, province_ids, last_sync_timestamp, tables):
        if tables is not None and "sensitizations" not in tables:
            return {"created": [], "updated": [], "deleted": []}

        qs = SensitizationTraining.objects.select_related("location__parent__parent")
        if province_ids is not None:
            qs = qs.filter(location__parent__parent__id__in=province_ids)

        # SensitizationTraining uses a plain Model (no is_deleted) — filter by date
        active_qs = qs
        if last_sync_timestamp:
            try:
                since = datetime.fromisoformat(last_sync_timestamp)
            except (ValueError, TypeError):
                try:
                    since = datetime.utcfromtimestamp(float(last_sync_timestamp) / 1000).replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    since = None
            if since:
                active_qs = qs  # no date_updated on plain model; return all in scope
            created_records = list(active_qs)
            updated_records = []
        else:
            created_records = list(active_qs)
            updated_records = []

        return {
            "created": [cls._serialize_sensitization(r) for r in created_records],
            "updated": [cls._serialize_sensitization(r) for r in updated_records],
            "deleted": [],
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @classmethod
    def pull(cls, user, last_sync_timestamp=None, tables=None):
        """
        Return WatermelonDB sync payload:
          { changes: { <table>: { created, updated, deleted } }, timestamp }
        """
        province_ids = cls._get_user_province_ids(user)

        changes = {
            "beneficiaries": cls._pull_beneficiaries(province_ids, last_sync_timestamp, tables),
            "individuals": cls._pull_individuals(province_ids, last_sync_timestamp, tables),
            "groupIndividuals": cls._pull_group_individuals(province_ids, last_sync_timestamp, tables),
            "tickets": cls._pull_tickets(province_ids, last_sync_timestamp, tables),
            "comments": cls._pull_comments(province_ids, last_sync_timestamp, tables),
            "sensitizations": cls._pull_sensitizations(province_ids, last_sync_timestamp, tables),
        }

        return {
            "changes": changes,
            "timestamp": _now_ts(),
        }

    @classmethod
    def push(cls, user, changes):
        """
        Apply offline changes from mobile client.
        Only tickets and comments are pushable from the mobile app.
        Returns { success: true, errors: [...] }
        """
        errors = []

        ticket_changes = changes.get("tickets", {})
        comment_changes = changes.get("comments", {})

        # --- Tickets (created offline) ---
        for record in ticket_changes.get("created", []):
            try:
                ticket_id = record.get("id")
                Ticket.objects.create(
                    id=ticket_id,
                    title=record.get("title"),
                    description=record.get("description"),
                    status=record.get("status", Ticket.TicketStatus.RECEIVED),
                    priority=record.get("priority"),
                    category=record.get("category"),
                    channel=record.get("channel"),
                    flags=record.get("flags"),
                    date_of_incident=record.get("dateOfIncident"),
                    key=record.get("key"),
                )
            except Exception as exc:
                logger.exception("sync push: error creating ticket %s", record.get("id"))
                errors.append({"table": "tickets", "id": record.get("id"), "error": str(exc)})

        # --- Tickets (updated offline) ---
        for record in ticket_changes.get("updated", []):
            try:
                ticket = Ticket.objects.get(id=record["id"])
                for field, col in [
                    ("title", "title"),
                    ("description", "description"),
                    ("status", "status"),
                    ("priority", "priority"),
                    ("category", "category"),
                    ("channel", "channel"),
                    ("flags", "flags"),
                    ("resolution", "resolution"),
                ]:
                    if field in record:
                        setattr(ticket, col, record[field])
                ticket.save(user=user)
            except Ticket.DoesNotExist:
                errors.append({"table": "tickets", "id": record.get("id"), "error": "not found"})
            except Exception as exc:
                logger.exception("sync push: error updating ticket %s", record.get("id"))
                errors.append({"table": "tickets", "id": record.get("id"), "error": str(exc)})

        # --- Comments (created offline) ---
        for record in comment_changes.get("created", []):
            try:
                ticket_id = record.get("ticketId")
                ticket = Ticket.objects.get(id=ticket_id)
                Comment.objects.create(
                    id=record.get("id"),
                    ticket=ticket,
                    comment=record.get("comment", ""),
                    is_resolution=record.get("isResolution", False),
                )
            except Ticket.DoesNotExist:
                errors.append({"table": "comments", "id": record.get("id"), "error": "ticket not found"})
            except Exception as exc:
                logger.exception("sync push: error creating comment %s", record.get("id"))
                errors.append({"table": "comments", "id": record.get("id"), "error": str(exc)})

        # --- Comments (updated offline) ---
        for record in comment_changes.get("updated", []):
            try:
                comment = Comment.objects.get(id=record["id"])
                if "comment" in record:
                    comment.comment = record["comment"]
                if "isResolution" in record:
                    comment.is_resolution = record["isResolution"]
                comment.save(user=user)
            except Comment.DoesNotExist:
                errors.append({"table": "comments", "id": record.get("id"), "error": "not found"})
            except Exception as exc:
                logger.exception("sync push: error updating comment %s", record.get("id"))
                errors.append({"table": "comments", "id": record.get("id"), "error": str(exc)})

        return {"success": len(errors) == 0, "errors": errors}
