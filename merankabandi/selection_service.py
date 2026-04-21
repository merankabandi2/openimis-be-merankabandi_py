import datetime
import logging

from core.signals import register_service_signal
from individual.models import Group
from social_protection.models import BenefitPlan, GroupBeneficiary, BeneficiaryStatus

logger = logging.getLogger(__name__)


class SelectionService:
    """Service for managing the selection lifecycle on Group.json_ext"""

    @classmethod
    def _update_selection_status(cls, group, status, extra_fields=None, username='system'):
        """Update selection_status in Group.json_ext and append to selection_history."""
        ext = group.json_ext or {}
        ext['selection_status'] = status
        if extra_fields:
            ext.update(extra_fields)
        group.json_ext = ext
        group.save(username=username)

    @classmethod
    @register_service_signal('selection_service.apply_quota_selection')
    def apply_quota_selection(cls, benefit_plan_id, targeting_round=1):
        """
        Rank PMT_SCORED groups by pmt_score (lowest = poorest = selected first).
        Apply per-colline quotas: top N = SELECTED, rest = WAITING_LIST.
        """
        from merankabandi.models import SelectionQuota

        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        quotas = SelectionQuota.objects.filter(
            benefit_plan=benefit_plan,
            targeting_round=targeting_round,
        )

        total_selected = 0
        total_waiting = 0
        updated_groups = []

        for quota in quotas:
            colline = quota.location
            # Scope by benefit_plan via GroupBeneficiary join — prevents
            # cross-program state leakage when multiple programs target the
            # same colline.
            groups = Group.objects.filter(
                location=colline,
                json_ext__selection_status='PMT_SCORED',
                groupbeneficiary__benefit_plan=benefit_plan,
                groupbeneficiary__is_deleted=False,
            ).distinct().order_by('json_ext__pmt_score')

            for i, group in enumerate(groups):
                ext = group.json_ext or {}
                if i < quota.quota:
                    ext['selection_status'] = 'SELECTED'
                    ext['selection_rank'] = i + 1
                    total_selected += 1
                else:
                    ext['selection_status'] = 'WAITING_LIST'
                    ext['selection_rank'] = i + 1
                    total_waiting += 1
                group.json_ext = ext
                updated_groups.append(group)

        if updated_groups:
            Group.objects.bulk_update(updated_groups, ['json_ext'], batch_size=500)

        logger.info(
            "Quota selection: %d selected, %d waiting list for plan %s round %d",
            total_selected, total_waiting, benefit_plan.code, targeting_round,
        )
        return {
            "selected": total_selected,
            "waiting_list": total_waiting,
            "program_name": benefit_plan.name or benefit_plan.code,
            "round": targeting_round,
            "selected_count": total_selected,
        }

    @classmethod
    def apply_criteria_selection(cls, benefit_plan_id, username='system'):
        """
        Apply advanced_criteria from BenefitPlan.json_ext to SURVEYED groups.
        Matching = SELECTED, non-matching = NOT_SELECTED.
        """
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        criteria = (benefit_plan.json_ext or {}).get('advanced_criteria', {})
        potential_criteria = criteria.get('POTENTIAL', [])

        groups = Group.objects.filter(
            json_ext__selection_status='SURVEYED',
            groupbeneficiary__benefit_plan_id=benefit_plan_id,
            groupbeneficiary__is_deleted=False,
        ).distinct()

        selected = 0
        not_selected = 0

        for group in groups:
            ext = group.json_ext or {}
            if cls._matches_criteria(ext, potential_criteria):
                cls._update_selection_status(group, 'SELECTED', username=username)
                selected += 1
            else:
                cls._update_selection_status(group, 'NOT_SELECTED', username=username)
                not_selected += 1

        return {"selected": selected, "not_selected": not_selected}

    @classmethod
    def select_all(cls, benefit_plan_id, username='system'):
        """Mark all SURVEYED groups as SELECTED (for programs without filtering)."""
        groups = Group.objects.filter(
            json_ext__selection_status='SURVEYED',
            groupbeneficiary__benefit_plan_id=benefit_plan_id,
            groupbeneficiary__is_deleted=False,
        ).distinct()
        count = 0
        for group in groups:
            cls._update_selection_status(group, 'SELECTED', username=username)
            count += 1
        return {"selected": count}

    @classmethod
    @register_service_signal('selection_service.promote_to_beneficiary')
    def promote_to_beneficiary(cls, benefit_plan_id, username):
        """
        Activate GroupBeneficiary records for COMMUNITY_VALIDATED groups.

        Contract:
        - Groups already have a GroupBeneficiary(POTENTIAL) row (created at
          import_households_to_benefit_plan time, or at selection start).
        - Promotion transitions the status POTENTIAL → ACTIVE for groups whose
          selection_status is COMMUNITY_VALIDATED (or SELECTED when community
          validation is disabled).
        - If a Group has no GroupBeneficiary yet, one is created directly at
          ACTIVE — this preserves the prior contract for plans that populate
          GroupBeneficiary late.
        - A selection_history entry is appended on the Group for traceability.
        """
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        targeting = (benefit_plan.json_ext or {}).get('targeting', {})
        require_cv = targeting.get('require_community_validation', False)

        source_status = 'COMMUNITY_VALIDATED' if require_cv else 'SELECTED'

        groups = list(Group.objects.filter(
            json_ext__selection_status=source_status,
            groupbeneficiary__benefit_plan=benefit_plan,
            groupbeneficiary__is_deleted=False,
        ).distinct())

        existing_gbs = {
            gb.group_id: gb
            for gb in GroupBeneficiary.objects.filter(
                benefit_plan=benefit_plan, is_deleted=False
            )
        }

        activated = 0
        created = 0
        groups_to_update = []

        for group in groups:
            gb = existing_gbs.get(group.id)
            if gb is None:
                # Edge case: Group without upfront GroupBeneficiary — create one
                # directly at ACTIVE so summary counts reflect promotion.
                new_gb = GroupBeneficiary(
                    group=group,
                    benefit_plan=benefit_plan,
                    status=BeneficiaryStatus.ACTIVE,
                    date_valid_from=datetime.date.today(),
                )
                new_gb.save(username=username)
                created += 1
            elif gb.status != BeneficiaryStatus.ACTIVE:
                gb.status = BeneficiaryStatus.ACTIVE
                gb.save(username=username)
                activated += 1

            ext = group.json_ext or {}
            history = ext.get('selection_history', [])
            history.append({
                'benefit_plan_id': str(benefit_plan.id),
                'benefit_plan_name': benefit_plan.name,
                'round': targeting.get('targeting_round', 1),
                'status': 'BENEFICIARY_ACTIVE',
                'date': datetime.date.today().isoformat(),
                'pmt_score': ext.get('pmt_score'),
            })
            ext['selection_history'] = history
            ext['selection_status'] = 'ENROLLED'
            group.json_ext = ext
            groups_to_update.append(group)

        if groups_to_update:
            Group.objects.bulk_update(groups_to_update, ['json_ext'], batch_size=500)

        total = activated + created
        logger.info(
            "Promoted %d groups (activated=%d, created=%d) for plan %s",
            total, activated, created, benefit_plan.code,
        )
        return {
            "created": created,
            "activated": activated,
            "program_name": benefit_plan.name or benefit_plan.code,
            "promoted_count": total,
        }

    @classmethod
    def promote_from_waiting_list(cls, benefit_plan_id, colline_id, count, username):
        """
        Promote top N from WAITING_LIST to COMMUNITY_VALIDATED in a colline.
        Used when community validation rejects some SELECTED households.
        """
        if not isinstance(count, int) or count < 1 or count > 1000:
            raise ValueError("count must be an integer between 1 and 1000")
        # Scope by benefit_plan so promoting a colline in one program does not
        # flip Group state for a different program sharing the colline.
        groups = Group.objects.filter(
            location_id=colline_id,
            json_ext__selection_status='WAITING_LIST',
            groupbeneficiary__benefit_plan_id=benefit_plan_id,
            groupbeneficiary__is_deleted=False,
        ).distinct().order_by('json_ext__selection_rank')[:count]

        promoted = 0
        for group in groups:
            cls._update_selection_status(group, 'COMMUNITY_VALIDATED', username=username)
            promoted += 1

        return {"promoted": promoted}

    @staticmethod
    def _matches_criteria(json_ext, criteria_list):
        """Evaluate a list of criteria against json_ext fields."""
        for criterion in criteria_list:
            field = criterion.get('field', '')
            value = criterion.get('value', '')
            filter_type = criterion.get('filter', 'exact')
            field_value = str(json_ext.get(field, ''))

            if filter_type == 'icontains':
                if value.lower() not in field_value.lower():
                    return False
            elif filter_type == 'exact':
                if field_value != value:
                    return False
            elif filter_type == 'gt':
                try:
                    if float(field_value) <= float(value):
                        return False
                except (ValueError, TypeError):
                    return False
            elif filter_type == 'lt':
                try:
                    if float(field_value) >= float(value):
                        return False
                except (ValueError, TypeError):
                    return False
        return True
