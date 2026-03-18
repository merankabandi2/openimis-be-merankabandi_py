import datetime
import logging

from django.db import transaction
from individual.models import Group
from social_protection.models import BenefitPlan, GroupBeneficiary, BeneficiaryStatus

logger = logging.getLogger(__name__)


class SelectionService:
    """Service for managing the selection lifecycle on Group.json_ext"""

    @classmethod
    def _update_selection_status(cls, group, status, extra_fields=None):
        """Update selection_status in Group.json_ext and append to selection_history."""
        ext = group.json_ext or {}
        ext['selection_status'] = status
        if extra_fields:
            ext.update(extra_fields)
        group.json_ext = ext
        group.save()

    @classmethod
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

        for quota in quotas:
            colline = quota.location
            groups = Group.objects.filter(
                location=colline,
                json_ext__selection_status='PMT_SCORED',
            ).order_by('json_ext__pmt_score')

            for i, group in enumerate(groups):
                if i < quota.quota:
                    cls._update_selection_status(group, 'SELECTED', {'selection_rank': i + 1})
                    total_selected += 1
                else:
                    cls._update_selection_status(group, 'WAITING_LIST', {'selection_rank': i + 1})
                    total_waiting += 1

        logger.info(
            "Quota selection: %d selected, %d waiting list for plan %s round %d",
            total_selected, total_waiting, benefit_plan.code, targeting_round,
        )
        return {"selected": total_selected, "waiting_list": total_waiting}

    @classmethod
    def apply_criteria_selection(cls, benefit_plan_id):
        """
        Apply advanced_criteria from BenefitPlan.json_ext to SURVEYED groups.
        Matching = SELECTED, non-matching = NOT_SELECTED.
        """
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        criteria = (benefit_plan.json_ext or {}).get('advanced_criteria', {})
        potential_criteria = criteria.get('POTENTIAL', [])

        groups = Group.objects.filter(
            json_ext__selection_status='SURVEYED',
        )

        selected = 0
        not_selected = 0

        for group in groups:
            ext = group.json_ext or {}
            if cls._matches_criteria(ext, potential_criteria):
                cls._update_selection_status(group, 'SELECTED')
                selected += 1
            else:
                cls._update_selection_status(group, 'NOT_SELECTED')
                not_selected += 1

        return {"selected": selected, "not_selected": not_selected}

    @classmethod
    def select_all(cls, benefit_plan_id):
        """Mark all SURVEYED groups as SELECTED (for programs without filtering)."""
        groups = Group.objects.filter(
            json_ext__selection_status='SURVEYED',
        )
        count = 0
        for group in groups:
            cls._update_selection_status(group, 'SELECTED')
            count += 1
        return {"selected": count}

    @classmethod
    def promote_to_beneficiary(cls, benefit_plan_id, username):
        """
        Create GroupBeneficiary records for COMMUNITY_VALIDATED groups.
        If no community validation required, promote SELECTED groups directly.
        """
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        targeting = (benefit_plan.json_ext or {}).get('targeting', {})
        require_cv = targeting.get('require_community_validation', False)

        source_status = 'COMMUNITY_VALIDATED' if require_cv else 'SELECTED'

        groups = Group.objects.filter(
            json_ext__selection_status=source_status,
        )

        created = 0
        for group in groups:
            if not GroupBeneficiary.objects.filter(
                group=group, benefit_plan=benefit_plan, is_deleted=False
            ).exists():
                gb = GroupBeneficiary(
                    group=group,
                    benefit_plan=benefit_plan,
                    status=BeneficiaryStatus.POTENTIAL,
                )
                gb.save(username=username)
                created += 1

                ext = group.json_ext or {}
                history = ext.get('selection_history', [])
                history.append({
                    'benefit_plan_id': str(benefit_plan.id),
                    'benefit_plan_name': benefit_plan.name,
                    'round': targeting.get('targeting_round', 1),
                    'status': 'BENEFICIARY',
                    'date': datetime.date.today().isoformat(),
                    'pmt_score': ext.get('pmt_score'),
                })
                ext['selection_history'] = history
                group.json_ext = ext
                group.save()

        logger.info(
            "Promoted %d groups to beneficiary for plan %s", created, benefit_plan.code
        )
        return {"created": created}

    @classmethod
    def promote_from_waiting_list(cls, benefit_plan_id, colline_id, count, username):
        """
        Promote top N from WAITING_LIST to COMMUNITY_VALIDATED in a colline.
        Used when community validation rejects some SELECTED households.
        """
        groups = Group.objects.filter(
            location_id=colline_id,
            json_ext__selection_status='WAITING_LIST',
        ).order_by('json_ext__selection_rank')[:count]

        promoted = 0
        for group in groups:
            cls._update_selection_status(group, 'COMMUNITY_VALIDATED')
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
