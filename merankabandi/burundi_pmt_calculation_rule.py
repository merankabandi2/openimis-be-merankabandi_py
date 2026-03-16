from calcrule_social_protection.calculation_rule import SocialProtectionCalculationRule
from social_protection.models import BeneficiaryStatus
import logging

logger = logging.getLogger(__name__)


class BurundiPMTCalculationRule(SocialProtectionCalculationRule):
    version = 1
    uuid = "42d96b58-898a-460a-b357-5fd4b95cd87d"
    calculation_rule_name = "Burundi PMT - Proxy Means Test"
    description = "Calculates PMT score for households based on Burundi-specific indicators"

    # Placeholder weights — replace with real Burundi PMT formula coefficients
    PMT_WEIGHTS = {
        'roof_type_durable': 15.5,
        'wall_type_durable': 12.0,
        'has_electricity': 25.0,
        'has_radio': 5.5,
        'has_bicycle': 8.0,
        'household_size': -2.5,
        'no_education_head': -10.0,
    }

    INTERCEPT = 100.0

    @classmethod
    def calculate(cls, payment_plan, **kwargs):
        benefit_plan = kwargs.get('benefit_plan') or (
            payment_plan.benefit_plan if payment_plan else None
        )
        if not benefit_plan:
            raise ValueError("No benefit plan provided.")

        beneficiaries_qs = kwargs.get(
            'beneficiaries_queryset',
            cls._get_targeting_queryset(benefit_plan),
        )

        updated_count = 0
        for beneficiary in beneficiaries_qs:
            score = cls._score_household(beneficiary)

            if not beneficiary.json_ext:
                beneficiary.json_ext = {}
            beneficiary.json_ext['pmt_score'] = round(score, 2)
            beneficiary.save()
            updated_count += 1

        logger.info(
            "PMT calculation completed: %d beneficiaries in plan %s",
            updated_count, benefit_plan.code,
        )
        return f"Scored {updated_count} households."

    @classmethod
    def _score_household(cls, beneficiary):
        data = beneficiary.json_ext or {}
        if hasattr(beneficiary, 'group') and beneficiary.group:
            data = {**data, **(beneficiary.group.json_ext or {})}

        score = cls.INTERCEPT
        for variable, weight in cls.PMT_WEIGHTS.items():
            value = data.get(variable, 0)
            if isinstance(value, bool):
                score += weight if value else 0
            elif isinstance(value, (int, float)):
                score += weight * value
            elif isinstance(value, str) and value.lower() in (
                'yes', 'true', 'durable', 'bricks', 'iron',
            ):
                score += weight
        return score

    @classmethod
    def _get_targeting_queryset(cls, benefit_plan):
        from social_protection.models import GroupBeneficiary, Beneficiary
        model = (
            GroupBeneficiary
            if benefit_plan.type == benefit_plan.BenefitPlanType.GROUP_TYPE
            else Beneficiary
        )
        return model.objects.filter(
            benefit_plan=benefit_plan,
            status=BeneficiaryStatus.POTENTIAL,
        )
