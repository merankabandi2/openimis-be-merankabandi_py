"""
Merankabandi calculation rules for payroll benefit generation.

These appear in the PaymentPlan calculation dropdown and route to
Mera-specific strategies that include Burundi payment data in benefits.

Program-specific subclasses (THIMO, Refugees, Crisis) can extend
MeraPaymentCalcRule and override only what differs.
"""
from calcrule_social_protection.calculation_rule import SocialProtectionCalculationRule
from core import datetime


class MeraPaymentCalcRule(SocialProtectionCalculationRule):
    """Base calculation rule for Merankabandi payment programs."""
    version = 1
    uuid = "3ac78a68-b4a9-47dc-a8d0-fd7fca034870"
    calculation_rule_name = "Mera - Transferts Réguliers"
    description = "Payment rule for Merankabandi programs with Burundi-specific converters."
    date_valid_from = datetime.datetime(2000, 1, 1)
    date_valid_to = None
    status = "active"
    type = "social_protection"
    sub_type = "benefit_plan"

    @classmethod
    def calculate(cls, payment_plan, **kwargs):
        from social_protection.models import BenefitPlan
        from merankabandi.calcrule_strategies import (
            MeraGroupBenefitPackageStrategy,
            MeraIndividualBenefitPackageStrategy,
        )
        if payment_plan.benefit_plan.type == BenefitPlan.BenefitPlanType.GROUP_TYPE:
            MeraGroupBenefitPackageStrategy.calculate(cls, payment_plan, **kwargs)
        else:
            MeraIndividualBenefitPackageStrategy.calculate(cls, payment_plan, **kwargs)

    @classmethod
    def convert(cls, payment_plan, **kwargs):
        from social_protection.models import BenefitPlan
        from merankabandi.calcrule_strategies import (
            MeraGroupBenefitPackageStrategy,
            MeraIndividualBenefitPackageStrategy,
        )
        if payment_plan.benefit_plan.type == BenefitPlan.BenefitPlanType.GROUP_TYPE:
            MeraGroupBenefitPackageStrategy.convert(payment_plan, **kwargs)
        else:
            MeraIndividualBenefitPackageStrategy.convert(payment_plan, **kwargs)

    @classmethod
    def check_calculation(cls, payment_plan, **kwargs):
        return str(payment_plan.calculation) == cls.uuid
