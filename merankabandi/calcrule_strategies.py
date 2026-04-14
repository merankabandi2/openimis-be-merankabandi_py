"""
Merankabandi-specific benefit package strategies.

Override CONVERTER_BENEFIT to use Mera converters that add payment method data.
Override amount calculation to include:
  - Back-pay for missed rounds (72K × unpaid_rounds)
  - Top-up/compensatory from CommunePaymentSchedule
  - Fee from AgencyFeeConfig
"""
import logging
from decimal import Decimal

from calcrule_social_protection.strategies.benefit_package_group_strategy import GroupBenefitPackageStrategy
from calcrule_social_protection.strategies.benefit_package_individual_strategy import IndividualBenefitPackageStrategy
from merankabandi.calcrule_converters import MeraGroupToBenefitConverter, MeraBeneficiaryToBenefitConverter

logger = logging.getLogger(__name__)


class MeraAmountMixin:
    """Mixin for Merankabandi amount computation with fee + topup."""

    @classmethod
    def _collect_convert_results(cls, calculation, payment_plan, **kwargs):
        """Override to inject Mera-specific amount (back-pay + topup + fee)."""
        from merankabandi.models import (
            AgencyFeeConfig, CommunePaymentSchedule, ProvincePaymentAgency,
            STANDARD_TRANSFER_AMOUNT,
        )
        from merankabandi.payment_schedule_service import PaymentScheduleService

        amount = kwargs.get('amount', None)
        entity = kwargs.get('entity')
        payment_cycle = kwargs.get('payment_cycle')

        # Try to resolve the CommunePaymentSchedule for this payroll
        payroll_json = kwargs.get('payroll_json_ext') or {}
        schedule_id = payroll_json.get('payment_schedule_id')
        topup = Decimal('0')
        if schedule_id:
            schedule = CommunePaymentSchedule.objects.filter(id=schedule_id).first()
            if schedule:
                topup = schedule.topup_amount or Decimal('0')

        # Compute fee
        fee_amount = Decimal('0')
        fee_rate = Decimal('0')
        fee_included = False
        if entity and payment_plan:
            try:
                benefit_plan = payment_plan.benefit_plan
                # Resolve province from entity's group location
                group = getattr(entity, 'group', entity)
                location = getattr(group, 'location', None)
                province = None
                if location:
                    province = getattr(location, 'parent', None)
                    if province and province.parent:
                        province = province.parent  # colline → commune → province

                # Find agency for this province
                ppa = ProvincePaymentAgency.objects.filter(
                    benefit_plan=benefit_plan,
                    province=province,
                    is_active=True,
                ).select_related('payment_agency').first()

                if ppa:
                    fee_config = AgencyFeeConfig.lookup(
                        ppa.payment_agency, benefit_plan, province
                    )
                    if fee_config:
                        fee_rate = fee_config.fee_rate
                        fee_included = fee_config.fee_included
            except Exception as e:
                logger.debug(f"Fee lookup failed: {e}")

        # Adjust amount
        if amount is not None:
            base_amount = Decimal(str(amount))
            total_amount = base_amount + topup
            if not fee_included and fee_rate:
                fee_amount = total_amount * fee_rate

            kwargs['amount'] = float(total_amount)

        result = super()._collect_convert_results(calculation, payment_plan, **kwargs)

        # Enrich benefit json_ext with breakdown
        if result and len(result) >= 2:
            benefit_data = result[1].get('benefit_data', {})
            json_ext = benefit_data.get('json_ext', {}) or {}
            json_ext.update({
                'regular_amount': float(amount) if amount else 0,
                'topup_amount': float(topup),
                'fee_rate': float(fee_rate),
                'fee_amount': float(fee_amount),
                'fee_included': fee_included,
                'total_with_fee': float(Decimal(str(kwargs.get('amount', 0))) + fee_amount),
            })
            benefit_data['json_ext'] = json_ext

        return result


class MeraGroupBenefitPackageStrategy(MeraAmountMixin, GroupBenefitPackageStrategy):
    CONVERTER_BENEFIT = MeraGroupToBenefitConverter

    @classmethod
    def calculate(cls, calculation, payment_plan, **kwargs):
        """Override to fix queryset: upstream PayrollService passes Beneficiary
        (individual) queryset, but GROUP plans need GroupBeneficiary."""
        from django.db.models import Q
        from social_protection.models import GroupBeneficiary, BeneficiaryStatus

        beneficiaries_qs = kwargs.get('beneficiaries_queryset', None)

        # Check if upstream passed the wrong model (Beneficiary instead of GroupBeneficiary)
        if beneficiaries_qs is not None and beneficiaries_qs.model != GroupBeneficiary:
            # Rebuild from GroupBeneficiary with same benefit_plan filter
            beneficiaries_qs = GroupBeneficiary.objects.filter(
                benefit_plan=payment_plan.benefit_plan,
                status=BeneficiaryStatus.ACTIVE,
                is_deleted=False,
            )
            # Re-apply location filter from payroll json_ext
            payroll = kwargs.get('payroll')
            if payroll:
                ext = payroll.json_ext if isinstance(payroll.json_ext, dict) else {}
                location_ids = ext.get('filter_criteria', {}).get('location_ids', [])
                if location_ids:
                    beneficiaries_qs = beneficiaries_qs.filter(
                        Q(group__location__uuid__in=location_ids)
                        | Q(group__location__parent__uuid__in=location_ids)
                    )
            kwargs['beneficiaries_queryset'] = beneficiaries_qs

        return super().calculate(calculation, payment_plan, **kwargs)


class MeraIndividualBenefitPackageStrategy(MeraAmountMixin, IndividualBenefitPackageStrategy):
    CONVERTER_BENEFIT = MeraBeneficiaryToBenefitConverter
