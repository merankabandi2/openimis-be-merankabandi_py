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
        # MRO ordering: this mixin runs BEFORE the parent strategy, so the
        # parent hasn't yet added 'entity' to kwargs. The parent's caller
        # always supplies either 'group' (GroupBenefitPackageStrategy) or
        # 'beneficiary' (IndividualBenefitPackageStrategy), and the parent
        # then maps that to 'entity'. We replicate that fallback chain here
        # so the fee lookup works regardless of which strategy invoked us.
        entity = (
            kwargs.get('entity')
            or kwargs.get('group')
            or kwargs.get('beneficiary')
        )
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
                # Surface as warning, not debug — silent failures here cause
                # systematic underpayment (the beneficiary nets less than the
                # program intends) and the bug went undetected on Bisoro T9
                # 2026-04-27 because the message was at debug level.
                logger.warning(
                    "Fee lookup failed for entity=%r benefit_plan=%r: %s",
                    entity, getattr(payment_plan, 'benefit_plan', None), e,
                )

        # Adjust amount
        if amount is not None:
            base_amount = Decimal(str(amount))
            total_amount = base_amount + topup
            # Always compute fee_amount when an active AgencyFeeConfig sets a
            # fee_rate. The fee is recorded for audit/reporting; it is NOT
            # added into ``benefit.amount`` (which stays at the net amount
            # the beneficiary should receive). Whether the fee is paid on top
            # at transfer time is controlled by ``fee_included`` and applied
            # downstream in the gateway connector.
            if fee_rate:
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


class _JsonExtBenefitMixin:
    """Override create_and_save_business_entities_batch to include json_ext on BenefitConsumption.

    The installed upstream strategy constructs BenefitConsumption without json_ext.
    This mixin duplicates the parent method but adds json_ext to the constructor.
    """

    @classmethod
    def create_and_save_business_entities_batch(cls, batch_bill_results, batch_benefit_results, payroll_id, user):
        from datetime import datetime as py_datetime
        import uuid as uuid_module
        from invoice.models import Bill, BillItem
        from payroll.models import (
            BenefitConsumption, BenefitAttachment,
            PayrollBenefitConsumption,
        )
        from payroll.services import PayrollService, BenefitConsumptionService
        from invoice.services import BillService

        now = py_datetime.now()

        if len(batch_bill_results) != len(batch_benefit_results):
            raise ValueError(
                f"Mismatch between bill and benefit batch sizes for payroll {payroll_id}"
            )

        bill_instances = []
        bill_item_instances = []
        benefit_instances = []
        attachment_instances = []
        payroll_benefit_instances = []

        for bill_result, benefit_result in zip(batch_bill_results, batch_benefit_results):
            bill_data = bill_result['bill_data']
            bill_line_items = bill_result['bill_data_line']
            benefit_data = benefit_result['benefit_data']

            if not benefit_data.get('individual_id'):
                continue

            bill_uuid = uuid_module.uuid4()
            benefit_uuid = uuid_module.uuid4()

            bill = cls._stamp_audit(Bill(
                id=bill_uuid,
                subject_type_id=bill_data.get('subject_type_id'),
                subject_id=bill_data.get('subject_id'),
                thirdparty_type_id=bill_data.get('thirdparty_type_id'),
                thirdparty_id=bill_data.get('thirdparty_id'),
                code=bill_data.get('code', ''),
                code_tp=bill_data.get('code_tp'),
                code_ext=bill_data.get('code_ext'),
                date_due=bill_data.get('date_due'),
                date_bill=bill_data.get('date_bill'),
                date_valid_from=bill_data.get('date_valid_from'),
                date_valid_to=bill_data.get('date_valid_to'),
                currency_tp_code=bill_data.get('currency_tp_code'),
                currency_code=bill_data.get('currency_code'),
                status=bill_data.get('status', Bill.Status.VALIDATED),
                terms=bill_data.get('terms'),
                note=bill_data.get('note'),
                amount_net=cls._sum_line_items(bill_line_items, 'amount_total'),
                amount_total=cls._sum_line_items(bill_line_items, 'amount_total'),
                amount_discount=cls._sum_line_items(bill_line_items, 'discount'),
            ), user, now)
            bill_instances.append(bill)

            for line_item_data in bill_line_items:
                bill_item = cls._stamp_audit(BillItem(
                    bill_id=bill_uuid,
                    line_type_id=line_item_data.get('line_type_id'),
                    line_id=line_item_data.get('line_id'),
                    code=line_item_data.get('code', ''),
                    quantity=line_item_data.get('quantity', 1),
                    unit_price=line_item_data.get('unit_price', 0),
                    amount_total=line_item_data.get('amount_total', 0),
                    amount_net=line_item_data.get('amount_total', 0),
                    discount=line_item_data.get('discount', 0),
                    deduction=line_item_data.get('deduction', 0),
                    date_valid_from=line_item_data.get('date_valid_from'),
                    date_valid_to=line_item_data.get('date_valid_to'),
                ), user, now)
                bill_item_instances.append(bill_item)

            benefit = cls._stamp_audit(BenefitConsumption(
                id=benefit_uuid,
                individual_id=benefit_data.get('individual_id'),
                code=benefit_data.get('code', ''),
                date_due=benefit_data.get('date_due'),
                amount=benefit_data.get('amount'),
                type=benefit_data.get('type'),
                status=benefit_data.get('status'),
                json_ext=benefit_data.get('json_ext'),
                date_valid_from=benefit_data.get('date_valid_from'),
                date_valid_to=benefit_data.get('date_valid_to'),
            ), user, now)
            benefit_instances.append(benefit)

            attachment = cls._stamp_audit(BenefitAttachment(
                benefit_id=benefit_uuid,
                bill_id=bill_uuid,
            ), user, now)
            attachment_instances.append(attachment)

            if payroll_id:
                pbc = cls._stamp_audit(PayrollBenefitConsumption(
                    payroll_id=payroll_id,
                    benefit_id=benefit_uuid,
                ), user, now)
                payroll_benefit_instances.append(pbc)

        try:
            BillService.bulk_create_bills(bill_instances)
            BillService.bulk_create_bill_items(bill_item_instances)

            benefit_service = BenefitConsumptionService(user)
            benefit_service.bulk_create(benefit_instances)
            benefit_service.bulk_create_attachments(attachment_instances)

            if payroll_benefit_instances:
                payroll_service = PayrollService(user=user)
                payroll_service.bulk_attach_benefits(payroll_benefit_instances)
        except Exception:
            logger.error(
                f"Failed to bulk create entities for payroll {payroll_id}",
                exc_info=True,
            )
            raise


class MeraGroupBenefitPackageStrategy(_JsonExtBenefitMixin, MeraAmountMixin, GroupBenefitPackageStrategy):
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


class MeraIndividualBenefitPackageStrategy(_JsonExtBenefitMixin, MeraAmountMixin, IndividualBenefitPackageStrategy):
    CONVERTER_BENEFIT = MeraBeneficiaryToBenefitConverter
