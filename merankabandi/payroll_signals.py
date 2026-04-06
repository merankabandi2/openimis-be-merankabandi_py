"""
Merankabandi payroll signal handlers.

Commune-level constraint: a commune's payroll must be fully reconciled
before creating a new one for the same payment plan.
"""
import logging
from django.utils.translation import gettext as _

logger = logging.getLogger(__name__)


def validate_commune_reconciliation(**kwargs):
    """Block payroll creation if same commune + payment_plan has unreconciled benefits.

    Skipped when from_failed_invoices_payroll_id is present (rattrapage flow).
    """
    from payroll.models import (
        Payroll, PayrollStatus,
        BenefitConsumption, BenefitConsumptionStatus,
    )
    from location.models import Location

    obj_data = kwargs.get('data', {})

    # Skip for rattrapage (catch-up payments for rejected benefits)
    if obj_data.get('from_failed_invoices_payroll_id'):
        return

    payment_plan_id = obj_data.get('payment_plan_id')
    location_id = obj_data.get('location_id')
    if not payment_plan_id or not location_id:
        return

    # Resolve commune (type W) from location_id stored in payroll data
    try:
        location = Location.objects.get(LocationId=location_id)
    except Location.DoesNotExist:
        return

    commune = location
    while commune and commune.type != 'W':
        commune = commune.parent
    if not commune:
        return

    # Find all collines under this commune
    colline_ids = list(
        Location.objects.filter(parent=commune, type='V')
        .values_list('LocationId', flat=True)
    )
    location_ids = colline_ids + [commune.LocationId]

    # Find existing non-terminal payrolls for same payment_plan + commune
    # Use json_ext to match commune since we no longer use payment_point
    existing_payrolls = Payroll.objects.filter(
        payment_plan_id=payment_plan_id,
        json_ext__commune_id__in=[str(lid) for lid in location_ids],
        is_deleted=False,
    ).exclude(
        status__in=[PayrollStatus.RECONCILED, PayrollStatus.REJECTED]
    )

    for payroll in existing_payrolls:
        has_unreconciled = BenefitConsumption.objects.filter(
            payrollbenefitconsumption__payroll=payroll,
            is_deleted=False,
        ).exclude(
            status=BenefitConsumptionStatus.RECONCILED
        ).exists()

        if has_unreconciled:
            raise ValueError(
                _("merankabandi.payroll.commune_not_reconciled")
            )


def sync_payment_schedule_on_payroll_change(**kwargs):
    """Auto-sync CommunePaymentSchedule when payroll status changes."""
    from merankabandi.models import CommunePaymentSchedule
    from payroll.models import Payroll, BenefitConsumption

    obj_data = kwargs.get('data', {})
    payroll_id = obj_data.get('id')
    if not payroll_id:
        return

    schedules = CommunePaymentSchedule.objects.filter(payroll_id=payroll_id)
    if not schedules.exists():
        return

    try:
        payroll = Payroll.objects.get(id=payroll_id)
    except Payroll.DoesNotExist:
        return

    for schedule in schedules:
        schedule.sync_from_payroll()
        benefits = BenefitConsumption.objects.filter(
            payrollbenefitconsumption__payroll=payroll, is_deleted=False,
        )
        schedule.total_beneficiaries = benefits.count()
        schedule.reconciled_count = benefits.filter(status='RECONCILED').count()
        schedule.failed_count = benefits.filter(status='REJECTED').count()
        if schedule.total_beneficiaries > 0:
            schedule.total_amount = schedule.amount_per_beneficiary * schedule.total_beneficiaries
        schedule.save()
