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
        Payroll, PayrollStatus, PaymentPoint,
        BenefitConsumption, BenefitConsumptionStatus,
    )
    from location.models import Location

    obj_data = kwargs.get('data', {})

    # Skip for rattrapage (catch-up payments for rejected benefits)
    if obj_data.get('from_failed_invoices_payroll_id'):
        return

    payment_plan_id = obj_data.get('payment_plan_id')
    payment_point_id = obj_data.get('payment_point_id')
    if not payment_plan_id or not payment_point_id:
        return

    # Resolve commune (type W) from payment point location
    try:
        payment_point = PaymentPoint.objects.get(id=payment_point_id)
    except PaymentPoint.DoesNotExist:
        return

    if not payment_point.location:
        return

    commune = payment_point.location
    # Walk up to commune level if payment point is at colline level
    while commune and commune.type != 'W':
        commune = commune.parent
    if not commune:
        return

    # Find all collines under this commune
    colline_ids = list(
        Location.objects.filter(parent=commune, type='V')
        .values_list('id', flat=True)
    )
    # Include commune itself (payment point could be at commune level)
    location_ids = colline_ids + [commune.id]

    # Find existing non-terminal payrolls for same payment_plan + commune locations
    existing_payrolls = Payroll.objects.filter(
        payment_plan_id=payment_plan_id,
        payment_point__location_id__in=location_ids,
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
