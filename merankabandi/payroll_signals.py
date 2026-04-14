"""
Merankabandi payroll signal handlers.

Commune-level constraint: a commune's payroll must be fully reconciled
before creating a new one for the same payment plan.
"""
import logging
from django.utils.translation import gettext as _

logger = logging.getLogger(__name__)


def _extract_signal_data(kwargs):
    """Extract the obj_data dict from signal kwargs.

    Signal data is [func_args, func_kwargs] where func_args[0] is the obj_data dict.
    """
    raw = kwargs.get('data', {})
    if isinstance(raw, (list, tuple)):
        return raw[0][0] if raw and raw[0] else {}
    return raw


def validate_commune_reconciliation(**kwargs):
    """Block payroll creation if same commune + payment_plan has unreconciled benefits.

    Skipped when from_failed_invoices_payroll_id is present (rattrapage flow).
    """
    from payroll.models import (
        Payroll, PayrollStatus,
        BenefitConsumption, BenefitConsumptionStatus,
    )
    from location.models import Location

    obj_data = _extract_signal_data(kwargs)

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


def intercept_payroll_for_verification(**kwargs):
    """After upstream creates the approval task, downgrade to PENDING_VERIFICATION.

    Mera workflow: GENERATING → PENDING_VERIFICATION → PENDING_APPROVAL → APPROVE_FOR_PAYMENT
    Upstream only does: GENERATING → PENDING_APPROVAL

    This AFTER signal on payroll_service.create_task:
    1. Changes payroll status from PENDING_APPROVAL to PENDING_VERIFICATION
    2. Updates the task source to 'payroll_verification' so the FE renders the
       verification UI instead of the approval UI
    """
    from payroll.models import Payroll
    from tasks_management.models import Task
    from django.contrib.contenttypes.models import ContentType

    # Extract payroll_id from signal data
    obj_data = _extract_signal_data(kwargs)
    payroll_id = obj_data.get('id')
    if not payroll_id:
        return

    try:
        payroll = Payroll.objects.get(id=payroll_id)
    except Payroll.DoesNotExist:
        return

    # Only intercept if payroll was just set to PENDING_APPROVAL by upstream
    if payroll.status != 'PENDING_APPROVAL':
        return

    # Downgrade to PENDING_VERIFICATION
    payroll.status = 'PENDING_VERIFICATION'
    payroll.save(username='admin')
    logger.info(f"Payroll {payroll_id} status changed to PENDING_VERIFICATION (Mera workflow)")

    # Update the task that was just created to use verification source
    payroll_ct = ContentType.objects.get_for_model(Payroll)
    task = Task.objects.filter(
        entity_type=payroll_ct,
        entity_id=str(payroll_id),
        source='payroll',
    ).order_by('-date_created').first()

    if task:
        task.source = 'payroll_verification'
        # Keep business_event as payroll.accept_payroll so upstream handler
        # transitions to APPROVE_FOR_PAYMENT when task is resolved
        task.save(username='admin')
        logger.info(f"Task {task.id} source changed to payroll_verification")


def on_verification_task_completed(**kwargs):
    """When verification task is approved, transition to PENDING_APPROVAL and create approval task.

    Mera workflow: PENDING_VERIFICATION → (verify) → PENDING_APPROVAL → (approve) → APPROVE_FOR_PAYMENT
    """
    from payroll.models import Payroll
    from payroll.services import PayrollService
    from tasks_management.models import Task
    from core.models import User

    try:
        result = kwargs.get('result', None)
        if not result or not result.get('success'):
            return

        task = result['data']['task']
        if task.get('business_event') != 'merankabandi.verify_payroll':
            return

        task_status = task['status']
        payroll = Payroll.objects.get(id=task['entity_id'])
        user = User.objects.get(id=result['data']['user']['id'])

        if task_status == Task.Status.COMPLETED:
            # Verification approved → move to PENDING_APPROVAL
            payroll.status = 'PENDING_APPROVAL'
            payroll.save(username=user.username)
            logger.info(f"Payroll {payroll.id} verified → PENDING_APPROVAL")

            # Create the approval task
            svc = PayrollService(user)
            svc.create_accept_payroll_task(payroll.id, {'id': str(payroll.id)})

        elif task_status == Task.Status.FAILED:
            # Verification rejected
            payroll.status = 'REJECTED'
            payroll.save(username=user.username)
            logger.info(f"Payroll {payroll.id} verification rejected → REJECTED")

    except Exception as exc:
        logger.error("Error in on_verification_task_completed", exc_info=exc)


def sync_payment_schedule_on_payroll_change(**kwargs):
    """Auto-sync CommunePaymentSchedule when payroll status changes."""
    from merankabandi.models import CommunePaymentSchedule
    from payroll.models import Payroll, BenefitConsumption

    obj_data = _extract_signal_data(kwargs)
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
