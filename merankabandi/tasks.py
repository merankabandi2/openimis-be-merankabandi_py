"""
Merankabandi-specific Celery tasks.
"""
import logging
from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task
def send_partial_reconciliation(payroll_id, user_id):
    """Reconcile ACCEPTED benefits in a payroll via the payment gateway.

    Mera-specific: iterates benefits individually through the Burundi gateway.
    """
    from core.models import User
    from payroll.models import Payroll, BenefitConsumptionStatus
    from payroll.payments_registry import PaymentMethodStorage

    payroll = Payroll.objects.get(id=payroll_id)
    user = User.objects.get(id=user_id)
    strategy = PaymentMethodStorage.get_chosen_payment_method(payroll.payment_method)

    if not strategy:
        raise ValueError(f"No payment strategy for method '{payroll.payment_method}'")

    strategy.initialize_payment_gateway(payroll.payment_point)
    benefits = strategy.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
    gateway = strategy.PAYMENT_GATEWAY
    to_reconcile = []

    for benefit in benefits:
        result = gateway.reconcile(benefit.code, benefit.amount)
        if benefit.json_ext is None:
            benefit.json_ext = {}
        if result:
            benefit.json_ext['output_gateway'] = result
            benefit.json_ext['gateway_reconciliation_success'] = True
            to_reconcile.append(benefit)
        else:
            benefit.json_ext['gateway_reconciliation_success'] = False
            benefit.save(username=user.login_name)

    if to_reconcile:
        strategy.reconcile_benefit_consumption(to_reconcile, user)

    logger.info(
        "Partial reconciliation for payroll %s: %d/%d benefits reconciled",
        payroll_id, len(to_reconcile), benefits.count(),
    )
