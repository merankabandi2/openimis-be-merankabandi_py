"""
Merankabandi-specific Celery tasks.
"""
import io
import logging
from datetime import datetime

from celery import shared_task
from django.conf import settings

logger = logging.getLogger(__name__)




@shared_task
def create_result_framework_snapshot(user_id, name=None):
    """Create a result framework snapshot asynchronously, generate xlsx, notify user."""
    from core.models import User
    from .result_framework_service import ResultFrameworkService
    from .result_framework_mutations import _generate_xlsx
    import os

    user = User.objects.get(id=user_id)
    if not name:
        name = f"Snapshot {datetime.now().strftime('%d/%m/%Y %H:%M')}"

    try:
        # Create snapshot (slow — computes all indicators)
        service = ResultFrameworkService()
        snapshot = service.create_snapshot(name=name, description="Auto-generated", user=user)

        # Generate xlsx from snapshot data
        sections_data = snapshot.data.get('sections', [])
        snapshot_date_str = snapshot.snapshot_date.strftime('%d/%m/%Y')
        wb = _generate_xlsx(sections_data, snapshot_date=snapshot_date_str)

        # Save xlsx
        doc_dir = os.path.join(getattr(settings, 'MEDIA_ROOT', 'file_storage'), 'result_framework_docs')
        os.makedirs(doc_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'cadre_resultats_{timestamp}.xlsx'
        filepath = os.path.join(doc_dir, filename)
        wb.save(filepath)

        # Update snapshot with document path
        snapshot.document_path = f'result_framework_docs/{filename}'
        snapshot.status = 'FINALIZED'
        snapshot.save()

        # Send in-app notification
        _notify_snapshot_ready(user, snapshot)

        logger.info(f"Snapshot '{name}' created and saved to {filepath}")
        return str(snapshot.id)

    except Exception as e:
        logger.error(f"Snapshot creation failed: {e}", exc_info=True)
        raise


def _notify_snapshot_ready(user, snapshot):
    """Create in-app notification for snapshot ready."""
    try:
        from notification.models import Notification, NotificationEventType

        event_type = NotificationEventType.objects.filter(code='report.snapshot_ready').first()
        if not event_type:
            logger.warning("Notification event type 'report.snapshot_ready' not found — run seed_notification_templates")
            return

        download_url = f'/api/file_storage/{snapshot.document_path}' if snapshot.document_path else ''

        Notification.objects.create(
            event_type=event_type,
            recipient=user,
            channel='in_app',
            title='Cadre de résultats prêt',
            body=f'Le cadre de résultats "{snapshot.name}" est prêt. Cliquez pour télécharger.',
            entity_url=download_url,
            delivery_status='delivered',
        )
    except Exception as e:
        logger.warning(f"Failed to create notification: {e}")


@shared_task
def send_partial_reconciliation(payroll_id, user_id):
    """Reconcile ACCEPTED benefits in a payroll via the payment gateway.

    Mera-specific: iterates benefits individually through the Burundi gateway.
    """
    from core.models import User
    from payroll.models import Payroll, BenefitConsumptionStatus
    from payroll.payments_registry import PaymentMethodStorage

    from merankabandi.payment_gateway.source_resolver import resolve_gateway_source

    payroll = Payroll.objects.get(id=payroll_id)
    user = User.objects.get(id=user_id)
    strategy = PaymentMethodStorage.get_chosen_payment_method(payroll.payment_method)

    if not strategy:
        raise ValueError(f"No payment strategy for method '{payroll.payment_method}'")

    source = resolve_gateway_source(payroll)
    if source is None:
        logger.error(
            "Cannot reconcile payroll %s: no PaymentAgency (json_ext.agency_code=%r) "
            "and no payment_point.",
            payroll_id, (payroll.json_ext or {}).get('agency_code'),
        )
        return

    strategy.initialize_payment_gateway(source)
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
