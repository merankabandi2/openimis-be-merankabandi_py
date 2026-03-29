"""
Merankabandi signal bindings.

Discovered automatically by openIMIS core (searches for bind_service_signals in module_name.signals).
"""
from core.signals import bind_service_signal
from core.service_signals import ServiceSignalBindType


def bind_service_signals():
    from merankabandi.payroll_signals import validate_commune_reconciliation
    bind_service_signal(
        'payroll_service.create',
        validate_commune_reconciliation,
        ServiceSignalBindType.BEFORE,
    )

    # Notification signal bindings
    from merankabandi.notification_signals import (
        on_payroll_created,
        on_task_completed,
        on_task_created,
        on_grievance_created,
        on_grievance_updated,
        on_grievance_comment,
        on_grievance_reopened,
    )

    bind_service_signal(
        'payroll_service.create',
        on_payroll_created,
        ServiceSignalBindType.AFTER,
    )
    bind_service_signal(
        'task_service.complete_task',
        on_task_completed,
        ServiceSignalBindType.AFTER,
    )
    bind_service_signal(
        'task_service.create',
        on_task_created,
        ServiceSignalBindType.AFTER,
    )
    bind_service_signal(
        'ticket_service.create',
        on_grievance_created,
        ServiceSignalBindType.AFTER,
    )
    bind_service_signal(
        'ticket_service.update',
        on_grievance_updated,
        ServiceSignalBindType.AFTER,
    )
    bind_service_signal(
        'comment_service.create',
        on_grievance_comment,
        ServiceSignalBindType.AFTER,
    )
    bind_service_signal(
        'ticket_service.reopen_ticket',
        on_grievance_reopened,
        ServiceSignalBindType.AFTER,
    )
