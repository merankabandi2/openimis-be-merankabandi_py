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
