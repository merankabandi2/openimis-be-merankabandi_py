"""Resolve the payment gateway source (PaymentAgency or PaymentPoint) for a payroll.

Mera-flow payrolls (created via ``CommunePaymentSchedule.batch_generate_payrolls``)
carry the payment agency reference in ``json_ext.agency_code`` and have
``payment_point=None``. Legacy/upstream-shaped payrolls have a populated
``payment_point`` FK and no agency_code in json_ext.

This helper is used by the Push strategy and the partial-reconciliation task.
"""
import logging

logger = logging.getLogger(__name__)


def resolve_gateway_source(payroll):
    """Return the best gateway config source for ``payroll``.

    Preference:
      1. ``merankabandi.PaymentAgency`` matched by
         ``payroll.json_ext.agency_code`` (or ``payment_agency_name``) —
         **only if** its ``gateway_config`` is populated.  An agency with
         empty ``gateway_config`` is informational (used for routing) but
         can't drive the connector, so we fall through to (2).
      2. ``payroll.payment_point`` (legacy) — has ``json_ext.paymentMethodConfig``
         that the connectors have always read.
      3. ``PaymentPoint`` looked up by ``name__iexact=agency_code`` —
         fallback for Mera-flow payrolls (no payment_point) when the agency's
         ``gateway_config`` hasn't been backfilled yet. The migration
         ``0016_payment_agency`` enforces this naming convention.

    Returns ``None`` if nothing resolves.
    """
    json_ext = payroll.json_ext or {}
    agency_code = json_ext.get('agency_code') or json_ext.get('payment_agency_name')

    # 1. PaymentAgency whose gateway_config yields a usable connector class.
    # An agency with empty gateway_config (or `{}`) has no per-instance
    # payment_gateway_class — settings.PAYMENT_GATEWAYS doesn't supply it
    # either, so the connector wouldn't load. Fall through in that case.
    if agency_code:
        from merankabandi.models import PaymentAgency
        from merankabandi.payment_gateway.payment_gateway_config import PaymentGatewayConfig
        agency = PaymentAgency.objects.filter(
            code__iexact=agency_code, is_active=True,
        ).first()
        if agency:
            try:
                if PaymentGatewayConfig(agency).payment_gateway_class:
                    return agency
            except Exception as exc:
                logger.warning(
                    "Could not evaluate PaymentAgency(%r) config: %s; falling back",
                    agency_code, exc,
                )
        else:
            logger.warning(
                "Payroll %s references agency_code=%r but no active PaymentAgency matches",
                payroll.id, agency_code,
            )

    # 2. Legacy payment_point
    pp = getattr(payroll, 'payment_point', None)
    if pp is not None:
        return pp

    # 3. Last-resort: PaymentPoint by name match against agency_code
    if agency_code:
        from payroll.models import PaymentPoint
        pp = PaymentPoint.objects.filter(
            name__iexact=agency_code, is_deleted=False,
        ).first()
        if pp:
            logger.info(
                "Payroll %s: falling back to PaymentPoint %r (agency %r has empty gateway_config)",
                payroll.id, pp.name, agency_code,
            )
            return pp

    return None
