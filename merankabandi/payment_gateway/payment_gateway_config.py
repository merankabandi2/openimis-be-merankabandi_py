import importlib
import json
import base64
import logging
from django.conf import settings

logger = logging.getLogger(__name__)


class PaymentGatewayConfig:
    """
    Resolves gateway connection settings for a payment ``source``.

    A source is either:
      * ``merankabandi.PaymentAgency`` — first-class Mera entity. Carries
        ``payment_gateway`` (key into ``settings.PAYMENT_GATEWAYS``) and
        ``gateway_config`` (TextField holding a JSON overlay).
      * ``payroll.PaymentPoint`` — legacy upstream entity. Carries ``name``
        (key into ``settings.PAYMENT_GATEWAYS``) and ``json_ext.paymentMethodConfig``
        (overlay dict).

    The merge precedence is settings → per-source overlay (overlay wins).
    """

    def __init__(self, source):
        gateway_key, overlay = _extract_source_config(source)
        merged = {
            **settings.PAYMENT_GATEWAYS.get(gateway_key, {}),
            **overlay,
        }

        self.source = source
        self.gateway_key = gateway_key

        # Common configuration
        self.gateway_base_url = merged.get('gateway_base_url', '') or ''
        self.endpoint_payment = merged.get('endpoint_payment', '') or ''
        self.endpoint_reconciliation = merged.get('endpoint_reconciliation', '') or ''
        self.api_key = merged.get('payment_gateway_api_key', '') or ''
        self.basic_auth_username = merged.get('payment_gateway_basic_auth_username', '') or ''
        self.basic_auth_password = merged.get('payment_gateway_basic_auth_password', '') or ''
        self.timeout = merged.get('payment_gateway_timeout', 30)
        self.auth_type = merged.get('payment_gateway_auth_type', 'none')

        # IBB specific
        self.partner_name = merged.get('partner_name', '') or ''
        self.partner_pin = merged.get('partner_pin', '') or ''

        # Lumicash specific
        self.partner_code = merged.get('partner_code', '') or ''

        # Connector class path
        self.payment_gateway_class = merged.get('payment_gateway_class', '') or ''

    def get_headers(self):
        """HTTP headers for API requests, based on ``auth_type``."""
        headers = {'Content-Type': 'application/json'}

        if self.auth_type == 'token':
            headers['Authorization'] = f'Bearer {self.api_key}'

        elif self.auth_type == 'basic':
            auth_str = f"{self.basic_auth_username}:{self.basic_auth_password}"
            auth_bytes = auth_str.encode('utf-8')
            auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
            headers['Authorization'] = f'Basic {auth_base64}'

        return headers

    def get_payment_gateway_connector(self):
        if not self.payment_gateway_class:
            raise ValueError(
                f"No payment_gateway_class configured for source "
                f"{self.source!r} (gateway_key={self.gateway_key!r})"
            )
        module_name, class_name = self.payment_gateway_class.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    def get_payment_endpoint(self):
        return self.endpoint_payment

    def get_reconciliation_endpoint(self):
        return self.endpoint_reconciliation


def _extract_source_config(source):
    """Return ``(gateway_key, overlay_dict)`` for a PaymentAgency or PaymentPoint.

    Detection is duck-typed so the caller never has to know which model they hold:
      * PaymentAgency → has ``payment_gateway`` and ``gateway_config`` (TextField).
      * PaymentPoint  → has ``name`` and ``json_ext`` (JSONField).
    """
    if source is None:
        return '', {}

    # PaymentAgency (Merankabandi-native)
    if hasattr(source, 'payment_gateway') and hasattr(source, 'gateway_config'):
        # The gateway_key indexes settings.PAYMENT_GATEWAYS (e.g. 'INTERBANK',
        # 'LUMICASH') — that's PaymentAgency.code, NOT .payment_gateway (which
        # stores the strategy class name like 'StrategyOnlinePaymentPush').
        gateway_key = (getattr(source, 'code', '') or '').strip()
        overlay = _parse_overlay(source.gateway_config, source)
        return gateway_key, overlay

    # PaymentPoint (upstream)
    name = getattr(source, 'name', '') or ''
    json_ext = getattr(source, 'json_ext', None) or {}
    overlay = json_ext.get('paymentMethodConfig', {}) or {}
    return name, overlay


def _parse_overlay(raw, source):
    """Decode ``PaymentAgency.gateway_config`` into a dict.

    The field is a TextField holding JSON. Some rows are double-JSON-encoded
    (a JSON string whose value is itself a JSON object) due to the FE
    convention `JSON.stringify(JSON.stringify(value))` — peel an extra layer
    when needed.
    """
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid JSON in PaymentAgency(code=%r).gateway_config; using empty overlay",
            getattr(source, 'code', None),
        )
        return {}
    # Double-encoded: peel one more layer
    if isinstance(decoded, str):
        try:
            decoded = json.loads(decoded)
        except (ValueError, TypeError):
            logger.warning(
                "PaymentAgency(code=%r).gateway_config is a non-JSON string; using empty overlay",
                getattr(source, 'code', None),
            )
            return {}
    if not isinstance(decoded, dict):
        logger.warning(
            "PaymentAgency(code=%r).gateway_config decodes to %s, expected dict",
            getattr(source, 'code', None), type(decoded).__name__,
        )
        return {}
    return decoded
