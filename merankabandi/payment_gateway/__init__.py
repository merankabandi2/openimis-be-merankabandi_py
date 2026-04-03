from merankabandi.payment_gateway.payment_gateway_connector import PaymentGatewayConnector
from merankabandi.payment_gateway.lumicash_payment_gateway_connector import LumicashPaymentGatewayConnector
from merankabandi.payment_gateway.interbank_payment_gateway_connector import IBBPaymentGatewayConnector


class PaymentGatewayRegistry:
    """
    Auto-registry of available payment gateway connectors.
    Each connector registers with its module path and a human-readable label.
    """
    _connectors = {}

    @classmethod
    def register(cls, key, connector_class, label=None):
        cls._connectors[key] = {
            'class': connector_class,
            'class_path': f"{connector_class.__module__}.{connector_class.__name__}",
            'label': label or connector_class.__name__,
        }

    @classmethod
    def get_all(cls):
        return [
            {'key': k, 'label': v['label'], 'classPath': v['class_path']}
            for k, v in cls._connectors.items()
        ]

    @classmethod
    def get(cls, key):
        entry = cls._connectors.get(key)
        return entry['class'] if entry else None


# Auto-register known connectors
PaymentGatewayRegistry.register(
    'LUMICASH', LumicashPaymentGatewayConnector, 'Lumicash'
)
PaymentGatewayRegistry.register(
    'INTERBANK', IBBPaymentGatewayConnector, 'Interbank (IBB)'
)
