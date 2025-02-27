import logging
import requests
from merankabandi.payment_gateway.payment_gateway_config import PaymentGatewayConfig

logger = logging.getLogger(__name__)


class PaymentGatewayConnector:
    def __init__(self, paymentpoint):
        self.config = PaymentGatewayConfig(paymentpoint)
        self.session = requests.Session()
        self.session.headers.update(self.config.get_headers())

    def send_request(self, endpoint, payload):
        url = f'{self.config.gateway_base_url}{endpoint}'
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None

    def send_payment(self, invoice_id, amount, **kwargs):
        pass

    def reconcile(self, invoice_id, amount, **kwargs):
        pass
