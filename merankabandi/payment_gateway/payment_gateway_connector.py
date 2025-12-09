import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from merankabandi.payment_gateway.payment_gateway_config import PaymentGatewayConfig

logger = logging.getLogger(__name__)


class PaymentGatewayConnector:
    def __init__(self, paymentpoint, pool_connections=30, pool_maxsize=30):
        """
        Initialize payment gateway connector with configurable connection pool

        Args:
            paymentpoint: Payment point configuration
            pool_connections: Number of connection pools to cache (default: 30)
            pool_maxsize: Maximum number of connections in each pool (default: 30)
        """
        self.config = PaymentGatewayConfig(paymentpoint)
        self.session = requests.Session()

        # Configure connection pool for high-concurrency parallel requests
        # This prevents "Connection pool is full" warnings when processing
        # multiple payments simultaneously (e.g., 20 parallel threads)
        adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=Retry(
                total=3,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

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
