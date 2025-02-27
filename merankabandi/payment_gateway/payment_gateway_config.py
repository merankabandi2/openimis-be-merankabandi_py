import importlib
import base64
from django.conf import settings
from payroll.apps import PayrollConfig


class PaymentGatewayConfig:
    def __init__(self, paymentpoint):
        # Merge default gateway config with payment point specific config
        gateway_config = {**settings.PAYMENT_GATEWAYS.get(paymentpoint.name, {}), 
                         **paymentpoint.json_ext.get('paymentMethodConfig', {})}
        
        # Common configuration
        self.gateway_base_url = gateway_config.get('gateway_base_url', '')
        self.endpoint_payment = gateway_config.get('endpoint_payment', '')
        self.endpoint_reconciliation = gateway_config.get('endpoint_reconciliation', '')
        self.api_key = gateway_config.get('payment_gateway_api_key', '')
        self.basic_auth_username = gateway_config.get('payment_gateway_basic_auth_username', '')
        self.basic_auth_password = gateway_config.get('payment_gateway_basic_auth_password', '')
        self.timeout = gateway_config.get('payment_gateway_timeout', 30)
        self.auth_type = gateway_config.get('payment_gateway_auth_type', 'none')
        
        # IBB specific configuration
        self.partner_name = gateway_config.get('partner_name', '')
        self.partner_pin = gateway_config.get('partner_pin', '')
        
        # Lumicash specific configuration
        self.partner_code = gateway_config.get('partner_code', '')
        
        # Store payment gateway connector class
        self.payment_gateway_class = gateway_config.get('payment_gateway_class', '')

    def get_headers(self):
        """
        Get HTTP headers for API requests based on auth type
        """
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
        module_name, class_name = self.payment_gateway_class.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, class_name)


    def get_payment_endpoint(self):
        return self.endpoint_payment

    def get_reconciliation_endpoint(self):
        return self.endpoint_reconciliation