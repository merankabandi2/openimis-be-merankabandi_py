from merankabandi.payment_gateway.payment_gateway_connector import PaymentGatewayConnector
import logging
import hashlib
import datetime

logger = logging.getLogger(__name__)

class LumicashPaymentGatewayConnector(PaymentGatewayConnector):
    """
    Connector for Lumicash API Integration
    Supports 'Pay on behalf of Partner' API
    """
    
    def _format_amount(self, amount):
        """
        Format amount to ####.## format as required by the API
        """
        return "{:.2f}".format(float(amount))
    
    def _generate_request_id(self, invoice_id):
        """
        Generate a request ID in the format PPPPyyMMddHHmmssfff
        Uses invoice_id as part of the unique identifier
        """
        prefix = self.config.partner_code[:4]
        now = datetime.datetime.now()
        timestamp = now.strftime("%y%m%d%H%M%S%f")[:15]  # Take first 15 digits
        
        # Use invoice_id as a suffix if needed for uniqueness
        # We truncate and combine to ensure we still match the required format
        invoice_suffix = str(invoice_id)[-4:] if len(str(invoice_id)) > 4 else str(invoice_id)
        timestamp = timestamp[:(15-len(invoice_suffix))] + invoice_suffix
        
        return f"{prefix}{timestamp}"
    
    def _generate_request_date(self):
        """
        Generate a request date in the format yyyyMMddHHmmssfff
        """
        now = datetime.datetime.now()
        return now.strftime("%Y%m%d%H%M%S%f")[:17]  # yyyyMMddHHmmssfff format
    
    def _generate_signature(self, request_date, amount, des_mobile, request_id):
        """
        Generate signature using MD5 hash
        MD5(Key + RequestDate + TransAmount.ToString("####.##") + PartnerCode + DesMobile + RequestId)
        """
        formatted_amount = self._format_amount(amount)
        raw_text = self.config.api_key + request_date + formatted_amount + self.config.partner_code + des_mobile + request_id
        return hashlib.md5(raw_text.encode()).hexdigest()

    def send_payment(self, invoice_id, amount, **kwargs):
        from payroll.models import BenefitConsumption
        """
        Send payment using Lumicash API
        
        Required kwargs:
        - phone_number: Recipient's phone number
        - content: Optional payment content/description
        """
        username = kwargs.get('username')
        # Get phone number from kwargs
        phone_number = kwargs.get('phone_number')
        if not phone_number:
            logger.error("Phone number is required for Lumicash payment")
            return False
        
        # Generate required fields
        request_id = self._generate_request_id(invoice_id)
        request_date = self._generate_request_date()
        
        # Generate signature
        signature = self._generate_signature(
            request_date, 
            amount, 
            phone_number, 
            request_id
        )
        
        # Prepare payment payload
        payload = {
            "RequestId": request_id,
            "RequestDate": request_date,
            "PartnerCode": self.config.partner_code,
            "DesMobile": phone_number,
            "TransAmount": int(amount),
            "Content": kwargs.get('content', f"Payment for invoice {invoice_id}"),
            "Description": kwargs.get('description', ''),
            "Signature": signature
        }
        
        # Send request
        url = f"{self.config.gateway_base_url}/api/3rd/customer/transaction/payonbehalf"
        
        try:
            response = self.send_request(url, payload)
            if not response:
                return False

            data = response.json()
            response_code = data.get('ResponseCode')
            benefit = BenefitConsumption.objects.get(code=invoice_id)
            
            if response_code == "01":  # Success
                logger.info(f"Lumicash payment successful: {data.get('TransCode')}")
                # Save the transaction code for later reference (for reconciliation)
                benefit.receipt = data.get('TransCode')
                benefit.json_ext['payment_request'] = data
                benefit.save(username=username)
                return True
            else:
                logger.error(f"Lumicash payment failed with code: {response_code}, message: {data.get('ResponseMessage')}")
                benefit.json_ext['payment_request'] = data
                benefit.save(username=username)
                return False
                
        except Exception as e:
            logger.error(f"Lumicash payment request failed: {e}")
            return False

    # TODO: Implement reconciliation
    def reconcile(self, invoice_id, amount, **kwargs):
        pass
        
    def send_request(self, url, payload):
        """
        Override send_request to handle Lumicash specific requirements
        """
        try:
            # Use the configured session with proper headers
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            return response
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None