from merankabandi.payment_gateway.payment_gateway_connector import PaymentGatewayConnector
import logging
import requests
import time
import threading

logger = logging.getLogger(__name__)

class IBBPaymentGatewayConnector(PaymentGatewayConnector):
    """
    Connector for IBB M+ API Integration
    Thread-safe implementation for parallel payment processing
    """
    def __init__(self, paymentpoint):
        super().__init__(paymentpoint)
        self.token = None
        self.token_expiry = 0
        self._token_lock = threading.Lock()
        self._session_lock = threading.Lock()

    def _refresh_token_if_needed(self):
        """
        Check if token is expired and refresh if needed
        Thread-safe: Only one thread will refresh the token at a time
        """
        current_time = time.time()
        if not self.token or current_time >= self.token_expiry:
            # Use lock to prevent multiple threads from refreshing simultaneously
            with self._token_lock:
                # Double-check after acquiring lock (another thread may have refreshed)
                current_time = time.time()
                if not self.token or current_time >= self.token_expiry:
                    logger.info("Token expired or not found, refreshing token")
                    self._get_auth_token()

    def _get_auth_token(self):
        """
        Get authentication token from IBB API
        Returns True if token was successfully retrieved and set, False otherwise
        """
        url = f'{self.config.gateway_base_url}/ipg/Ibb/auth/token'
        payload = {
            "login": self.config.basic_auth_username,
            "password": self.config.basic_auth_password
        }

        try:
            # Basic headers without auth for token endpoint
            headers = {'Content-Type': 'application/json'}
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()

            token_data = response.json()
            if 'token' in token_data and token_data['token']:
                new_token = token_data['token']

                # Validate token is non-empty
                if not new_token or len(new_token) < 10:
                    logger.error(f"Invalid token received: {new_token}")
                    return False

                # Update token and expiry
                self.token = new_token
                # Set token expiry to 50 seconds (IBB tokens typically last 1 minute)
                self.token_expiry = time.time() + 50

                # Update session headers with the new token
                self.session.headers.update({'Authorization': f'Bearer {self.token}'})

                logger.info(f"Token successfully refreshed, expires in 50 seconds")
                return True

            logger.error("Token not found in response")
            return False

        except requests.exceptions.RequestException as e:
            logger.error(f"Token request failed: {e}")
            return False

    def _lookup_customer(self, phone_number):
        """
        Look up customer details by phone number
        Returns tuple (success, customer_name)

        Note: This endpoint does NOT require authentication/token
        """
        if not phone_number:
            logger.error("Phone number is required for customer lookup")
            return False, None

        # Ensure phone number format (strip country code if present)
        if phone_number.startswith('+'):
            phone_number = phone_number[1:]
        if phone_number.startswith('257'):  # Burundi country code
            phone_number = phone_number[3:]

        url = f'{self.config.gateway_base_url}/ipg/Ibb/IoService/customerLookUp/{phone_number}'
        max_retries = 2
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # Customer lookup is a public endpoint, no authentication required
                response = self.session.get(url)
                response.raise_for_status()
                data = response.json()

                if data.get('statusCode') == 200:
                    return True, data.get('customerName')
                else:
                    logger.error(f"Customer lookup failed with status: {data.get('statusCode')}")
                    return False, None

            except requests.exceptions.RequestException as e:
                logger.error(f"Customer lookup request failed: {e}")
                if retry_count < max_retries:
                    retry_count += 1
                    logger.warning(f"Retrying customer lookup ({retry_count}/{max_retries})...")
                    time.sleep(0.5)
                    continue
                return False, None

        # All retries exhausted
        logger.error(f"Customer lookup failed after {max_retries + 1} attempts")
        return False, None

    def send_payment(self, invoice_id, amount, **kwargs):
        from payroll.models import BenefitConsumption
        """
        Send payment using IBB M+ API

        Required kwargs:
        - phone_number: Recipient's phone number
        """
        username = kwargs.get('username')
        # Get phone number from kwargs
        phone_number = kwargs.get('phone_number')
        if not phone_number:
            logger.error("Phone number is required for IBB payment")
            return False

        # Optional: Verify customer exists first
        # customer_exists, customer_name = self._lookup_customer(phone_number)
        # if not customer_exists:
        #     logger.error(f"Customer with phone {phone_number} not found")
        #     return False

        # Prepare payment payload
        payload = {
            "msisdn": phone_number,
            "transactionID": str(invoice_id),
            "partner": self.config.partner_name,
            "amount": int(amount),
            "pin": self.config.partner_pin
        }

        # Send payment request with retry logic for session errors
        url = f'{self.config.gateway_base_url}/ipg/Ibb/IoService/inBoundTransfer'
        max_retries = 1
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # Token is refreshed at batch level before parallel processing
                # Use session lock to ensure thread-safe access to the session
                with self._session_lock:
                    response = self.session.post(url, json=payload)
                    response.raise_for_status()
                    data = response.json()

                benefit = BenefitConsumption.objects.get(code=invoice_id)

                # Check for session error (65203 = "La session n'existe pas")
                if data.get('statusCode') == "401" and retry_count < max_retries:
                    logger.warning(f"Session error on payment attempt {retry_count + 1}/{max_retries + 1}, refreshing token and retrying")
                    # Force token refresh with lock to ensure thread safety
                    with self._token_lock:
                        self._refresh_token_if_needed()

                    retry_count += 1
                    time.sleep(0.5)  # Brief delay to allow token propagation
                    logger.info(f"Token refreshed successfully, retrying payment with new token")
                    continue

                if data.get('statusCode') == "200":
                    logger.info(f"Payment successful: {data.get('ibbTransactionID')}")
                    benefit.receipt = data.get('ibbTransactionID')
                    benefit.json_ext['payment_request'] = data
                    benefit.save(username=username)
                    return True
                else:
                    logger.error(f"Payment failed with status: {data.get('statusCode')}, message: {data.get('statusDesc')}")
                    benefit.json_ext['payment_request'] = data
                    benefit.save(username=username)
                    return False

            except requests.exceptions.RequestException as e:
                logger.error(f"Payment request failed: {e}")
                retry_count = max_retries
                return False

        # All retries exhausted
        logger.error(f"Payment failed after {max_retries + 1} attempts")
        return False

    def reconcile(self, invoice_id, amount, **kwargs):
        from payroll.models import BenefitConsumption
        """
        Check transaction status using IBB M+ API
        """
        url = f'{self.config.gateway_base_url}/ipg/Ibb/IoService/trxLookUp/{invoice_id}'
        
        username = kwargs.get('username')
        try:
            benefit = BenefitConsumption.objects.get(code=invoice_id)
            # Ensure we have a valid token
            self._refresh_token_if_needed()

            # Use session lock to ensure thread-safe access
            with self._session_lock:
                response = self.session.get(url)
                response.raise_for_status()
                data = response.json()

            if data.get('status') == "200":
                from payroll.models import BenefitConsumptionStatus
                # Verify the transaction amount
                tx_amount = float(data.get('amount', 0))
                expected_amount = float(amount)

                if abs(tx_amount - expected_amount) < 0.01:  # Allow small rounding differences
                    ibbTransactionId = data.get('ibbTransactionId')
                    if ibbTransactionId and benefit.receipt != ibbTransactionId:
                        benefit.receipt = ibbTransactionId
                    benefit.json_ext['payment_reconciliation'] = data
                    benefit.status = BenefitConsumptionStatus.RECONCILED
                    benefit.save(username=username)
                    return True
                else:
                    logger.error(f"Amount mismatch: expected {expected_amount}, got {tx_amount}")
                    benefit.json_ext['payment_reconciliation'] = data
                    benefit.json_ext['payment_reconciliation']["error_message"] = f"Amount mismatch: expected {expected_amount}, got {tx_amount}"
                    benefit.save(username=username)
                    return False
            else:
                logger.error(f"Transaction lookup failed with status: {data.get('status')}")
                benefit.json_ext['payment_reconciliation'] = data
                benefit.save(username=username)
                return False

        except requests.exceptions.RequestException as e:
            logger.error(f"Reconciliation request failed: {e}")
            return False