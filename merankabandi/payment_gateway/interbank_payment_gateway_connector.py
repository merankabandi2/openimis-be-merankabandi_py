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
    def __init__(self, source):
        super().__init__(source)
        self.token = None
        self.token_expiry = 0
        self._token_lock = threading.Lock()
        # NOTE: requests.Session is thread-safe for concurrent POSTs.
        # The previous _session_lock serialized every request and defeated
        # the ThreadPoolExecutor parallelism — measured ~3s/payment instead of
        # ~3s/batch-of-10. Lock kept ONLY around mutations to session.headers
        # (which happens during token refresh).

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

                logger.info("Token successfully refreshed, expires in 50 seconds")
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
        """Send payment via IBB M+ API.

        **Pure I/O — does NOT touch the DB.** Returns a dict the caller can
        persist later on a single thread, so we don't open a Django ORM
        connection from every parallel worker thread (avoids Postgres
        "too many clients already" under load).

        Required kwargs:
            phone_number: Recipient's phone number.

        Returns:
            {'success': bool, 'data': <ibb response dict or None>, 'error': str|None}
        """
        phone_number = kwargs.get('phone_number')
        if not phone_number:
            logger.error("Phone number is required for IBB payment %s", invoice_id)
            return {'success': False, 'data': None, 'error': 'phone_number_missing'}

        payload = {
            "msisdn": phone_number,
            "transactionID": str(invoice_id),
            "partner": self.config.partner_name,
            "amount": int(amount),
            "pin": self.config.partner_pin,
        }
        url = f'{self.config.gateway_base_url}/ipg/Ibb/IoService/inBoundTransfer'

        # Retry conditions:
        #   - HTTP 401/403 (token expired mid-flight): force-refresh, retry once
        #   - data.statusCode == "401" (IBB session error in body): same handling
        max_retries = 1
        for attempt in range(max_retries + 1):
            try:
                # requests.Session is thread-safe for POSTs; no lock needed.
                response = self.session.post(url, json=payload, timeout=self.config.timeout)

                if response.status_code in (401, 403) and attempt < max_retries:
                    logger.warning(
                        "HTTP %s on payment %s — forcing token refresh and retrying",
                        response.status_code, invoice_id,
                    )
                    self._force_refresh_token()
                    time.sleep(0.2)
                    continue

                response.raise_for_status()
                data = response.json()

                if data.get('statusCode') == "401" and attempt < max_retries:
                    logger.warning(
                        "Body-level 401 on payment %s — refreshing token and retrying",
                        invoice_id,
                    )
                    self._force_refresh_token()
                    time.sleep(0.2)
                    continue

                if data.get('statusCode') == "200":
                    logger.info("Payment successful: %s", data.get('ibbTransactionID'))
                    return {'success': True, 'data': data, 'error': None}

                logger.error(
                    "Payment %s failed: status=%s message=%s",
                    invoice_id, data.get('statusCode'), data.get('statusDesc'),
                )
                return {'success': False, 'data': data, 'error': data.get('statusDesc')}

            except requests.exceptions.RequestException as e:
                logger.error("Payment request failed for %s: %s", invoice_id, e)
                return {'success': False, 'data': None, 'error': str(e)}

        logger.error("Payment %s failed after %d attempts", invoice_id, max_retries + 1)
        return {'success': False, 'data': None, 'error': 'retries_exhausted'}

    def _force_refresh_token(self):
        """Force a token refresh regardless of expiry — call after a 401."""
        with self._token_lock:
            self.token = None
            self.token_expiry = 0
            self._get_auth_token()

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

            response = self.session.get(url, timeout=self.config.timeout)
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
