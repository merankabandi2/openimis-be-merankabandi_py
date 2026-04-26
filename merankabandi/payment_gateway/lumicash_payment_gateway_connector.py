import datetime
import hashlib
import logging

import requests

from merankabandi.payment_gateway.payment_gateway_connector import PaymentGatewayConnector

logger = logging.getLogger(__name__)


class LumicashPaymentGatewayConnector(PaymentGatewayConnector):
    """Connector for Lumicash 'Pay on behalf of Partner' API.

    Shares the same return contract as the IBB connector so
    ``StrategyOnlinePaymentPush._send_payment_data_to_gateway`` can persist
    results uniformly. ``send_payment`` is pure I/O — it does NOT touch the
    DB, so the ThreadPoolExecutor in the strategy doesn't open an ORM
    connection per worker thread.
    """

    def _format_amount(self, amount):
        """Format amount to ####.## as required by the API."""
        return "{:.2f}".format(float(amount))

    def _generate_request_id(self, invoice_id):
        """Generate a request ID in the format PPPPyyMMddHHmmssfff.

        Lumicash's ``RequestId`` is partner-side and must be unique per call.
        We embed the last 4 chars of the invoice_id so a retry with the same
        ``invoice_id`` still collides deterministically at the millisecond
        granularity (same timestamp → same id).
        """
        prefix = self.config.partner_code[:4]
        now = datetime.datetime.now()
        timestamp = now.strftime("%y%m%d%H%M%S%f")[:15]  # first 15 digits
        invoice_suffix = str(invoice_id)[-4:] if len(str(invoice_id)) > 4 else str(invoice_id)
        timestamp = timestamp[:(15 - len(invoice_suffix))] + invoice_suffix
        return f"{prefix}{timestamp}"

    def _generate_request_date(self):
        """yyyyMMddHHmmssfff."""
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]

    def _generate_signature(self, request_date, amount, des_mobile, request_id):
        """MD5(Key + RequestDate + TransAmount.ToString("####.##") + PartnerCode + DesMobile + RequestId)."""
        raw = (
            self.config.api_key
            + request_date
            + self._format_amount(amount)
            + self.config.partner_code
            + des_mobile
            + request_id
        )
        return hashlib.md5(raw.encode()).hexdigest()

    def send_payment(self, invoice_id, amount, **kwargs):
        """Send payment via Lumicash API.

        **Pure I/O — does NOT touch the DB.** The caller persists the result
        on a single thread (see ``StrategyOnlinePaymentPush._send_payment_data_to_gateway``).

        Required kwargs:
            phone_number: Recipient's phone number.

        Optional kwargs:
            fee_amount: Agency fee the program pays on top of ``amount`` so the
                beneficiary nets the full ``amount`` after Lumicash deducts its
                fee. The connector sends ``TransAmount = amount + fee_amount``
                to Lumicash. ``benefit.amount`` itself stays at the net value
                so reporting and dashboards reflect what the beneficiary
                actually receives. If absent or 0, the gross equals the net.
            content: Payment content/description shown to the recipient.
            description: Free-text description, stored with the transaction.

        Returns:
            ``{'success': bool, 'data': <lumicash response dict or None>, 'error': str|None}``

            On success, ``data`` contains the full Lumicash response:
            ``ResponseCode`` (``"01"``), ``ResponseMessage`` (``"SUCCESS"``),
            ``RequestId``, ``TransCode``, ``TransDate``, ``TransAmount``,
            ``TransFee``. The strategy stores this whole dict under
            ``benefit.json_ext.payment_request``, and the ``TransCode`` is
            set as ``benefit.receipt`` for reconciliation lookups.
        """
        phone_number = kwargs.get('phone_number')
        if not phone_number:
            logger.error("Phone number is required for Lumicash payment %s", invoice_id)
            return {'success': False, 'data': None, 'error': 'phone_number_missing'}

        # Agency fee uplift: ``benefit.amount`` is the net amount the
        # beneficiary should receive; ``fee_amount`` (from json_ext, passed
        # by the strategy) is the agency fee from AgencyFeeConfig. We add
        # it to the transfer ONLY when ``fee_included`` is True, meaning the
        # program covers the fee on top of the net. When False, the fee is
        # informational and the beneficiary would absorb it from the gross
        # the gateway deducts.
        fee_amount = float(kwargs.get('fee_amount') or 0)
        fee_included = bool(kwargs.get('fee_included', False))
        transfer_amount = float(amount) + (fee_amount if fee_included else 0)

        request_id = self._generate_request_id(invoice_id)
        request_date = self._generate_request_date()
        # Signature must be over the actual TransAmount we send, not the net.
        signature = self._generate_signature(request_date, transfer_amount, phone_number, request_id)

        payload = {
            "RequestId": request_id,
            "RequestDate": request_date,
            "PartnerCode": self.config.partner_code,
            "DesMobile": phone_number,
            "TransAmount": int(transfer_amount),
            "Content": kwargs.get('content', f"Payment for invoice {invoice_id}"),
            "Description": kwargs.get('description', ''),
            "Signature": signature,
        }
        url = f"{self.config.gateway_base_url}/api/3rd/customer/transaction/payonbehalf"

        try:
            response = self.session.post(url, json=payload, timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()

            # ResponseCode "01" is the Lumicash SUCCESS sentinel.
            if data.get('ResponseCode') == "01":
                logger.info(
                    "Lumicash payment %s successful: TransCode=%s",
                    invoice_id, data.get('TransCode'),
                )
                return {'success': True, 'data': data, 'error': None}

            logger.error(
                "Lumicash payment %s failed: code=%s message=%s",
                invoice_id, data.get('ResponseCode'), data.get('ResponseMessage'),
            )
            return {
                'success': False,
                'data': data,
                'error': data.get('ResponseMessage') or f"ResponseCode={data.get('ResponseCode')}",
            }

        except requests.exceptions.RequestException as e:
            logger.error("Lumicash payment request failed for %s: %s", invoice_id, e)
            return {'success': False, 'data': None, 'error': str(e)}

    def reconcile(self, invoice_id, amount, **kwargs):
        # TODO: implement once Wallee delivers the transaction-status API
        # (mentioned 2026-04-23). Expected: POST with RequestId/TransCode,
        # response with same fields as send_payment — map status to
        # BenefitConsumptionStatus.RECONCILED and persist via _safe_save.
        pass
