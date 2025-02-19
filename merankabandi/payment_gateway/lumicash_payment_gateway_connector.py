from payroll.payment_gateway.payment_gateway_connector import PaymentGatewayConnector


class LumicashPaymentGatewayConnector(PaymentGatewayConnector):
    def send_payment(self, invoice_id, amount, **kwargs):
        payload = {"invoiceId": str(invoice_id), "amount": str(amount)}
        response = self.send_request(self.config.endpoint_payment, payload)
        if response:
            response_text = response.text
            expected_message = f"{invoice_id} invoice of {amount} accepted to be paid"
            if response_text == expected_message:
                return True
        return False

    def reconcile(self, invoice_id, amount, **kwargs):
        payload = {"invoiceId": str(invoice_id), "amount": str(amount)}
        response = self.send_request(self.config.endpoint_reconciliation, payload)
        if response:
            response_text = response.text.strip().lower()
            if response_text == "true":
                return True
            elif response_text == "false":
                return False
        return False
