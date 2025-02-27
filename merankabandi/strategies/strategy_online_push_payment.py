import logging

from payroll.strategies.strategy_online_payment import StrategyOnlinePayment
from merankabandi.payment_gateway.payment_gateway_config import PaymentGatewayConfig

logger = logging.getLogger(__name__)


class StrategyOnlinePaymentPush(StrategyOnlinePayment):

    @classmethod
    def initialize_payment_gateway(cls, paymentpoint):
        gateway_config = PaymentGatewayConfig(paymentpoint)
        payment_gateway_connector_class = gateway_config.get_payment_gateway_connector()
        cls.PAYMENT_GATEWAY = payment_gateway_connector_class(paymentpoint)

    @classmethod
    def _send_payment_data_to_gateway(cls, payroll, user):
        from payroll.models import BenefitConsumptionStatus
        benefits = cls.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
        payment_gateway_connector = cls.PAYMENT_GATEWAY
        benefits_to_approve = []
        for benefit in benefits:
            if payment_gateway_connector.send_payment(benefit.code, benefit.amount, phone_number=benefit.json_ext.get('phoneNumber', ''), username=user.login_name):
                benefits_to_approve.append(benefit)
            else:
                # Handle the case where a benefit payment is rejected
                logger.info(f"Payment for benefit ({benefit.code}) was rejected.")
        if benefits_to_approve:
            cls.approve_for_payment_benefit_consumption(benefits_to_approve, user)


    @classmethod
    def _process_accepted_payroll(cls, payroll, user, **kwargs):
        from payroll.models import PayrollStatus
        from payroll.tasks import send_requests_to_gateway_payment
        cls.change_status_of_payroll(payroll, PayrollStatus.APPROVE_FOR_PAYMENT, user)
        send_requests_to_gateway_payment.delay(payroll.id, user.id)
