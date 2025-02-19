import logging

from django.db.models import Q, Sum
from django.db import transaction

from core.signals import register_service_signal
from payroll.strategies.strategy_online_payment import StrategyOnlinePayment
from payroll.utils import CodeGenerator

logger = logging.getLogger(__name__)


class StrategyOnlinePaymentPull(StrategyOnlinePayment):
    WORKFLOW_NAME = "payment-adaptor"
    WORKFLOW_GROUP = "openimis-coremis-payment-adaptor"
    PAYMENT_GATEWAY = None

    @classmethod
    def initialize_payment_gateway(cls):
        from payroll.payment_gateway import PaymentGatewayConfig
        gateway_config = PaymentGatewayConfig()
        payment_gateway_connector_class = gateway_config.get_payment_gateway_connector()
        cls.PAYMENT_GATEWAY = payment_gateway_connector_class()

    @classmethod
    def make_payment_for_payroll(cls, payroll, user, **kwargs):
        cls._send_payment_data_to_gateway(payroll, user)

    @classmethod
    def _send_payment_data_to_gateway(cls, payroll, user):
        from payroll.models import BenefitConsumptionStatus
        benefits = cls.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
        payment_gateway_connector = cls.PAYMENT_GATEWAY
        benefits_to_approve = []
        for benefit in benefits:
            if payment_gateway_connector.send_payment(benefit.code, benefit.amount):
                benefits_to_approve.append(benefit)
            else:
                # Handle the case where a benefit payment is rejected
                logger.info(f"Payment for benefit ({benefit.code}) was rejected.")
        if benefits_to_approve:
            cls.approve_for_payment_benefit_consumption(benefits_to_approve, user)
