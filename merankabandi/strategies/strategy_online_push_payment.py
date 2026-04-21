import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from payroll.strategies.strategy_online_payment import StrategyOnlinePayment
from merankabandi.payment_gateway.payment_gateway_config import PaymentGatewayConfig
from merankabandi.payment_gateway.source_resolver import resolve_gateway_source

logger = logging.getLogger(__name__)


class StrategyOnlinePaymentPush(StrategyOnlinePayment):
    # Initial source supplied by the upstream task (typically payroll.payment_point).
    # May be None for Mera-flow payrolls; in that case we re-resolve in
    # make_payment_for_payroll once we have payroll context.
    _initial_source = None

    @classmethod
    def initialize_payment_gateway(cls, source):
        """Lazy-init: stash the source so we can re-resolve from payroll context.

        The upstream task at payroll/tasks.py:34 calls
        ``strategy.initialize_payment_gateway(payroll.payment_point)`` — for
        Mera payrolls that's None, so building the connector here would crash.
        We defer the real init to ``make_payment_for_payroll`` where the
        payroll object is available and we can resolve a PaymentAgency.
        """
        cls._initial_source = source
        if source is None:
            cls.PAYMENT_GATEWAY = None
            return
        try:
            gateway_config = PaymentGatewayConfig(source)
            connector_cls = gateway_config.get_payment_gateway_connector()
            cls.PAYMENT_GATEWAY = connector_cls(source)
        except (ValueError, ImportError) as exc:
            # Don't fail the task setup; let make_payment_for_payroll re-resolve.
            logger.info(
                "Eager gateway init failed for source=%r (%s); will re-resolve "
                "from payroll context",
                source, exc,
            )
            cls.PAYMENT_GATEWAY = None

    @classmethod
    def make_payment_for_payroll(cls, payroll, user, **kwargs):
        # Re-resolve the gateway from payroll context. For Mera-flow payrolls
        # (PaymentAgency in json_ext, no payment_point) this is the only path
        # that produces a usable connector.
        if cls.PAYMENT_GATEWAY is None or resolve_gateway_source(payroll) is not cls._initial_source:
            source = resolve_gateway_source(payroll)
            if source is None:
                logger.error(
                    "Cannot dispatch payments for payroll %s: no PaymentAgency "
                    "(json_ext.agency_code=%r) and no payment_point.",
                    payroll.id, (payroll.json_ext or {}).get('agency_code'),
                )
                return
            gateway_config = PaymentGatewayConfig(source)
            connector_cls = gateway_config.get_payment_gateway_connector()
            cls.PAYMENT_GATEWAY = connector_cls(source)

        cls._send_payment_data_to_gateway(payroll, user)

    @classmethod
    def _send_payment_data_to_gateway(cls, payroll, user):
        from payroll.models import BenefitConsumptionStatus
        benefits = cls.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
        payment_gateway_connector = cls.PAYMENT_GATEWAY
        benefits_to_approve = []

        # Process benefits in batches with parallel requests
        # Adjust based on server capacity and payment gateway limits
        batch_size = 10
        total_benefits = len(benefits)

        def send_single_payment(benefit):
            """Helper function to send a single payment"""
            try:
                success = payment_gateway_connector.send_payment(
                    benefit.code,
                    benefit.amount,
                    phone_number=benefit.json_ext.get('phoneNumber', ''),
                    username=user.login_name
                )
                return (benefit, success)
            except Exception as e:
                logger.error(f"Error sending payment for benefit ({benefit.code}): {e}")
                return (benefit, False)

        # Process benefits in batches
        for i in range(0, total_benefits, batch_size):
            batch = benefits[i:i + batch_size]
            logger.info(
                f"Processing batch {i // batch_size + 1} of {(total_benefits + batch_size - 1) // batch_size} ({len(batch)}/{total_benefits} benefits)")

            # Refresh token once before processing the entire batch
            # This ensures all parallel requests in the batch use the same valid token
            if hasattr(payment_gateway_connector, '_refresh_token_if_needed'):
                payment_gateway_connector._refresh_token_if_needed()
                logger.info(f"Token refreshed for batch {i // batch_size + 1}")

            # Use ThreadPoolExecutor to parallelize requests within the batch
            with ThreadPoolExecutor(max_workers=min(batch_size, len(batch))) as executor:
                # Submit all payment requests in the batch
                future_to_benefit = {executor.submit(send_single_payment, benefit): benefit for benefit in batch}

                # Collect results as they complete
                for future in as_completed(future_to_benefit):
                    benefit, success = future.result()
                    if success:
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

    @classmethod
    def reconcile_benefit_consumption(cls, benefits, user):
        from payroll.models import BenefitConsumptionStatus
        for benefit in benefits:
            try:
                benefit.status = BenefitConsumptionStatus.RECONCILED
                benefit.save(username=user.login_name)
            except Exception as e:
                logger.debug(f"Failed to approve benefit consumption {benefit.code}: {str(e)}")
