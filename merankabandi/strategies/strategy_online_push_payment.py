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
        """Dispatch IBB payments in parallel batches, persist results at the end.

        DB writes happen ONLY in the main thread after all I/O completes —
        this avoids opening a Django ORM connection from each worker thread,
        which under load exhausts Postgres' max_connections ("too many
        clients already").
        """
        from payroll.models import BenefitConsumptionStatus
        benefits = cls.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
        payment_gateway_connector = cls.PAYMENT_GATEWAY

        batch_size = 10
        total_benefits = len(benefits)

        # Each entry: code -> {'success': bool, 'data': dict|None, 'error': str|None}
        results = {}

        def send_single(benefit):
            try:
                je = benefit.json_ext or {}
                # Agency fee handling: the connector adds fee_amount on top of
                # benefit.amount only when fee_included is True (program pays
                # the fee). fee_amount and fee_included come from json_ext,
                # populated by the calc rule from AgencyFeeConfig.
                # benefit.amount itself stays at the NET value for reporting.
                return benefit.code, payment_gateway_connector.send_payment(
                    benefit.code,
                    benefit.amount,
                    phone_number=je.get('phoneNumber', ''),
                    fee_amount=je.get('fee_amount', 0),
                    fee_included=je.get('fee_included', False),
                    username=user.login_name,
                )
            except Exception as e:
                logger.error("Error sending payment for benefit %s: %s", benefit.code, e)
                return benefit.code, {'success': False, 'data': None, 'error': str(e)}

        for i in range(0, total_benefits, batch_size):
            batch = benefits[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_benefits + batch_size - 1) // batch_size
            logger.info(
                "Processing batch %d of %d (%d/%d benefits)",
                batch_num, total_batches, len(batch), total_benefits,
            )

            if hasattr(payment_gateway_connector, '_refresh_token_if_needed'):
                payment_gateway_connector._refresh_token_if_needed()
                logger.info("Token refreshed for batch %d", batch_num)

            with ThreadPoolExecutor(max_workers=min(batch_size, len(batch))) as executor:
                future_map = {executor.submit(send_single, b): b for b in batch}
                for future in as_completed(future_map):
                    code, result = future.result()
                    results[code] = result

        # Persist results on the main thread — single Django ORM connection,
        # one autocommit per save. No atomic wrapping: an IBB call already
        # happened, so a save failure on one benefit must not roll back saves
        # for benefits whose payments did succeed.
        benefits_to_approve = []
        for benefit in benefits:
            result = results.get(benefit.code)
            if result is None:
                continue
            gateway_data = result.get('data')
            if gateway_data:
                if benefit.json_ext is None:
                    benefit.json_ext = {}
                benefit.json_ext['payment_request'] = gateway_data
            if result['success']:
                if gateway_data:
                    # Gateway-agnostic receipt extraction: IBB stores the
                    # provider-side transaction id under 'ibbTransactionID',
                    # Lumicash under 'TransCode'. Both are the lookup key
                    # used later by reconcile(), so whichever the connector
                    # returns becomes benefit.receipt.
                    benefit.receipt = (
                        gateway_data.get('ibbTransactionID')
                        or gateway_data.get('TransCode')
                    )
                benefits_to_approve.append(benefit)
            try:
                benefit.save(username=user.login_name)
            except Exception as e:
                logger.error("Failed to persist benefit %s: %s", benefit.code, e)

        if benefits_to_approve:
            cls.approve_for_payment_benefit_consumption(benefits_to_approve, user)

        success_count = sum(1 for r in results.values() if r.get('success'))
        logger.info(
            "Payroll %s: dispatched %d benefits, %d succeeded, %d failed",
            payroll.id, len(results), success_count, len(results) - success_count,
        )

    @classmethod
    def _process_accepted_payroll(cls, payroll, user, **kwargs):
        from payroll.models import PayrollStatus
        from payroll.tasks import send_requests_to_gateway_payment
        cls.change_status_of_payroll(payroll, PayrollStatus.APPROVE_FOR_PAYMENT, user)
        send_requests_to_gateway_payment.delay(payroll.id, user.id)

    @classmethod
    def reconcile_payroll(cls, payroll, user):
        """Route full reconciliation through the Mera-flow task.

        Upstream's ``payroll.tasks.send_request_to_reconcile`` passes
        ``payroll.payment_point`` directly to ``initialize_payment_gateway`` —
        None for Mera payrolls (agency lives in ``json_ext.agency_code``),
        so that path crashes before any benefit work. We substitute our task
        which routes through ``resolve_gateway_source``.
        """
        from merankabandi.tasks import send_full_reconciliation
        send_full_reconciliation.delay(payroll.id, user.id)

    @classmethod
    def reconcile_benefit_consumption(cls, benefits, user):
        from payroll.models import BenefitConsumptionStatus
        for benefit in benefits:
            try:
                benefit.status = BenefitConsumptionStatus.RECONCILED
                benefit.save(username=user.login_name)
            except Exception as e:
                logger.debug(f"Failed to approve benefit consumption {benefit.code}: {str(e)}")
