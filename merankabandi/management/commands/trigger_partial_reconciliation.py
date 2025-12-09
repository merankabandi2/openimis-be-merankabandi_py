import logging
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth import get_user_model

from payroll.models import Payroll, BenefitConsumptionStatus
from payroll.tasks import send_partial_reconciliation

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Trigger partial reconciliation for a payroll by ID'

    def add_arguments(self, parser):
        parser.add_argument(
            'payroll_id',
            type=str,
            help='The UUID of the payroll to reconcile'
        )
        parser.add_argument(
            '--user',
            type=str,
            default='admin',
            help='Username of the user triggering the reconciliation (default: admin)'
        )
        parser.add_argument(
            '--no-confirm',
            action='store_true',
            help='Skip confirmation prompt'
        )

    def handle(self, *args, **options):
        payroll_id = options['payroll_id']
        username = options['user']
        skip_confirm = options['no_confirm']

        # Get the user
        User = get_user_model()
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            raise CommandError(f'User "{username}" does not exist')

        # Get the payroll
        try:
            payroll = Payroll.objects.get(id=payroll_id, is_deleted=False)
        except Payroll.DoesNotExist:
            raise CommandError(f'Payroll with ID "{payroll_id}" does not exist or is deleted')

        # Count benefits to reconcile
        from payroll.payments_registry import PaymentMethodStorage
        strategy = PaymentMethodStorage.get_chosen_payment_method(payroll.payment_method)

        if not strategy:
            raise CommandError(f'No payment strategy found for payment method "{payroll.payment_method}"')

        # Check if reconciliation is supported
        if not strategy.reconcile_payroll or strategy.reconcile_payroll.__code__.co_code == (lambda: None).__code__.co_code:
            raise CommandError(
                f'Reconciliation is not supported for payment method "{payroll.payment_method}"'
            )

        # Get benefits count
        benefits = strategy.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
        benefits_count = benefits.count()

        # Log payroll details
        self.stdout.write(self.style.NOTICE(f'Payroll ID: {payroll.id}'))
        self.stdout.write(self.style.NOTICE(f'Payroll Name: {payroll.name}'))
        self.stdout.write(self.style.NOTICE(f'Payroll Status: {payroll.status}'))
        self.stdout.write(self.style.NOTICE(f'Payment Point: {payroll.payment_point.name if payroll.payment_point else "N/A"}'))
        self.stdout.write(self.style.NOTICE(f'Payment Method: {payroll.payment_method}'))
        self.stdout.write(self.style.NOTICE(f'Benefits to reconcile (ACCEPTED status): {benefits_count}'))

        if benefits_count == 0:
            self.stdout.write(
                self.style.WARNING(
                    f'No benefits with ACCEPTED status found for payroll "{payroll.name}". '
                    f'Partial reconciliation requires benefits in ACCEPTED status.'
                )
            )
            return

        # Confirm before proceeding
        if not skip_confirm:
            confirm = input(
                f'\nTrigger partial reconciliation for payroll "{payroll.name}" '
                f'(ID: {payroll.id}, {benefits_count} benefit(s))? [y/N]: '
            )
            if confirm.lower() not in ['y', 'yes']:
                self.stdout.write(self.style.WARNING('Partial reconciliation cancelled'))
                return

        # Trigger partial reconciliation
        try:
            self.stdout.write(self.style.NOTICE('Triggering partial reconciliation...'))
            send_partial_reconciliation.delay(payroll_id, user.id)

            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully triggered partial reconciliation for payroll "{payroll.name}" (ID: {payroll.id}). '
                    f'Reconciliation processing has been queued as a background task.'
                )
            )
            self.stdout.write(
                self.style.NOTICE(
                    f'The task will attempt to reconcile {benefits_count} benefit(s) with ACCEPTED status.\n'
                    f'Note: The actual reconciliation processing happens asynchronously. '
                    f'Check the payroll status and logs for completion.'
                )
            )
        except Exception as e:
            logger.error(f"Error triggering partial reconciliation for payroll {payroll_id}: {e}", exc_info=True)
            raise CommandError(f'Failed to trigger partial reconciliation: {str(e)}')
