import logging
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth import get_user_model

from payroll.models import Payroll
from payroll.services import PayrollService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Trigger payment for a payroll by ID'

    def add_arguments(self, parser):
        parser.add_argument(
            'payroll_id',
            type=str,
            help='The UUID of the payroll to trigger payment for'
        )
        parser.add_argument(
            '--user',
            type=str,
            default='admin',
            help='Username of the user triggering the payment (default: admin)'
        )

    def handle(self, *args, **options):
        payroll_id = options['payroll_id']
        username = options['user']

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

        # Log payroll details
        self.stdout.write(self.style.NOTICE(f'Payroll ID: {payroll.id}'))
        self.stdout.write(self.style.NOTICE(f'Payroll Name: {payroll.name}'))
        self.stdout.write(self.style.NOTICE(f'Payroll Status: {payroll.status}'))
        self.stdout.write(self.style.NOTICE(f'Payment Point: {payroll.payment_point.name if payroll.payment_point else "N/A"}'))
        self.stdout.write(self.style.NOTICE(f'Payment Method: {payroll.payment_method}'))

        # Confirm before proceeding
        confirm = input(f'\nTrigger payment for payroll "{payroll.name}" (ID: {payroll.id})? [y/N]: ')
        if confirm.lower() not in ['y', 'yes']:
            self.stdout.write(self.style.WARNING('Payment trigger cancelled'))
            return

        # Trigger payment
        try:
            self.stdout.write(self.style.NOTICE('Triggering payment...'))
            payroll_service = PayrollService(user)
            payroll_service.make_payment_for_payroll({'id': payroll_id})

            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully triggered payment for payroll "{payroll.name}" (ID: {payroll.id}). '
                    f'Payment processing has been queued as a background task.'
                )
            )
            self.stdout.write(
                self.style.NOTICE(
                    'Note: The actual payment processing happens asynchronously. '
                    'Check the payroll status and logs for completion.'
                )
            )
        except Exception as e:
            logger.error(f"Error triggering payment for payroll {payroll_id}: {e}", exc_info=True)
            raise CommandError(f'Failed to trigger payment: {str(e)}')
