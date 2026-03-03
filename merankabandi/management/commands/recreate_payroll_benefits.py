import logging
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth import get_user_model
from django.db import transaction

from individual.models import GroupIndividual
from payroll.models import (
    Payroll,
    PayrollStatus,
    PayrollBenefitConsumption,
    BenefitConsumption,
    BenefitConsumptionStatus,
)
from payroll.services import PayrollService
from social_protection.models import GroupBeneficiary, BeneficiaryStatus

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        'Create a new payroll from an existing one, generating fresh benefits '
        'for the same group beneficiaries instead of moving the existing ones.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            'from_payroll_id',
            type=str,
            help='The UUID of the source payroll to copy beneficiaries from',
        )
        parser.add_argument(
            'new_payroll_name',
            type=str,
            help='Name for the new payroll',
        )
        parser.add_argument(
            '--user',
            type=str,
            default='admin',
            help='Username of the user performing the operation (default: admin)',
        )
        parser.add_argument(
            '--status-filter',
            type=str,
            nargs='+',
            choices=[BenefitConsumptionStatus.ACCEPTED, BenefitConsumptionStatus.APPROVE_FOR_PAYMENT, BenefitConsumptionStatus.REJECTED],
            default=[BenefitConsumptionStatus.REJECTED],
            help='Only include benefits with these statuses from the source payroll (default: REJECTED)',
        )
        parser.add_argument(
            '--yes',
            action='store_true',
            help='Skip confirmation prompt',
        )

    def _get_group_beneficiaries_from_payroll(self, source_payroll, status_filter):
        """Trace back from source payroll benefits to GroupBeneficiary queryset.

        BenefitConsumption.individual → GroupIndividual.group → GroupBeneficiary
        """
        individual_ids = (
            BenefitConsumption.objects.filter(
                payrollbenefitconsumption__payroll=source_payroll,
                payrollbenefitconsumption__is_deleted=False,
                status__in=status_filter,
                is_deleted=False,
            )
            .values_list('individual_id', flat=True)
            .distinct()
        )

        group_ids = (
            GroupIndividual.objects.filter(
                individual_id__in=individual_ids,
                is_deleted=False,
            )
            .values_list('group_id', flat=True)
            .distinct()
        )

        return GroupBeneficiary.objects.filter(
            group_id__in=group_ids,
            benefit_plan=source_payroll.payment_plan.benefit_plan,
            status=BeneficiaryStatus.ACTIVE,
            is_deleted=False,
        )

    def handle(self, *args, **options):
        from_payroll_id = options['from_payroll_id']
        new_payroll_name = options['new_payroll_name']
        username = options['user']
        status_filter = options['status_filter']
        skip_confirm = options['yes']

        # Resolve user
        User = get_user_model()
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            raise CommandError(f'User "{username}" does not exist')

        # Resolve source payroll
        try:
            source_payroll = Payroll.objects.get(id=from_payroll_id, is_deleted=False)
        except Payroll.DoesNotExist:
            raise CommandError(f'Payroll with ID "{from_payroll_id}" does not exist or is deleted')

        if not source_payroll.payment_plan:
            raise CommandError('Source payroll has no payment plan — cannot generate benefits')
        if not source_payroll.payment_cycle:
            raise CommandError('Source payroll has no payment cycle — cannot generate benefits')

        # Trace back: benefits → individuals → groups → GroupBeneficiary
        group_beneficiaries_qs = self._get_group_beneficiaries_from_payroll(source_payroll, status_filter)
        group_beneficiary_count = group_beneficiaries_qs.count()

        if group_beneficiary_count == 0:
            raise CommandError(
                'No active group beneficiaries found from the source payroll benefits'
            )

        # Display summary
        self.stdout.write(self.style.NOTICE('--- Source Payroll ---'))
        self.stdout.write(f'  ID:             {source_payroll.id}')
        self.stdout.write(f'  Name:           {source_payroll.name}')
        self.stdout.write(f'  Status:         {source_payroll.status}')
        self.stdout.write(f'  Payment Plan:   {source_payroll.payment_plan}')
        self.stdout.write(f'  Payment Cycle:  {source_payroll.payment_cycle}')
        self.stdout.write(f'  Payment Point:  {source_payroll.payment_point.name if source_payroll.payment_point else "N/A"}')
        self.stdout.write(f'  Payment Method: {source_payroll.payment_method or "N/A"}')
        self.stdout.write('')
        self.stdout.write(self.style.NOTICE('--- New Payroll ---'))
        self.stdout.write(f'  Name:              {new_payroll_name}')
        self.stdout.write(f'  Group Beneficiaries: {group_beneficiary_count}')
        self.stdout.write('')

        if not skip_confirm:
            confirm = input(
                f'Create new payroll "{new_payroll_name}" with {group_beneficiary_count} group beneficiaries? [y/N]: '
            )
            if confirm.lower() not in ('y', 'yes'):
                self.stdout.write(self.style.WARNING('Operation cancelled'))
                return

        # Create new payroll and generate fresh benefits via PayrollService
        try:
            with transaction.atomic():
                new_payroll = Payroll(
                    name=new_payroll_name,
                    payment_plan=source_payroll.payment_plan,
                    payment_cycle=source_payroll.payment_cycle,
                    payment_point=source_payroll.payment_point,
                    payment_method=source_payroll.payment_method,
                    status=PayrollStatus.PENDING_VERIFICATION,
                    json_ext={
                        'source_payroll_id': str(source_payroll.id),
                        'recreated': True,
                    },
                )
                new_payroll.save(username=username)

                self.stdout.write(self.style.NOTICE(f'Created payroll {new_payroll.id}, generating benefits...'))

                payment_plan = source_payroll.payment_plan
                payment_cycle = source_payroll.payment_cycle

                # Reuse PayrollService._generate_benefits
                payroll_service = PayrollService(user)
                payroll_service._generate_benefits(
                    payment_plan,
                    group_beneficiaries_qs,
                    payment_cycle.start_date,
                    payment_cycle.end_date,
                    new_payroll,
                    payment_cycle,
                )

                new_benefit_count = PayrollBenefitConsumption.objects.filter(
                    payroll=new_payroll, is_deleted=False
                ).count()

                # Enter the normal task workflow: verify → accept
                payroll_service.create_verify_payroll_task(new_payroll.id, {
                    'id': str(new_payroll.id),
                    'name': new_payroll.name,
                    'payment_plan_id': str(payment_plan.id),
                    'payment_cycle_id': str(payment_cycle.id),
                })

            self.stdout.write(self.style.SUCCESS(
                f'Successfully created payroll "{new_payroll_name}" (ID: {new_payroll.id}) '
                f'with {new_benefit_count} new benefits for {group_beneficiary_count} group beneficiaries.'
            ))
        except Exception as e:
            logger.error(f"Error creating payroll from {from_payroll_id}: {e}", exc_info=True)
            raise CommandError(f'Failed to create payroll: {e}')
