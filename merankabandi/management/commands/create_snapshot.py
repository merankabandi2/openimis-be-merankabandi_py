from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.utils import timezone
from datetime import datetime
from merankabandi.result_framework_service import ResultFrameworkService


User = get_user_model()


class Command(BaseCommand):
    help = 'Create a result framework snapshot'

    def add_arguments(self, parser):
        parser.add_argument(
            '--name',
            type=str,
            required=True,
            help='Name for the snapshot',
        )
        parser.add_argument(
            '--description',
            type=str,
            default='',
            help='Description for the snapshot',
        )
        parser.add_argument(
            '--user',
            type=str,
            help='Username of the user creating the snapshot (default: first superuser)',
        )
        parser.add_argument(
            '--date-from',
            type=str,
            help='Start date for the snapshot period (format: YYYY-MM-DD)',
        )
        parser.add_argument(
            '--date-to',
            type=str,
            help='End date for the snapshot period (format: YYYY-MM-DD)',
        )
        parser.add_argument(
            '--finalize',
            action='store_true',
            help='Set snapshot status to FINALIZED instead of DRAFT',
        )

    def handle(self, *args, **options):
        try:
            # Get or create user
            username = options.get('user')
            if username:
                try:
                    user = User.objects.get(username=username)
                except User.DoesNotExist:
                    self.stdout.write(
                        self.style.ERROR(f'User "{username}" not found')
                    )
                    return
            else:
                # Use first superuser
                user = User.objects.filter(is_superuser=True).first()
                if not user:
                    self.stdout.write(
                        self.style.ERROR('No superuser found. Please create a superuser first.')
                    )
                    return
                self.stdout.write(f'Using user: {user.username}')

            # Parse dates
            date_from = None
            date_to = None

            if options.get('date_from'):
                try:
                    date_from = datetime.strptime(options['date_from'], '%Y-%m-%d').date()
                    self.stdout.write(f'Date from: {date_from}')
                except ValueError:
                    self.stdout.write(
                        self.style.ERROR('Invalid date_from format. Use YYYY-MM-DD')
                    )
                    return

            if options.get('date_to'):
                try:
                    date_to = datetime.strptime(options['date_to'], '%Y-%m-%d').date()
                    self.stdout.write(f'Date to: {date_to}')
                except ValueError:
                    self.stdout.write(
                        self.style.ERROR('Invalid date_to format. Use YYYY-MM-DD')
                    )
                    return

            # Create snapshot
            self.stdout.write('Creating snapshot...')
            service = ResultFrameworkService()

            snapshot = service.create_snapshot(
                name=options['name'],
                description=options['description'],
                user=user,
                date_from=date_from,
                date_to=date_to
            )

            # Update status if finalized
            if options.get('finalize'):
                snapshot.status = 'FINALIZED'
                snapshot.save()
                self.stdout.write('Snapshot status set to FINALIZED')

            # Display summary
            self.stdout.write(
                self.style.SUCCESS(
                    f'\nSuccessfully created snapshot:'
                )
            )
            self.stdout.write(f'  ID: {snapshot.id}')
            self.stdout.write(f'  Name: {snapshot.name}')
            self.stdout.write(f'  Description: {snapshot.description or "(none)"}')
            self.stdout.write(f'  Status: {snapshot.status}')
            self.stdout.write(f'  Created by: {snapshot.created_by.username}')
            self.stdout.write(f'  Created at: {snapshot.snapshot_date}')

            if date_from or date_to:
                period = []
                if date_from:
                    period.append(f'from {date_from}')
                if date_to:
                    period.append(f'to {date_to}')
                self.stdout.write(f'  Period: {" ".join(period)}')

            # Display data summary
            data = snapshot.data
            sections_count = len(data.get('sections', []))
            indicators_count = sum(
                len(section.get('indicators', []))
                for section in data.get('sections', [])
            )

            self.stdout.write(f'\nSnapshot contains:')
            self.stdout.write(f'  Sections: {sections_count}')
            self.stdout.write(f'  Indicators: {indicators_count}')

            # Show indicators by section
            if sections_count > 0:
                self.stdout.write('\nIndicators by section:')
                for section in data.get('sections', []):
                    section_name = section.get('name', 'Unknown')
                    indicator_count = len(section.get('indicators', []))
                    self.stdout.write(f'  - {section_name}: {indicator_count} indicators')

            # Show completion statistics
            total_indicators = 0
            completed_80_plus = 0
            completed_50_plus = 0
            completed_below_50 = 0

            for section in data.get('sections', []):
                for indicator in section.get('indicators', []):
                    total_indicators += 1
                    percentage = indicator.get('percentage', 0)
                    if percentage >= 80:
                        completed_80_plus += 1
                    elif percentage >= 50:
                        completed_50_plus += 1
                    else:
                        completed_below_50 += 1

            if total_indicators > 0:
                self.stdout.write('\nCompletion statistics:')
                self.stdout.write(f'  ≥80% completion: {completed_80_plus} indicators')
                self.stdout.write(f'  50-79% completion: {completed_50_plus} indicators')
                self.stdout.write(f'  <50% completion: {completed_below_50} indicators')

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error creating snapshot: {str(e)}')
            )
            import traceback
            self.stdout.write(traceback.format_exc())
