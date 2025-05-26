from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import transaction
import os


class Command(BaseCommand):
    help = 'Load development and intermediate indicators sections and indicators'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing sections and indicators before loading',
        )
        parser.add_argument(
            '--with-achievements',
            action='store_true',
            help='Also load sample achievements data',
        )

    def handle(self, *args, **options):
        try:
            with transaction.atomic():
                if options['clear']:
                    self.stdout.write('Clearing existing data...')
                    from merankabandi.models import Section, Indicator, IndicatorAchievement
                    
                    # Delete in correct order to respect foreign keys
                    IndicatorAchievement.objects.all().delete()
                    Indicator.objects.all().delete()
                    Section.objects.all().delete()
                    
                    self.stdout.write(self.style.SUCCESS('Existing data cleared'))

                # Load the fixture
                fixture_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                    'fixtures',
                    'development_intermediate_indicators_simple.json'
                )
                
                self.stdout.write(f'Loading fixture from {fixture_path}...')
                call_command('loaddata', fixture_path)
                
                # Load achievements if requested
                if options['with_achievements']:
                    achievements_fixture_path = os.path.join(
                        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                        'fixtures',
                        'sample_achievements_simple.json'
                    )
                    self.stdout.write(f'Loading achievements from {achievements_fixture_path}...')
                    call_command('loaddata', achievements_fixture_path)
                
                # Display summary
                from merankabandi.models import Section, Indicator, IndicatorAchievement
                sections_count = Section.objects.count()
                indicators_count = Indicator.objects.count()
                achievements_count = IndicatorAchievement.objects.count() if options['with_achievements'] else 0
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f'Successfully loaded {sections_count} sections and {indicators_count} indicators'
                        + (f' and {achievements_count} achievements' if options['with_achievements'] else '')
                    )
                )
                
                # Show breakdown by section
                self.stdout.write('\nIndicators by section:')
                for section in Section.objects.all():
                    indicator_count = section.indicators.count()
                    self.stdout.write(f'  - {section.name}: {indicator_count} indicators')
                
                # Show indicators with achievements if loaded
                if options['with_achievements'] and achievements_count > 0:
                    self.stdout.write('\nIndicators with achievements:')
                    for indicator in Indicator.objects.filter(achievements__isnull=False).distinct():
                        latest_achievement = indicator.achievements.latest('date')
                        self.stdout.write(
                            f'  - {indicator.name[:50]}...: {latest_achievement.achieved} '
                            f'(as of {latest_achievement.date})'
                        )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error loading indicators: {str(e)}')
            )