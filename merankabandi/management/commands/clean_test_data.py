from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        'Remove test/placeholder Section and Indicator records. '
        'Deletes Sections whose name contains "Test" (case-insensitive) '
        'and Indicators whose name contains "Delete Me" (case-insensitive).'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting',
        )

    def handle(self, *args, **options):
        from merankabandi.models import Section, Indicator

        dry_run = options['dry_run']

        test_sections = Section.objects.filter(name__icontains='Test')
        delete_me_indicators = Indicator.objects.filter(name__icontains='Delete Me')

        self.stdout.write(f'Sections matching "Test": {test_sections.count()}')
        for s in test_sections:
            self.stdout.write(f'  - Section #{s.pk}: {s.name}')

        self.stdout.write(f'Indicators matching "Delete Me": {delete_me_indicators.count()}')
        for i in delete_me_indicators:
            self.stdout.write(f'  - Indicator #{i.pk}: {i.name}')

        if dry_run:
            self.stdout.write(self.style.WARNING('Dry run -- nothing deleted.'))
            return

        deleted_i = delete_me_indicators.delete()
        deleted_s = test_sections.delete()

        self.stdout.write(self.style.SUCCESS(
            f'Cleaned: Sections {deleted_s}, Indicators {deleted_i}'
        ))
