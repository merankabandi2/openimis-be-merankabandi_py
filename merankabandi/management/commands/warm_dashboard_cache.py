from django.core.management.base import BaseCommand
from django.core.cache import cache
from merankabandi.cached_dashboard_views import warm_dashboard_cache
import time

class Command(BaseCommand):
    help = 'Warms up dashboard cache for optimal performance'

    def add_arguments(self, parser):
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing cache before warming',
        )
        parser.add_argument(
            '--continuous',
            action='store_true',
            help='Run continuously, refreshing cache periodically',
        )
        parser.add_argument(
            '--interval',
            type=int,
            default=300,
            help='Interval in seconds for continuous mode (default: 300)',
        )

    def handle(self, *args, **options):
        if options['clear']:
            self.stdout.write('Clearing existing cache...')
            cache.clear()
            self.stdout.write(self.style.SUCCESS('Cache cleared'))

        if options['continuous']:
            self.stdout.write('Starting continuous cache warming...')
            while True:
                self._warm_cache()
                self.stdout.write(f'Sleeping for {options["interval"]} seconds...')
                time.sleep(options['interval'])
        else:
            self._warm_cache()

    def _warm_cache(self):
        self.stdout.write('Warming dashboard cache...')
        start_time = time.time()
        
        try:
            warm_dashboard_cache()
            elapsed = time.time() - start_time
            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully warmed cache in {elapsed:.2f} seconds'
                )
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error warming cache: {str(e)}')
            )