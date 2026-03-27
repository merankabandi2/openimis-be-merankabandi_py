from django.core.management.base import BaseCommand
from merankabandi.dashboard_service import DashboardService


class Command(BaseCommand):
    help = 'Warm dashboard cache by refreshing materialized views'

    def handle(self, *args, **options):
        DashboardService.refresh_views_if_needed()
        self.stdout.write(self.style.SUCCESS('Dashboard materialized views refreshed'))
