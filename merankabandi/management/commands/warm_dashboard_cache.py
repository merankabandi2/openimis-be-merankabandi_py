from django.core.management.base import BaseCommand
from merankabandi.optimized_dashboard_service import OptimizedDashboardService


class Command(BaseCommand):
    help = 'Warm dashboard cache by refreshing materialized views'

    def handle(self, *args, **options):
        service = OptimizedDashboardService()
        service.refresh_views_if_needed()
        self.stdout.write(self.style.SUCCESS('Dashboard materialized views refreshed'))
