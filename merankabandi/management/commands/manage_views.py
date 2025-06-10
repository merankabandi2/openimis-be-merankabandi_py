"""
Unified Management Command for All Materialized Views
Single entry point for creating, refreshing, and managing all dashboard views
"""

from django.core.management.base import BaseCommand
from django.db import connection
import time
import logging
from ...views_manager import MaterializedViewsManager

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Unified management command for all materialized views'

    def add_arguments(self, parser):
        parser.add_argument(
            '--action',
            type=str,
            choices=['create', 'refresh', 'drop', 'stats'],
            default='create',
            help='Action to perform on views (default: create)'
        )
        parser.add_argument(
            '--category',
            type=str,
            choices=['beneficiary', 'grievance', 'payment', 'monitoring', 'utility', 'all'],
            default='all',
            help='Category of views to manage (default: all)'
        )
        parser.add_argument(
            '--view',
            type=str,
            help='Specific view name to manage'
        )
        parser.add_argument(
            '--concurrent',
            action='store_true',
            default=True,
            help='Use concurrent refresh (default: True, requires unique indexes)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without executing'
        )

    def handle(self, *args, **options):
        action = options['action']
        category = options['category'] if options['category'] != 'all' else None
        view_name = options.get('view')
        concurrent = options['concurrent']
        dry_run = options['dry_run']

        self.stdout.write(f"=== Materialized Views Manager ===")
        self.stdout.write(f"Action: {action}")
        self.stdout.write(f"Category: {category or 'all'}")
        self.stdout.write(f"View: {view_name or 'all'}")
        self.stdout.write(f"Concurrent: {concurrent}")
        self.stdout.write(f"Dry Run: {dry_run}")
        self.stdout.write("=" * 50)

        if dry_run:
            self.show_dry_run_info(action, category, view_name)
            return

        start_time = time.time()

        try:
            if action == 'create':
                self.handle_create(category, view_name)
            elif action == 'refresh':
                self.handle_refresh(category, view_name, concurrent)
            elif action == 'drop':
                self.handle_drop(category, view_name)
            elif action == 'stats':
                self.handle_stats(category, view_name)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"✗ Command failed: {str(e)}"))
            logger.error(f"Materialized views command failed: {str(e)}")
            return

        elapsed = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(f"\n✓ Command completed in {elapsed:.2f} seconds"))

    def handle_create(self, category, view_name):
        """Handle view creation"""
        self.stdout.write("Creating materialized views...")
        
        if view_name:
            success = MaterializedViewsManager.create_single_view(view_name)
            if success:
                self.stdout.write(self.style.SUCCESS(f"✓ Created view: {view_name}"))
            else:
                self.stdout.write(self.style.ERROR(f"✗ Failed to create view: {view_name}"))
        else:
            results = MaterializedViewsManager.create_all_views(category)
            
            successful = sum(1 for success in results.values() if success)
            failed = sum(1 for success in results.values() if not success)
            
            self.stdout.write(f"\nResults:")
            for view_name, success in results.items():
                status = "✓" if success else "✗"
                style = self.style.SUCCESS if success else self.style.ERROR
                self.stdout.write(style(f"  {status} {view_name}"))
            
            self.stdout.write(f"\nSummary: {successful} created, {failed} failed")

    def handle_refresh(self, category, view_name, concurrent):
        """Handle view refresh"""
        self.stdout.write("Refreshing materialized views...")
        
        if view_name:
            success = MaterializedViewsManager.refresh_single_view(view_name, concurrent)
            if success:
                self.stdout.write(self.style.SUCCESS(f"✓ Refreshed view: {view_name}"))
            else:
                self.stdout.write(self.style.ERROR(f"✗ Failed to refresh view: {view_name}"))
        else:
            results = MaterializedViewsManager.refresh_all_views(category, concurrent)
            
            successful = sum(1 for success in results.values() if success)
            failed = sum(1 for success in results.values() if not success)
            
            self.stdout.write(f"\nResults:")
            for view_name, success in results.items():
                status = "✓" if success else "✗"
                style = self.style.SUCCESS if success else self.style.ERROR
                self.stdout.write(style(f"  {status} {view_name}"))
            
            self.stdout.write(f"\nSummary: {successful} refreshed, {failed} failed")

    def handle_drop(self, category, view_name):
        """Handle view dropping"""
        self.stdout.write(self.style.WARNING("Dropping materialized views..."))
        
        if view_name:
            # Drop single view
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                self.stdout.write(self.style.SUCCESS(f"✓ Dropped view: {view_name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"✗ Failed to drop view {view_name}: {str(e)}"))
        else:
            results = MaterializedViewsManager.drop_all_views(category)
            
            successful = sum(1 for success in results.values() if success)
            failed = sum(1 for success in results.values() if not success)
            
            self.stdout.write(f"\nResults:")
            for view_name, success in results.items():
                status = "✓" if success else "✗"
                style = self.style.SUCCESS if success else self.style.ERROR
                self.stdout.write(style(f"  {status} {view_name}"))
            
            self.stdout.write(f"\nSummary: {successful} dropped, {failed} failed")

    def handle_stats(self, category, view_name):
        """Handle stats display"""
        self.stdout.write("Materialized Views Statistics")
        self.stdout.write("=" * 50)
        
        if view_name:
            # Show stats for single view
            stats = MaterializedViewsManager.get_view_stats()
            if view_name in stats:
                self.show_view_stats(view_name, stats[view_name])
            else:
                self.stdout.write(self.style.ERROR(f"View '{view_name}' not found"))
        else:
            stats = MaterializedViewsManager.get_view_stats(category)
            
            # Group by category for display
            for cat_name, cat_views in MaterializedViewsManager.ALL_VIEWS.items():
                if category and category != cat_name:
                    continue
                    
                self.stdout.write(f"\n{cat_name.upper()} VIEWS:")
                self.stdout.write("-" * 30)
                
                for view_name in cat_views.keys():
                    if view_name in stats:
                        self.show_view_stats(view_name, stats[view_name])

    def show_view_stats(self, view_name, stats):
        """Display stats for a single view"""
        if 'error' in stats:
            self.stdout.write(self.style.ERROR(f"  {view_name}: ERROR - {stats['error']}"))
        elif stats['exists']:
            self.stdout.write(f"  {view_name}:")
            self.stdout.write(f"    Rows: {stats['row_count']:,}")
            self.stdout.write(f"    Size: {stats['size']}")
        else:
            self.stdout.write(self.style.WARNING(f"  {view_name}: NOT FOUND"))

    def show_dry_run_info(self, action, category, view_name):
        """Show what would be done in dry run mode"""
        self.stdout.write(self.style.WARNING("DRY RUN - No changes will be made"))
        self.stdout.write("\nWould perform the following actions:")
        
        if view_name:
            self.stdout.write(f"  - {action.upper()} view: {view_name}")
        else:
            if category:
                views = MaterializedViewsManager.get_views_by_category(category)
                self.stdout.write(f"  - {action.upper()} {len(views)} views in category '{category}':")
                for view_name in views.keys():
                    self.stdout.write(f"    • {view_name}")
            else:
                total_views = len(MaterializedViewsManager.get_all_view_names())
                self.stdout.write(f"  - {action.upper()} all {total_views} views across all categories:")
                
                for cat_name, cat_views in MaterializedViewsManager.ALL_VIEWS.items():
                    self.stdout.write(f"    {cat_name.upper()} ({len(cat_views)} views):")
                    for view_name in cat_views.keys():
                        self.stdout.write(f"      • {view_name}")

        self.stdout.write(f"\nTo execute, run the same command without --dry-run")
