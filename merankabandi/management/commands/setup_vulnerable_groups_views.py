"""
Django management command to set up enhanced materialized views with vulnerable groups
"""

from django.core.management.base import BaseCommand
from django.db import connection
from merankabandi.materialized_views_vulnerable_groups import (
    ENHANCED_MATERIALIZED_VIEWS_SQL,
    VULNERABLE_GROUPS_INDEXES,
    MaterializedViewManager
)
import time


class Command(BaseCommand):
    help = 'Set up enhanced materialized views with vulnerable groups reporting'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--refresh-only',
            action='store_true',
            help='Only refresh existing views, do not create new ones'
        )
        parser.add_argument(
            '--concurrent',
            action='store_true', 
            default=True,
            help='Use concurrent refresh (default: True)'
        )
        parser.add_argument(
            '--view',
            type=str,
            help='Create/refresh specific view only'
        )
        parser.add_argument(
            '--stats',
            action='store_true',
            help='Show view statistics only'
        )
        parser.add_argument(
            '--drop',
            action='store_true',
            help='Drop all enhanced materialized views'
        )
    
    def handle(self, *args, **options):
        if options['drop']:
            self.drop_views()
            return
            
        if options['stats']:
            self.show_stats()
            return
            
        if options['refresh_only']:
            self.refresh_all_views(options['concurrent'])
            return
            
        if options['view']:
            self.create_or_refresh_single_view(options['view'], options['concurrent'])
            return
            
        # Default: create all enhanced views
        self.setup_views()
    
    def setup_views(self):
        """Create all enhanced materialized views with vulnerable groups"""
        self.stdout.write("Setting up enhanced materialized views with vulnerable groups...")
        
        start_time = time.time()
        
        try:
            # Create all enhanced views
            for view_name, view_sql in ENHANCED_MATERIALIZED_VIEWS_SQL.items():
                self.stdout.write(f"Creating view: {view_name}...")
                MaterializedViewManager.create_or_replace_view(view_name, view_sql)
            
            # Create additional indexes
            self.stdout.write("Creating vulnerable groups indexes...")
            with connection.cursor() as cursor:
                for index_sql in VULNERABLE_GROUPS_INDEXES:
                    try:
                        cursor.execute(index_sql)
                        self.stdout.write(f"✓ Created index")
                    except Exception as e:
                        self.stdout.write(f"✗ Error creating index: {e}")
            
            elapsed = time.time() - start_time
            self.stdout.write(
                self.style.SUCCESS(
                    f"✓ Successfully set up enhanced views in {elapsed:.2f} seconds"
                )
            )
            
            # Show statistics
            self.show_stats()
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error setting up views: {e}")
            )
            raise
    
    def refresh_all_views(self, concurrent=True):
        """Refresh all enhanced materialized views"""
        self.stdout.write("Refreshing all enhanced materialized views...")
        
        start_time = time.time()
        successful = 0
        failed = 0
        
        for view_name in ENHANCED_MATERIALIZED_VIEWS_SQL.keys():
            try:
                self.stdout.write(f"Refreshing {view_name}...")
                MaterializedViewManager.refresh_view(view_name, concurrent)
                successful += 1
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"✗ Error refreshing {view_name}: {e}")
                )
                failed += 1
        
        elapsed = time.time() - start_time
        self.stdout.write(
            self.style.SUCCESS(
                f"✓ Refreshed {successful} views in {elapsed:.2f} seconds ({failed} failed)"
            )
        )
    
    def create_or_refresh_single_view(self, view_name, concurrent=True):
        """Create or refresh a single view"""
        if view_name not in ENHANCED_MATERIALIZED_VIEWS_SQL:
            self.stdout.write(
                self.style.ERROR(f"✗ Unknown view: {view_name}")
            )
            self.stdout.write("Available views:")
            for name in ENHANCED_MATERIALIZED_VIEWS_SQL.keys():
                self.stdout.write(f"  - {name}")
            return
        
        try:
            # Check if view exists
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT matviewname FROM pg_matviews WHERE matviewname = %s",
                    [view_name]
                )
                exists = cursor.fetchone() is not None
            
            if exists:
                self.stdout.write(f"Refreshing existing view: {view_name}...")
                MaterializedViewManager.refresh_view(view_name, concurrent)
            else:
                self.stdout.write(f"Creating new view: {view_name}...")
                MaterializedViewManager.create_or_replace_view(
                    view_name, 
                    ENHANCED_MATERIALIZED_VIEWS_SQL[view_name]
                )
            
            self.stdout.write(
                self.style.SUCCESS(f"✓ Successfully processed {view_name}")
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error processing {view_name}: {e}")
            )
            raise
    
    def show_stats(self):
        """Show statistics for enhanced materialized views"""
        self.stdout.write("\n" + "="*80)
        self.stdout.write("ENHANCED MATERIALIZED VIEWS STATISTICS")
        self.stdout.write("="*80)
        
        with connection.cursor() as cursor:
            # Get all enhanced views
            cursor.execute("""
                SELECT 
                    matviewname,
                    pg_size_pretty(pg_total_relation_size(matviewname::regclass)) as size,
                    definition
                FROM pg_matviews 
                WHERE matviewname LIKE 'dashboard_%enhanced'
                   OR matviewname = 'dashboard_vulnerable_groups_summary'
                   OR matviewname = 'dashboard_payment_vulnerable_groups'
                   OR matviewname = 'dashboard_master_summary_enhanced'
                ORDER BY matviewname;
            """)
            
            views = cursor.fetchall()
            
            if not views:
                self.stdout.write("No enhanced views found.")
                return
            
            for view_name, size, definition in views:
                self.stdout.write(f"\n{view_name}:")
                self.stdout.write(f"  Size: {size}")
                
                # Get row count
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
                    count = cursor.fetchone()[0]
                    self.stdout.write(f"  Rows: {count:,}")
                except:
                    self.stdout.write(f"  Rows: Unable to count")
                
                # Check if it has vulnerable groups data
                if 'handicap' in definition or 'maladie_chro' in definition:
                    self.stdout.write("  ✓ Includes disability and chronic illness data")
                if 'is_twa' in definition:
                    self.stdout.write("  ✓ Includes Twa/Batwa data")
                if 'type_menage' in definition:
                    self.stdout.write("  ✓ Includes refugee/returnee data")
        
        self.stdout.write("\n" + "="*80)
    
    def drop_views(self):
        """Drop all enhanced materialized views"""
        self.stdout.write("Dropping all enhanced materialized views...")
        
        with connection.cursor() as cursor:
            for view_name in ENHANCED_MATERIALIZED_VIEWS_SQL.keys():
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE;")
                    self.stdout.write(f"✓ Dropped {view_name}")
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f"✗ Error dropping {view_name}: {e}")
                    )