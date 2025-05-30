"""
Django management command to set up materialized views for dashboard optimization
"""

from django.core.management.base import BaseCommand
from django.db import connection
from merankabandi.materialized_views import MaterializedViewManager
from merankabandi.grievance_category_views import GRIEVANCE_CATEGORY_VIEWS
from merankabandi.grievance_channel_views import GRIEVANCE_CHANNEL_VIEWS
from merankabandi.materialized_views_grievance_update import GrievanceMaterializedViewsUpdate
import time


class Command(BaseCommand):
    help = 'Set up materialized views for dashboard performance optimization'
    
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
            help='Refresh specific view only'
        )
        parser.add_argument(
            '--stats',
            action='store_true',
            help='Show view statistics only'
        )
        parser.add_argument(
            '--category',
            type=str,
            choices=['all', 'dashboard', 'payment', 'grievance'],
            default='all',
            help='Category of views to manage (default: all)'
        )
        parser.add_argument(
            '--sync-opensearch',
            action='store_true',
            help='Automatically sync materialized views to OpenSearch after refresh'
        )
        parser.add_argument(
            '--drop',
            action='store_true',
            help='Drop all materialized views'
        )
    
    def handle(self, *args, **options):
        if options['drop']:
            self.drop_views(options['category'])
            return
            
        if options['stats']:
            self.show_stats(options['category'])
            return
            
        if options['refresh_only']:
            self.refresh_all_views(options['concurrent'], options['category'])
            return
            
        if options['view']:
            self.refresh_single_view(options['view'], options['concurrent'])
            return
            
        # Default: create all views
        self.setup_views(options['category'])
    
    def setup_views(self, category='all'):
        """Create materialized views based on category"""
        self.stdout.write(f"Setting up {category} materialized views...")
        
        start_time = time.time()
        
        try:
            if category in ['all', 'dashboard']:
                self.stdout.write("Creating dashboard views...")
                MaterializedViewManager.create_all_views()
                
            if category in ['all', 'payment']:
                self.stdout.write("Creating payment reporting views...")
                self.create_payment_views()
                
            if category in ['all', 'grievance']:
                self.stdout.write("Creating additional grievance views...")
                try:
                    GrievanceMaterializedViewsUpdate.create_all_views()
                except:
                    # If the regular grievance views are already created, just update categories
                    pass
                self.create_grievance_category_views()
                self.create_grievance_channel_views()
            
            elapsed = time.time() - start_time
            self.stdout.write(
                self.style.SUCCESS(
                    f"✓ Successfully set up {category} views in {elapsed:.2f} seconds"
                )
            )
            
            # Show statistics
            self.show_stats(category)
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error setting up views: {e}")
            )
            raise
    
    def create_payment_views(self):
        """Create payment reporting views"""
        with connection.cursor() as cursor:
            # Create monetary transfers view
            sql = """
            DROP MATERIALIZED VIEW IF EXISTS payment_reporting_monetary_transfers CASCADE;
            
            CREATE MATERIALIZED VIEW payment_reporting_monetary_transfers AS
            SELECT 
                EXTRACT(YEAR FROM mt.transfer_date) as year,
                EXTRACT(MONTH FROM mt.transfer_date) as month,
                EXTRACT(QUARTER FROM mt.transfer_date) as quarter,
                mt.transfer_date,
                loc."LocationId" as location_id,
                loc."LocationName" as location_name,
                loc."LocationType" as location_type,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                mt.programme_id,
                bp."code" as programme_code,
                bp."name" as programme_name,
                bp."ceiling_per_beneficiary" as amount_per_beneficiary,
                mt.payment_agency_id,
                pp."name" as payment_agency_name,
                mt.planned_women,
                mt.planned_men,
                mt.planned_twa,
                mt.planned_women + mt.planned_men + mt.planned_twa as total_planned,
                mt.paid_women,
                mt.paid_men,
                mt.paid_twa,
                mt.paid_women + mt.paid_men + mt.paid_twa as total_paid,
                COALESCE(bp."ceiling_per_beneficiary", 0) * (mt.paid_women + mt.paid_men + mt.paid_twa) as total_amount_paid,
                CASE WHEN (mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                    THEN mt.paid_women::numeric / (mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                    ELSE 0 
                END as female_percentage,
                CASE WHEN (mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                    THEN mt.paid_twa::numeric / (mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                    ELSE 0 
                END as twa_percentage,
                CASE WHEN (mt.planned_women + mt.planned_men + mt.planned_twa) > 0 
                    THEN (mt.paid_women + mt.paid_men + mt.paid_twa)::numeric / (mt.planned_women + mt.planned_men + mt.planned_twa) * 100 
                    ELSE 0 
                END as completion_rate
            FROM merankabandi_monetarytransfer mt
            LEFT JOIN social_protection_benefitplan bp ON bp."UUID" = mt.programme_id
            LEFT JOIN payroll_paymentpoint pp ON pp."UUID" = mt.payment_agency_id
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = mt.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            WHERE mt.transfer_date IS NOT NULL;
            
            CREATE INDEX idx_payment_mt_date ON payment_reporting_monetary_transfers(year, month);
            CREATE INDEX idx_payment_mt_location ON payment_reporting_monetary_transfers(province_id, commune_id, location_id);
            CREATE INDEX idx_payment_mt_programme ON payment_reporting_monetary_transfers(programme_id);
            """
            cursor.execute(sql)
            self.stdout.write("✓ Created payment_reporting_monetary_transfers")
            
            # Create location summary
            sql = """
            DROP MATERIALIZED VIEW IF EXISTS payment_reporting_location_summary CASCADE;
            
            CREATE MATERIALIZED VIEW payment_reporting_location_summary AS
            SELECT 
                year::integer,
                month::integer,
                province_id,
                province_name,
                commune_id,
                commune_name,
                programme_id,
                programme_name,
                COUNT(*) as transfer_count,
                SUM(total_planned) as total_planned_beneficiaries,
                SUM(total_paid) as total_paid_beneficiaries,
                SUM(total_amount_paid) as total_amount,
                SUM(paid_women) as total_women,
                SUM(paid_men) as total_men,
                SUM(paid_twa) as total_twa,
                AVG(female_percentage) as avg_female_percentage,
                AVG(twa_percentage) as avg_twa_percentage,
                AVG(completion_rate) as avg_completion_rate
            FROM payment_reporting_monetary_transfers
            GROUP BY 
                year, month,
                province_id, province_name,
                commune_id, commune_name,
                programme_id, programme_name;
            
            CREATE INDEX idx_payment_summary_location ON payment_reporting_location_summary(province_id, commune_id);
            CREATE INDEX idx_payment_summary_time ON payment_reporting_location_summary(year, month);
            """
            cursor.execute(sql)
            self.stdout.write("✓ Created payment_reporting_location_summary")
    
    def create_grievance_category_views(self):
        """Create grievance category views that handle JSON arrays"""
        with connection.cursor() as cursor:
            for view_name, view_sql in GRIEVANCE_CATEGORY_VIEWS.items():
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    cursor.execute(view_sql)
                    
                    # Create indexes
                    if view_name == 'dashboard_grievance_category_summary':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_group ON {view_name}(category_group)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_status ON {view_name}(status)")
                    elif view_name == 'dashboard_grievance_category_details':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_category ON {view_name}(individual_category)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_group ON {view_name}(category_group)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_status ON {view_name}(status)")
                    
                    self.stdout.write(f"✓ Created {view_name}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"✗ Failed to create {view_name}: {e}"))
    
    def create_grievance_channel_views(self):
        """Create grievance channel views that handle space-separated multi-value channels"""
        with connection.cursor() as cursor:
            for view_name, view_sql in GRIEVANCE_CHANNEL_VIEWS.items():
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    cursor.execute(view_sql)
                    
                    # Create indexes
                    if view_name == 'dashboard_grievance_channel_summary':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_channel ON {view_name}(channel)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_count ON {view_name}(ticket_count)")
                    elif view_name == 'dashboard_grievance_channel_details':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_normalized ON {view_name}(normalized_channel)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_status ON {view_name}(status)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_count ON {view_name}(channel_count)")
                    
                    self.stdout.write(f"✓ Created {view_name}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"✗ Failed to create {view_name}: {e}"))
    
    def refresh_all_views(self, concurrent=True, category='all'):
        """Refresh materialized views based on category"""
        self.stdout.write(f"Refreshing {category} views...")
        
        start_time = time.time()
        
        try:
            if category in ['all', 'dashboard']:
                self.stdout.write("Refreshing dashboard views...")
                MaterializedViewManager.refresh_all_views()
                
            if category in ['all', 'payment']:
                self.stdout.write("Refreshing payment reporting views...")
                self.refresh_payment_views(concurrent)
                
            if category in ['all', 'grievance']:
                self.stdout.write("Refreshing grievance views...")
                try:
                    GrievanceMaterializedViewsUpdate.refresh_all_views(concurrent)
                except:
                    pass
                self.refresh_grievance_category_views(concurrent)
                self.refresh_grievance_channel_views(concurrent)
            
            # Sync to OpenSearch if requested
            if options.get('sync_opensearch'):
                self.sync_to_opensearch()
            
            elapsed = time.time() - start_time
            self.stdout.write(
                self.style.SUCCESS(
                    f"✓ Successfully refreshed {category} views in {elapsed:.2f} seconds"
                )
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error refreshing views: {e}")
            )
            raise
    
    def refresh_payment_views(self, concurrent=True):
        """Refresh payment views"""
        views = [
            'payment_reporting_monetary_transfers',
            'payment_reporting_location_summary'
        ]
        
        concurrently = "CONCURRENTLY" if concurrent else ""
        
        with connection.cursor() as cursor:
            for view in views:
                try:
                    sql = f"REFRESH MATERIALIZED VIEW {concurrently} {view}"
                    cursor.execute(sql)
                    self.stdout.write(f"✓ Refreshed {view}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"✗ Error refreshing {view}: {e}"))
    
    def refresh_grievance_category_views(self, concurrent=True):
        """Refresh grievance category views"""
        views = list(GRIEVANCE_CATEGORY_VIEWS.keys())
        
        concurrently = "CONCURRENTLY" if concurrent else ""
        
        with connection.cursor() as cursor:
            for view in views:
                try:
                    sql = f"REFRESH MATERIALIZED VIEW {concurrently} {view}"
                    cursor.execute(sql)
                    self.stdout.write(f"✓ Refreshed {view}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"✗ Error refreshing {view}: {e}"))
    
    def refresh_grievance_channel_views(self, concurrent=True):
        """Refresh grievance channel views"""
        views = list(GRIEVANCE_CHANNEL_VIEWS.keys())
        
        concurrently = "CONCURRENTLY" if concurrent else ""
        
        with connection.cursor() as cursor:
            for view in views:
                try:
                    sql = f"REFRESH MATERIALIZED VIEW {concurrently} {view}"
                    cursor.execute(sql)
                    self.stdout.write(f"✓ Refreshed {view}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"✗ Error refreshing {view}: {e}"))
    
    def refresh_single_view(self, view_name, concurrent=True):
        """Refresh a specific materialized view"""
        self.stdout.write(f"Refreshing view: {view_name}")
        
        start_time = time.time()
        
        try:
            with connection.cursor() as cursor:
                if concurrent:
                    cursor.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
                else:
                    cursor.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
            
            elapsed = time.time() - start_time
            self.stdout.write(
                self.style.SUCCESS(
                    f"✓ Successfully refreshed {view_name} in {elapsed:.2f} seconds"
                )
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error refreshing {view_name}: {e}")
            )
            raise
    
    def drop_views(self, category='all'):
        """Drop materialized views based on category"""
        self.stdout.write(f"Dropping {category} views...")
        
        try:
            if category in ['all', 'dashboard']:
                MaterializedViewManager.drop_all_views()
                
            if category in ['all', 'payment']:
                with connection.cursor() as cursor:
                    cursor.execute("DROP MATERIALIZED VIEW IF EXISTS payment_reporting_location_summary CASCADE")
                    cursor.execute("DROP MATERIALIZED VIEW IF EXISTS payment_reporting_monetary_transfers CASCADE")
                
            if category in ['all', 'grievance']:
                GrievanceMaterializedViewsUpdate.drop_all_views()
                with connection.cursor() as cursor:
                    cursor.execute("DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_category_details CASCADE")
                    cursor.execute("DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_category_summary CASCADE")
            
            self.stdout.write(
                self.style.SUCCESS(f"✓ Successfully dropped {category} views")
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error dropping views: {e}")
            )
            raise
    
    def show_stats(self, category='all'):
        """Show statistics for materialized views based on category"""
        self.stdout.write(f"\n{category.title()} View Statistics:")
        self.stdout.write("=" * 80)
        
        try:
            # Build WHERE clause based on category
            where_clause = "WHERE mv.schemaname = 'public'"
            if category == 'dashboard':
                where_clause += " AND mv.matviewname LIKE 'dashboard_%'"
            elif category == 'payment':
                where_clause += " AND mv.matviewname LIKE 'payment_reporting_%'"
            elif category == 'grievance':
                where_clause += " AND mv.matviewname LIKE 'dashboard_grievance_%'"
            else:  # all
                where_clause += " AND (mv.matviewname LIKE 'dashboard_%' OR mv.matviewname LIKE 'payment_reporting_%')"
            
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        mv.matviewname as view_name,
                        pg_size_pretty(pg_total_relation_size(c.oid)) as total_size,
                        obj_description(c.oid, 'pg_class') as description
                    FROM pg_matviews mv
                    JOIN pg_class c ON c.relname = mv.matviewname
                    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = mv.schemaname
                    {where_clause}
                    ORDER BY mv.matviewname
                """)
                
                rows = cursor.fetchall()
                
                if not rows:
                    self.stdout.write("No materialized views found.")
                    return
                
                self.stdout.write(f"{'View Name':<60} {'Size':>10}")
                self.stdout.write("-" * 72)
                
                for row in rows:
                    view_name, size, description = row
                    self.stdout.write(f"{view_name:<60} {size:>10}")
                
                # Get total count and size
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as view_count,
                        pg_size_pretty(SUM(pg_total_relation_size(c.oid))) as total_size
                    FROM pg_matviews mv
                    JOIN pg_class c ON c.relname = mv.matviewname
                    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = mv.schemaname
                    {where_clause}
                """)
                
                count, total = cursor.fetchone()
                
                self.stdout.write("\n" + "=" * 72)
                self.stdout.write(f"Total Views: {count}")
                self.stdout.write(f"Total Size: {total}")
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error getting statistics: {e}")
            )
    
    def sync_to_opensearch(self):
        """Sync materialized views data to OpenSearch for analytics"""
        try:
            self.stdout.write("\nSyncing materialized views to OpenSearch...")
            
            # Import the sync service
            from django.core.management import call_command
            
            # Call the OpenSearch sync command
            call_command('sync_dashboard_to_opensearch')
            
            self.stdout.write("✓ OpenSearch sync completed")
            
        except ImportError:
            self.stdout.write(
                self.style.WARNING(
                    "⚠️ OpenSearch sync not available. Install opensearch_reports module."
                )
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error syncing to OpenSearch: {e}")
            )