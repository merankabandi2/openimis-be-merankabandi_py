"""
Management command for setting up payment reporting materialized views
"""

from django.core.management.base import BaseCommand
from merankabandi.materialized_views_payment_reporting import PaymentReportingMaterializedViews


class Command(BaseCommand):
    """Setup and manage payment reporting materialized views"""
    
    help = 'Setup and manage payment reporting materialized views for comprehensive payment analytics'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--action',
            type=str,
            choices=['create', 'refresh', 'drop', 'stats'],
            default='create',
            help='Action to perform on materialized views'
        )
        parser.add_argument(
            '--concurrent',
            action='store_true',
            help='Refresh views concurrently (requires unique indexes)'
        )
    
    def handle(self, *args, **options):
        action = options['action']
        
        if action == 'create':
            self.stdout.write("Creating payment reporting materialized views...")
            PaymentReportingMaterializedViews.create_all_views()
            self.stdout.write(self.style.SUCCESS("✓ All payment reporting views created successfully"))
            self.stdout.write("\nViews created:")
            self.stdout.write("  • payment_reporting_unified_summary")
            self.stdout.write("  • payment_reporting_by_location")
            self.stdout.write("  • payment_reporting_by_benefit_plan")
            self.stdout.write("  • payment_reporting_time_series")
            self.stdout.write("  • payment_reporting_kpi_summary")
            
        elif action == 'refresh':
            self.stdout.write("Refreshing payment reporting materialized views...")
            PaymentReportingMaterializedViews.refresh_all_views(
                concurrent=options['concurrent']
            )
            self.stdout.write(self.style.SUCCESS("✓ All views refreshed successfully"))
            
        elif action == 'drop':
            self.stdout.write("Dropping payment reporting materialized views...")
            PaymentReportingMaterializedViews.drop_all_views()
            self.stdout.write(self.style.SUCCESS("✓ All views dropped successfully"))
            
        elif action == 'stats':
            self.stdout.write("Payment Reporting View Statistics:")
            self.stdout.write("=" * 50)
            
            from django.db import connection
            with connection.cursor() as cursor:
                # Get view statistics
                cursor.execute("""
                    SELECT 
                        matviewname,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size,
                        obj_description(c.oid, 'pg_class') as description
                    FROM pg_matviews m
                    JOIN pg_class c ON c.relname = m.matviewname
                    WHERE matviewname LIKE 'payment_reporting_%'
                    ORDER BY matviewname
                """)
                
                views = cursor.fetchall()
                if views:
                    for view in views:
                        self.stdout.write(f"\n{view[0]}:")
                        self.stdout.write(f"  Size: {view[1]}")
                        if view[2]:
                            self.stdout.write(f"  Description: {view[2]}")
                else:
                    self.stdout.write("No payment reporting views found.")
                
                # Get row counts
                self.stdout.write("\nRow Counts:")
                self.stdout.write("-" * 30)
                
                view_names = [
                    'payment_reporting_unified_summary',
                    'payment_reporting_by_location',
                    'payment_reporting_by_benefit_plan',
                    'payment_reporting_time_series',
                    'payment_reporting_kpi_summary',
                ]
                
                for view_name in view_names:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
                        count = cursor.fetchone()[0]
                        self.stdout.write(f"{view_name}: {count:,} rows")
                    except:
                        self.stdout.write(f"{view_name}: Not found")