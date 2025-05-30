"""
Management command to update grievance materialized views with JSON array category handling
"""
from django.core.management.base import BaseCommand
from django.db import connection
from ...grievance_category_views import GRIEVANCE_CATEGORY_VIEWS


class Command(BaseCommand):
    help = 'Update grievance materialized views to properly handle JSON array categories'

    def handle(self, *args, **options):
        self.stdout.write("Updating grievance category materialized views...")
        
        with connection.cursor() as cursor:
            for view_name, view_sql in GRIEVANCE_CATEGORY_VIEWS.items():
                try:
                    # Drop existing view if it exists
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    
                    # Create new view
                    cursor.execute(view_sql)
                    
                    # Create indexes for better performance
                    if view_name == 'dashboard_grievance_category_summary':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_group ON {view_name}(category_group)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_status ON {view_name}(status)")
                    elif view_name == 'dashboard_grievance_category_details':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_category ON {view_name}(individual_category)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_group ON {view_name}(category_group)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_status ON {view_name}(status)")
                    
                    self.stdout.write(self.style.SUCCESS(f"✓ Created/Updated view: {view_name}"))
                    
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"✗ Failed to create view {view_name}: {str(e)}"))
        
        self.stdout.write(self.style.SUCCESS("\nGrievance category views update complete!"))
        self.stdout.write("\nTo refresh the data in these views, run:")
        self.stdout.write("  python manage.py refresh_dashboard_views")