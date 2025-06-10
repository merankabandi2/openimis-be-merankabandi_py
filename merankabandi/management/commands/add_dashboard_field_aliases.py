from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import Count, Sum, Q, F
from datetime import datetime
import json


class Command(BaseCommand):
    help = 'Add field aliases to existing dashboard views to match frontend expectations'

    def handle(self, *args, **options):
        self.stdout.write("Adding field aliases to dashboard views...\n")
        
        with connection.cursor() as cursor:
            # Get list of materialized views
            cursor.execute("""
                SELECT matviewname 
                FROM pg_matviews 
                WHERE schemaname = 'public' 
                AND matviewname LIKE 'dashboard_%'
                ORDER BY matviewname
            """)
            views = [row[0] for row in cursor.fetchall()]
            
            self.stdout.write(f"Found {len(views)} dashboard views\n")
            
            # Check each view's columns
            for view in views:
                self.stdout.write(f"\nChecking {view}...")
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{view}'
                    ORDER BY ordinal_position
                """)
                columns = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Create wrapper views with aliases based on what columns exist
                if view == 'dashboard_beneficiary_summary':
                    self.create_beneficiary_aliases(view, columns, cursor)
                elif view == 'dashboard_monetary_transfers':
                    self.create_monetary_aliases(view, columns, cursor)
                elif view == 'dashboard_activities_summary':
                    self.create_activities_aliases(view, columns, cursor)
                elif view == 'dashboard_master_summary':
                    self.fix_master_summary(view, columns, cursor)
                elif view == 'dashboard_grievances':
                    self.verify_grievance_counts(view, columns, cursor)
                    
        self.stdout.write(self.style.SUCCESS("\n\nField aliases added successfully!"))
        
    def create_beneficiary_aliases(self, view_name, columns, cursor):
        """Create view with field aliases for beneficiary summary"""
        if 'beneficiary_count' in columns:
            sql = f"""
            CREATE OR REPLACE VIEW {view_name}_with_aliases AS
            SELECT 
                *,
                beneficiary_count as total_beneficiaries,
                COALESCE(male_count, 0) as total_male,
                COALESCE(female_count, 0) as total_female,
                COALESCE(twa_count, 0) as total_twa
            FROM {view_name};
            """
            
            try:
                cursor.execute(sql)
                self.stdout.write(self.style.SUCCESS(f"  ✓ Created aliases for {view_name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"  ✗ Error creating aliases: {e}"))
                
    def create_monetary_aliases(self, view_name, columns, cursor):
        """Create view with field aliases for monetary transfers"""
        needed_aliases = {
            'planned_women': 'plannedWomen',
            'planned_men': 'plannedMen', 
            'planned_twa': 'plannedTwa',
            'planned_amount': 'plannedAmount',
            'paid_women': 'paidWomen',
            'paid_men': 'paidMen',
            'paid_twa': 'paidTwa',
            'transferred_amount': 'transferredAmount'
        }
        
        alias_list = []
        for snake_case, camel_case in needed_aliases.items():
            if snake_case in columns:
                alias_list.append(f"{snake_case} as \"{camel_case}\"")
                
        if alias_list:
            sql = f"""
            CREATE OR REPLACE VIEW {view_name}_with_aliases AS
            SELECT 
                *,
                {', '.join(alias_list)}
            FROM {view_name};
            """
            
            try:
                cursor.execute(sql)
                self.stdout.write(self.style.SUCCESS(f"  ✓ Created {len(alias_list)} aliases for {view_name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"  ✗ Error creating aliases: {e}"))
                
    def create_activities_aliases(self, view_name, columns, cursor):
        """Create view with field aliases for activities summary"""
        if 'male_participants' in columns:
            sql = f"""
            CREATE OR REPLACE VIEW {view_name}_with_aliases AS
            SELECT 
                *,
                COALESCE(activity_count, 0) as total_activities,
                COALESCE(male_participants, 0) as total_male,
                COALESCE(female_participants, 0) as total_female,
                COALESCE(twa_participants, 0) as total_twa
            FROM {view_name};
            """
            
            try:
                cursor.execute(sql)
                self.stdout.write(self.style.SUCCESS(f"  ✓ Created aliases for {view_name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"  ✗ Error creating aliases: {e}"))
                
    def fix_master_summary(self, view_name, columns, cursor):
        """Check and report master summary issues"""
        if 'total_beneficiaries' in columns:
            # Get current value
            cursor.execute(f"SELECT total_beneficiaries FROM {view_name} LIMIT 1")
            result = cursor.fetchone()
            if result:
                count = result[0]
                self.stdout.write(f"  Current total_beneficiaries: {count:,}")
                
                # Compare with detail view if it exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_matviews 
                        WHERE matviewname = 'dashboard_beneficiary_summary'
                    )
                """)
                if cursor.fetchone()[0]:
                    cursor.execute("""
                        SELECT COALESCE(SUM(beneficiary_count), 0) 
                        FROM dashboard_beneficiary_summary
                    """)
                    detail_count = cursor.fetchone()[0]
                    if count != detail_count:
                        self.stdout.write(self.style.WARNING(
                            f"  ⚠ Count mismatch: Master={count:,}, Detail={detail_count:,}"
                        ))
                        
    def verify_grievance_counts(self, view_name, columns, cursor):
        """Verify grievance sensitive counts are correct"""
        if 'sensitive_tickets' in columns:
            cursor.execute(f"SELECT sensitive_tickets FROM {view_name} LIMIT 1")
            result = cursor.fetchone()
            if result:
                count = result[0]
                self.stdout.write(f"  Sensitive tickets: {count}")
                
                # Also check category breakdown
                cursor.execute("""
                    SELECT category, count 
                    FROM dashboard_grievance_category_summary 
                    WHERE category IN ('cas_sensibles', 'cas_speciaux', 'cas_non_sensibles')
                    ORDER BY category
                """)
                for category, count in cursor.fetchall():
                    self.stdout.write(f"    {category}: {count}")