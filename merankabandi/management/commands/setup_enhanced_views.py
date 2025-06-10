"""
Django management command to create enhanced materialized views with comprehensive dimensions
"""

from django.core.management.base import BaseCommand
from django.db import connection
from ...materialized_views_enhanced import (
    DASHBOARD_ENHANCED_BENEFICIARY_VIEW,
    DASHBOARD_HOUSEHOLD_SUMMARY_VIEW, 
    DASHBOARD_VULNERABLE_GROUPS_VIEW,
    CREATE_ENHANCED_VIEWS,
    REFRESH_ENHANCED_VIEWS
)
import time
from datetime import datetime


class Command(BaseCommand):
    help = 'Create or refresh enhanced materialized views with comprehensive JSON_ext dimensions'

    def add_arguments(self, parser):
        parser.add_argument(
            '--refresh-only',
            action='store_true',
            help='Only refresh existing views without recreating them',
        )
        parser.add_argument(
            '--view',
            type=str,
            help='Specific view to create/refresh',
        )
        parser.add_argument(
            '--stats',
            action='store_true',
            help='Show statistics for views',
        )

    def handle(self, *args, **options):
        start_time = time.time()
        
        if options['stats']:
            self.show_view_stats()
            return
        
        refresh_only = options.get('refresh_only', False)
        specific_view = options.get('view')
        
        with connection.cursor() as cursor:
            try:
                if specific_view:
                    self.handle_specific_view(cursor, specific_view, refresh_only)
                else:
                    if refresh_only:
                        self.stdout.write('Refreshing enhanced materialized views...')
                        cursor.execute(REFRESH_ENHANCED_VIEWS)
                    else:
                        self.stdout.write('Creating enhanced materialized views...')
                        cursor.execute(CREATE_ENHANCED_VIEWS)
                
                # Show summary
                self.show_summary(cursor)
                
                elapsed_time = time.time() - start_time
                self.stdout.write(
                    self.style.SUCCESS(
                        f'\n✓ Operation completed successfully in {elapsed_time:.2f} seconds'
                    )
                )
                
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'\n✗ Error: {e}')
                )
                raise

    def handle_specific_view(self, cursor, view_name, refresh_only):
        """Handle creation/refresh of a specific view"""
        view_mapping = {
            'beneficiary_enhanced': DASHBOARD_ENHANCED_BENEFICIARY_VIEW,
            'households': DASHBOARD_HOUSEHOLD_SUMMARY_VIEW,
            'vulnerable_groups': DASHBOARD_VULNERABLE_GROUPS_VIEW,
        }
        
        if view_name not in view_mapping:
            self.stdout.write(
                self.style.ERROR(
                    f'Unknown view: {view_name}. Available views: {", ".join(view_mapping.keys())}'
                )
            )
            return
        
        full_view_name = f'dashboard_{view_name}'
        
        if refresh_only:
            self.stdout.write(f'Refreshing {full_view_name}...')
            cursor.execute(f'REFRESH MATERIALIZED VIEW CONCURRENTLY {full_view_name}')
        else:
            self.stdout.write(f'Creating {full_view_name}...')
            cursor.execute(f'DROP MATERIALIZED VIEW IF EXISTS {full_view_name} CASCADE')
            cursor.execute(view_mapping[view_name])

    def show_view_stats(self):
        """Show statistics for enhanced views"""
        views = [
            'dashboard_beneficiary_enhanced',
            'dashboard_households', 
            'dashboard_vulnerable_groups'
        ]
        
        with connection.cursor() as cursor:
            self.stdout.write('\nEnhanced Materialized View Statistics:')
            self.stdout.write('=' * 80)
            
            total_size = 0
            total_rows = 0
            
            for view in views:
                cursor.execute(f"""
                    SELECT 
                        pg_size_pretty(pg_total_relation_size('{view}'::regclass)) as size,
                        (SELECT COUNT(*) FROM {view}) as row_count,
                        obj_description('{view}'::regclass) as description
                """)
                
                result = cursor.fetchone()
                if result:
                    size, row_count, description = result
                    self.stdout.write(f'\n{view}:')
                    self.stdout.write(f'  Size: {size}')
                    self.stdout.write(f'  Rows: {row_count:,}')
                    if description:
                        self.stdout.write(f'  Description: {description}')
                    
                    # Get sample of available dimensions
                    cursor.execute(f"""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = '{view}'
                        AND column_name IN (
                            'household_type', 'is_batwa', 'has_disability', 
                            'education_level', 'pmt_score_range', 'community_type'
                        )
                        ORDER BY ordinal_position
                    """)
                    
                    dimensions = cursor.fetchall()
                    if dimensions:
                        self.stdout.write('  Key Dimensions:')
                        for col_name, data_type in dimensions:
                            self.stdout.write(f'    - {col_name} ({data_type})')

    def show_summary(self, cursor):
        """Show summary statistics after creation/refresh"""
        self.stdout.write('\n' + '=' * 80)
        self.stdout.write('Enhanced View Summary:')
        self.stdout.write('=' * 80)
        
        # Beneficiary enhanced view stats
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT individual_id) as total_individuals,
                COUNT(DISTINCT household_id) as total_households,
                COUNT(DISTINCT CASE WHEN is_batwa THEN individual_id END) as batwa_individuals,
                COUNT(DISTINCT CASE WHEN has_disability THEN individual_id END) as disabled_individuals,
                COUNT(DISTINCT province) as provinces
            FROM dashboard_beneficiary_enhanced
        """)
        
        result = cursor.fetchone()
        if result:
            self.stdout.write('\nBeneficiary Enhanced View:')
            self.stdout.write(f'  Total Individuals: {result[0]:,}')
            self.stdout.write(f'  Total Households: {result[1]:,}')
            self.stdout.write(f'  Batwa Individuals: {result[2]:,}')
            self.stdout.write(f'  Disabled Individuals: {result[3]:,}')
            self.stdout.write(f'  Provinces Covered: {result[4]}')
        
        # Household view stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total_households,
                AVG(household_size) as avg_household_size,
                AVG(housing_quality_score) as avg_housing_quality,
                COUNT(CASE WHEN is_twa_household THEN 1 END) as twa_households,
                COUNT(CASE WHEN benefit_plan_enrolled THEN 1 END) as enrolled_households
            FROM dashboard_households
        """)
        
        result = cursor.fetchone()
        if result:
            self.stdout.write('\nHousehold View:')
            self.stdout.write(f'  Total Households: {result[0]:,}')
            self.stdout.write(f'  Avg Household Size: {result[1]:.1f}')
            self.stdout.write(f'  Avg Housing Quality: {result[2]:.1f}/100')
            self.stdout.write(f'  Twa Households: {result[3]:,}')
            self.stdout.write(f'  Enrolled Households: {result[4]:,}')
        
        # Vulnerable groups stats
        cursor.execute("""
            SELECT 
                group_type,
                SUM(total_households) as households,
                SUM(total_individuals) as individuals,
                AVG(coverage_rate) as avg_coverage
            FROM dashboard_vulnerable_groups
            GROUP BY group_type
            ORDER BY households DESC
        """)
        
        results = cursor.fetchall()
        if results:
            self.stdout.write('\nVulnerable Groups:')
            for group_type, households, individuals, coverage in results:
                self.stdout.write(
                    f'  {group_type}: {households:,} households, '
                    f'{individuals:,} individuals, '
                    f'{coverage:.1%} coverage'
                )