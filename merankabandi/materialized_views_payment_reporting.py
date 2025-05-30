"""
Payment Reporting Materialized Views
Combines MonetaryTransfer and BenefitConsumption data for comprehensive payment analytics
"""

from django.db import connection
from django.core.management.base import BaseCommand
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class PaymentReportingMaterializedViews:
    """
    Creates and manages materialized views for payment reporting
    combining external (MonetaryTransfer) and internal (BenefitConsumption) payments
    """
    
    @staticmethod
    def create_unified_payment_summary_view():
        """
        Create unified payment summary view combining both payment sources
        """
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS payment_reporting_unified_summary CASCADE;
        
        CREATE MATERIALIZED VIEW payment_reporting_unified_summary AS
        WITH external_payments AS (
            -- MonetaryTransfer (pre-aggregated external payment data)
            SELECT 
                mt.id as payment_id,
                'EXTERNAL' as payment_source,
                mt.transfer_date as payment_date,
                EXTRACT(YEAR FROM mt.transfer_date) as year,
                EXTRACT(MONTH FROM mt.transfer_date) as month,
                EXTRACT(QUARTER FROM mt.transfer_date) as quarter,
                mt.programme_id as benefit_plan_id,
                bp.name as benefit_plan_name,
                mt.location_id,
                loc.id as colline_id,
                loc.name as colline_name,
                loc.parent_id as commune_id,
                com.name as commune_name,
                com.parent_id as province_id,
                prov.name as province_name,
                -- Pre-aggregated gender counts
                mt.paid_women as female_count,
                mt.paid_men as male_count,
                mt.paid_twa as twa_count,
                (mt.paid_women + mt.paid_men + mt.paid_twa) as total_beneficiaries,
                -- Calculate amounts based on benefit plan amount per beneficiary
                COALESCE(bp.ceiling_per_cycle, 0) * (mt.paid_women + mt.paid_men + mt.paid_twa) as total_amount
            FROM merankabandi_monetarytransfer mt
            LEFT JOIN social_protection_benefitplan bp ON bp.id = mt.programme_id
            LEFT JOIN location_location loc ON loc.id = mt.location_id
            LEFT JOIN location_location com ON com.id = loc.parent_id
            LEFT JOIN location_location prov ON prov.id = com.parent_id
            WHERE mt.transfer_date IS NOT NULL
        ),
        internal_payments AS (
            -- BenefitConsumption (internal payments)
            SELECT 
                bc.id as payment_id,
                'INTERNAL' as payment_source,
                bc.amount as payment_amount,
                bc.date_due as payment_date,
                EXTRACT(YEAR FROM bc.date_due) as year,
                EXTRACT(MONTH FROM bc.date_due) as month,
                EXTRACT(QUARTER FROM bc.date_due) as quarter,
                pbc.payroll__payment_plan__benefit_plan_id as benefit_plan_id,
                bp.name as benefit_plan_name,
                gb.id as group_beneficiary_id,
                g.id as group_id,
                g.location_id,
                loc.id as colline_id,
                loc.name as colline_name,
                loc.parent_id as commune_id,
                com.name as commune_name,
                com.parent_id as province_id,
                prov.name as province_name,
                -- Individual data
                bc.individual_id,
                COALESCE(i.json_ext->>'sexe', 'U') as gender,
                CASE 
                    WHEN g.json_ext->>'menage_mutwa' = 'OUI' THEN true
                    ELSE false
                END as is_twa,
                CASE 
                    WHEN g.json_ext->>'type_menage' = 'Communauté hôte' THEN 'HOST'
                    WHEN g.json_ext->>'type_menage' = 'Refugie' THEN 'REFUGEE'
                    ELSE 'OTHER'
                END as community_type,
                bc.validity_from as created_at,
                bc.validity_to as updated_at
            FROM payroll_benefitconsumption bc
            INNER JOIN payroll_payrollbenefitconsumption pbc ON pbc.benefit_consumption_id = bc.id
            INNER JOIN individual_individual i ON i.id = bc.individual_id AND i.is_deleted = false
            INNER JOIN individual_groupindividual gi ON gi.individual_id = i.id AND gi.is_deleted = false
            INNER JOIN social_protection_group g ON g.id = gi.group_id AND g.is_deleted = false
            INNER JOIN social_protection_groupbeneficiary gb ON gb.group_id = g.id AND gb.is_deleted = false
            LEFT JOIN social_protection_benefitplan bp ON bp.id = pbc.payroll__payment_plan__benefit_plan_id
            LEFT JOIN location_location loc ON loc.id = g.location_id
            LEFT JOIN location_location com ON com.id = loc.parent_id
            LEFT JOIN location_location prov ON prov.id = com.parent_id
            WHERE bc.is_deleted = false
                AND bc.status = 'RECONCILED'
        )
        -- Combine and aggregate
        SELECT 
            payment_source,
            year,
            month,
            quarter,
            benefit_plan_id,
            benefit_plan_name,
            province_id,
            province_name,
            commune_id,
            commune_name,
            colline_id,
            colline_name,
            gender,
            is_twa,
            community_type,
            COUNT(DISTINCT payment_id) as total_payment_count,
            SUM(payment_amount) as total_payment_amount,
            COUNT(DISTINCT individual_id) as unique_beneficiaries,
            AVG(payment_amount) as avg_payment_per_beneficiary,
            -- Gender breakdown
            COUNT(DISTINCT CASE WHEN gender = 'F' THEN individual_id END) as female_beneficiaries,
            COUNT(DISTINCT CASE WHEN gender = 'M' THEN individual_id END) as male_beneficiaries,
            COUNT(DISTINCT CASE WHEN gender = 'F' THEN individual_id END)::numeric / 
                NULLIF(COUNT(DISTINCT individual_id), 0) * 100 as female_percentage,
            -- TWA breakdown
            COUNT(DISTINCT CASE WHEN is_twa THEN individual_id END) as twa_beneficiaries,
            COUNT(DISTINCT CASE WHEN is_twa THEN individual_id END)::numeric / 
                NULLIF(COUNT(DISTINCT individual_id), 0) * 100 as twa_percentage,
            CURRENT_DATE as last_updated
        FROM (
            SELECT * FROM external_payments
            UNION ALL
            SELECT * FROM internal_payments
        ) all_payments
        GROUP BY 
            payment_source, year, month, quarter,
            benefit_plan_id, benefit_plan_name,
            province_id, province_name,
            commune_id, commune_name,
            colline_id, colline_name,
            gender, is_twa, community_type;
        
        CREATE INDEX idx_payment_unified_year_month ON payment_reporting_unified_summary(year, month);
        CREATE INDEX idx_payment_unified_location ON payment_reporting_unified_summary(province_id, commune_id, colline_id);
        CREATE INDEX idx_payment_unified_benefit_plan ON payment_reporting_unified_summary(benefit_plan_id);
        CREATE INDEX idx_payment_unified_source ON payment_reporting_unified_summary(payment_source);
        CREATE INDEX idx_payment_unified_demographics ON payment_reporting_unified_summary(gender, is_twa, community_type);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        
        logger.info("Created payment_reporting_unified_summary materialized view")
    
    @staticmethod
    def create_location_aggregation_view():
        """
        Create location-based aggregation view
        """
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS payment_reporting_by_location CASCADE;
        
        CREATE MATERIALIZED VIEW payment_reporting_by_location AS
        SELECT 
            payment_source,
            year,
            month,
            benefit_plan_id,
            province_id,
            province_name,
            commune_id,
            commune_name,
            colline_id,
            colline_name,
            SUM(total_payment_count) as total_payment_count,
            SUM(total_payment_amount) as total_payment_amount,
            SUM(unique_beneficiaries) as unique_beneficiaries,
            AVG(avg_payment_per_beneficiary) as avg_payment_per_beneficiary,
            AVG(female_percentage) as female_percentage,
            AVG(twa_percentage) as twa_percentage
        FROM payment_reporting_unified_summary
        GROUP BY 
            payment_source, year, month, benefit_plan_id,
            province_id, province_name,
            commune_id, commune_name,
            colline_id, colline_name;
        
        CREATE INDEX idx_payment_location_hierarchy ON payment_reporting_by_location(province_id, commune_id, colline_id);
        CREATE INDEX idx_payment_location_time ON payment_reporting_by_location(year, month);
        CREATE INDEX idx_payment_location_plan ON payment_reporting_by_location(benefit_plan_id);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        
        logger.info("Created payment_reporting_by_location materialized view")
    
    @staticmethod
    def create_benefit_plan_aggregation_view():
        """
        Create benefit plan aggregation view
        """
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS payment_reporting_by_benefit_plan CASCADE;
        
        CREATE MATERIALIZED VIEW payment_reporting_by_benefit_plan AS
        SELECT 
            benefit_plan_id,
            benefit_plan_name,
            payment_source,
            year,
            month,
            province_id,
            SUM(total_payment_count) as total_payment_count,
            SUM(total_payment_amount) as total_payment_amount,
            SUM(unique_beneficiaries) as unique_beneficiaries,
            AVG(avg_payment_per_beneficiary) as avg_payment_per_beneficiary,
            AVG(female_percentage) as female_percentage,
            AVG(twa_percentage) as twa_percentage
        FROM payment_reporting_unified_summary
        GROUP BY 
            benefit_plan_id, benefit_plan_name,
            payment_source, year, month, province_id;
        
        CREATE INDEX idx_payment_plan_id ON payment_reporting_by_benefit_plan(benefit_plan_id);
        CREATE INDEX idx_payment_plan_time ON payment_reporting_by_benefit_plan(year, month);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        
        logger.info("Created payment_reporting_by_benefit_plan materialized view")
    
    @staticmethod
    def create_time_series_view():
        """
        Create time series view for trend analysis
        """
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS payment_reporting_time_series CASCADE;
        
        CREATE MATERIALIZED VIEW payment_reporting_time_series AS
        WITH payment_dates AS (
            SELECT DISTINCT
                date_trunc('day', payment_date) as payment_date,
                date_trunc('week', payment_date) as week_start,
                date_trunc('month', payment_date) as month_start,
                EXTRACT(YEAR FROM payment_date) as year,
                EXTRACT(MONTH FROM payment_date) as month,
                EXTRACT(QUARTER FROM payment_date) as quarter
            FROM (
                SELECT date_paid as payment_date FROM social_protection_monetarytransfer 
                WHERE is_deleted = false AND date_paid IS NOT NULL
                UNION ALL
                SELECT date_due as payment_date FROM payroll_benefitconsumption 
                WHERE is_deleted = false AND date_due IS NOT NULL
            ) all_dates
        )
        SELECT 
            pd.payment_date,
            pd.week_start,
            pd.month_start,
            pd.year,
            pd.month,
            pd.quarter,
            us.payment_source,
            us.benefit_plan_id,
            us.province_id,
            SUM(us.total_payment_count) as total_payment_count,
            SUM(us.total_payment_amount) as total_payment_amount,
            SUM(us.unique_beneficiaries) as unique_beneficiaries,
            AVG(us.female_percentage) as female_percentage,
            AVG(us.twa_percentage) as twa_percentage
        FROM payment_dates pd
        LEFT JOIN payment_reporting_unified_summary us ON 
            us.year = pd.year AND us.month = pd.month
        GROUP BY 
            pd.payment_date, pd.week_start, pd.month_start,
            pd.year, pd.month, pd.quarter,
            us.payment_source, us.benefit_plan_id, us.province_id;
        
        CREATE INDEX idx_payment_time_date ON payment_reporting_time_series(payment_date);
        CREATE INDEX idx_payment_time_month ON payment_reporting_time_series(year, month);
        CREATE INDEX idx_payment_time_plan ON payment_reporting_time_series(benefit_plan_id);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        
        logger.info("Created payment_reporting_time_series materialized view")
    
    @staticmethod
    def create_kpi_summary_view():
        """
        Create KPI summary view for executive dashboards
        """
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS payment_reporting_kpi_summary CASCADE;
        
        CREATE MATERIALIZED VIEW payment_reporting_kpi_summary AS
        SELECT 
            year,
            month,
            SUM(total_payment_amount) as total_payment_amount,
            SUM(unique_beneficiaries) as unique_beneficiaries,
            AVG(avg_payment_per_beneficiary) as avg_payment_per_beneficiary,
            -- By source
            SUM(CASE WHEN payment_source = 'EXTERNAL' THEN total_payment_amount ELSE 0 END) as external_amount,
            SUM(CASE WHEN payment_source = 'INTERNAL' THEN total_payment_amount ELSE 0 END) as internal_amount,
            -- Inclusion metrics
            AVG(female_percentage) as female_inclusion_rate,
            AVG(twa_percentage) as twa_inclusion_rate,
            -- Coverage
            COUNT(DISTINCT province_id) as provinces_covered,
            COUNT(DISTINCT commune_id) as communes_covered,
            COUNT(DISTINCT colline_id) as collines_covered,
            COUNT(DISTINCT benefit_plan_id) as active_programs,
            -- Efficiency score (placeholder - customize based on business rules)
            CASE 
                WHEN AVG(female_percentage) >= 50 AND AVG(twa_percentage) >= 10 THEN 100
                WHEN AVG(female_percentage) >= 45 AND AVG(twa_percentage) >= 8 THEN 85
                WHEN AVG(female_percentage) >= 40 AND AVG(twa_percentage) >= 5 THEN 70
                ELSE 50
            END as payment_efficiency_score,
            CURRENT_DATE as last_updated
        FROM payment_reporting_unified_summary
        GROUP BY year, month;
        
        CREATE INDEX idx_payment_kpi_time ON payment_reporting_kpi_summary(year, month);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        
        logger.info("Created payment_reporting_kpi_summary materialized view")
    
    @staticmethod
    def create_all_views():
        """Create all payment reporting materialized views"""
        PaymentReportingMaterializedViews.create_unified_payment_summary_view()
        PaymentReportingMaterializedViews.create_location_aggregation_view()
        PaymentReportingMaterializedViews.create_benefit_plan_aggregation_view()
        PaymentReportingMaterializedViews.create_time_series_view()
        PaymentReportingMaterializedViews.create_kpi_summary_view()
        logger.info("All payment reporting materialized views created successfully")
    
    @staticmethod
    def refresh_all_views(concurrent=False):
        """Refresh all payment reporting materialized views"""
        views = [
            'payment_reporting_unified_summary',
            'payment_reporting_by_location',
            'payment_reporting_by_benefit_plan',
            'payment_reporting_time_series',
            'payment_reporting_kpi_summary',
        ]
        
        concurrently = "CONCURRENTLY" if concurrent else ""
        
        with connection.cursor() as cursor:
            for view in views:
                try:
                    sql = f"REFRESH MATERIALIZED VIEW {concurrently} {view}"
                    cursor.execute(sql)
                    logger.info(f"Refreshed {view}")
                except Exception as e:
                    logger.error(f"Error refreshing {view}: {e}")
    
    @staticmethod
    def drop_all_views():
        """Drop all payment reporting materialized views"""
        views = [
            'payment_reporting_kpi_summary',
            'payment_reporting_time_series',
            'payment_reporting_by_benefit_plan',
            'payment_reporting_by_location',
            'payment_reporting_unified_summary',
        ]
        
        with connection.cursor() as cursor:
            for view in views:
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE")
                    logger.info(f"Dropped {view}")
                except Exception as e:
                    logger.error(f"Error dropping {view}: {e}")


class Command(BaseCommand):
    """Management command for payment reporting materialized views"""
    
    help = 'Manage payment reporting materialized views'
    
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
            self.stdout.write(self.style.SUCCESS("All views created successfully"))
            
        elif action == 'refresh':
            self.stdout.write("Refreshing payment reporting materialized views...")
            PaymentReportingMaterializedViews.refresh_all_views(
                concurrent=options['concurrent']
            )
            self.stdout.write(self.style.SUCCESS("All views refreshed successfully"))
            
        elif action == 'drop':
            self.stdout.write("Dropping payment reporting materialized views...")
            PaymentReportingMaterializedViews.drop_all_views()
            self.stdout.write(self.style.SUCCESS("All views dropped successfully"))
            
        elif action == 'stats':
            self.stdout.write("Payment reporting view statistics:")
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        schemaname,
                        matviewname,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
                    FROM pg_matviews
                    WHERE matviewname LIKE 'payment_reporting_%'
                    ORDER BY matviewname
                """)
                for row in cursor.fetchall():
                    self.stdout.write(f"  {row[1]}: {row[2]}")