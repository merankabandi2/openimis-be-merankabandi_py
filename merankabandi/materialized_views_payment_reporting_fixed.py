"""
Payment Reporting Materialized Views - Fixed Version
Handles pre-aggregated MonetaryTransfer data and individual-level BenefitConsumption data
"""

from django.db import connection
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
        WITH external_summary AS (
            -- MonetaryTransfer (pre-aggregated external payment data)
            SELECT 
                'EXTERNAL' as payment_source,
                EXTRACT(YEAR FROM mt.transfer_date) as year,
                EXTRACT(MONTH FROM mt.transfer_date) as month,
                EXTRACT(QUARTER FROM mt.transfer_date) as quarter,
                mt.programme_id as benefit_plan_id,
                bp.name as benefit_plan_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                loc."LocationId" as colline_id,
                loc."LocationName" as colline_name,
                -- Pre-aggregated counts
                COUNT(*) as payment_count,
                SUM(mt.paid_women + mt.paid_men + mt.paid_twa) as total_beneficiaries,
                SUM(mt.paid_women) as female_beneficiaries,
                SUM(mt.paid_men) as male_beneficiaries,  
                SUM(mt.paid_twa) as twa_beneficiaries,
                -- Calculate total amount based on benefit plan
                SUM(COALESCE(bp.ceiling_per_beneficiary, 0) * (mt.paid_women + mt.paid_men + mt.paid_twa)) as total_amount,
                -- Percentages
                CASE WHEN SUM(mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                    THEN SUM(mt.paid_women)::numeric / SUM(mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                    ELSE 0 
                END as female_percentage,
                CASE WHEN SUM(mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                    THEN SUM(mt.paid_twa)::numeric / SUM(mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                    ELSE 0 
                END as twa_percentage
            FROM merankabandi_monetarytransfer mt
            LEFT JOIN social_protection_benefitplan bp ON bp."UUID" = mt.programme_id
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = mt.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            WHERE mt.transfer_date IS NOT NULL
            GROUP BY 
                EXTRACT(YEAR FROM mt.transfer_date),
                EXTRACT(MONTH FROM mt.transfer_date),
                EXTRACT(QUARTER FROM mt.transfer_date),
                mt.programme_id,
                bp.name,
                prov."LocationId", prov."LocationName",
                com."LocationId", com."LocationName",
                loc."LocationId", loc."LocationName"
        ),
        internal_summary AS (
            -- BenefitConsumption (individual-level internal payments aggregated)
            SELECT 
                'INTERNAL' as payment_source,
                EXTRACT(YEAR FROM bc.date_due) as year,
                EXTRACT(MONTH FROM bc.date_due) as month,
                EXTRACT(QUARTER FROM bc.date_due) as quarter,
                pbc.payroll__payment_plan__benefit_plan_id as benefit_plan_id,
                bp.name as benefit_plan_name,
                prov."LocationId" as province_id,
                prov."LocationName" as province_name,
                com."LocationId" as commune_id,
                com."LocationName" as commune_name,
                loc."LocationId" as colline_id,
                loc."LocationName" as colline_name,
                -- Aggregated counts
                COUNT(DISTINCT bc.id) as payment_count,
                COUNT(DISTINCT bc.individual_id) as total_beneficiaries,
                COUNT(DISTINCT CASE WHEN i.json_ext->>'sexe' = 'F' THEN bc.individual_id END) as female_beneficiaries,
                COUNT(DISTINCT CASE WHEN i.json_ext->>'sexe' = 'M' THEN bc.individual_id END) as male_beneficiaries,
                COUNT(DISTINCT CASE WHEN g.json_ext->>'menage_mutwa' = 'OUI' THEN bc.individual_id END) as twa_beneficiaries,
                SUM(bc.amount) as total_amount,
                -- Percentages
                CASE WHEN COUNT(DISTINCT bc.individual_id) > 0
                    THEN COUNT(DISTINCT CASE WHEN i.json_ext->>'sexe' = 'F' THEN bc.individual_id END)::numeric / 
                         COUNT(DISTINCT bc.individual_id) * 100
                    ELSE 0
                END as female_percentage,
                CASE WHEN COUNT(DISTINCT bc.individual_id) > 0
                    THEN COUNT(DISTINCT CASE WHEN g.json_ext->>'menage_mutwa' = 'OUI' THEN bc.individual_id END)::numeric / 
                         COUNT(DISTINCT bc.individual_id) * 100
                    ELSE 0
                END as twa_percentage
            FROM payroll_benefitconsumption bc
            INNER JOIN payroll_payrollbenefitconsumption pbc ON pbc.benefit_consumption_id = bc.id
            INNER JOIN individual_individual i ON i.id = bc.individual_id AND i.is_deleted = false
            INNER JOIN individual_groupindividual gi ON gi.individual_id = i.id AND gi.is_deleted = false
            INNER JOIN social_protection_group g ON g.id = gi.group_id AND g.is_deleted = false
            LEFT JOIN social_protection_benefitplan bp ON bp."UUID" = pbc.payroll__payment_plan__benefit_plan_id
            LEFT JOIN "tblLocations" loc ON loc."LocationId" = g.location_id
            LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
            LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            WHERE bc.is_deleted = false
                AND bc.status = 'RECONCILED'
                AND bc.date_due IS NOT NULL
            GROUP BY 
                EXTRACT(YEAR FROM bc.date_due),
                EXTRACT(MONTH FROM bc.date_due),
                EXTRACT(QUARTER FROM bc.date_due),
                pbc.payroll__payment_plan__benefit_plan_id,
                bp.name,
                prov."LocationId", prov."LocationName",
                com."LocationId", com."LocationName",
                loc."LocationId", loc."LocationName"
        )
        -- Combine both sources
        SELECT 
            payment_source,
            year::integer,
            month::integer,
            quarter::integer,
            benefit_plan_id,
            benefit_plan_name,
            province_id,
            province_name,
            commune_id,
            commune_name,
            colline_id,
            colline_name,
            payment_count,
            total_beneficiaries,
            female_beneficiaries,
            male_beneficiaries,
            twa_beneficiaries,
            total_amount,
            female_percentage,
            twa_percentage,
            CURRENT_DATE as last_updated
        FROM external_summary
        UNION ALL
        SELECT 
            payment_source,
            year::integer,
            month::integer,
            quarter::integer,
            benefit_plan_id,
            benefit_plan_name,
            province_id,
            province_name,
            commune_id,
            commune_name,
            colline_id,
            colline_name,
            payment_count,
            total_beneficiaries,
            female_beneficiaries,
            male_beneficiaries,
            twa_beneficiaries,
            total_amount,
            female_percentage,
            twa_percentage,
            CURRENT_DATE as last_updated
        FROM internal_summary;
        
        CREATE INDEX idx_payment_unified_year_month ON payment_reporting_unified_summary(year, month);
        CREATE INDEX idx_payment_unified_location ON payment_reporting_unified_summary(province_id, commune_id, colline_id);
        CREATE INDEX idx_payment_unified_benefit_plan ON payment_reporting_unified_summary(benefit_plan_id);
        CREATE INDEX idx_payment_unified_source ON payment_reporting_unified_summary(payment_source);
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
            benefit_plan_name,
            province_id,
            province_name,
            commune_id,
            commune_name,
            colline_id,
            colline_name,
            SUM(payment_count) as total_payments,
            SUM(total_beneficiaries) as total_beneficiaries,
            SUM(female_beneficiaries) as female_beneficiaries,
            SUM(male_beneficiaries) as male_beneficiaries,
            SUM(twa_beneficiaries) as twa_beneficiaries,
            SUM(total_amount) as total_amount,
            AVG(female_percentage) as avg_female_percentage,
            AVG(twa_percentage) as avg_twa_percentage
        FROM payment_reporting_unified_summary
        GROUP BY 
            payment_source, year, month, 
            benefit_plan_id, benefit_plan_name,
            province_id, province_name,
            commune_id, commune_name,
            colline_id, colline_name;
        
        CREATE INDEX idx_payment_loc_hierarchy ON payment_reporting_by_location(province_id, commune_id, colline_id);
        CREATE INDEX idx_payment_loc_time ON payment_reporting_by_location(year, month);
        CREATE INDEX idx_payment_loc_plan ON payment_reporting_by_location(benefit_plan_id);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        
        logger.info("Created payment_reporting_by_location materialized view")
    
    @staticmethod
    def create_time_series_view():
        """
        Create time series view for trend analysis
        """
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS payment_reporting_time_series CASCADE;
        
        CREATE MATERIALIZED VIEW payment_reporting_time_series AS
        SELECT 
            payment_source,
            year,
            month,
            quarter,
            benefit_plan_id,
            benefit_plan_name,
            SUM(payment_count) as total_payments,
            SUM(total_beneficiaries) as total_beneficiaries,
            SUM(female_beneficiaries) as female_beneficiaries,
            SUM(male_beneficiaries) as male_beneficiaries,
            SUM(twa_beneficiaries) as twa_beneficiaries,
            SUM(total_amount) as total_amount,
            AVG(female_percentage) as avg_female_percentage,
            AVG(twa_percentage) as avg_twa_percentage,
            -- Running totals
            SUM(SUM(total_amount)) OVER (PARTITION BY benefit_plan_id ORDER BY year, month) as cumulative_amount,
            SUM(SUM(total_beneficiaries)) OVER (PARTITION BY benefit_plan_id ORDER BY year, month) as cumulative_beneficiaries
        FROM payment_reporting_unified_summary
        GROUP BY 
            payment_source, year, month, quarter,
            benefit_plan_id, benefit_plan_name
        ORDER BY year, month;
        
        CREATE INDEX idx_payment_ts_time ON payment_reporting_time_series(year, month);
        CREATE INDEX idx_payment_ts_plan ON payment_reporting_time_series(benefit_plan_id);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        
        logger.info("Created payment_reporting_time_series materialized view")
    
    @staticmethod
    def create_all_views():
        """Create all payment reporting views"""
        PaymentReportingMaterializedViews.create_unified_payment_summary_view()
        PaymentReportingMaterializedViews.create_location_aggregation_view()
        PaymentReportingMaterializedViews.create_time_series_view()
        logger.info("All payment reporting views created successfully")
    
    @staticmethod
    def refresh_all_views(concurrent=False):
        """Refresh all payment reporting views"""
        views = [
            'payment_reporting_unified_summary',
            'payment_reporting_by_location',
            'payment_reporting_time_series',
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
        """Drop all payment reporting views"""
        views = [
            'payment_reporting_time_series',
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