"""
Management command to create payment reporting views
"""

from django.core.management.base import BaseCommand
from django.db import connection
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Create payment reporting materialized views'

    def handle(self, *args, **options):
        self.stdout.write("Creating payment reporting views...")
        
        # First, let's create a simple view for monetary transfers
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS payment_reporting_monetary_transfers CASCADE;
        
        CREATE MATERIALIZED VIEW payment_reporting_monetary_transfers AS
        SELECT 
            -- Time dimensions
            EXTRACT(YEAR FROM mt.transfer_date) as year,
            EXTRACT(MONTH FROM mt.transfer_date) as month,
            EXTRACT(QUARTER FROM mt.transfer_date) as quarter,
            mt.transfer_date,
            
            -- Location hierarchy
            loc."LocationId" as location_id,
            loc."LocationName" as location_name,
            loc."LocationType" as location_type,
            com."LocationId" as commune_id,
            com."LocationName" as commune_name,
            prov."LocationId" as province_id,
            prov."LocationName" as province_name,
            
            -- Programme/Benefit Plan
            mt.programme_id,
            bp."code" as programme_code,
            bp."name" as programme_name,
            bp."ceiling_per_beneficiary" as amount_per_beneficiary,
            
            -- Payment Agency
            mt.payment_agency_id,
            pp."name" as payment_agency_name,
            
            -- Beneficiary counts (pre-aggregated)
            mt.planned_women,
            mt.planned_men,
            mt.planned_twa,
            mt.planned_women + mt.planned_men + mt.planned_twa as total_planned,
            
            mt.paid_women,
            mt.paid_men,
            mt.paid_twa,
            mt.paid_women + mt.paid_men + mt.paid_twa as total_paid,
            
            -- Calculate amounts
            COALESCE(bp."ceiling_per_beneficiary", 0) * (mt.paid_women + mt.paid_men + mt.paid_twa) as total_amount_paid,
            
            -- Percentages
            CASE WHEN (mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                THEN mt.paid_women::numeric / (mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                ELSE 0 
            END as female_percentage,
            
            CASE WHEN (mt.paid_women + mt.paid_men + mt.paid_twa) > 0 
                THEN mt.paid_twa::numeric / (mt.paid_women + mt.paid_men + mt.paid_twa) * 100 
                ELSE 0 
            END as twa_percentage,
            
            -- Payment completion rate
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
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
            self.stdout.write(self.style.SUCCESS("✓ Created payment_reporting_monetary_transfers view"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"✗ Failed to create monetary transfers view: {e}"))
            return
        
        # Create summary by location
        sql_summary = """
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
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_summary)
            self.stdout.write(self.style.SUCCESS("✓ Created payment_reporting_location_summary view"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"✗ Failed to create location summary view: {e}"))
            return
        
        self.stdout.write(self.style.SUCCESS("\nPayment reporting views created successfully!"))
        self.stdout.write("\nTo refresh the data, run: python manage.py refresh_dashboard_views")