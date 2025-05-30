"""
Updated Materialized Views with Benefit Plan Support
Includes benefit_plan_id for filtering across all relevant views
"""

from django.db import models
from django.core.management.sql import emit_post_migrate_signal
from django.db.models.signals import post_migrate
from django.dispatch import receiver
from django.db import connection

# SQL for creating materialized views with benefit plan support
MATERIALIZED_VIEWS_SQL = {
    
    # 1. CORE BENEFICIARY AGGREGATIONS (with benefit plan)
    'dashboard_beneficiary_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_beneficiary_summary AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', gb."DateCreated") as month,
            DATE_TRUNC('quarter', gb."DateCreated") as quarter,
            EXTRACT(year FROM gb."DateCreated") as year,
            
            -- Location dimensions (if available)
            l3."LocationName" as province,
            l2."LocationName" as commune,
            l1."LocationName" as colline,
            l3."LocationId" as province_id,
            l2."LocationId" as commune_id,
            l1."LocationId" as colline_id,
            
            -- Community type classification
            CASE 
                WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') 
                THEN 'HOST'
                ELSE 'REFUGEE'
            END as community_type,
            
            -- Demographic dimensions from JSON
            COALESCE(i."Json_ext"->>'sexe', 'UNKNOWN') as gender,
            CASE WHEN i."Json_ext"->>'is_twa' = 'true' THEN true ELSE false END as is_twa,
            
            -- Age calculations
            CASE 
                WHEN i.dob IS NULL THEN 'UNKNOWN'
                WHEN DATE_PART('year', AGE(i.dob)) < 18 THEN 'UNDER_18'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 18 AND 35 THEN 'ADULT_18_35'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 36 AND 60 THEN 'ADULT_36_60'
                ELSE 'OVER_60'
            END as age_group,
            
            -- Benefit plan information
            bp."UUID" as benefit_plan_id,
            bp.code as benefit_plan_code,
            bp.name as benefit_plan_name,
            
            -- Status tracking
            gb.status,
            gb."isDeleted",
            
            -- Aggregated metrics
            COUNT(*) as beneficiary_count,
            COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN 1 END) as male_count,
            COUNT(CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN 1 END) as female_count,
            COUNT(CASE WHEN i."Json_ext"->>'is_twa' = 'true' THEN 1 END) as twa_count,
            COUNT(CASE WHEN gb.status = 'ACTIVE' THEN 1 END) as active_count,
            COUNT(CASE WHEN gb.status = 'SUSPENDED' THEN 1 END) as suspended_count
            
        FROM social_protection_groupbeneficiary gb
        JOIN social_protection_benefitplan bp ON gb.benefit_plan_id = bp."UUID"
        JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
        JOIN individual_individual i ON gi.individual_id = i."UUID"
        LEFT JOIN "tblLocations" l1 ON i.location_id = l1."LocationId"
        LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
        LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        WHERE gb."isDeleted" = false
        GROUP BY 
            month, quarter, year,
            province, commune, colline, province_id, commune_id, colline_id,
            community_type, gender, is_twa, age_group,
            bp."UUID", bp.code, bp.name, gb.status, gb."isDeleted";
    """,
    
    # 2. MONETARY TRANSFER AGGREGATIONS (with benefit plan from programme field)
    'dashboard_monetary_transfers': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_monetary_transfers AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', mt.transfer_date) as month,
            DATE_TRUNC('quarter', mt.transfer_date) as quarter,
            EXTRACT(year FROM mt.transfer_date) as year,
            
            -- Location dimensions
            l3."LocationName" as province,
            l2."LocationName" as commune,
            l3."LocationId" as province_id,
            l2."LocationId" as commune_id,
            
            -- Community classification
            CASE 
                WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') 
                THEN 'HOST'
                ELSE 'REFUGEE'
            END as community_type,
            
            -- Transfer details (actual fields)
            'MONETARY' as transfer_type,  -- Fixed value since no transfer_type field
            
            -- Benefit plan information
            mt.programme_id as benefit_plan_id,
            bp.code as benefit_plan_code,
            bp.name as benefit_plan_name,
            
            -- Planned beneficiaries
            COALESCE(mt.planned_men, 0) as planned_men,
            COALESCE(mt.planned_women, 0) as planned_women,
            COALESCE(mt.planned_twa, 0) as planned_twa,
            (COALESCE(mt.planned_men, 0) + COALESCE(mt.planned_women, 0) + COALESCE(mt.planned_twa, 0)) as total_planned,
            
            -- Paid beneficiaries
            COALESCE(mt.paid_men, 0) as paid_men,
            COALESCE(mt.paid_women, 0) as paid_women,
            COALESCE(mt.paid_twa, 0) as paid_twa,
            (COALESCE(mt.paid_men, 0) + COALESCE(mt.paid_women, 0) + COALESCE(mt.paid_twa, 0)) as total_paid,
            
            -- Performance metrics
            CASE 
                WHEN (COALESCE(mt.planned_men, 0) + COALESCE(mt.planned_women, 0) + COALESCE(mt.planned_twa, 0)) > 0 
                THEN (COALESCE(mt.paid_men, 0) + COALESCE(mt.paid_women, 0) + COALESCE(mt.paid_twa, 0))::numeric / 
                     (COALESCE(mt.planned_men, 0) + COALESCE(mt.planned_women, 0) + COALESCE(mt.planned_twa, 0))::numeric * 100
                ELSE 0
            END as completion_rate,
            
            -- Gender percentages
            CASE 
                WHEN (COALESCE(mt.paid_men, 0) + COALESCE(mt.paid_women, 0) + COALESCE(mt.paid_twa, 0)) > 0
                THEN COALESCE(mt.paid_women, 0)::numeric / 
                     (COALESCE(mt.paid_men, 0) + COALESCE(mt.paid_women, 0) + COALESCE(mt.paid_twa, 0))::numeric * 100
                ELSE 0
            END as female_percentage,
            
            -- Twa inclusion rate
            CASE 
                WHEN (COALESCE(mt.paid_men, 0) + COALESCE(mt.paid_women, 0) + COALESCE(mt.paid_twa, 0)) > 0
                THEN COALESCE(mt.paid_twa, 0)::numeric / 
                     (COALESCE(mt.paid_men, 0) + COALESCE(mt.paid_women, 0) + COALESCE(mt.paid_twa, 0))::numeric * 100
                ELSE 0
            END as twa_inclusion_rate,
            
            -- Record count
            COUNT(*) as transfer_count
            
        FROM merankabandi_monetarytransfer mt
        JOIN social_protection_benefitplan bp ON mt.programme_id = bp."UUID"
        JOIN "tblLocations" l2 ON mt.location_id = l2."LocationId"
        JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        GROUP BY 
            month, quarter, year,
            province, commune, province_id, commune_id,
            community_type,
            mt.programme_id, bp.code, bp.name,
            mt.planned_men, mt.planned_women, mt.planned_twa,
            mt.paid_men, mt.paid_women, mt.paid_twa;
    """,
    
    # 3. GRIEVANCE DASHBOARD AGGREGATIONS (with benefit plan through beneficiary)
    'dashboard_grievances': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_grievances AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', t."DateCreated") as month,
            DATE_TRUNC('quarter', t."DateCreated") as quarter,
            EXTRACT(year FROM t."DateCreated") as year,
            DATE_TRUNC('week', t."DateCreated") as week,
            
            -- Location from text fields
            t.province,
            t.commune,
            
            -- Benefit plan from beneficiary reporter (if applicable)
            CASE 
                WHEN t.reporter_type_id = (SELECT id FROM django_content_type WHERE app_label = 'social_protection' AND model = 'beneficiary')
                THEN (
                    SELECT gb.benefit_plan_id 
                    FROM social_protection_beneficiary b
                    JOIN social_protection_groupbeneficiary gb ON gb.group_id = b.group_id
                    WHERE b."UUID" = t.reporter_id::uuid
                    LIMIT 1
                )
                ELSE NULL
            END as benefit_plan_id,
            
            -- Ticket characteristics
            t.status,
            t.category,
            t.channel,
            t.priority,
            t.is_anonymous,
            CASE WHEN t.category = 'cas_sensibles' THEN true ELSE false END as is_sensitive,
            
            -- Resolution tracking
            CASE 
                WHEN t.is_resolved = 'true' AND t."DateUpdated" IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (t."DateUpdated" - t."DateCreated")) / 86400
                ELSE NULL
            END as resolution_days,
            
            -- Aggregations
            COUNT(*) as ticket_count,
            COUNT(CASE WHEN t.status = 'OPEN' THEN 1 END) as open_count,
            COUNT(CASE WHEN t.status = 'IN_PROGRESS' THEN 1 END) as in_progress_count,
            COUNT(CASE WHEN t.status = 'RESOLVED' THEN 1 END) as resolved_count,
            COUNT(CASE WHEN t.status = 'CLOSED' THEN 1 END) as closed_count,
            COUNT(CASE WHEN t.category = 'cas_sensibles' THEN 1 END) as sensitive_count,
            COUNT(CASE WHEN t.is_anonymous = 'true' THEN 1 END) as anonymous_count,
            
            -- Performance metrics
            AVG(
                CASE 
                    WHEN t.is_resolved = 'true' AND t."DateUpdated" IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (t."DateUpdated" - t."DateCreated")) / 86400
                    ELSE NULL
                END
            ) as avg_resolution_days
            
        FROM grievance_social_protection_ticket t
        GROUP BY 
            month, quarter, year, week,
            province, commune,
            benefit_plan_id,
            t.status, t.category, t.channel, t.priority, t.is_anonymous, is_sensitive;
    """,
    
    # 4. MASTER DASHBOARD SUMMARY (with benefit plan aggregation)
    'dashboard_master_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_master_summary AS
        WITH beneficiary_stats AS (
            SELECT 
                month, quarter, year,
                province, province_id,
                community_type,
                benefit_plan_id,
                SUM(beneficiary_count) as total_beneficiaries,
                SUM(active_count) as active_beneficiaries,
                SUM(male_count) as male_beneficiaries,
                SUM(female_count) as female_beneficiaries,
                SUM(twa_count) as twa_beneficiaries,
                CASE 
                    WHEN SUM(beneficiary_count) > 0 
                    THEN (SUM(female_count)::numeric / SUM(beneficiary_count)::numeric) * 100
                    ELSE 0 
                END as female_percentage,
                CASE 
                    WHEN SUM(beneficiary_count) > 0 
                    THEN (SUM(twa_count)::numeric / SUM(beneficiary_count)::numeric) * 100
                    ELSE 0 
                END as twa_inclusion_rate
            FROM dashboard_beneficiary_summary
            GROUP BY month, quarter, year, province, province_id, community_type, benefit_plan_id
        ),
        transfer_stats AS (
            SELECT 
                month, quarter, year,
                province, province_id,
                community_type,
                benefit_plan_id,
                SUM(transfer_count) as total_transfers,
                SUM(total_paid) as total_paid_beneficiaries,
                SUM(total_paid * 15000) as total_amount_paid,  -- Assuming 15000 BIF per transfer
                AVG(completion_rate) as avg_transfer_completion_rate,
                AVG(female_percentage) as avg_female_percentage,
                AVG(twa_inclusion_rate) as avg_twa_inclusion_rate
            FROM dashboard_monetary_transfers
            GROUP BY month, quarter, year, province, province_id, community_type, benefit_plan_id
        ),
        activity_stats AS (
            SELECT 
                month, quarter, year,
                province, province_id,
                community_type,
                COUNT(*) as total_activities,
                SUM(total_participants) as total_activity_participants
            FROM dashboard_activities_summary
            GROUP BY month, quarter, year, province, province_id, community_type
        ),
        project_stats AS (
            SELECT 
                month, quarter, year,
                province, province_id,
                community_type,
                SUM(project_count) as total_projects,
                SUM(project_count) as completed_projects  -- All recorded projects assumed completed
            FROM dashboard_microprojects
            GROUP BY month, quarter, year, province, province_id, community_type
        ),
        grievance_stats AS (
            SELECT 
                month, quarter, year,
                benefit_plan_id,
                SUM(ticket_count) as total_grievances,
                SUM(resolved_count + closed_count) as resolved_grievances,
                AVG(avg_resolution_days) as avg_grievance_resolution_days
            FROM dashboard_grievances
            GROUP BY month, quarter, year, benefit_plan_id
        )
        SELECT 
            b.month, b.quarter, b.year,
            b.province, b.province_id,
            b.community_type,
            b.benefit_plan_id,
            
            -- Beneficiary metrics
            b.total_beneficiaries,
            b.active_beneficiaries,
            b.male_beneficiaries,
            b.female_beneficiaries,
            b.twa_beneficiaries,
            b.female_percentage,
            b.twa_inclusion_rate,
            
            -- Transfer metrics
            COALESCE(t.total_transfers, 0) as total_transfers,
            COALESCE(t.total_paid_beneficiaries, 0) as total_paid_beneficiaries,
            COALESCE(t.total_amount_paid, 0) as total_amount_paid,
            COALESCE(t.avg_transfer_completion_rate, 0) as avg_transfer_completion_rate,
            COALESCE(t.avg_female_percentage, b.female_percentage) as avg_female_percentage,
            COALESCE(t.avg_twa_inclusion_rate, b.twa_inclusion_rate) as avg_twa_inclusion_rate,
            
            -- Activity metrics
            COALESCE(a.total_activities, 0) as total_activities,
            COALESCE(a.total_activity_participants, 0) as total_activity_participants,
            
            -- Project metrics
            COALESCE(p.total_projects, 0) as total_projects,
            COALESCE(p.completed_projects, 0) as completed_projects,
            
            -- Grievance metrics
            COALESCE(g.total_grievances, 0) as total_grievances,
            COALESCE(g.resolved_grievances, 0) as resolved_grievances,
            COALESCE(g.avg_grievance_resolution_days, 0) as avg_grievance_resolution_days
            
        FROM beneficiary_stats b
        LEFT JOIN transfer_stats t ON 
            b.month = t.month AND 
            b.quarter = t.quarter AND 
            b.year = t.year AND
            b.province = t.province AND
            b.community_type = t.community_type AND
            b.benefit_plan_id = t.benefit_plan_id
        LEFT JOIN activity_stats a ON 
            b.month = a.month AND 
            b.quarter = a.quarter AND 
            b.year = a.year AND
            b.province = a.province AND
            b.community_type = a.community_type
        LEFT JOIN project_stats p ON 
            b.month = p.month AND 
            b.quarter = p.quarter AND 
            b.year = p.year AND
            b.province = p.province AND
            b.community_type = p.community_type
        LEFT JOIN grievance_stats g ON 
            b.month = g.month AND 
            b.quarter = g.quarter AND 
            b.year = g.year AND
            b.benefit_plan_id = g.benefit_plan_id;
    """
}


class MaterializedViewManager:
    """Manager for creating and refreshing materialized views with benefit plan support"""
    
    @classmethod
    def create_all_views(cls):
        """Create all materialized views"""
        with connection.cursor() as cursor:
            for view_name, sql in MATERIALIZED_VIEWS_SQL.items():
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    cursor.execute(sql)
                    print(f"Created materialized view: {view_name}")
                except Exception as e:
                    print(f"Error creating view {view_name}: {e}")
    
    @classmethod
    def refresh_view(cls, view_name, concurrent=True):
        """Refresh a specific materialized view"""
        with connection.cursor() as cursor:
            try:
                concurrently = "CONCURRENTLY" if concurrent else ""
                cursor.execute(f"REFRESH MATERIALIZED VIEW {concurrently} {view_name}")
                print(f"Refreshed view: {view_name}")
            except Exception as e:
                print(f"Error refreshing view {view_name}: {e}")
    
    @classmethod
    def refresh_all_views(cls, concurrent=True):
        """Refresh all materialized views"""
        for view_name in MATERIALIZED_VIEWS_SQL.keys():
            cls.refresh_view(view_name, concurrent)


# Signal handler to create views after migrations
@receiver(post_migrate)
def create_materialized_views(sender, **kwargs):
    if sender.name == 'merankabandi':
        MaterializedViewManager.create_all_views()