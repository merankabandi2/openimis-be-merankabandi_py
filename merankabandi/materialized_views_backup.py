"""
Materialized Views for Dashboard Performance Optimization
Comprehensive views covering all dashboard aggregation requirements from:
- Social Protection dashboards (MEDashboard, MonetaryTransfer, Activities, Results Framework)
- Grievance dashboards (Ticket aggregations and trends)
- Cross-module integrations

These views provide pre-calculated aggregations for fast dashboard loading.
"""

from django.db import models
from django.core.management.sql import emit_post_migrate_signal
from django.db.models.signals import post_migrate
from django.dispatch import receiver
from django.db import connection


# SQL for creating materialized views
MATERIALIZED_VIEWS_SQL = {
    
    # 1. CORE BENEFICIARY AGGREGATIONS
    'dashboard_beneficiary_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_beneficiary_summary AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', gb.DateCreated) as month,
            DATE_TRUNC('quarter', gb.DateCreated) as quarter,
            EXTRACT(year FROM gb.DateCreated) as year,
            
            -- Location dimensions
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
            
            -- Demographic dimensions
            i.Json_ext->>'sexe' as gender,
            CASE WHEN i.Json_ext->>'is_twa' = 'true' THEN true ELSE false END as is_twa,
            
            -- Age calculations
            CASE 
                WHEN DATE_PART('year', AGE(i.dob)) < 18 THEN 'UNDER_18'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 18 AND 35 THEN 'ADULT_18_35'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 36 AND 60 THEN 'ADULT_36_60'
                ELSE 'OVER_60'
            END as age_group,
            
            -- Benefit plan information
            bp.code as benefit_plan_code,
            bp.name as benefit_plan_name,
            
            -- Status tracking
            gb.status,
            gb.isDeleted,
            
            -- Aggregated metrics
            COUNT(*) as beneficiary_count,
            COUNT(CASE WHEN i.gender = 'M' THEN 1 END) as male_count,
            COUNT(CASE WHEN i.gender = 'F' THEN 1 END) as female_count,
            COUNT(CASE WHEN i.Json_ext->>'is_twa' = 'true' THEN 1 END) as twa_count,
            COUNT(CASE WHEN gb.status = 'ACTIVE' THEN 1 END) as active_count,
            COUNT(CASE WHEN gb.status = 'SUSPENDED' THEN 1 END) as suspended_count
            
        FROM social_protection_groupbeneficiary gb
        JOIN social_protection_benefitplan bp ON gb.benefit_plan_id = bp."UUID"
        JOIN individual_groupindividual gi ON gi.group_id = gb.group_id
        JOIN individual_individual i ON gi.individual_id = i."UUID"
        LEFT JOIN "tblLocations" l1 ON i.location_id = l1."LocationId"
        LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
        LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        WHERE gb.isDeleted = false
        GROUP BY 
            month, quarter, year,
            province, commune, colline, province_id, commune_id, colline_id,
            community_type, gender, is_twa, age_group,
            bp.code, bp.name, gb.status, gb.isDeleted;
    """,
    
    # 2. MONETARY TRANSFER AGGREGATIONS
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
            
            -- Transfer details
            mt.transfer_type,
            mt.payment_agency,
            mt.status as transfer_status,
            
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
            
            -- Financial metrics
            COALESCE(mt.amount_planned, 0) as amount_planned,
            COALESCE(mt.amount_paid, 0) as amount_paid,
            
            -- Performance metrics
            CASE 
                WHEN (COALESCE(mt.planned_men, 0) + COALESCE(mt.planned_women, 0) + COALESCE(mt.planned_twa, 0)) > 0 
                THEN (COALESCE(mt.paid_men, 0) + COALESCE(mt.paid_women, 0) + COALESCE(mt.paid_twa, 0))::numeric / 
                     (COALESCE(mt.planned_men, 0) + COALESCE(mt.planned_women, 0) + COALESCE(mt.planned_twa, 0))::numeric * 100
                ELSE 0
            END as completion_rate,
            
            CASE 
                WHEN mt.amount_planned > 0 
                THEN mt.amount_paid::numeric / mt.amount_planned::numeric * 100
                ELSE 0
            END as financial_completion_rate,
            
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
        JOIN "tblLocations" l2 ON mt.location_id = l2."LocationId"
        JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        WHERE mt.isDeleted = false
        GROUP BY 
            month, quarter, year,
            province, commune, province_id, commune_id,
            community_type, mt.transfer_type, mt.payment_agency, mt.status,
            mt.planned_men, mt.planned_women, mt.planned_twa,
            mt.paid_men, mt.paid_women, mt.paid_twa,
            mt.amount_planned, mt.amount_paid;
    """,
    
    # 3. ACTIVITIES AGGREGATIONS (Training & Sensitization)
    'dashboard_activities_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_activities_summary AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', combined.activity_date) as month,
            DATE_TRUNC('quarter', combined.activity_date) as quarter,
            EXTRACT(year FROM combined.activity_date) as year,
            
            -- Location
            combined.province,
            combined.commune,
            combined.province_id,
            combined.commune_id,
            
            -- Activity details
            combined.activity_type,
            combined.activity_category,
            combined.community_type,
            
            -- Participant aggregations
            SUM(combined.male_participants) as total_male,
            SUM(combined.female_participants) as total_female,
            SUM(combined.twa_participants) as total_twa,
            SUM(combined.total_participants) as total_participants,
            
            -- Activity counts
            COUNT(*) as activity_count,
            
            -- Performance metrics
            AVG(combined.completion_rate) as avg_completion_rate,
            SUM(CASE WHEN combined.completion_rate >= 80 THEN 1 ELSE 0 END) as successful_activities,
            
            -- Gender balance metrics
            CASE 
                WHEN SUM(combined.total_participants) > 0
                THEN SUM(combined.female_participants)::numeric / SUM(combined.total_participants)::numeric * 100
                ELSE 0
            END as female_participation_rate,
            
            -- Twa inclusion metrics
            CASE 
                WHEN SUM(combined.total_participants) > 0
                THEN SUM(combined.twa_participants)::numeric / SUM(combined.total_participants)::numeric * 100
                ELSE 0
            END as twa_participation_rate
            
        FROM (
            -- Sensitization Training
            SELECT 
                st.DateCreated as activity_date,
                'SENSITIZATION' as activity_type,
                st.activity_type as activity_category,
                l3."LocationName" as province,
                l2."LocationName" as commune,
                l3."LocationId" as province_id,
                l2."LocationId" as commune_id,
                CASE 
                    WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') 
                    THEN 'HOST' 
                    ELSE 'REFUGEE' 
                END as community_type,
                COALESCE(st.men, 0) as male_participants,
                COALESCE(st.women, 0) as female_participants,
                COALESCE(st.twa, 0) as twa_participants,
                (COALESCE(st.men, 0) + COALESCE(st.women, 0) + COALESCE(st.twa, 0)) as total_participants,
                CASE 
                    WHEN st.planned_participants > 0 
                    THEN (COALESCE(st.men, 0) + COALESCE(st.women, 0) + COALESCE(st.twa, 0))::numeric / st.planned_participants::numeric * 100
                    ELSE 100
                END as completion_rate
            FROM merankabandi_sensitizationtraining st
            JOIN "tblLocations" l2 ON st.location_id = l2."LocationId"
            JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
            WHERE st.isDeleted = false
            
            UNION ALL
            
            -- Behavior Change Promotion
            SELECT 
                bcp.DateCreated as activity_date,
                'BEHAVIOR_CHANGE' as activity_type,
                bcp.activity_type as activity_category,
                l3."LocationName" as province,
                l2."LocationName" as commune,
                l3."LocationId" as province_id,
                l2."LocationId" as commune_id,
                CASE 
                    WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') 
                    THEN 'HOST' 
                    ELSE 'REFUGEE' 
                END as community_type,
                COALESCE(bcp.men, 0) as male_participants,
                COALESCE(bcp.women, 0) as female_participants,
                COALESCE(bcp.twa, 0) as twa_participants,
                (COALESCE(bcp.men, 0) + COALESCE(bcp.women, 0) + COALESCE(bcp.twa, 0)) as total_participants,
                CASE 
                    WHEN bcp.planned_participants > 0 
                    THEN (COALESCE(bcp.men, 0) + COALESCE(bcp.women, 0) + COALESCE(bcp.twa, 0))::numeric / bcp.planned_participants::numeric * 100
                    ELSE 100
                END as completion_rate
            FROM merankabandi_behaviorchangepromotion bcp
            JOIN "tblLocations" l2 ON bcp.location_id = l2."LocationId"
            JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
            WHERE bcp.isDeleted = false
        ) combined
        GROUP BY 
            month, quarter, year,
            combined.province, combined.commune, combined.province_id, combined.commune_id,
            combined.activity_type, combined.activity_category, combined.community_type;
    """,
    
    # 4. MICRO-PROJECTS AGGREGATIONS
    'dashboard_microprojects': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_microprojects AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', mp.DateCreated) as month,
            DATE_TRUNC('quarter', mp.DateCreated) as quarter,
            EXTRACT(year FROM mp.DateCreated) as year,
            
            -- Location
            l3."LocationName" as province,
            l2."LocationName" as commune,
            l3."LocationId" as province_id,
            l2."LocationId" as commune_id,
            
            -- Project details
            mp.project_type,
            mp.status as project_status,
            
            -- Community classification
            CASE 
                WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') 
                THEN 'HOST'
                ELSE 'REFUGEE'
            END as community_type,
            
            -- Aggregations
            COUNT(*) as project_count,
            SUM(COALESCE(mp.planned_beneficiaries, 0)) as total_planned_beneficiaries,
            SUM(COALESCE(mp.actual_beneficiaries, 0)) as total_actual_beneficiaries,
            SUM(COALESCE(mp.budget_allocated, 0)) as total_budget,
            SUM(COALESCE(mp.budget_spent, 0)) as total_spent,
            
            -- Performance metrics
            AVG(
                CASE 
                    WHEN mp.planned_beneficiaries > 0 
                    THEN mp.actual_beneficiaries::numeric / mp.planned_beneficiaries::numeric * 100
                    ELSE 0
                END
            ) as avg_beneficiary_achievement_rate,
            
            AVG(
                CASE 
                    WHEN mp.budget_allocated > 0 
                    THEN mp.budget_spent::numeric / mp.budget_allocated::numeric * 100
                    ELSE 0
                END
            ) as avg_budget_utilization_rate,
            
            COUNT(CASE WHEN mp.status = 'COMPLETED' THEN 1 END) as completed_projects,
            COUNT(CASE WHEN mp.status = 'IN_PROGRESS' THEN 1 END) as ongoing_projects,
            COUNT(CASE WHEN mp.status = 'PLANNED' THEN 1 END) as planned_projects
            
        FROM merankabandi_microproject mp
        JOIN "tblLocations" l2 ON mp.location_id = l2."LocationId"
        JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        WHERE mp.isDeleted = false
        GROUP BY 
            month, quarter, year,
            province, commune, province_id, commune_id,
            mp.project_type, mp.status, community_type;
    """,
    
    # 5. GRIEVANCE DASHBOARD AGGREGATIONS
    'dashboard_grievances': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_grievances AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', t.DateCreated) as month,
            DATE_TRUNC('quarter', t.DateCreated) as quarter,
            EXTRACT(year FROM t.DateCreated) as year,
            DATE_TRUNC('week', t.DateCreated) as week,
            
            -- Location
            l3."LocationName" as province,
            l2."LocationName" as commune,
            l3."LocationId" as province_id,
            l2."LocationId" as commune_id,
            
            -- Ticket characteristics
            t.status,
            t.category,
            t.channel,
            t.priority,
            t.is_anonymous,
            CASE WHEN t.category = 'cas_sensibles' THEN true ELSE false END as is_sensitive,
            
            -- Reporter demographics
            CASE 
                WHEN i.Json_ext->>'sexe' IS NOT NULL THEN i.Json_ext->>'sexe'
                ELSE 'UNKNOWN'
            END as reporter_gender,
            
            CASE 
                WHEN DATE_PART('year', AGE(i.dob)) < 18 THEN 'UNDER_18'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 18 AND 35 THEN 'ADULT_18_35'
                WHEN DATE_PART('year', AGE(i.dob)) BETWEEN 36 AND 60 THEN 'ADULT_36_60'
                WHEN DATE_PART('year', AGE(i.dob)) > 60 THEN 'OVER_60'
                ELSE 'UNKNOWN'
            END as reporter_age_group,
            
            -- Resolution tracking
            CASE 
                WHEN t.is_resolved = 'true' AND t.DateUpdated IS NOT NULL 
                THEN EXTRACT(EPOCH FROM (t.DateUpdated - t.DateCreated)) / 86400
                ELSE NULL
            END as resolution_days,
            
            -- Aggregations
            COUNT(*) as ticket_count,
            COUNT(CASE WHEN t.status = 'OPEN' THEN 1 END) as open_count,
            COUNT(CASE WHEN t.status = 'IN_PROGRESS' THEN 1 END) as in_progress_count,
            COUNT(CASE WHEN t.status = 'RESOLVED' THEN 1 END) as resolved_count,
            COUNT(CASE WHEN t.status = 'CLOSED' THEN 1 END) as closed_count,
            COUNT(CASE WHEN t.category = 'cas_sensibles' THEN 1 END) as sensitive_count,
            COUNT(CASE WHEN t.is_anonymous = true THEN 1 END) as anonymous_count,
            
            -- Performance metrics
            AVG(
                CASE 
                    WHEN t.is_resolved = 'true' AND t.DateUpdated IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (t.DateUpdated - t.DateCreated)) / 86400
                    ELSE NULL
                END
            ) as avg_resolution_days,
            
            -- Trend calculations (vs previous period)
            LAG(COUNT(*)) OVER (
                PARTITION BY l3."LocationId", t.category 
                ORDER BY DATE_TRUNC('month', t.DateCreated)
            ) as previous_month_count
            
        FROM grievance_social_protection_ticket t
        LEFT JOIN individual_individual i ON i."UUID"::text = t.reporter_id
        LEFT JOIN "tblLocations" l2 ON LOWER(l2."LocationName") = LOWER(t.commune)
        LEFT JOIN "tblLocations" l3 ON LOWER(l3."LocationName") = LOWER(t.province)
        WHERE t.isDeleted = false
        GROUP BY 
            month, quarter, year, week,
            province, commune, province_id, commune_id,
            t.status, t.category, t.channel, t.priority, t.is_anonymous, is_sensitive,
            reporter_gender, reporter_age_group, resolution_days;
    """,
    
    # 6. INDICATORS AND ACHIEVEMENTS
    'dashboard_indicators': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_indicators AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', ia.date) as month,
            DATE_TRUNC('quarter', ia.date) as quarter,
            EXTRACT(year FROM ia.date) as year,
            
            -- Indicator details
            i.name as indicator_name,
            i.id as indicator_code,
            i.target as indicator_target,
            'unit' as indicator_unit,  -- No unit field in indicator table
            s.name as section_name,
            
            -- No location data available in indicators
            
            -- Achievement data
            SUM(ia.achieved) as total_achieved,
            AVG(ia.achieved) as avg_achieved,
            MAX(ia.achieved) as max_achieved,
            MIN(ia.achieved) as min_achieved,
            
            -- Target performance
            CASE 
                WHEN i.target > 0 
                THEN SUM(ia.achieved)::numeric / i.target::numeric * 100
                ELSE NULL
            END as achievement_percentage,
            
            -- Counts
            COUNT(*) as achievement_records,
            COUNT(CASE WHEN ia.achieved >= i.target THEN 1 END) as target_met_count
            
        FROM merankabandi_indicatorachievement ia
        JOIN merankabandi_indicator i ON ia.indicator_id = i.id
        JOIN merankabandi_section s ON i.section_id = s.id
        WHERE ia.isDeleted = false AND i.isDeleted = false
        GROUP BY 
            month, quarter, year,
            i.name, i.id, i.target, s.name;
    """,
    
    # 7. CROSS-MODULE SUMMARY (Master Dashboard)
    'dashboard_master_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_master_summary AS
        SELECT 
            -- Time dimension
            month,
            quarter, 
            year,
            
            -- Location dimension
            province,
            province_id,
            community_type,
            
            -- Beneficiaries
            SUM(beneficiary_count) as total_beneficiaries,
            SUM(active_count) as active_beneficiaries,
            SUM(male_count) as male_beneficiaries,
            SUM(female_count) as female_beneficiaries,
            SUM(twa_count) as twa_beneficiaries,
            
            -- Gender metrics
            CASE 
                WHEN SUM(beneficiary_count) > 0
                THEN SUM(female_count)::numeric / SUM(beneficiary_count)::numeric * 100
                ELSE 0
            END as female_percentage,
            
            -- Twa inclusion
            CASE 
                WHEN SUM(beneficiary_count) > 0
                THEN SUM(twa_count)::numeric / SUM(beneficiary_count)::numeric * 100
                ELSE 0
            END as twa_inclusion_rate,
            
            -- Transfer summary (from monetary_transfers view)
            COALESCE(mt_summary.total_transfers, 0) as total_transfers,
            COALESCE(mt_summary.total_amount_paid, 0) as total_amount_paid,
            COALESCE(mt_summary.avg_completion_rate, 0) as avg_transfer_completion_rate,
            
            -- Activities summary
            COALESCE(act_summary.total_activities, 0) as total_activities,
            COALESCE(act_summary.total_participants, 0) as total_activity_participants,
            
            -- Projects summary
            COALESCE(proj_summary.total_projects, 0) as total_projects,
            COALESCE(proj_summary.completed_projects, 0) as completed_projects,
            
            -- Grievances summary
            COALESCE(griev_summary.total_grievances, 0) as total_grievances,
            COALESCE(griev_summary.resolved_grievances, 0) as resolved_grievances,
            COALESCE(griev_summary.avg_resolution_days, 0) as avg_grievance_resolution_days
            
        FROM dashboard_beneficiary_summary dbs
        
        -- Join transfer data
        LEFT JOIN (
            SELECT 
                month, quarter, year, province, province_id, community_type,
                COUNT(*) as total_transfers,
                SUM(amount_paid) as total_amount_paid,
                AVG(completion_rate) as avg_completion_rate
            FROM dashboard_monetary_transfers
            GROUP BY month, quarter, year, province, province_id, community_type
        ) mt_summary ON (
            dbs.month = mt_summary.month AND 
            dbs.province_id = mt_summary.province_id AND
            dbs.community_type = mt_summary.community_type
        )
        
        -- Join activities data
        LEFT JOIN (
            SELECT 
                month, quarter, year, province, province_id, community_type,
                SUM(activity_count) as total_activities,
                SUM(total_participants) as total_participants
            FROM dashboard_activities_summary
            GROUP BY month, quarter, year, province, province_id, community_type
        ) act_summary ON (
            dbs.month = act_summary.month AND 
            dbs.province_id = act_summary.province_id AND
            dbs.community_type = act_summary.community_type
        )
        
        -- Join projects data
        LEFT JOIN (
            SELECT 
                month, quarter, year, province, province_id, community_type,
                SUM(project_count) as total_projects,
                SUM(completed_projects) as completed_projects
            FROM dashboard_microprojects
            GROUP BY month, quarter, year, province, province_id, community_type
        ) proj_summary ON (
            dbs.month = proj_summary.month AND 
            dbs.province_id = proj_summary.province_id AND
            dbs.community_type = proj_summary.community_type
        )
        
        -- Join grievances data
        LEFT JOIN (
            SELECT 
                month, quarter, year, province, province_id,
                SUM(ticket_count) as total_grievances,
                SUM(resolved_count) as resolved_grievances,
                AVG(avg_resolution_days) as avg_resolution_days
            FROM dashboard_grievances
            GROUP BY month, quarter, year, province, province_id
        ) griev_summary ON (
            dbs.month = griev_summary.month AND 
            dbs.province_id = griev_summary.province_id
        )
        
        GROUP BY 
            month, quarter, year, province, province_id, community_type,
            mt_summary.total_transfers, mt_summary.total_amount_paid, mt_summary.avg_completion_rate,
            act_summary.total_activities, act_summary.total_participants,
            proj_summary.total_projects, proj_summary.completed_projects,
            griev_summary.total_grievances, griev_summary.resolved_grievances, griev_summary.avg_resolution_days;
    """
}

# Indexes for materialized views
MATERIALIZED_VIEW_INDEXES = {
    'dashboard_beneficiary_summary': [
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_month ON dashboard_beneficiary_summary(month);',
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_quarter ON dashboard_beneficiary_summary(quarter);',
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_year ON dashboard_beneficiary_summary(year);',
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_province ON dashboard_beneficiary_summary(province_id);',
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_commune ON dashboard_beneficiary_summary(commune_id);',
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_community ON dashboard_beneficiary_summary(community_type);',
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_gender ON dashboard_beneficiary_summary(gender);',
        'CREATE INDEX IF NOT EXISTS idx_ben_summary_status ON dashboard_beneficiary_summary(status);',
    ],
    'dashboard_monetary_transfers': [
        'CREATE INDEX IF NOT EXISTS idx_mt_month ON dashboard_monetary_transfers(month);',
        'CREATE INDEX IF NOT EXISTS idx_mt_quarter ON dashboard_monetary_transfers(quarter);',
        'CREATE INDEX IF NOT EXISTS idx_mt_year ON dashboard_monetary_transfers(year);',
        'CREATE INDEX IF NOT EXISTS idx_mt_province ON dashboard_monetary_transfers(province_id);',
        'CREATE INDEX IF NOT EXISTS idx_mt_commune ON dashboard_monetary_transfers(commune_id);',
        'CREATE INDEX IF NOT EXISTS idx_mt_type ON dashboard_monetary_transfers(transfer_type);',
        'CREATE INDEX IF NOT EXISTS idx_mt_status ON dashboard_monetary_transfers(transfer_status);',
        'CREATE INDEX IF NOT EXISTS idx_mt_community ON dashboard_monetary_transfers(community_type);',
    ],
    'dashboard_activities_summary': [
        'CREATE INDEX IF NOT EXISTS idx_act_month ON dashboard_activities_summary(month);',
        'CREATE INDEX IF NOT EXISTS idx_act_quarter ON dashboard_activities_summary(quarter);',
        'CREATE INDEX IF NOT EXISTS idx_act_year ON dashboard_activities_summary(year);',
        'CREATE INDEX IF NOT EXISTS idx_act_province ON dashboard_activities_summary(province_id);',
        'CREATE INDEX IF NOT EXISTS idx_act_type ON dashboard_activities_summary(activity_type);',
        'CREATE INDEX IF NOT EXISTS idx_act_category ON dashboard_activities_summary(activity_category);',
        'CREATE INDEX IF NOT EXISTS idx_act_community ON dashboard_activities_summary(community_type);',
    ],
    'dashboard_microprojects': [
        'CREATE INDEX IF NOT EXISTS idx_mp_month ON dashboard_microprojects(month);',
        'CREATE INDEX IF NOT EXISTS idx_mp_quarter ON dashboard_microprojects(quarter);',
        'CREATE INDEX IF NOT EXISTS idx_mp_year ON dashboard_microprojects(year);',
        'CREATE INDEX IF NOT EXISTS idx_mp_province ON dashboard_microprojects(province_id);',
        'CREATE INDEX IF NOT EXISTS idx_mp_type ON dashboard_microprojects(project_type);',
        'CREATE INDEX IF NOT EXISTS idx_mp_status ON dashboard_microprojects(project_status);',
        'CREATE INDEX IF NOT EXISTS idx_mp_community ON dashboard_microprojects(community_type);',
    ],
    'dashboard_grievances': [
        'CREATE INDEX IF NOT EXISTS idx_griev_month ON dashboard_grievances(month);',
        'CREATE INDEX IF NOT EXISTS idx_griev_quarter ON dashboard_grievances(quarter);',
        'CREATE INDEX IF NOT EXISTS idx_griev_year ON dashboard_grievances(year);',
        'CREATE INDEX IF NOT EXISTS idx_griev_week ON dashboard_grievances(week);',
        'CREATE INDEX IF NOT EXISTS idx_griev_province ON dashboard_grievances(province_id);',
        'CREATE INDEX IF NOT EXISTS idx_griev_status ON dashboard_grievances(status);',
        'CREATE INDEX IF NOT EXISTS idx_griev_category ON dashboard_grievances(category);',
        'CREATE INDEX IF NOT EXISTS idx_griev_channel ON dashboard_grievances(channel);',
        'CREATE INDEX IF NOT EXISTS idx_griev_priority ON dashboard_grievances(priority);',
        'CREATE INDEX IF NOT EXISTS idx_griev_sensitive ON dashboard_grievances(is_sensitive);',
    ],
    'dashboard_indicators': [
        'CREATE INDEX IF NOT EXISTS idx_ind_month ON dashboard_indicators(month);',
        'CREATE INDEX IF NOT EXISTS idx_ind_quarter ON dashboard_indicators(quarter);',
        'CREATE INDEX IF NOT EXISTS idx_ind_year ON dashboard_indicators(year);',
        'CREATE INDEX IF NOT EXISTS idx_ind_province ON dashboard_indicators(province_id);',
        'CREATE INDEX IF NOT EXISTS idx_ind_code ON dashboard_indicators(indicator_code);',
    ],
    'dashboard_master_summary': [
        'CREATE INDEX IF NOT EXISTS idx_master_month ON dashboard_master_summary(month);',
        'CREATE INDEX IF NOT EXISTS idx_master_quarter ON dashboard_master_summary(quarter);',
        'CREATE INDEX IF NOT EXISTS idx_master_year ON dashboard_master_summary(year);',
        'CREATE INDEX IF NOT EXISTS idx_master_province ON dashboard_master_summary(province_id);',
        'CREATE INDEX IF NOT EXISTS idx_master_community ON dashboard_master_summary(community_type);',
    ]
}

# Refresh functions
REFRESH_FUNCTIONS = """
-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_dashboard_views(
    concurrent boolean DEFAULT true
) RETURNS void AS $$
DECLARE
    view_name text;
    start_time timestamp;
    end_time timestamp;
BEGIN
    start_time := clock_timestamp();
    
    -- Refresh in dependency order
    FOREACH view_name IN ARRAY ARRAY[
        'dashboard_beneficiary_summary',
        'dashboard_monetary_transfers', 
        'dashboard_activities_summary',
        'dashboard_microprojects',
        'dashboard_grievances',
        'dashboard_indicators',
        'dashboard_master_summary'
    ] LOOP
        RAISE NOTICE 'Refreshing view: %', view_name;
        
        IF concurrent THEN
            EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I', view_name);
        ELSE
            EXECUTE format('REFRESH MATERIALIZED VIEW %I', view_name);
        END IF;
    END LOOP;
    
    end_time := clock_timestamp();
    RAISE NOTICE 'All dashboard views refreshed in %', end_time - start_time;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh specific view
CREATE OR REPLACE FUNCTION refresh_dashboard_view(
    view_name text,
    concurrent boolean DEFAULT true
) RETURNS void AS $$
BEGIN
    IF concurrent THEN
        EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I', view_name);
    ELSE
        EXECUTE format('REFRESH MATERIALIZED VIEW %I', view_name);
    END IF;
    
    RAISE NOTICE 'View % refreshed', view_name;
END;
$$ LANGUAGE plpgsql;

-- Function to get view statistics
CREATE OR REPLACE FUNCTION get_dashboard_view_stats()
RETURNS TABLE(
    view_name text,
    row_count bigint,
    size_mb numeric,
    last_refresh timestamp
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        schemaname||'.'||matviewname as view_name,
        (SELECT reltuples::bigint FROM pg_class WHERE relname = matviewname) as row_count,
        ROUND((pg_total_relation_size(schemaname||'.'||matviewname) / 1024.0 / 1024.0)::numeric, 2) as size_mb,
        (SELECT last_refresh_time FROM pg_stat_user_tables WHERE relname = matviewname) as last_refresh
    FROM pg_matviews 
    WHERE matviewname LIKE 'dashboard_%'
    ORDER BY size_mb DESC;
END;
$$ LANGUAGE plpgsql;
"""


class MaterializedViewManager:
    """Manager for creating and maintaining materialized views"""
    
    @staticmethod
    def create_all_views():
        """Create all materialized views and indexes"""
        with connection.cursor() as cursor:
            # Create views
            for view_name, sql in MATERIALIZED_VIEWS_SQL.items():
                try:
                    cursor.execute(sql)
                    print(f"✓ Created materialized view: {view_name}")
                except Exception as e:
                    print(f"✗ Error creating view {view_name}: {e}")
            
            # Create indexes
            for view_name, indexes in MATERIALIZED_VIEW_INDEXES.items():
                for index_sql in indexes:
                    try:
                        cursor.execute(index_sql)
                    except Exception as e:
                        print(f"✗ Error creating index for {view_name}: {e}")
                        
            print(f"✓ Created all indexes")
            
            # Create refresh functions
            try:
                cursor.execute(REFRESH_FUNCTIONS)
                print("✓ Created refresh functions")
            except Exception as e:
                print(f"✗ Error creating refresh functions: {e}")
    
    @staticmethod
    def refresh_all_views(concurrent=True):
        """Refresh all materialized views"""
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT refresh_dashboard_views({concurrent})")
    
    @staticmethod
    def refresh_view(view_name, concurrent=True):
        """Refresh specific materialized view"""
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT refresh_dashboard_view('{view_name}', {concurrent})")
    
    @staticmethod
    def get_view_stats():
        """Get statistics for all dashboard views"""
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM get_dashboard_view_stats()")
            return cursor.fetchall()


@receiver(post_migrate)
def create_materialized_views(sender, **kwargs):
    """Create materialized views after migration"""
    if sender.name == 'merankabandi':
        MaterializedViewManager.create_all_views()