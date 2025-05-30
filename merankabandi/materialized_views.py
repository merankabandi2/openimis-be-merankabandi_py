"""
CORRECTED Materialized Views for Dashboard Performance Optimization
Based on actual database schema analysis
"""

from django.db import models
from django.core.management.sql import emit_post_migrate_signal
from django.db.models.signals import post_migrate
from django.dispatch import receiver
from django.db import connection

# SQL for creating materialized views with correct field names
MATERIALIZED_VIEWS_SQL = {
    
    # 1. CORE BENEFICIARY AGGREGATIONS
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
            bp.code, bp.name, gb.status, gb."isDeleted";
    """,
    
    # 2. OPTIMIZED MONETARY TRANSFER DASHBOARD AGGREGATIONS  
    'dashboard_monetary_transfers': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_monetary_transfers AS
        SELECT 
            -- Summary for fast overview queries
            'TRANSFERS_SUMMARY' as summary_type,
            
            -- Payment cycle metrics (from payroll)
            (SELECT COUNT(*) FROM payroll_payroll WHERE "isDeleted" = false) as total_payment_cycles,
            (SELECT COUNT(*) FROM payroll_benefitconsumption WHERE "isDeleted" = false) as total_benefit_consumptions,
            (SELECT COALESCE(SUM(CAST("Amount" AS decimal)), 0) FROM payroll_benefitconsumption WHERE "isDeleted" = false) as total_amount_paid,
            
            -- Average amount per beneficiary
            CASE 
                WHEN (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false) > 0
                THEN (SELECT COALESCE(SUM(CAST("Amount" AS decimal)), 0) FROM payroll_benefitconsumption WHERE "isDeleted" = false) / 
                     (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false)
                ELSE 0
            END as avg_amount_per_beneficiary,
            
            -- Completion rate (benefit consumptions vs beneficiaries)
            CASE 
                WHEN (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false) > 0
                THEN (SELECT COUNT(DISTINCT individual_id) FROM payroll_benefitconsumption WHERE "isDeleted" = false)::numeric / 
                     (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false)::numeric * 100
                ELSE 0
            END as payment_completion_rate,
            
            -- Current date for freshness
            CURRENT_DATE as report_date,
            DATE_TRUNC('month', CURRENT_DATE) as month,
            DATE_TRUNC('quarter', CURRENT_DATE) as quarter,
            EXTRACT(year FROM CURRENT_DATE) as year;
    """,

    # 2b. BENEFIT CONSUMPTION BY PROVINCE (for geographic breakdowns)
    'dashboard_transfers_by_province': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_transfers_by_province AS
        SELECT 
            l3."LocationName" as province,
            l3."LocationId" as province_id,
            COUNT(bc.*) as consumption_count,
            COALESCE(SUM(CAST(bc."Amount" AS decimal)), 0) as total_amount,
            COUNT(DISTINCT bc.individual_id) as unique_beneficiaries,
            AVG(CAST(bc."Amount" AS decimal)) as avg_amount_per_consumption,
            
            -- Percentage of total consumptions
            COUNT(bc.*)::numeric / (SELECT COUNT(*) FROM payroll_benefitconsumption WHERE "isDeleted" = false)::numeric * 100 as percentage_of_consumptions,
            
            -- Current date for freshness
            CURRENT_DATE as report_date
            
        FROM payroll_benefitconsumption bc
        JOIN individual_individual i ON bc.individual_id = i."UUID"
        JOIN "tblLocations" l1 ON i.location_id = l1."LocationId"  -- colline
        JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"  -- commune  
        JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"  -- province
        WHERE bc."isDeleted" = false AND l3."LocationType" = 'D'
        GROUP BY l3."LocationName", l3."LocationId", report_date
        ORDER BY total_amount DESC;
    """,

    # 2c. BENEFIT CONSUMPTION BY TIME (for trends)
    'dashboard_transfers_by_time': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_transfers_by_time AS
        SELECT 
            DATE_TRUNC('month', bc."DateCreated") as month,
            DATE_TRUNC('quarter', bc."DateCreated") as quarter,
            EXTRACT(year FROM bc."DateCreated") as year,
            COUNT(*) as consumption_count,
            COALESCE(SUM(CAST(bc."Amount" AS decimal)), 0) as total_amount,
            COUNT(DISTINCT bc.individual_id) as unique_beneficiaries,
            AVG(CAST(bc."Amount" AS decimal)) as avg_amount_per_consumption,
            
            -- Current date for freshness
            CURRENT_DATE as report_date
            
        FROM payroll_benefitconsumption bc
        WHERE bc."isDeleted" = false
        GROUP BY month, quarter, year, report_date
        ORDER BY year, quarter, month;
    """,
    
    # 3. OPTIMIZED ACTIVITIES DASHBOARD AGGREGATIONS
    'dashboard_activities_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_activities_summary AS
        SELECT 
            -- Summary for fast overview queries
            'ACTIVITIES_SUMMARY' as summary_type,
            
            -- Overall activity metrics
            COUNT(*) as total_activities,
            SUM(COALESCE(male_participants, 0)) as total_male_participants,
            SUM(COALESCE(female_participants, 0)) as total_female_participants,
            SUM(COALESCE(twa_participants, 0)) as total_twa_participants,
            SUM(COALESCE(male_participants, 0) + COALESCE(female_participants, 0) + COALESCE(twa_participants, 0)) as total_participants,
            
            -- Gender participation rates
            CASE 
                WHEN SUM(COALESCE(male_participants, 0) + COALESCE(female_participants, 0) + COALESCE(twa_participants, 0)) > 0
                THEN SUM(COALESCE(female_participants, 0))::numeric / SUM(COALESCE(male_participants, 0) + COALESCE(female_participants, 0) + COALESCE(twa_participants, 0))::numeric * 100
                ELSE 0
            END as female_participation_rate,
            
            CASE 
                WHEN SUM(COALESCE(male_participants, 0) + COALESCE(female_participants, 0) + COALESCE(twa_participants, 0)) > 0
                THEN SUM(COALESCE(twa_participants, 0))::numeric / SUM(COALESCE(male_participants, 0) + COALESCE(female_participants, 0) + COALESCE(twa_participants, 0))::numeric * 100
                ELSE 0
            END as twa_participation_rate,
            
            -- Current date for freshness
            CURRENT_DATE as report_date,
            DATE_TRUNC('month', CURRENT_DATE) as month,
            DATE_TRUNC('quarter', CURRENT_DATE) as quarter,
            EXTRACT(year FROM CURRENT_DATE) as year
            
        FROM (
            -- Sensitization Training
            SELECT 
                sensitization_date as activity_date,
                'SENSITIZATION' as activity_type,
                category as activity_category,
                male_participants,
                female_participants,
                twa_participants
            FROM merankabandi_sensitizationtraining
            
            UNION ALL
            
            -- Behavior Change Promotion
            SELECT 
                report_date as activity_date,
                'BEHAVIOR_CHANGE' as activity_type,
                'BEHAVIOR_CHANGE' as activity_category,
                male_participants,
                female_participants,
                twa_participants
            FROM merankabandi_behaviorchangepromotion
        ) combined
        GROUP BY summary_type, report_date, month, quarter, year;
    """,

    # 3b. ACTIVITIES BY TYPE (for detailed breakdowns)
    'dashboard_activities_by_type': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_activities_by_type AS
        SELECT 
            activity_type,
            activity_category,
            COUNT(*) as activity_count,
            SUM(COALESCE(male_participants, 0)) as male_participants,
            SUM(COALESCE(female_participants, 0)) as female_participants,
            SUM(COALESCE(twa_participants, 0)) as twa_participants,
            SUM(COALESCE(male_participants, 0) + COALESCE(female_participants, 0) + COALESCE(twa_participants, 0)) as total_participants,
            
            -- Percentages
            COUNT(*)::numeric / (SELECT COUNT(*) FROM (
                SELECT 1 FROM merankabandi_sensitizationtraining
                UNION ALL
                SELECT 1 FROM merankabandi_behaviorchangepromotion
            ) all_activities)::numeric * 100 as percentage_of_activities,
            
            -- Current date for freshness
            CURRENT_DATE as report_date
            
        FROM (
            SELECT 
                'SENSITIZATION' as activity_type,
                category as activity_category,
                male_participants,
                female_participants,
                twa_participants
            FROM merankabandi_sensitizationtraining
            
            UNION ALL
            
            SELECT 
                'BEHAVIOR_CHANGE' as activity_type,
                'BEHAVIOR_CHANGE' as activity_category,
                male_participants,
                female_participants,
                twa_participants
            FROM merankabandi_behaviorchangepromotion
        ) combined
        GROUP BY activity_type, activity_category, report_date
        ORDER BY activity_count DESC;
    """,
    
    # 4. MICRO-PROJECTS AGGREGATIONS (simplified based on actual fields)
    'dashboard_microprojects': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_microprojects AS
        SELECT 
            -- Time dimensions
            DATE_TRUNC('month', mp.report_date) as month,
            DATE_TRUNC('quarter', mp.report_date) as quarter,
            EXTRACT(year FROM mp.report_date) as year,
            
            -- Location
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
            
            -- Aggregations based on actual fields
            COUNT(*) as project_count,
            SUM(COALESCE(mp.male_participants, 0)) as total_male_participants,
            SUM(COALESCE(mp.female_participants, 0)) as total_female_participants,
            SUM(COALESCE(mp.twa_participants, 0)) as total_twa_participants,
            SUM(COALESCE(mp.agriculture_beneficiaries, 0)) as total_agriculture_beneficiaries,
            SUM(COALESCE(mp.livestock_beneficiaries, 0)) as total_livestock_beneficiaries,
            SUM(COALESCE(mp.commerce_services_beneficiaries, 0)) as total_commerce_beneficiaries,
            
            -- Total beneficiaries across all types
            SUM(
                COALESCE(mp.agriculture_beneficiaries, 0) + 
                COALESCE(mp.livestock_beneficiaries, 0) + 
                COALESCE(mp.commerce_services_beneficiaries, 0)
            ) as total_beneficiaries
            
        FROM merankabandi_microproject mp
        JOIN "tblLocations" l2 ON mp.location_id = l2."LocationId"
        JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        GROUP BY 
            month, quarter, year,
            province, commune, province_id, commune_id,
            community_type;
    """,
    
    # 5. OPTIMIZED GRIEVANCE DASHBOARD AGGREGATIONS  
    'dashboard_grievances': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_grievances AS
        SELECT 
            -- Overall summary (for fast summary queries)
            'SUMMARY' as summary_type,
            
            -- Summary metrics
            COUNT(*) as total_tickets,
            COUNT(CASE WHEN status = 'OPEN' THEN 1 END) as open_tickets,
            COUNT(CASE WHEN status = 'IN_PROGRESS' THEN 1 END) as in_progress_tickets,
            COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) as resolved_tickets,
            COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) as closed_tickets,
            COUNT(CASE WHEN category = 'cas_sensibles' THEN 1 END) as sensitive_tickets,
            COUNT(CASE WHEN is_anonymous = 'true' THEN 1 END) as anonymous_tickets,
            
            -- Average resolution time for resolved tickets
            AVG(
                CASE 
                    WHEN is_resolved = 'true' AND "DateUpdated" IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM ("DateUpdated" - "DateCreated")) / 86400
                    ELSE NULL
                END
            ) as avg_resolution_days,
            
            -- Current date for freshness tracking
            CURRENT_DATE as report_date,
            DATE_TRUNC('month', CURRENT_DATE) as month,
            DATE_TRUNC('quarter', CURRENT_DATE) as quarter,
            EXTRACT(year FROM CURRENT_DATE) as year
            
        FROM grievance_social_protection_ticket 
        WHERE "isDeleted" = false
        GROUP BY summary_type, report_date, month, quarter, year;
    """,

    # 6. GRIEVANCE STATUS DISTRIBUTION (for fast status breakdown)
    'dashboard_grievance_status': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_grievance_status AS
        SELECT 
            status,
            COUNT(*) as count,
            COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
            
            -- Current date for freshness tracking
            CURRENT_DATE as report_date
            
        FROM grievance_social_protection_ticket 
        WHERE "isDeleted" = false AND status IS NOT NULL
        GROUP BY status, report_date
        ORDER BY count DESC;
    """,

    # 7. GRIEVANCE CATEGORY VIEWS MOVED TO grievance_category_views.py
    # (removed to avoid conflicts with JSON array category handling)
    
    # 8. OPTIMIZED RESULTS FRAMEWORK DASHBOARD AGGREGATIONS
    'dashboard_results_framework': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_results_framework AS
        SELECT 
            -- Summary for fast overview queries
            'RESULTS_FRAMEWORK_SUMMARY' as summary_type,
            
            -- Overall framework metrics
            (SELECT COUNT(*) FROM merankabandi_section) as total_sections,
            (SELECT COUNT(*) FROM merankabandi_indicator) as total_indicators,
            (SELECT COUNT(*) FROM merankabandi_indicatorachievement) as total_achievements,
            
            -- Achievement performance
            (SELECT COUNT(*) FROM merankabandi_indicatorachievement ia 
             JOIN merankabandi_indicator i ON ia.indicator_id = i.id 
             WHERE ia.achieved >= i.target) as indicators_meeting_target,
             
            -- Overall achievement rate
            CASE 
                WHEN (SELECT COUNT(*) FROM merankabandi_indicator WHERE target > 0) > 0
                THEN (SELECT COUNT(*) FROM merankabandi_indicatorachievement ia 
                      JOIN merankabandi_indicator i ON ia.indicator_id = i.id 
                      WHERE ia.achieved >= i.target)::numeric / 
                     (SELECT COUNT(*) FROM merankabandi_indicator WHERE target > 0)::numeric * 100
                ELSE 0
            END as overall_achievement_rate,
            
            -- Average achievement percentage across all indicators
            (SELECT AVG(
                CASE 
                    WHEN i.target > 0 THEN ia.achieved::numeric / i.target::numeric * 100
                    ELSE NULL
                END
            ) FROM merankabandi_indicatorachievement ia 
            JOIN merankabandi_indicator i ON ia.indicator_id = i.id 
            WHERE i.target > 0) as avg_achievement_percentage,
            
            -- Current date for freshness
            CURRENT_DATE as report_date,
            DATE_TRUNC('month', CURRENT_DATE) as month,
            DATE_TRUNC('quarter', CURRENT_DATE) as quarter,
            EXTRACT(year FROM CURRENT_DATE) as year
            
        GROUP BY summary_type, report_date, month, quarter, year;
    """,

    # 8b. INDICATORS BY SECTION (for section breakdown)
    'dashboard_indicators_by_section': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_indicators_by_section AS
        SELECT 
            s.id as section_id,
            s.name as section_name,
            COUNT(i.*) as indicator_count,
            COUNT(ia.*) as achievement_count,
            
            -- Achievement metrics per section
            COALESCE(AVG(ia.achieved), 0) as avg_achieved,
            COALESCE(SUM(ia.achieved), 0) as total_achieved,
            COALESCE(MAX(ia.achieved), 0) as max_achieved,
            
            -- Target performance per section
            CASE 
                WHEN COUNT(CASE WHEN i.target > 0 THEN 1 END) > 0
                THEN COUNT(CASE WHEN ia.achieved >= i.target THEN 1 END)::numeric / 
                     COUNT(CASE WHEN i.target > 0 THEN 1 END)::numeric * 100
                ELSE 0
            END as section_achievement_rate,
            
            -- Average achievement percentage for section
            AVG(
                CASE 
                    WHEN i.target > 0 THEN ia.achieved::numeric / i.target::numeric * 100
                    ELSE NULL
                END
            ) as avg_achievement_percentage,
            
            -- Current date for freshness
            CURRENT_DATE as report_date
            
        FROM merankabandi_section s
        LEFT JOIN merankabandi_indicator i ON s.id = i.section_id
        LEFT JOIN merankabandi_indicatorachievement ia ON i.id = ia.indicator_id
        GROUP BY s.id, s.name, report_date
        ORDER BY section_achievement_rate DESC;
    """,

    # 8c. INDICATOR PERFORMANCE DETAILS (for detailed indicator view)
    'dashboard_indicator_performance': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_indicator_performance AS
        SELECT 
            i.id as indicator_id,
            i.name as indicator_name,
            i.target as indicator_target,
            s.name as section_name,
            
            -- Achievement data
            COUNT(ia.*) as achievement_count,
            COALESCE(SUM(ia.achieved), 0) as total_achieved,
            COALESCE(AVG(ia.achieved), 0) as avg_achieved,
            COALESCE(MAX(ia.achieved), 0) as max_achieved,
            COALESCE(MIN(ia.achieved), 0) as min_achieved,
            
            -- Target performance
            CASE 
                WHEN i.target > 0 AND COUNT(ia.*) > 0
                THEN SUM(ia.achieved)::numeric / i.target::numeric * 100
                ELSE 0
            END as achievement_percentage,
            
            -- Target achievement status
            CASE 
                WHEN COUNT(ia.*) > 0 AND MAX(ia.achieved) >= i.target THEN 'TARGET_MET'
                WHEN COUNT(ia.*) > 0 AND MAX(ia.achieved) >= (i.target * 0.8) THEN 'NEARLY_MET'
                WHEN COUNT(ia.*) > 0 THEN 'BELOW_TARGET'
                ELSE 'NO_DATA'
            END as achievement_status,
            
            -- Current date for freshness
            CURRENT_DATE as report_date
            
        FROM merankabandi_indicator i
        JOIN merankabandi_section s ON i.section_id = s.id
        LEFT JOIN merankabandi_indicatorachievement ia ON i.id = ia.indicator_id
        GROUP BY i.id, i.name, i.target, s.name, report_date
        ORDER BY achievement_percentage DESC;
    """,
    
    # 9. REAL MASTER SUMMARY WITH ACTUAL DATA
    'dashboard_master_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_master_summary AS
        SELECT 
            -- Time dimensions
            CURRENT_DATE as report_date,
            DATE_TRUNC('month', CURRENT_DATE) as month,
            DATE_TRUNC('quarter', CURRENT_DATE) as quarter,
            EXTRACT(year FROM CURRENT_DATE) as year,
            
            -- High-level counts
            'SUMMARY' as summary_type,
            
            -- Real beneficiary counts
            (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false) as total_beneficiaries,
            (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false) as active_beneficiaries,
            
            -- Real payment cycle counts (transfers) - using correct table
            (SELECT COUNT(*) FROM payroll_payroll WHERE "isDeleted" = false) as total_transfers,
            
            -- Real total amount paid from benefit consumption
            (SELECT COALESCE(SUM(CAST("Amount" AS decimal)), 0) FROM payroll_benefitconsumption WHERE "isDeleted" = false) as total_amount_paid,
            
            -- Real household counts
            (SELECT COUNT(*) FROM individual_group WHERE "isDeleted" = false) as total_households,
            
            -- Real individual counts
            (SELECT COUNT(*) FROM individual_individual WHERE "isDeleted" = false) as total_individuals,
            (SELECT COUNT(CASE WHEN "Json_ext"->>'sexe' = 'M' THEN 1 END) FROM individual_individual WHERE "isDeleted" = false) as total_male,
            (SELECT COUNT(CASE WHEN "Json_ext"->>'sexe' = 'F' THEN 1 END) FROM individual_individual WHERE "isDeleted" = false) as total_female,
            (SELECT COUNT(CASE WHEN "Json_ext"->>'is_twa' = 'true' THEN 1 END) FROM individual_individual WHERE "isDeleted" = false) as total_twa,
            
            -- Real grievance counts
            (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false) as total_grievances,
            (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false AND status = 'RESOLVED') as resolved_grievances,
            
            -- Provinces with active beneficiaries (correct hierarchy)
            (SELECT COUNT(DISTINCT l3."LocationId") 
             FROM social_protection_groupbeneficiary gb
             JOIN individual_group ig ON gb.group_id = ig."UUID"
             JOIN individual_groupindividual gi ON gi.group_id = ig."UUID"
             JOIN individual_individual i ON gi.individual_id = i."UUID"
             JOIN "tblLocations" l1 ON i.location_id = l1."LocationId"  -- colline
             JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"  -- commune
             JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"  -- province
             WHERE gb."isDeleted" = false AND l3."LocationType" = 'D') as active_provinces,
            
            -- Other metrics
            (SELECT COUNT(*) FROM merankabandi_indicatorachievement) as total_achievements
            
        GROUP BY report_date, month, quarter, year, summary_type;
    """,

    # 10. INDIVIDUAL AND HOUSEHOLD BREAKDOWN FOR FAST GENDER/DEMOGRAPHIC QUERIES
    'dashboard_individual_summary': """
        CREATE MATERIALIZED VIEW IF NOT EXISTS dashboard_individual_summary AS
        SELECT 
            -- Time dimensions
            CURRENT_DATE as report_date,
            DATE_TRUNC('month', CURRENT_DATE) as month,
            DATE_TRUNC('quarter', CURRENT_DATE) as quarter,
            EXTRACT(year FROM CURRENT_DATE) as year,
            
            -- Summary type
            'INDIVIDUAL_SUMMARY' as summary_type,
            
            -- Individual gender breakdown
            COUNT(*) as total_individuals,
            COUNT(CASE WHEN "Json_ext"->>'sexe' = 'M' THEN 1 END) as total_male,
            COUNT(CASE WHEN "Json_ext"->>'sexe' = 'F' THEN 1 END) as total_female,
            COUNT(CASE WHEN "Json_ext"->>'is_twa' = 'true' THEN 1 END) as total_twa,
            
            -- Gender percentages
            CASE 
                WHEN COUNT(*) > 0 
                THEN COUNT(CASE WHEN "Json_ext"->>'sexe' = 'M' THEN 1 END)::numeric / COUNT(*)::numeric * 100
                ELSE 0
            END as male_percentage,
            
            CASE 
                WHEN COUNT(*) > 0 
                THEN COUNT(CASE WHEN "Json_ext"->>'sexe' = 'F' THEN 1 END)::numeric / COUNT(*)::numeric * 100
                ELSE 0
            END as female_percentage,
            
            CASE 
                WHEN COUNT(*) > 0 
                THEN COUNT(CASE WHEN "Json_ext"->>'is_twa' = 'true' THEN 1 END)::numeric / COUNT(*)::numeric * 100
                ELSE 0
            END as twa_percentage,
            
            -- Household counts
            (SELECT COUNT(*) FROM individual_group WHERE "isDeleted" = false) as total_households,
            
            -- Beneficiary counts
            (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false) as total_beneficiaries
            
        FROM individual_individual 
        WHERE "isDeleted" = false
        GROUP BY report_date, month, quarter, year, summary_type;
    """
}

# Updated indexes for the corrected views
MATERIALIZED_VIEW_INDEXES = {
    'dashboard_beneficiary_summary': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beneficiary_summary_month ON dashboard_beneficiary_summary (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beneficiary_summary_province ON dashboard_beneficiary_summary (province);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beneficiary_summary_community ON dashboard_beneficiary_summary (community_type);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beneficiary_summary_gender ON dashboard_beneficiary_summary (gender);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_beneficiary_summary_status ON dashboard_beneficiary_summary (status);'
    ],
    'dashboard_monetary_transfers': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_summary_type ON dashboard_monetary_transfers (summary_type);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_month ON dashboard_monetary_transfers (month);'
    ],
    'dashboard_transfers_by_province': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_province_id ON dashboard_transfers_by_province (province_id);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_province_amount ON dashboard_transfers_by_province (total_amount);'
    ],
    'dashboard_transfers_by_time': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_time_month ON dashboard_transfers_by_time (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_time_year ON dashboard_transfers_by_time (year);'
    ],
    'dashboard_activities_summary': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_activities_summary_type ON dashboard_activities_summary (summary_type);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_activities_month ON dashboard_activities_summary (month);'
    ],
    'dashboard_activities_by_type': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_activities_type ON dashboard_activities_by_type (activity_type);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_activities_category ON dashboard_activities_by_type (activity_category);'
    ],
    'dashboard_microprojects': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_microprojects_month ON dashboard_microprojects (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_microprojects_province ON dashboard_microprojects (province);'
    ],
    'dashboard_grievances': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_grievances_month ON dashboard_grievances (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_grievances_summary_type ON dashboard_grievances (summary_type);'
    ],
    'dashboard_grievance_status': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_grievance_status_status ON dashboard_grievance_status (status);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_grievance_status_report_date ON dashboard_grievance_status (report_date);'
    ],
    'dashboard_indicators': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_indicators_month ON dashboard_indicators (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_indicators_name ON dashboard_indicators (indicator_name);'
    ],
    'dashboard_master_summary': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_master_summary_month ON dashboard_master_summary (month);'
    ],
    'dashboard_individual_summary': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_individual_summary_month ON dashboard_individual_summary (month);'
    ]
}

# Refresh functions (same as before)
REFRESH_FUNCTIONS = """
-- Function to refresh a specific materialized view
CREATE OR REPLACE FUNCTION refresh_dashboard_view(view_name text)
RETURNS boolean AS $$
BEGIN
    EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || view_name;
    RETURN true;
EXCEPTION
    WHEN OTHERS THEN
        RETURN false;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh all dashboard views
CREATE OR REPLACE FUNCTION refresh_dashboard_views(concurrent boolean DEFAULT true)
RETURNS TABLE(view_name text, success boolean, error_message text) AS $$
DECLARE
    view_names text[] := ARRAY['dashboard_beneficiary_summary', 'dashboard_monetary_transfers', 
                              'dashboard_transfers_by_province', 'dashboard_transfers_by_time',
                              'dashboard_activities_summary', 'dashboard_activities_by_type', 'dashboard_microprojects',
                              'dashboard_grievances', 'dashboard_grievance_status',
                              'dashboard_indicators', 'dashboard_master_summary', 'dashboard_individual_summary'];
    view_name text;
    refresh_sql text;
BEGIN
    FOREACH view_name IN ARRAY view_names LOOP
        BEGIN
            IF concurrent THEN
                refresh_sql := 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || view_name;
            ELSE
                refresh_sql := 'REFRESH MATERIALIZED VIEW ' || view_name;
            END IF;
            
            EXECUTE refresh_sql;
            
            RETURN QUERY SELECT view_name, true, ''::text;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN QUERY SELECT view_name, false, SQLERRM;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Function to get view refresh statistics
CREATE OR REPLACE FUNCTION get_dashboard_view_stats()
RETURNS TABLE(view_name text, last_refresh timestamp, size_mb numeric) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        schemaname || '.' || matviewname as view_name,
        CURRENT_TIMESTAMP as last_refresh,  -- Simplified since we can't get actual refresh time easily
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname))::numeric as size_mb
    FROM pg_matviews 
    WHERE schemaname = 'public' 
    AND matviewname LIKE 'dashboard_%'
    ORDER BY matviewname;
END;
$$ LANGUAGE plpgsql;
"""


class MaterializedViewManager:
    """Manager for handling materialized view operations"""
    
    @staticmethod
    def create_all_views():
        """Create all materialized views"""
        with connection.cursor() as cursor:
            # Create views
            for view_name, sql in MATERIALIZED_VIEWS_SQL.items():
                try:
                    cursor.execute(sql)
                    print(f"Created materialized view: {view_name}")
                except Exception as e:
                    print(f"Error creating view {view_name}: {e}")
            
            # Create indexes
            for view_name, indexes in MATERIALIZED_VIEW_INDEXES.items():
                for index_sql in indexes:
                    try:
                        cursor.execute(index_sql)
                    except Exception as e:
                        print(f"Error creating index for {view_name}: {e}")
            
            # Create refresh functions
            try:
                cursor.execute(REFRESH_FUNCTIONS)
                print("Created refresh functions")
            except Exception as e:
                print(f"Error creating refresh functions: {e}")
    
    @staticmethod
    def refresh_all_views():
        """Refresh all materialized views"""
        with connection.cursor() as cursor:
            try:
                cursor.execute("SELECT refresh_dashboard_views(false)")
                print("Refreshed all dashboard views")
            except Exception as e:
                print(f"Error refreshing views: {e}")
    
    @staticmethod
    def drop_all_views():
        """Drop all materialized views"""
        with connection.cursor() as cursor:
            view_names = list(MATERIALIZED_VIEWS_SQL.keys())
            view_names.reverse()
            
            for view_name in view_names:
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    print(f"Dropped materialized view: {view_name}")
                except Exception as e:
                    print(f"Error dropping view {view_name}: {e}")


@receiver(post_migrate)
def create_materialized_views(sender, **kwargs):
    """Create materialized views after migration"""
    if sender.name == 'merankabandi':
        try:
            MaterializedViewManager.create_all_views()
        except Exception as e:
            print(f"Error in post_migrate signal: {e}")