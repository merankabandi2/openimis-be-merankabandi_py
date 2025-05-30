"""
CORRECTED Materialized Views for Dashboard Performance Optimization
Based on actual database schema analysis
"""

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
    
    # 2. MONETARY TRANSFER AGGREGATIONS (simplified based on actual fields)
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
        JOIN "tblLocations" l2 ON mt.location_id = l2."LocationId"
        JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        GROUP BY 
            month, quarter, year,
            province, commune, province_id, commune_id,
            community_type,
            mt.planned_men, mt.planned_women, mt.planned_twa,
            mt.paid_men, mt.paid_women, mt.paid_twa;
    """,
    
    # 3. ACTIVITIES AGGREGATIONS (corrected field names)
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
            
            -- Performance metrics
            AVG(combined.completion_rate) as avg_completion_rate,
            
            -- Gender inclusion rates
            CASE 
                WHEN SUM(combined.total_participants) > 0
                THEN SUM(combined.female_participants)::numeric / SUM(combined.total_participants)::numeric * 100
                ELSE 0
            END as female_participation_rate,
            
            CASE 
                WHEN SUM(combined.total_participants) > 0
                THEN SUM(combined.twa_participants)::numeric / SUM(combined.total_participants)::numeric * 100
                ELSE 0
            END as twa_participation_rate
            
        FROM (
            -- Sensitization Training
            SELECT 
                st.sensitization_date as activity_date,
                'SENSITIZATION' as activity_type,
                st.category as activity_category,
                l3."LocationName" as province,
                l2."LocationName" as commune,
                l3."LocationId" as province_id,
                l2."LocationId" as commune_id,
                CASE 
                    WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') 
                    THEN 'HOST' 
                    ELSE 'REFUGEE' 
                END as community_type,
                COALESCE(st.male_participants, 0) as male_participants,
                COALESCE(st.female_participants, 0) as female_participants,
                COALESCE(st.twa_participants, 0) as twa_participants,
                (COALESCE(st.male_participants, 0) + COALESCE(st.female_participants, 0) + COALESCE(st.twa_participants, 0)) as total_participants,
                100 as completion_rate  -- No planned field available
            FROM merankabandi_sensitizationtraining st
            JOIN "tblLocations" l2 ON st.location_id = l2."LocationId"
            JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
            
            UNION ALL
            
            -- Behavior Change Promotion
            SELECT 
                bcp.report_date as activity_date,
                'BEHAVIOR_CHANGE' as activity_type,
                'BEHAVIOR_CHANGE' as activity_category,  -- No category field
                l3."LocationName" as province,
                l2."LocationName" as commune,
                l3."LocationId" as province_id,
                l2."LocationId" as commune_id,
                CASE 
                    WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') 
                    THEN 'HOST' 
                    ELSE 'REFUGEE' 
                END as community_type,
                COALESCE(bcp.male_participants, 0) as male_participants,
                COALESCE(bcp.female_participants, 0) as female_participants,
                COALESCE(bcp.twa_participants, 0) as twa_participants,
                (COALESCE(bcp.male_participants, 0) + COALESCE(bcp.female_participants, 0) + COALESCE(bcp.twa_participants, 0)) as total_participants,
                100 as completion_rate  -- No planned field available
            FROM merankabandi_behaviorchangepromotion bcp
            JOIN "tblLocations" l2 ON bcp.location_id = l2."LocationId"
            JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
        ) combined
        GROUP BY 
            month, quarter, year,
            combined.province, combined.commune, combined.province_id, combined.commune_id,
            combined.activity_type, combined.activity_category, combined.community_type;
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
    
    # 5. GRIEVANCE DASHBOARD AGGREGATIONS (corrected)
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
        WHERE t."isDeleted" = false
        GROUP BY 
            month, quarter, year, week,
            t.province, t.commune,
            t.status, t.category, t.channel, t.priority, t.is_anonymous, is_sensitive,
            resolution_days;
    """,
    
    # 6. INDICATORS AND ACHIEVEMENTS (corrected)
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
            s.name as section_name,
            
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
        GROUP BY 
            month, quarter, year,
            i.name, i.id, i.target, s.name;
    """,
    
    # 7. SIMPLIFIED MASTER SUMMARY
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
            
            -- Get counts from base tables (simplified)
            (SELECT COUNT(*) FROM social_protection_groupbeneficiary WHERE "isDeleted" = false) as total_beneficiaries,
            (SELECT COUNT(*) FROM merankabandi_monetarytransfer) as total_transfers,
            (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false) as total_grievances,
            (SELECT COUNT(*) FROM merankabandi_indicatorachievement) as total_achievements
            
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
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_month ON dashboard_monetary_transfers (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_province ON dashboard_monetary_transfers (province);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_community ON dashboard_monetary_transfers (community_type);'
    ],
    'dashboard_activities_summary': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_activities_month ON dashboard_activities_summary (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_activities_type ON dashboard_activities_summary (activity_type);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_activities_province ON dashboard_activities_summary (province);'
    ],
    'dashboard_microprojects': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_microprojects_month ON dashboard_microprojects (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_microprojects_province ON dashboard_microprojects (province);'
    ],
    'dashboard_grievances': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_grievances_month ON dashboard_grievances (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_grievances_status ON dashboard_grievances (status);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_grievances_category ON dashboard_grievances (category);'
    ],
    'dashboard_indicators': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_indicators_month ON dashboard_indicators (month);',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_indicators_name ON dashboard_indicators (indicator_name);'
    ],
    'dashboard_master_summary': [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_master_summary_month ON dashboard_master_summary (month);'
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
                              'dashboard_activities_summary', 'dashboard_microprojects',
                              'dashboard_grievances', 'dashboard_indicators', 'dashboard_master_summary'];
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