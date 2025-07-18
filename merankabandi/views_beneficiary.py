"""
Beneficiary Materialized Views
All views related to beneficiaries, households, and demographics
"""

BENEFICIARY_VIEWS = {
    'dashboard_beneficiary_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_beneficiary_summary AS
SELECT date_trunc('month'::text, gb."DateCreated") AS month, date_trunc('quarter'::text, gb."DateCreated") AS quarter, EXTRACT(year FROM gb."DateCreated") AS year, l3."LocationName" AS province, l2."LocationName" AS commune, l1."LocationName" AS colline, l3."LocationId" AS province_id, l2."LocationId" AS commune_id, l1."LocationId" AS colline_id, CASE WHEN ((l2."LocationName")::text = ANY ((ARRAY['Butezi'::character varying, 'Ruyigi'::character varying, 'Kiremba'::character varying, 'Gasorwe'::character varying, 'Gashoho'::character varying, 'Muyinga'::character varying, 'Cankuzo'::character varying])::text[])) THEN 'HOST'::text ELSE 'REFUGEE'::text END AS community_type, COALESCE((i."Json_ext" ->> 'sexe'::text), 'UNKNOWN'::text) AS gender, CASE WHEN ((i."Json_ext" ->> 'is_twa'::text) = 'true'::text) THEN true ELSE false END AS is_twa, CASE WHEN (i.dob IS NULL) THEN 'UNKNOWN'::text WHEN (date_part('year'::text, age((i.dob)::timestamp with time zone)) < (18)::double precision) THEN 'UNDER_18'::text WHEN ((date_part('year'::text, age((i.dob)::timestamp with time zone)) >= (18)::double precision) AND (date_part('year'::text, age((i.dob)::timestamp with time zone)) <= (35)::double precision)) THEN 'ADULT_18_35'::text WHEN ((date_part('year'::text, age((i.dob)::timestamp with time zone)) >= (36)::double precision) AND (date_part('year'::text, age((i.dob)::timestamp with time zone)) <= (60)::double precision)) THEN 'ADULT_36_60'::text ELSE 'OVER_60'::text END AS age_group, bp."UUID" AS benefit_plan_id, bp.code AS benefit_plan_code, bp.name AS benefit_plan_name, gb.status, gb."isDeleted", count(*) AS beneficiary_count, count( CASE WHEN ((i."Json_ext" ->> 'sexe'::text) = 'M'::text) THEN 1 ELSE NULL::integer END) AS male_count, count( CASE WHEN ((i."Json_ext" ->> 'sexe'::text) = 'F'::text) THEN 1 ELSE NULL::integer END) AS female_count, count( CASE WHEN ((i."Json_ext" ->> 'is_twa'::text) = 'true'::text) THEN 1 ELSE NULL::integer END) AS twa_count, count( CASE WHEN ((gb.status)::text = 'ACTIVE'::text) THEN 1 ELSE NULL::integer END) AS active_count, count( CASE WHEN ((gb.status)::text = 'SUSPENDED'::text) THEN 1 ELSE NULL::integer END) AS suspended_count FROM ((((((social_protection_groupbeneficiary gb JOIN social_protection_benefitplan bp ON ((gb.benefit_plan_id = bp."UUID"))) JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) LEFT JOIN "tblLocations" l1 ON ((i.location_id = l1."LocationId"))) LEFT JOIN "tblLocations" l2 ON ((l1."ParentLocationId" = l2."LocationId"))) LEFT JOIN "tblLocations" l3 ON ((l2."ParentLocationId" = l3."LocationId"))) WHERE (gb."isDeleted" = false) GROUP BY (date_trunc('month'::text, gb."DateCreated")), (date_trunc('quarter'::text, gb."DateCreated")), (EXTRACT(year FROM gb."DateCreated")), l3."LocationName", l2."LocationName", l1."LocationName", l3."LocationId", l2."LocationId", l1."LocationId", CASE WHEN ((l2."LocationName")::text = ANY ((ARRAY['Butezi'::character varying, 'Ruyigi'::character varying, 'Kiremba'::character varying, 'Gasorwe'::character varying, 'Gashoho'::character varying, 'Muyinga'::character varying, 'Cankuzo'::character varying])::text[])) THEN 'HOST'::text ELSE 'REFUGEE'::text END, COALESCE((i."Json_ext" ->> 'sexe'::text), 'UNKNOWN'::text), CASE WHEN ((i."Json_ext" ->> 'is_twa'::text) = 'true'::text) THEN true ELSE false END, CASE WHEN (i.dob IS NULL) THEN 'UNKNOWN'::text WHEN (date_part('year'::text, age((i.dob)::timestamp with time zone)) < (18)::double precision) THEN 'UNDER_18'::text WHEN ((date_part('year'::text, age((i.dob)::timestamp with time zone)) >= (18)::double precision) AND (date_part('year'::text, age((i.dob)::timestamp with time zone)) <= (35)::double precision)) THEN 'ADULT_18_35'::text WHEN ((date_part('year'::text, age((i.dob)::timestamp with time zone)) >= (36)::double precision) AND (date_part('year'::text, age((i.dob)::timestamp with time zone)) <= (60)::double precision)) THEN 'ADULT_36_60'::text ELSE 'OVER_60'::text END, bp."UUID", bp.code, bp.name, gb.status, gb."isDeleted"''',
        'indexes': [
            """CREATE INDEX idx_dashboard_beneficiary_summary_commune ON dashboard_beneficiary_summary USING btree (commune_id);""",
            """CREATE INDEX idx_dashboard_beneficiary_summary_location ON dashboard_beneficiary_summary USING btree (colline_id);""",
            """CREATE INDEX idx_dashboard_beneficiary_summary_province ON dashboard_beneficiary_summary USING btree (province_id);""",
            """CREATE INDEX idx_dashboard_beneficiary_summary_benefit_plan ON dashboard_beneficiary_summary USING btree (benefit_plan_id);""",
        ]
    },
    'dashboard_individual_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_individual_summary AS
-- First part: Data grouped by location and benefit plan
SELECT 
    l3."LocationId" AS province_id,
    l3."LocationName" AS province,
    l2."LocationId" AS commune_id,
    l2."LocationName" AS commune,
    l1."LocationId" AS colline_id,
    l1."LocationName" AS colline,
    CASE 
        WHEN ig."Json_ext"->>'menage_refugie' = 'OUI' THEN 'REFUGEE'
        WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') THEN 'HOST'
        ELSE 'OTHER'
    END AS community_type,
    bp."UUID" AS benefit_plan_id,
    bp.code AS benefit_plan_code,
    bp.name AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    -- Count individuals in households with beneficiaries in this plan
    COUNT(DISTINCT i."UUID") AS total_individuals,
    COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN i."UUID" END) AS total_male,
    COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN i."UUID" END) AS total_female,
    COUNT(DISTINCT CASE 
        WHEN i."Json_ext"->>'is_twa' = 'true' OR ig."Json_ext"->>'menage_mutwa' = 'OUI' 
        THEN i."UUID" 
    END) AS total_twa,
    
    -- Percentages
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN i."UUID" END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN i."UUID" END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE 
            WHEN i."Json_ext"->>'is_twa' = 'true' OR ig."Json_ext"->>'menage_mutwa' = 'OUI' 
            THEN i."UUID" 
        END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS twa_percentage,
    
    -- Count households with beneficiaries in this plan
    COUNT(DISTINCT ig."UUID") AS total_households,
    COUNT(DISTINCT gb."UUID") AS total_beneficiaries,
    
    -- Payment data - count distinct payrolls that have consumptions in this location
    (SELECT COUNT(DISTINCT p2."UUID") 
     FROM payroll_payroll p2
     JOIN payroll_payrollbenefitconsumption pbc2 ON pbc2.payroll_id = p2."UUID" AND pbc2."isDeleted" = false
     JOIN payroll_benefitconsumption bc2 ON bc2."UUID" = pbc2.benefit_id AND bc2."isDeleted" = false
     JOIN individual_individual i2 ON i2."UUID" = bc2.individual_id AND i2."isDeleted" = false
     JOIN individual_groupindividual gi2 ON gi2.individual_id = i2."UUID" AND gi2."isDeleted" = false
     JOIN individual_group ig2 ON ig2."UUID" = gi2.group_id AND ig2."isDeleted" = false
     WHERE p2."isDeleted" = false
     AND ig2.location_id IN (SELECT ig3.location_id FROM individual_group ig3 
                            JOIN social_protection_groupbeneficiary gb3 ON gb3.group_id = ig3."UUID"
                            WHERE gb3.benefit_plan_id = bp."UUID" AND gb3."isDeleted" = false
                            AND ig3.location_id = l1."LocationId")) AS total_transfers,
    COALESCE(SUM(CASE WHEN bc.status = 'RECONCILED' THEN bc."Amount"::numeric END), 0) AS total_amount_paid,
    COALESCE(SUM(CASE WHEN bc.status <> 'RECONCILED' THEN bc."Amount"::numeric END), 0) AS total_amount_unpaid,
    COALESCE(SUM(bc."Amount"::numeric), 0) AS total_amount,

    -- Grievance data (would need to be joined from grievance tables)
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    -- Active provinces count
    COUNT(DISTINCT l3."LocationId") AS active_provinces,
    
    CURRENT_TIMESTAMP AS last_updated
FROM social_protection_groupbeneficiary gb
JOIN social_protection_benefitplan bp ON bp."UUID" = gb.benefit_plan_id AND bp."isDeleted" = false
JOIN individual_group ig ON ig."UUID" = gb.group_id AND ig."isDeleted" = false
JOIN individual_groupindividual gi ON gi.group_id = ig."UUID" AND gi."isDeleted" = false
JOIN individual_individual i ON i."UUID" = gi.individual_id AND i."isDeleted" = false
JOIN "tblLocations" l1 ON l1."LocationId" = ig.location_id
LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
-- Join payment data through benefit consumption
LEFT JOIN payroll_benefitconsumption bc ON bc.individual_id = i."UUID" AND bc."isDeleted" = false
LEFT JOIN payroll_payrollbenefitconsumption pbc ON pbc.benefit_id = bc."UUID" AND pbc."isDeleted" = false
LEFT JOIN payroll_payroll p ON p."UUID" = pbc.payroll_id AND p."isDeleted" = false
WHERE gb."isDeleted" = false
GROUP BY 
    l3."LocationId", l3."LocationName",
    l2."LocationId", l2."LocationName",
    l1."LocationId", l1."LocationName",
    CASE 
        WHEN ig."Json_ext"->>'menage_refugie' = 'OUI' THEN 'REFUGEE'
        WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') THEN 'HOST'
        ELSE 'OTHER'
    END,
    bp."UUID", bp.code, bp.name

UNION ALL

-- Add location-specific ALL rows (all benefit plans for each location)
SELECT 
    l3."LocationId" AS province_id,
    l3."LocationName" AS province,
    l2."LocationId" AS commune_id,
    l2."LocationName" AS commune,
    l1."LocationId" AS colline_id,
    l1."LocationName" AS colline,
    CASE 
        WHEN ig."Json_ext"->>'menage_refugie' = 'OUI' THEN 'REFUGEE'
        WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') THEN 'HOST'
        ELSE 'OTHER'
    END AS community_type,
    '00000000-0000-0000-0000-000000000000'::uuid AS benefit_plan_id,
    'ALL'::text AS benefit_plan_code,
    'ALL PLANS'::text AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    COUNT(DISTINCT i."UUID") AS total_individuals,
    COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN i."UUID" END) AS total_male,
    COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN i."UUID" END) AS total_female,
    COUNT(DISTINCT CASE 
        WHEN ig."Json_ext"->>'menage_mutwa' = 'OUI' 
        THEN i."UUID" 
    END) AS total_twa,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN i."UUID" END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN i."UUID" END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE 
            WHEN i."Json_ext"->>'is_twa' = 'true' OR ig."Json_ext"->>'menage_mutwa' = 'OUI' 
            THEN i."UUID" 
        END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS twa_percentage,
    COUNT(DISTINCT ig."UUID") AS total_households,
    COUNT(DISTINCT gb."UUID") AS total_beneficiaries,
    -- Count distinct payrolls for this location (all benefit plans)
    (SELECT COUNT(DISTINCT p2."UUID") 
     FROM payroll_payroll p2
     JOIN payroll_payrollbenefitconsumption pbc2 ON pbc2.payroll_id = p2."UUID" AND pbc2."isDeleted" = false
     JOIN payroll_benefitconsumption bc2 ON bc2."UUID" = pbc2.benefit_id AND bc2."isDeleted" = false
     JOIN individual_individual i2 ON i2."UUID" = bc2.individual_id AND i2."isDeleted" = false
     JOIN individual_groupindividual gi2 ON gi2.individual_id = i2."UUID" AND gi2."isDeleted" = false
     JOIN individual_group ig2 ON ig2."UUID" = gi2.group_id AND ig2."isDeleted" = false
     WHERE p2."isDeleted" = false
     AND ig2.location_id = l1."LocationId") AS total_transfers,
    COALESCE(SUM(CASE WHEN bc.status = 'RECONCILED' THEN bc."Amount"::numeric END), 0) AS total_amount_paid,
    COALESCE(SUM(CASE WHEN bc.status <> 'RECONCILED' THEN bc."Amount"::numeric END), 0) AS total_amount_unpaid,
    COALESCE(SUM(bc."Amount"::numeric), 0) AS total_amount,

    0 AS total_grievances,
    0 AS resolved_grievances,
    COUNT(DISTINCT l3."LocationId") AS active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM individual_group ig
LEFT JOIN individual_groupindividual gi ON gi.group_id = ig."UUID" AND gi."isDeleted" = false
LEFT JOIN individual_individual i ON i."UUID" = gi.individual_id AND i."isDeleted" = false
LEFT JOIN social_protection_groupbeneficiary gb ON gb.group_id = ig."UUID" AND gb."isDeleted" = false
LEFT JOIN "tblLocations" l1 ON l1."LocationId" = ig.location_id
LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
LEFT JOIN payroll_benefitconsumption bc ON bc.individual_id = i."UUID" AND bc."isDeleted" = false
LEFT JOIN payroll_payrollbenefitconsumption pbc ON pbc.benefit_id = bc."UUID" AND pbc."isDeleted" = false
LEFT JOIN payroll_payroll p ON p."UUID" = pbc.payroll_id AND p."isDeleted" = false
WHERE ig."isDeleted" = false
GROUP BY 
    l3."LocationId", l3."LocationName",
    l2."LocationId", l2."LocationName",
    l1."LocationId", l1."LocationName",
    CASE 
        WHEN ig."Json_ext"->>'menage_refugie' = 'OUI' THEN 'REFUGEE'
        WHEN l2."LocationName" IN ('Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo') THEN 'HOST'
        ELSE 'OTHER'
    END

UNION ALL

-- Add summary row with ALL locations and ALL plans
SELECT 
    NULL::integer AS province_id,
    'ALL'::text AS province,
    NULL::integer AS commune_id,
    'ALL'::text AS commune,
    NULL::integer AS colline_id,
    'ALL'::text AS colline,
    'ALL'::text AS community_type,
    '00000000-0000-0000-0000-000000000000'::uuid AS benefit_plan_id,
    'ALL'::text AS benefit_plan_code,
    'ALL PLANS'::text AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    COUNT(DISTINCT i."UUID") AS total_individuals,
    COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN i."UUID" END) AS total_male,
    COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN i."UUID" END) AS total_female,
    COUNT(DISTINCT CASE 
        WHEN i."Json_ext"->>'is_twa' = 'true' OR ig."Json_ext"->>'menage_mutwa' = 'OUI' 
        THEN i."UUID" 
    END) AS total_twa,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN i."UUID" END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN i."UUID" END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN COUNT(DISTINCT i."UUID") > 0 
        THEN (COUNT(DISTINCT CASE 
            WHEN i."Json_ext"->>'is_twa' = 'true' OR ig."Json_ext"->>'menage_mutwa' = 'OUI' 
            THEN i."UUID" 
        END)::numeric / COUNT(DISTINCT i."UUID")::numeric * 100)
        ELSE 0 
    END AS twa_percentage,
    COUNT(DISTINCT ig."UUID") AS total_households,
    COUNT(DISTINCT gb."UUID") AS total_beneficiaries,
    -- Payment data for ALL (count payrolls as transfers)
    (SELECT COUNT(DISTINCT pp."UUID") FROM payroll_payroll pp WHERE pp."isDeleted" = false) AS total_transfers,

    (SELECT COALESCE(SUM(CASE WHEN bc.status = 'RECONCILED' THEN bc."Amount"::numeric END), 0) FROM payroll_benefitconsumption bc WHERE bc."isDeleted" = false) AS total_amount_paid,
    (SELECT COALESCE(SUM(CASE WHEN bc.status <> 'RECONCILED' THEN bc."Amount"::numeric END), 0) FROM payroll_benefitconsumption bc WHERE bc."isDeleted" = false) AS total_amount_unpaid,
    (SELECT COALESCE(SUM(bc."Amount"::numeric), 0) FROM payroll_benefitconsumption bc WHERE bc."isDeleted" = false) AS total_amount,

    -- Grievance data
    0 AS total_grievances,
    0 AS resolved_grievances,
    -- Active provinces
    (SELECT COUNT(DISTINCT l."LocationId") FROM "tblLocations" l WHERE l."LocationType" = 'P') AS active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM individual_group ig
LEFT JOIN individual_groupindividual gi ON gi.group_id = ig."UUID" AND gi."isDeleted" = false
LEFT JOIN individual_individual i ON i."UUID" = gi.individual_id AND i."isDeleted" = false
LEFT JOIN social_protection_groupbeneficiary gb ON gb.group_id = ig."UUID" AND gb."isDeleted" = false
WHERE ig."isDeleted" = false''',
        'indexes': [
            """CREATE INDEX idx_individual_summary_month ON dashboard_individual_summary USING btree (month);""",
            """CREATE INDEX idx_individual_summary_province ON dashboard_individual_summary USING btree (province_id);""",
            """CREATE INDEX idx_individual_summary_commune ON dashboard_individual_summary USING btree (commune_id);""",
            """CREATE INDEX idx_individual_summary_colline ON dashboard_individual_summary USING btree (colline_id);""",
            """CREATE INDEX idx_individual_summary_benefit_plan ON dashboard_individual_summary USING btree (benefit_plan_id);""",
            """CREATE INDEX idx_individual_summary_year ON dashboard_individual_summary USING btree (year);""",
        ]
    },
    'dashboard_master_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_master_summary AS
-- Global summary view that aggregates without location/benefit plan granularity to avoid multiplication
WITH 
-- Configuration constants
config AS (
    SELECT 
        'ACTIVE'::text AS active_status,
        'RECONCILED'::text AS reconciled_status,
        'M'::text AS male_gender,
        'F'::text AS female_gender,
        'OUI'::text AS twa_indicator
),
-- Global beneficiary statistics with demographics
beneficiary_stats AS (
    SELECT
        COUNT(DISTINCT gb."UUID") AS total_beneficiaries,
        COUNT(DISTINCT gb."UUID") FILTER (WHERE gb.status = c.active_status) AS active_beneficiaries,
        COUNT(DISTINCT gb."UUID") FILTER (
            WHERE i."Json_ext"->>'sexe' = c.male_gender
        ) AS male_beneficiaries,
        COUNT(DISTINCT gb."UUID") FILTER (
            WHERE i."Json_ext"->>'sexe' = c.female_gender
        ) AS female_beneficiaries
    FROM social_protection_groupbeneficiary gb
    CROSS JOIN config c
    LEFT JOIN individual_groupindividual gi ON gi.group_id = gb.group_id  and (gi."recipient_type" = 'PRIMARY')
    LEFT JOIN individual_individual i 
        ON i."UUID" = gi.individual_id 
        AND i."isDeleted" = false
    WHERE gb."isDeleted" = false
),
-- Household statistics
household_stats AS (
    SELECT
        COUNT(DISTINCT ig."UUID") AS total_households,
        COUNT(DISTINCT ig."UUID") FILTER (
            WHERE (ig."Json_ext" ->> 'menage_mutwa'::TEXT) = c.twa_indicator
        ) AS total_twa
    FROM individual_group ig
    WHERE ig."isDeleted" = false
),
-- Individual demographics
individual_demographics AS (
    SELECT
        COUNT(DISTINCT i."UUID") AS total_individuals,
        COUNT(DISTINCT i."UUID") FILTER (
            WHERE i."Json_ext"->>'sexe' = c.male_gender
        ) AS total_male,
        COUNT(DISTINCT i."UUID") FILTER (
            WHERE i."Json_ext"->>'sexe' = c.female_gender
        ) AS total_female
    FROM individual_individual i
    CROSS JOIN config c
    WHERE i."isDeleted" = false
),
-- Payment and transfer statistics
payment_summary AS (
    SELECT
        COUNT(DISTINCT pp."UUID") AS total_transfers,
        COALESCE(SUM(bc."Amount"::numeric) FILTER (
            WHERE bc.status = c.reconciled_status
        ), 0) AS total_amount_paid,
        COALESCE(SUM(bc."Amount"::numeric) FILTER (
            WHERE bc.status <> c.reconciled_status
        ), 0) AS total_amount_unpaid,
        COALESCE(SUM(bc."Amount"::numeric), 0) AS total_amount
    FROM payroll_payroll pp
    CROSS JOIN config c
    LEFT JOIN payroll_payrollbenefitconsumption pbc 
        ON pbc.payroll_id = pp."UUID" 
        AND pbc."isDeleted" = false
    LEFT JOIN payroll_benefitconsumption bc 
        ON bc."UUID" = pbc.benefit_id 
        AND bc."isDeleted" = false
    WHERE pp."isDeleted" = false
),
-- Geographic coverage
geographic_coverage AS (
    SELECT COUNT(DISTINCT l3."LocationId") AS active_provinces
    FROM individual_group ig
    INNER JOIN "tblLocations" l1 ON ig.location_id = l1."LocationId"
    INNER JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    INNER JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    WHERE ig."isDeleted" = false
),
-- Grievance statistics (placeholder for future implementation)
grievance_stats AS (
    SELECT 
        0::bigint AS total_grievances,
        0::bigint AS resolved_grievances
    -- TODO: Implement actual grievance aggregation logic when table structure is defined
),
-- Time dimensions
time_dimensions AS (
    SELECT
        EXTRACT(year FROM CURRENT_DATE)::integer AS year,
        date_trunc('month', CURRENT_DATE)::date AS month,
        date_trunc('quarter', CURRENT_DATE)::date AS quarter,
        CURRENT_TIMESTAMP AS last_updated
)
-- Main query combining all statistics
SELECT
    -- Location hierarchy (NULL for global summary)
    NULL::integer AS province_id,
    'ALL'::text AS province,
    NULL::integer AS commune_id,
    'ALL'::text AS commune,
    NULL::integer AS colline_id,
    'ALL'::text AS colline,
    'ALL'::text AS community_type,
    
    -- Benefit plan (NULL for global summary)
    NULL::uuid AS benefit_plan_id,
    'ALL'::text AS benefit_plan_code,
    'ALL'::text AS benefit_plan_name,
    
    -- Time dimensions
    td.year,
    td.month,
    td.quarter,
    
    -- Beneficiary metrics
    bs.total_beneficiaries,
    bs.active_beneficiaries,
    bs.male_beneficiaries,
    bs.female_beneficiaries,
    hs.total_households,
    hs.total_twa,
    
    -- Individual demographics
    id.total_individuals,
    id.total_male,
    id.total_female,
    
    -- Payment metrics
    ps.total_transfers,
    ps.total_amount_paid,
    ps.total_amount_unpaid,
    
    -- Grievance metrics
    gs.total_grievances,
    gs.resolved_grievances,
    
    -- Geographic coverage
    gc.active_provinces,
    
    -- Metadata
    td.last_updated
FROM beneficiary_stats bs
CROSS JOIN household_stats hs
CROSS JOIN individual_demographics id
CROSS JOIN payment_summary ps
CROSS JOIN geographic_coverage gc
CROSS JOIN grievance_stats gs
CROSS JOIN time_dimensions td''',
        'indexes': [
            """CREATE INDEX idx_master_summary_month ON dashboard_master_summary USING btree (month);""",
            """CREATE INDEX idx_master_summary_province ON dashboard_master_summary USING btree (province_id);""",
            """CREATE INDEX idx_master_summary_commune ON dashboard_master_summary USING btree (commune_id);""",
            """CREATE INDEX idx_master_summary_colline ON dashboard_master_summary USING btree (colline_id);""",
            """CREATE INDEX idx_master_summary_benefit_plan ON dashboard_master_summary USING btree (benefit_plan_id);""",
            """CREATE INDEX idx_master_summary_year ON dashboard_master_summary USING btree (year);""",
        ]
    },
    'dashboard_master_summary_enhanced': {
        'sql': '''
            CREATE MATERIALIZED VIEW DASHBOARD_MASTER_SUMMARY_ENHANCED AS
            SELECT
                'MASTER_SUMMARY'::TEXT AS SUMMARY_TYPE,
                (
                    SELECT
                        COUNT(*) AS COUNT
                    FROM
                        SOCIAL_PROTECTION_GROUPBENEFICIARY
                    WHERE
                        (
                            (
                                SOCIAL_PROTECTION_GROUPBENEFICIARY."isDeleted" = FALSE
                            )
                            AND (
                                (SOCIAL_PROTECTION_GROUPBENEFICIARY.STATUS)::TEXT = 'ACTIVE'::TEXT
                            )
                        )
                ) AS TOTAL_BENEFICIARIES,
                (
                    SELECT
                        COUNT(
                            DISTINCT SOCIAL_PROTECTION_GROUPBENEFICIARY.GROUP_ID
                        ) AS COUNT
                    FROM
                        SOCIAL_PROTECTION_GROUPBENEFICIARY
                    WHERE
                        (
                            (
                                SOCIAL_PROTECTION_GROUPBENEFICIARY."isDeleted" = FALSE
                            )
                            AND (
                                (SOCIAL_PROTECTION_GROUPBENEFICIARY.STATUS)::TEXT = 'ACTIVE'::TEXT
                            )
                        )
                ) AS TOTAL_HOUSEHOLDS,
                (
                    SELECT
                        COUNT(*) AS COUNT
                    FROM
                        (
                            (
                                SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                            )
                            JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND ((I."Json_ext" ->> 'sexe'::TEXT) = 'M'::TEXT)
                        )
                ) AS TOTAL_MALE,
                (
                    SELECT
                        COUNT(*) AS COUNT
                    FROM
                        (
                            (
                                SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                            )
                            JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND ((I."Json_ext" ->> 'sexe'::TEXT) = 'F'::TEXT)
                        )
                ) AS TOTAL_FEMALE,
                (
                    SELECT
                        COUNT(DISTINCT I."UUID") AS COUNT
                    FROM
                        (
                            (
                                (
                                    SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                    JOIN INDIVIDUAL_GROUP G ON ((GB.GROUP_ID = G."UUID"))
                                )
                                JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                            )
                            JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                (G."Json_ext" ->> 'menage_mutwa'::TEXT) = 'OUI'::TEXT
                            )
                        )
                ) AS TOTAL_TWA,
                (
                    SELECT
                        COUNT(DISTINCT I."UUID") AS COUNT
                    FROM
                        (
                            (
                                SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                            )
                            JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND ((I."Json_ext" ->> 'handicap'::TEXT) = 'OUI'::TEXT)
                        )
                ) AS TOTAL_DISABLED,
                (
                    SELECT
                        COUNT(DISTINCT I."UUID") AS COUNT
                    FROM
                        (
                            (
                                SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                            )
                            JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                (I."Json_ext" ->> 'maladie_chro'::TEXT) = 'OUI'::TEXT
                            )
                        )
                ) AS TOTAL_CHRONIC_ILLNESS,
                (
                    SELECT
                        COUNT(DISTINCT I."UUID") AS COUNT
                    FROM
                        (
                            (
                                (
                                    SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                    JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                                )
                                JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                            )
                            JOIN INDIVIDUAL_GROUP G ON ((GI.GROUP_ID = G."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                (G."Json_ext" ->> 'menage_refugie'::TEXT) = 'OUI'::TEXT
                            )
                        )
                ) AS TOTAL_REFUGEES,
                (
                    SELECT
                        COUNT(DISTINCT I."UUID") AS COUNT
                    FROM
                        (
                            (
                                (
                                    SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                    JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                                )
                                JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                            )
                            JOIN INDIVIDUAL_GROUP G ON ((GI.GROUP_ID = G."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                (G."Json_ext" ->> 'menage_rapatrie'::TEXT) = 'OUI'::TEXT
                            )
                        )
                ) AS TOTAL_RETURNEES,
                (
                    SELECT
                        COUNT(DISTINCT I."UUID") AS COUNT
                    FROM
                        (
                            (
                                (
                                    SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                                    JOIN INDIVIDUAL_GROUPINDIVIDUAL GI ON ((GI.GROUP_ID = GB.GROUP_ID))
                                )
                                JOIN INDIVIDUAL_INDIVIDUAL I ON ((GI.INDIVIDUAL_ID = I."UUID"))
                            )
                            JOIN INDIVIDUAL_GROUP G ON ((GI.GROUP_ID = G."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                (G."Json_ext" ->> 'menage_deplace'::TEXT) = 'OUI'::TEXT
                            )
                        )
                ) AS TOTAL_DISPLACED,
                (
                    SELECT
                        COUNT(DISTINCT G."UUID") AS COUNT
                    FROM
                        (
                            SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                            JOIN INDIVIDUAL_GROUP G ON ((GB.GROUP_ID = G."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                (G."Json_ext" ->> 'menage_mutwa'::TEXT) = 'OUI'::TEXT
                            )
                        )
                ) AS TWA_HOUSEHOLDS,
                (
                    SELECT
                        COUNT(DISTINCT G."UUID") AS COUNT
                    FROM
                        (
                            SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                            JOIN INDIVIDUAL_GROUP G ON ((GB.GROUP_ID = G."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        (
                                            INDIVIDUAL_GROUPINDIVIDUAL GI2
                                            JOIN INDIVIDUAL_INDIVIDUAL I2 ON ((GI2.INDIVIDUAL_ID = I2."UUID"))
                                        )
                                    WHERE
                                        (
                                            (GI2.GROUP_ID = G."UUID")
                                            AND (
                                                (I2."Json_ext" ->> 'handicap'::TEXT) = 'OUI'::TEXT
                                            )
                                        )
                                )
                            )
                        )
                ) AS DISABLED_HOUSEHOLDS,
                (
                    SELECT
                        COUNT(DISTINCT G."UUID") AS COUNT
                    FROM
                        (
                            SOCIAL_PROTECTION_GROUPBENEFICIARY GB
                            JOIN INDIVIDUAL_GROUP G ON ((GB.GROUP_ID = G."UUID"))
                        )
                    WHERE
                        (
                            (GB."isDeleted" = FALSE)
                            AND ((GB.STATUS)::TEXT = 'ACTIVE'::TEXT)
                            AND (
                                EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        (
                                            INDIVIDUAL_GROUPINDIVIDUAL GI2
                                            JOIN INDIVIDUAL_INDIVIDUAL I2 ON ((GI2.INDIVIDUAL_ID = I2."UUID"))
                                        )
                                    WHERE
                                        (
                                            (GI2.GROUP_ID = G."UUID")
                                            AND (
                                                (I2."Json_ext" ->> 'maladie_chro'::TEXT) = 'OUI'::TEXT
                                            )
                                        )
                                )
                            )
                        )
                ) AS CHRONIC_ILLNESS_HOUSEHOLDS,
                (
                    (
                        SELECT
                            SUM(
                                MERANKABANDI_SENSITIZATIONTRAINING.TWA_PARTICIPANTS
                            ) AS SUM
                        FROM
                            MERANKABANDI_SENSITIZATIONTRAINING
                    ) + (
                        SELECT
                            SUM(
                                MERANKABANDI_BEHAVIORCHANGEPROMOTION.TWA_PARTICIPANTS
                            ) AS SUM
                        FROM
                            MERANKABANDI_BEHAVIORCHANGEPROMOTION
                    )
                ) AS TOTAL_TWA_ACTIVITY_PARTICIPANTS,
                CURRENT_TIMESTAMP AS LAST_UPDATED
            ''',
        'indexes': [
        ]
    },
    'dashboard_vulnerable_groups': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_vulnerable_groups AS
SELECT l3."LocationName" AS province, l3."LocationId" AS province_id, (g."Json_ext" ->> 'type_menage'::text) AS household_type, bp."UUID" AS benefit_plan_id, bp.code AS benefit_plan_code, bp.name AS benefit_plan_name, count(DISTINCT g."UUID") AS total_households, count(DISTINCT i."UUID") AS total_members, count(DISTINCT CASE WHEN ((gi.recipient_type)::text = 'PRIMARY'::text) THEN i."UUID" ELSE NULL::uuid END) AS total_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS twa_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS twa_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS twa_beneficiaries, count(DISTINCT CASE WHEN (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'handicap'::text) = 'OUI'::text)))) THEN g."UUID" ELSE NULL::uuid END) AS disabled_households, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'handicap'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS disabled_members, count(DISTINCT CASE WHEN (((i."Json_ext" ->> 'handicap'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS disabled_beneficiaries, count(DISTINCT CASE WHEN (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text)))) THEN g."UUID" ELSE NULL::uuid END) AS chronic_illness_households, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS chronic_illness_members, count(DISTINCT CASE WHEN (((i."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS chronic_illness_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS refugee_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS refugee_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS refugee_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS returnee_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS returnee_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS returnee_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS displaced_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS displaced_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS displaced_beneficiaries, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%physique%'::text) THEN i."UUID" ELSE NULL::uuid END) AS physical_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%mental%'::text) THEN i."UUID" ELSE NULL::uuid END) AS mental_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%visuel%'::text) THEN i."UUID" ELSE NULL::uuid END) AS visual_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%auditif%'::text) THEN i."UUID" ELSE NULL::uuid END) AS hearing_disability_count, CURRENT_DATE AS report_date FROM (((((((social_protection_groupbeneficiary gb JOIN social_protection_benefitplan bp ON ((gb.benefit_plan_id = bp."UUID"))) JOIN individual_group g ON ((gb.group_id = g."UUID"))) JOIN individual_groupindividual gi ON ((gi.group_id = g."UUID"))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) LEFT JOIN "tblLocations" l1 ON ((g.location_id = l1."LocationId"))) LEFT JOIN "tblLocations" l2 ON ((l1."ParentLocationId" = l2."LocationId"))) LEFT JOIN "tblLocations" l3 ON ((l2."ParentLocationId" = l3."LocationId"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text)) GROUP BY l3."LocationName", l3."LocationId", (g."Json_ext" ->> 'type_menage'::text), bp."UUID", bp.code, bp.name, CURRENT_DATE''',
        'indexes': [
            """CREATE INDEX idx_dashboard_vulnerable_groups_province ON dashboard_vulnerable_groups USING btree (province_id);""",
        ]
    },
    'dashboard_vulnerable_groups_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_vulnerable_groups_summary AS
SELECT l3."LocationName" AS province, l3."LocationId" AS province_id, (g."Json_ext" ->> 'type_menage'::text) AS household_type, bp."UUID" AS benefit_plan_id, bp.code AS benefit_plan_code, bp.name AS benefit_plan_name, count(DISTINCT g."UUID") AS total_households, count(DISTINCT i."UUID") AS total_members, count(DISTINCT CASE WHEN ((gi.recipient_type)::text = 'PRIMARY'::text) THEN i."UUID" ELSE NULL::uuid END) AS total_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS twa_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS twa_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS twa_beneficiaries, count(DISTINCT CASE WHEN (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'handicap'::text) = 'OUI'::text)))) THEN g."UUID" ELSE NULL::uuid END) AS disabled_households, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'handicap'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS disabled_members, count(DISTINCT CASE WHEN (((i."Json_ext" ->> 'handicap'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS disabled_beneficiaries, count(DISTINCT CASE WHEN (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text)))) THEN g."UUID" ELSE NULL::uuid END) AS chronic_illness_households, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS chronic_illness_members, count(DISTINCT CASE WHEN (((i."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS chronic_illness_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS refugee_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS refugee_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS refugee_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS returnee_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS returnee_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS returnee_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS displaced_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS displaced_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS displaced_beneficiaries, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%physique%'::text) THEN i."UUID" ELSE NULL::uuid END) AS physical_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%mental%'::text) THEN i."UUID" ELSE NULL::uuid END) AS mental_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%visuel%'::text) THEN i."UUID" ELSE NULL::uuid END) AS visual_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%auditif%'::text) THEN i."UUID" ELSE NULL::uuid END) AS hearing_disability_count, CURRENT_DATE AS report_date FROM (((((((social_protection_groupbeneficiary gb JOIN social_protection_benefitplan bp ON ((gb.benefit_plan_id = bp."UUID"))) JOIN individual_group g ON ((gb.group_id = g."UUID"))) JOIN individual_groupindividual gi ON ((gi.group_id = g."UUID"))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) LEFT JOIN "tblLocations" l1 ON ((g.location_id = l1."LocationId"))) LEFT JOIN "tblLocations" l2 ON ((l1."ParentLocationId" = l2."LocationId"))) LEFT JOIN "tblLocations" l3 ON ((l2."ParentLocationId" = l3."LocationId"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text)) GROUP BY l3."LocationName", l3."LocationId", (g."Json_ext" ->> 'type_menage'::text), bp."UUID", bp.code, bp.name, CURRENT_DATE''',
        'indexes': [
        ]
    },
}
