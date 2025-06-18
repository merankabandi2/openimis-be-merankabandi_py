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
WITH global_stats AS (
    -- Get total unique households and beneficiaries
    SELECT 
        (SELECT COUNT(DISTINCT ig."UUID") FROM individual_group ig WHERE ig."isDeleted" = false) AS total_households,
        COUNT(DISTINCT gb."UUID") AS total_beneficiaries,
        COUNT(DISTINCT CASE WHEN gb.status = 'ACTIVE' THEN gb."UUID" END) AS active_beneficiaries
    FROM social_protection_groupbeneficiary gb
    WHERE gb."isDeleted" = false
),
individual_stats AS (
    -- Get total unique individuals
    SELECT 
        COUNT(DISTINCT i."UUID") AS total_individuals,
        COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'M' THEN i."UUID" END) AS total_male,
        COUNT(DISTINCT CASE WHEN i."Json_ext"->>'sexe' = 'F' THEN i."UUID" END) AS total_female,
        COUNT(DISTINCT CASE WHEN i."Json_ext"->>'is_twa' = 'true' THEN i."UUID" END) AS total_twa
    FROM individual_individual i
    WHERE i."isDeleted" = false
),
payment_stats AS (
    -- Get payment statistics
    SELECT 
        COUNT(DISTINCT pp."UUID") AS total_transfers,
        COALESCE(SUM(CASE WHEN bc.status = 'RECONCILED' THEN bc."Amount"::numeric END), 0) AS total_amount_paid,
        COALESCE(SUM(CASE WHEN bc.status <> 'RECONCILED' THEN bc."Amount"::numeric END), 0) AS total_amount_unpaid,
        COALESCE(SUM(bc."Amount"::numeric), 0) AS total_amount
    FROM payroll_payroll pp
    LEFT JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = pp."UUID" AND pbc."isDeleted" = false
    LEFT JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    WHERE pp."isDeleted" = false
),
location_stats AS (
    -- Get active provinces count
    SELECT COUNT(DISTINCT l3."LocationId") AS active_provinces
    FROM individual_group ig
    JOIN "tblLocations" l1 ON ig.location_id = l1."LocationId"
    JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    WHERE ig."isDeleted" = false
)
SELECT 
    -- For global summary, we don't break down by location/plan
    NULL::integer AS province_id,
    'ALL'::text AS province,
    NULL::integer AS commune_id,
    'ALL'::text AS commune,
    NULL::integer AS colline_id,
    'ALL'::text AS colline,
    'ALL'::text AS community_type,
    NULL::uuid AS benefit_plan_id,
    'ALL'::text AS benefit_plan_code,
    'ALL'::text AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    -- Counts from CTEs (no multiplication)
    gs.total_beneficiaries,
    gs.active_beneficiaries,
    gs.total_households,
    
    ist.total_individuals,
    ist.total_male,
    ist.total_female,
    ist.total_twa,
    
    ps.total_transfers,
    ps.total_amount_paid,
    
    -- Grievances (TODO: add proper aggregation)
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    ls.active_provinces,
    
    CURRENT_TIMESTAMP AS last_updated
FROM global_stats gs
CROSS JOIN individual_stats ist
CROSS JOIN payment_stats ps
CROSS JOIN location_stats ls''',
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
        'sql': '''CREATE MATERIALIZED VIEW dashboard_master_summary_enhanced AS
SELECT 'MASTER_SUMMARY'::text AS summary_type, ( SELECT count(*) AS count FROM social_protection_groupbeneficiary WHERE ((social_protection_groupbeneficiary."isDeleted" = false) AND ((social_protection_groupbeneficiary.status)::text = 'ACTIVE'::text))) AS total_beneficiaries, ( SELECT count(DISTINCT social_protection_groupbeneficiary.group_id) AS count FROM social_protection_groupbeneficiary WHERE ((social_protection_groupbeneficiary."isDeleted" = false) AND ((social_protection_groupbeneficiary.status)::text = 'ACTIVE'::text))) AS total_households, ( SELECT count(*) AS count FROM ((social_protection_groupbeneficiary gb JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((i."Json_ext" ->> 'sexe'::text) = 'M'::text))) AS total_male, ( SELECT count(*) AS count FROM ((social_protection_groupbeneficiary gb JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((i."Json_ext" ->> 'sexe'::text) = 'F'::text))) AS total_female, ( SELECT count(DISTINCT i."UUID") AS count FROM (((social_protection_groupbeneficiary gb JOIN individual_group g ON ((gb.group_id = g."UUID"))) JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text))) AS total_twa, ( SELECT count(DISTINCT i."UUID") AS count FROM ((social_protection_groupbeneficiary gb JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((i."Json_ext" ->> 'handicap'::text) = 'OUI'::text))) AS total_disabled, ( SELECT count(DISTINCT i."UUID") AS count FROM ((social_protection_groupbeneficiary gb JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((i."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text))) AS total_chronic_illness, ( SELECT count(DISTINCT i."UUID") AS count FROM (((social_protection_groupbeneficiary gb JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) JOIN individual_group g ON ((gi.group_id = g."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text))) AS total_refugees, ( SELECT count(DISTINCT i."UUID") AS count FROM (((social_protection_groupbeneficiary gb JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) JOIN individual_group g ON ((gi.group_id = g."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text))) AS total_returnees, ( SELECT count(DISTINCT i."UUID") AS count FROM (((social_protection_groupbeneficiary gb JOIN individual_groupindividual gi ON ((gi.group_id = gb.group_id))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) JOIN individual_group g ON ((gi.group_id = g."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text))) AS total_displaced, ( SELECT count(DISTINCT g."UUID") AS count FROM (social_protection_groupbeneficiary gb JOIN individual_group g ON ((gb.group_id = g."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text))) AS twa_households, ( SELECT count(DISTINCT g."UUID") AS count FROM (social_protection_groupbeneficiary gb JOIN individual_group g ON ((gb.group_id = g."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'handicap'::text) = 'OUI'::text)))))) AS disabled_households, ( SELECT count(DISTINCT g."UUID") AS count FROM (social_protection_groupbeneficiary gb JOIN individual_group g ON ((gb.group_id = g."UUID"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text) AND (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text)))))) AS chronic_illness_households, (( SELECT sum(merankabandi_sensitizationtraining.twa_participants) AS sum FROM merankabandi_sensitizationtraining) + ( SELECT sum(merankabandi_behaviorchangepromotion.twa_participants) AS sum FROM merankabandi_behaviorchangepromotion)) AS total_twa_activity_participants, CURRENT_TIMESTAMP AS last_updated''',
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
