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
WITH 
-- Constants and configuration
constants AS (
    SELECT 
        'OUI'::text AS yes_value,
        'M'::text AS male_value,
        'F'::text AS female_value,
        'true'::text AS true_value,
        'menage_refugie'::text AS refugee_field,
        'menage_mutwa'::text AS twa_household_field,
        'is_twa'::text AS twa_individual_field,
        'sexe'::text AS sex_field,
        'REFUGEE'::text AS refugee_type,
        'HOST'::text AS host_type,
        'OTHER'::text AS other_type,
        'ALL'::text AS all_value,
        'ALL PLANS'::text AS all_plans_label,
        '00000000-0000-0000-0000-000000000000'::uuid AS all_plans_uuid,
        'RECONCILED'::text AS reconciled_status,
        ARRAY['Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo']::text[] AS host_communes
),

-- Base data: all groups with their location hierarchy and community type
base_groups AS (
    SELECT 
        ig."UUID" AS group_id,
        ig.location_id,
        ig."Json_ext",
        l1."LocationId" AS colline_id,
        l1."LocationName" AS colline,
        l2."LocationId" AS commune_id,
        l2."LocationName" AS commune,
        l3."LocationId" AS province_id,
        l3."LocationName" AS province
    FROM individual_group ig
    CROSS JOIN constants c
    JOIN "tblLocations" l1 ON l1."LocationId" = ig.location_id
    LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    WHERE ig."isDeleted" = false
),

-- Individuals with their group and demographic info
individuals_data AS (
    SELECT 
        gi.group_id,
        i."UUID" AS individual_id,
        i."Json_ext"->>c.sex_field AS sex,
        CASE 
            WHEN i."Json_ext"->>c.twa_individual_field = c.true_value THEN true
            ELSE false
        END AS is_twa_individual
    FROM individual_groupindividual gi
    CROSS JOIN constants c
    JOIN individual_individual i ON i."UUID" = gi.individual_id AND i."isDeleted" = false
    WHERE gi."isDeleted" = false
),

-- Group beneficiaries with their benefit plans
group_beneficiaries AS (
    SELECT 
        gb."UUID" AS beneficiary_id,
        gb.group_id,
        gb.benefit_plan_id,
        bp."UUID" AS plan_uuid,
        bp.code AS plan_code,
        bp.name AS plan_name
    FROM social_protection_groupbeneficiary gb
    JOIN social_protection_benefitplan bp ON bp."UUID" = gb.benefit_plan_id AND bp."isDeleted" = false
    WHERE gb."isDeleted" = false
),

-- Payment amounts aggregated by colline and benefit plan
payment_amounts_colline AS (
    SELECT 
        bg.colline_id,
        bg.commune_id,
        bg.province_id,
        gb.benefit_plan_id,
        COALESCE(SUM(CASE WHEN bc.status = c.reconciled_status THEN bc."Amount"::numeric END), 0) AS amount_paid,
        COALESCE(SUM(CASE WHEN bc.status <> c.reconciled_status THEN bc."Amount"::numeric END), 0) AS amount_unpaid,
        COALESCE(SUM(bc."Amount"::numeric), 0) AS amount_total
    FROM payroll_benefitconsumption bc
    CROSS JOIN constants c
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.benefit_id = bc."UUID" AND pbc."isDeleted" = false
    JOIN payroll_payroll p ON p."UUID" = pbc.payroll_id AND p."isDeleted" = false
    JOIN individual_individual i ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN individual_group ig ON ig."UUID" = gi.group_id AND ig."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = ig."UUID"
    LEFT JOIN group_beneficiaries gb ON gb.group_id = ig."UUID"
    WHERE bc."isDeleted" = false
    GROUP BY bg.colline_id, bg.commune_id, bg.province_id, gb.benefit_plan_id
),

-- Transfer counts by colline and benefit plan
transfer_counts_colline AS (
    SELECT 
        bg.colline_id,
        gb.benefit_plan_id,
        COUNT(DISTINCT p."UUID") AS transfer_count
    FROM payroll_payroll p
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = p."UUID" AND pbc."isDeleted" = false
    JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    JOIN individual_individual i ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN individual_group ig ON ig."UUID" = gi.group_id AND ig."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = ig."UUID"
    LEFT JOIN group_beneficiaries gb ON gb.group_id = ig."UUID"
    WHERE p."isDeleted" = false AND bc."isDeleted" = false
    GROUP BY bg.colline_id, gb.benefit_plan_id
),

-- Transfer counts by colline (all benefit plans)
transfer_counts_colline_all AS (
    SELECT 
        bg.colline_id,
        COUNT(DISTINCT p."UUID") AS transfer_count
    FROM payroll_payroll p
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = p."UUID" AND pbc."isDeleted" = false
    JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    JOIN individual_individual i ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN individual_group ig ON ig."UUID" = gi.group_id AND ig."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = ig."UUID"
    WHERE p."isDeleted" = false AND bc."isDeleted" = false
    GROUP BY bg.colline_id
),

-- Transfer counts by commune and benefit plan
transfer_counts_commune AS (
    SELECT 
        bg.commune_id,
        gb.benefit_plan_id,
        COUNT(DISTINCT p."UUID") AS transfer_count
    FROM payroll_payroll p
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = p."UUID" AND pbc."isDeleted" = false
    JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    JOIN individual_individual i ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN individual_group ig ON ig."UUID" = gi.group_id AND ig."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = ig."UUID"
    LEFT JOIN group_beneficiaries gb ON gb.group_id = ig."UUID"
    WHERE p."isDeleted" = false AND bc."isDeleted" = false
    GROUP BY bg.commune_id, gb.benefit_plan_id
),

-- Transfer counts by commune (all benefit plans)
transfer_counts_commune_all AS (
    SELECT 
        bg.commune_id,
        COUNT(DISTINCT p."UUID") AS transfer_count
    FROM payroll_payroll p
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = p."UUID" AND pbc."isDeleted" = false
    JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    JOIN individual_individual i ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN individual_group ig ON ig."UUID" = gi.group_id AND ig."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = ig."UUID"
    WHERE p."isDeleted" = false AND bc."isDeleted" = false
    GROUP BY bg.commune_id
),

-- Transfer counts by province and benefit plan
transfer_counts_province AS (
    SELECT 
        bg.province_id,
        gb.benefit_plan_id,
        COUNT(DISTINCT p."UUID") AS transfer_count
    FROM payroll_payroll p
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = p."UUID" AND pbc."isDeleted" = false
    JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    JOIN individual_individual i ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN individual_group ig ON ig."UUID" = gi.group_id AND ig."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = ig."UUID"
    LEFT JOIN group_beneficiaries gb ON gb.group_id = ig."UUID"
    WHERE p."isDeleted" = false AND bc."isDeleted" = false
    GROUP BY bg.province_id, gb.benefit_plan_id
),

-- Transfer counts by province (all benefit plans)
transfer_counts_province_all AS (
    SELECT 
        bg.province_id,
        COUNT(DISTINCT p."UUID") AS transfer_count
    FROM payroll_payroll p
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = p."UUID" AND pbc."isDeleted" = false
    JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    JOIN individual_individual i ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN individual_group ig ON ig."UUID" = gi.group_id AND ig."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = ig."UUID"
    WHERE p."isDeleted" = false AND bc."isDeleted" = false
    GROUP BY bg.province_id
),

-- Calculate distinct province count once
active_provinces_count AS (
    SELECT COUNT(DISTINCT province_id) AS active_provinces
    FROM base_groups
    WHERE province_id IS NOT NULL
),

-- Aggregated statistics by COLLINE and benefit plan
location_plan_stats_colline AS (
    SELECT 
        bg.province_id,
        bg.province,
        bg.commune_id,
        bg.commune,
        bg.colline_id,
        bg.colline,
        gb.plan_uuid AS benefit_plan_id,
        gb.plan_code AS benefit_plan_code,
        gb.plan_name AS benefit_plan_name,
        
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual OR bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS total_twa,
        
        COUNT(DISTINCT bg.group_id) AS total_households,
        COUNT(DISTINCT gb.beneficiary_id) AS total_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.male_value) AS male_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.female_value) AS female_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (
            WHERE bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS twa_beneficiaries
        
    FROM base_groups bg
    CROSS JOIN constants c
    LEFT JOIN individuals_data id ON id.group_id = bg.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
    WHERE gb.plan_uuid IS NOT NULL
    GROUP BY 
        bg.province_id, bg.province,
        bg.commune_id, bg.commune,
        bg.colline_id, bg.colline,
        gb.plan_uuid, gb.plan_code, gb.plan_name
),

-- Aggregated statistics by COMMUNE and benefit plan
location_plan_stats_commune AS (
    SELECT 
        bg.province_id,
        bg.province,
        bg.commune_id,
        bg.commune,
        gb.plan_uuid AS benefit_plan_id,
        gb.plan_code AS benefit_plan_code,
        gb.plan_name AS benefit_plan_name,
        
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual OR bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS total_twa,
        
        COUNT(DISTINCT bg.group_id) AS total_households,
        COUNT(DISTINCT gb.beneficiary_id) AS total_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.male_value) AS male_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.female_value) AS female_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (
            WHERE bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS twa_beneficiaries
        
    FROM base_groups bg
    CROSS JOIN constants c
    LEFT JOIN individuals_data id ON id.group_id = bg.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
    WHERE gb.plan_uuid IS NOT NULL
    GROUP BY 
        bg.province_id, bg.province,
        bg.commune_id, bg.commune,
        gb.plan_uuid, gb.plan_code, gb.plan_name
),

-- Aggregated statistics by PROVINCE and benefit plan
location_plan_stats_province AS (
    SELECT 
        bg.province_id,
        bg.province,
        gb.plan_uuid AS benefit_plan_id,
        gb.plan_code AS benefit_plan_code,
        gb.plan_name AS benefit_plan_name,
        
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual OR bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS total_twa,
        
        COUNT(DISTINCT bg.group_id) AS total_households,
        COUNT(DISTINCT gb.beneficiary_id) AS total_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.male_value) AS male_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.female_value) AS female_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (
            WHERE bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS twa_beneficiaries
        
    FROM base_groups bg
    CROSS JOIN constants c
    LEFT JOIN individuals_data id ON id.group_id = bg.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
    WHERE gb.plan_uuid IS NOT NULL
    GROUP BY 
        bg.province_id, bg.province,
        gb.plan_uuid, gb.plan_code, gb.plan_name
),

-- Aggregated statistics by COLLINE (all plans combined)
location_all_plans_stats_colline AS (
    SELECT 
        bg.province_id,
        bg.province,
        bg.commune_id,
        bg.commune,
        bg.colline_id,
        bg.colline,
        
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual OR bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS total_twa,
        
        COUNT(DISTINCT bg.group_id) AS total_households,
        COUNT(DISTINCT gb.beneficiary_id) AS total_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.male_value) AS male_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.female_value) AS female_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (
            WHERE bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS twa_beneficiaries
        
    FROM base_groups bg
    CROSS JOIN constants c
    LEFT JOIN individuals_data id ON id.group_id = bg.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
    GROUP BY 
        bg.province_id, bg.province,
        bg.commune_id, bg.commune,
        bg.colline_id, bg.colline
),

-- Aggregated statistics by COMMUNE (all plans combined)
location_all_plans_stats_commune AS (
    SELECT 
        bg.province_id,
        bg.province,
        bg.commune_id,
        bg.commune,
        
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual OR bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS total_twa,
        
        COUNT(DISTINCT bg.group_id) AS total_households,
        COUNT(DISTINCT gb.beneficiary_id) AS total_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.male_value) AS male_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.female_value) AS female_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (
            WHERE bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS twa_beneficiaries
        
    FROM base_groups bg
    CROSS JOIN constants c
    LEFT JOIN individuals_data id ON id.group_id = bg.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
    GROUP BY 
        bg.province_id, bg.province,
        bg.commune_id, bg.commune
),

-- Aggregated statistics by PROVINCE (all plans combined)
location_all_plans_stats_province AS (
    SELECT 
        bg.province_id,
        bg.province,
        
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual OR bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS total_twa,
        
        COUNT(DISTINCT bg.group_id) AS total_households,
        COUNT(DISTINCT gb.beneficiary_id) AS total_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.male_value) AS male_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.female_value) AS female_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (
            WHERE bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS twa_beneficiaries
        
    FROM base_groups bg
    CROSS JOIN constants c
    LEFT JOIN individuals_data id ON id.group_id = bg.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
    GROUP BY 
        bg.province_id, bg.province
),

-- Global statistics (all locations, all plans)
global_stats AS (
    SELECT 
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual OR bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS total_twa,
        
        COUNT(DISTINCT bg.group_id) AS total_households,
        COUNT(DISTINCT gb.beneficiary_id) AS total_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.male_value) AS male_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (WHERE id.sex = c.female_value) AS female_beneficiaries,
        COUNT(DISTINCT gb.beneficiary_id) FILTER (
            WHERE bg."Json_ext"->>c.twa_household_field = c.yes_value
        ) AS twa_beneficiaries
        
    FROM base_groups bg
    CROSS JOIN constants c
    LEFT JOIN individuals_data id ON id.group_id = bg.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
),

-- Global payment data
global_payment_data AS (
    SELECT 
        COUNT(DISTINCT p."UUID") AS transfer_count,
        COALESCE(SUM(CASE WHEN bc.status = c.reconciled_status THEN bc."Amount"::numeric END), 0) AS amount_paid,
        COALESCE(SUM(CASE WHEN bc.status <> c.reconciled_status THEN bc."Amount"::numeric END), 0) AS amount_unpaid,
        COALESCE(SUM(bc."Amount"::numeric), 0) AS amount_total
    FROM payroll_benefitconsumption bc
    CROSS JOIN constants c
    JOIN payroll_payrollbenefitconsumption pbc ON pbc.benefit_id = bc."UUID" AND pbc."isDeleted" = false
    JOIN payroll_payroll p ON p."UUID" = pbc.payroll_id AND p."isDeleted" = false
    WHERE bc."isDeleted" = false
)

-- ==========================================
-- COLLINE LEVEL - SPECIFIC BENEFIT PLAN
-- ==========================================
SELECT 
    lps.province_id,
    lps.province,
    lps.commune_id,
    lps.commune,
    lps.colline_id,
    lps.colline,
    lps.benefit_plan_id,
    lps.benefit_plan_code,
    lps.benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    lps.total_individuals,
    lps.total_male,
    lps.total_female,
    lps.total_twa,
    
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_male::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_female::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_twa::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS twa_percentage,
    
    lps.total_households,
    lps.total_beneficiaries,
    lps.male_beneficiaries,
    lps.female_beneficiaries,
    lps.twa_beneficiaries,
    
    COALESCE(tcc.transfer_count, 0) AS total_transfers,
    COALESCE(pa.amount_paid, 0) AS total_amount_paid,
    COALESCE(pa.amount_unpaid, 0) AS total_amount_unpaid,
    COALESCE(pa.amount_total, 0) AS total_amount,
    
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM location_plan_stats_colline lps
CROSS JOIN active_provinces_count apc
LEFT JOIN transfer_counts_colline tcc ON tcc.colline_id = lps.colline_id 
    AND tcc.benefit_plan_id = lps.benefit_plan_id
LEFT JOIN payment_amounts_colline pa ON pa.colline_id = lps.colline_id 
    AND pa.benefit_plan_id = lps.benefit_plan_id

UNION ALL

-- ==========================================
-- COMMUNE LEVEL - SPECIFIC BENEFIT PLAN
-- ==========================================
SELECT 
    lps.province_id,
    lps.province,
    lps.commune_id,
    lps.commune,
    NULL::integer AS colline_id,
    NULL::text AS colline,
    lps.benefit_plan_id,
    lps.benefit_plan_code,
    lps.benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    lps.total_individuals,
    lps.total_male,
    lps.total_female,
    lps.total_twa,
    
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_male::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_female::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_twa::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS twa_percentage,
    
    lps.total_households,
    lps.total_beneficiaries,
    lps.male_beneficiaries,
    lps.female_beneficiaries,
    lps.twa_beneficiaries,
    
    COALESCE(tcc.transfer_count, 0) AS total_transfers,
    COALESCE(SUM(pa.amount_paid), 0) AS total_amount_paid,
    COALESCE(SUM(pa.amount_unpaid), 0) AS total_amount_unpaid,
    COALESCE(SUM(pa.amount_total), 0) AS total_amount,
    
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM location_plan_stats_commune lps
CROSS JOIN active_provinces_count apc
LEFT JOIN transfer_counts_commune tcc ON tcc.commune_id = lps.commune_id 
    AND tcc.benefit_plan_id = lps.benefit_plan_id
LEFT JOIN payment_amounts_colline pa ON pa.commune_id = lps.commune_id 
AND pa.benefit_plan_id = lps.benefit_plan_id
GROUP BY 
    lps.province_id, lps.province,
    lps.commune_id, lps.commune,
    lps.benefit_plan_id, lps.benefit_plan_code, lps.benefit_plan_name,
    lps.total_individuals, lps.total_male, lps.total_female, lps.total_twa,
    lps.total_households, lps.total_beneficiaries,
    lps.male_beneficiaries, lps.female_beneficiaries, lps.twa_beneficiaries,
    apc.active_provinces, tcc.transfer_count

UNION ALL

-- ==========================================
-- PROVINCE LEVEL - SPECIFIC BENEFIT PLAN
-- ==========================================
SELECT 
    lps.province_id,
    lps.province,
    NULL::integer AS commune_id,
    NULL::text AS commune,
    NULL::integer AS colline_id,
    NULL::text AS colline,
    lps.benefit_plan_id,
    lps.benefit_plan_code,
    lps.benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    lps.total_individuals,
    lps.total_male,
    lps.total_female,
    lps.total_twa,
    
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_male::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_female::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN lps.total_individuals > 0 
        THEN ROUND((lps.total_twa::numeric / lps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS twa_percentage,
    
    lps.total_households,
    lps.total_beneficiaries,
    lps.male_beneficiaries,
    lps.female_beneficiaries,
    lps.twa_beneficiaries,
    
    COALESCE(tcp.transfer_count, 0) AS total_transfers,
    COALESCE(SUM(pa.amount_paid), 0) AS total_amount_paid,
    COALESCE(SUM(pa.amount_unpaid), 0) AS total_amount_unpaid,
    COALESCE(SUM(pa.amount_total), 0) AS total_amount,
    
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM location_plan_stats_province lps
CROSS JOIN active_provinces_count apc
LEFT JOIN transfer_counts_province tcp ON tcp.province_id = lps.province_id 
    AND tcp.benefit_plan_id = lps.benefit_plan_id
LEFT JOIN payment_amounts_colline pa ON pa.province_id = lps.province_id 
    AND pa.benefit_plan_id = lps.benefit_plan_id
GROUP BY 
    lps.province_id, lps.province,
    lps.benefit_plan_id, lps.benefit_plan_code, lps.benefit_plan_name,
    lps.total_individuals, lps.total_male, lps.total_female, lps.total_twa,
    lps.total_households, lps.total_beneficiaries,
    lps.male_beneficiaries, lps.female_beneficiaries, lps.twa_beneficiaries,
    apc.active_provinces, tcp.transfer_count

UNION ALL

-- ==========================================
-- COLLINE LEVEL - ALL BENEFIT PLANS
-- ==========================================
SELECT 
    laps.province_id,
    laps.province,
    laps.commune_id,
    laps.commune,
    laps.colline_id,
    laps.colline,
    c.all_plans_uuid AS benefit_plan_id,
    c.all_value AS benefit_plan_code,
    c.all_plans_label AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    laps.total_individuals,
    laps.total_male,
    laps.total_female,
    laps.total_twa,
    
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_male::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_female::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_twa::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS twa_percentage,
    
    laps.total_households,
    laps.total_beneficiaries,
    laps.male_beneficiaries,
    laps.female_beneficiaries,
    laps.twa_beneficiaries,
    
    COALESCE(tcca.transfer_count, 0) AS total_transfers,
    COALESCE(SUM(pa.amount_paid), 0) AS total_amount_paid,
    COALESCE(SUM(pa.amount_unpaid), 0) AS total_amount_unpaid,
    COALESCE(SUM(pa.amount_total), 0) AS total_amount,
    
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM location_all_plans_stats_colline laps
CROSS JOIN constants c
CROSS JOIN active_provinces_count apc
LEFT JOIN transfer_counts_colline_all tcca ON tcca.colline_id = laps.colline_id
LEFT JOIN payment_amounts_colline pa ON pa.colline_id = laps.colline_id
GROUP BY 
    laps.province_id, laps.province,
    laps.commune_id, laps.commune,
    laps.colline_id, laps.colline,
    c.all_plans_uuid, c.all_value, c.all_plans_label,
    laps.total_individuals, laps.total_male, laps.total_female, laps.total_twa,
    laps.total_households, laps.total_beneficiaries,
    laps.male_beneficiaries, laps.female_beneficiaries, laps.twa_beneficiaries,
    apc.active_provinces, tcca.transfer_count

UNION ALL

-- ==========================================
-- COMMUNE LEVEL - ALL BENEFIT PLANS
-- ==========================================
SELECT 
    laps.province_id,
    laps.province,
    laps.commune_id,
    laps.commune,
    NULL::integer AS colline_id,
    NULL::text AS colline,
    c.all_plans_uuid AS benefit_plan_id,
    c.all_value AS benefit_plan_code,
    c.all_plans_label AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    laps.total_individuals,
    laps.total_male,
    laps.total_female,
    laps.total_twa,
    
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_male::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_female::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_twa::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS twa_percentage,
    
    laps.total_households,
    laps.total_beneficiaries,
    laps.male_beneficiaries,
    laps.female_beneficiaries,
    laps.twa_beneficiaries,
    
    COALESCE(tcca.transfer_count, 0) AS total_transfers,
    COALESCE(SUM(pa.amount_paid), 0) AS total_amount_paid,
    COALESCE(SUM(pa.amount_unpaid), 0) AS total_amount_unpaid,
    COALESCE(SUM(pa.amount_total), 0) AS total_amount,
    
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM location_all_plans_stats_commune laps
CROSS JOIN constants c
CROSS JOIN active_provinces_count apc
LEFT JOIN transfer_counts_commune_all tcca ON tcca.commune_id = laps.commune_id
LEFT JOIN payment_amounts_colline pa ON pa.commune_id = laps.commune_id
GROUP BY 
    laps.province_id, laps.province,
    laps.commune_id, laps.commune,
    c.all_plans_uuid, c.all_value, c.all_plans_label,
    laps.total_individuals, laps.total_male, laps.total_female, laps.total_twa,
    laps.total_households, laps.total_beneficiaries,
    laps.male_beneficiaries, laps.female_beneficiaries, laps.twa_beneficiaries,
    apc.active_provinces, tcca.transfer_count

UNION ALL

-- ==========================================
-- PROVINCE LEVEL - ALL BENEFIT PLANS
-- ==========================================
SELECT 
    laps.province_id,
    laps.province,
    NULL::integer AS commune_id,
    NULL::text AS commune,
    NULL::integer AS colline_id,
    NULL::text AS colline,
    c.all_plans_uuid AS benefit_plan_id,
    c.all_value AS benefit_plan_code,
    c.all_plans_label AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    laps.total_individuals,
    laps.total_male,
    laps.total_female,
    laps.total_twa,
    
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_male::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_female::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN laps.total_individuals > 0 
        THEN ROUND((laps.total_twa::numeric / laps.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS twa_percentage,
    
    laps.total_households,
    laps.total_beneficiaries,
    laps.male_beneficiaries,
    laps.female_beneficiaries,
    laps.twa_beneficiaries,
    
    COALESCE(tcpa.transfer_count, 0) AS total_transfers,
    COALESCE(SUM(pa.amount_paid), 0) AS total_amount_paid,
    COALESCE(SUM(pa.amount_unpaid), 0) AS total_amount_unpaid,
    COALESCE(SUM(pa.amount_total), 0) AS total_amount,
    
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM location_all_plans_stats_province laps
CROSS JOIN constants c
CROSS JOIN active_provinces_count apc
LEFT JOIN transfer_counts_province_all tcpa ON tcpa.province_id = laps.province_id
LEFT JOIN payment_amounts_colline pa ON pa.province_id = laps.province_id
GROUP BY 
    laps.province_id, laps.province,
    c.all_plans_uuid, c.all_value, c.all_plans_label,
    laps.total_individuals, laps.total_male, laps.total_female, laps.total_twa,
    laps.total_households, laps.total_beneficiaries,
    laps.male_beneficiaries, laps.female_beneficiaries, laps.twa_beneficiaries,
    apc.active_provinces, tcpa.transfer_count

UNION ALL

-- ==========================================
-- GLOBAL LEVEL - ALL LOCATIONS, ALL PLANS
-- ==========================================
SELECT 
    NULL::integer AS province_id,
    c.all_value AS province,
    NULL::integer AS commune_id,
    c.all_value AS commune,
    NULL::integer AS colline_id,
    c.all_value AS colline,
    c.all_plans_uuid AS benefit_plan_id,
    c.all_value AS benefit_plan_code,
    c.all_plans_label AS benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,
    
    gs.total_individuals,
    gs.total_male,
    gs.total_female,
    gs.total_twa,
    
    CASE 
        WHEN gs.total_individuals > 0 
        THEN ROUND((gs.total_male::numeric / gs.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS male_percentage,
    CASE 
        WHEN gs.total_individuals > 0 
        THEN ROUND((gs.total_female::numeric / gs.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS female_percentage,
    CASE 
        WHEN gs.total_individuals > 0 
        THEN ROUND((gs.total_twa::numeric / gs.total_individuals::numeric * 100), 2)
        ELSE 0 
    END AS twa_percentage,
    
    gs.total_households,
    gs.total_beneficiaries,
    gs.male_beneficiaries,
    gs.female_beneficiaries,
    gs.twa_beneficiaries,
    
    gpd.transfer_count AS total_transfers,
    gpd.amount_paid AS total_amount_paid,
    gpd.amount_unpaid AS total_amount_unpaid,
    gpd.amount_total AS total_amount,
    
    0 AS total_grievances,
    0 AS resolved_grievances,
    
    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM global_stats gs
CROSS JOIN constants c
CROSS JOIN active_provinces_count apc
CROSS JOIN global_payment_data gpd;''',
        'indexes': [
            """CREATE INDEX idx_dashboard_summary_location_hierarchy ON dashboard_individual_summary USING btree (province_id, commune_id, colline_id);""",
            """CREATE INDEX idx_dashboard_summary_plan_location ON dashboard_individual_summary USING btree (benefit_plan_id, colline_id);""",
            """CREATE INDEX idx_dashboard_summary_temporal ON dashboard_individual_summary USING btree (year, month);""",
            """CREATE INDEX idx_dashboard_summary_community_location ON dashboard_individual_summary USING btree (community_type, province_id);""",
            """CREATE INDEX idx_dashboard_summary_covering ON dashboard_individual_summary USING btree (colline_id, benefit_plan_id, community_type) INCLUDE (total_individuals, total_households, total_beneficiaries, total_amount);""",
            """CREATE INDEX idx_dashboard_summary_benefit_plan ON dashboard_individual_summary USING btree (benefit_plan_id) WHERE benefit_plan_id != '00000000-0000-0000-0000-000000000000'::uuid;""",
            """CREATE INDEX idx_dashboard_summary_detail_rows ON dashboard_individual_summary USING btree (province_id, benefit_plan_id) WHERE province_id IS NOT NULL AND benefit_plan_id != '00000000-0000-0000-0000-000000000000'::uuid;""",
            """CREATE INDEX idx_dashboard_summary_quarter ON dashboard_individual_summary USING btree (year, quarter);""",
            """CREATE INDEX idx_dashboard_summary_community ON dashboard_individual_summary USING btree (community_type);""",
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
        ) AS female_beneficiaries,
		COUNT(DISTINCT gb."UUID") FILTER (
            WHERE (gb."Json_ext" ->> 'menage_mutwa'::TEXT) = c.twa_indicator
        ) AS twa_beneficiaries
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
    CROSS JOIN config c
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
	SELECT COUNT(*) AS total_grievances, 
	      COUNT(*) FILTER (
            WHERE g.status = 'RESOLVED'
        ) AS resolved_grievances
	FROM grievance_social_protection_ticket g
	WHERE g."isDeleted" = false
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
    bs.twa_beneficiaries,
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
    'dashboard_vulnerable_groups_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_vulnerable_groups_summary AS
SELECT l3."LocationName" AS province, l3."LocationId" AS province_id, (g."Json_ext" ->> 'type_menage'::text) AS household_type, bp."UUID" AS benefit_plan_id, bp.code AS benefit_plan_code, bp.name AS benefit_plan_name, count(DISTINCT g."UUID") AS total_households, count(DISTINCT i."UUID") AS total_members, count(DISTINCT CASE WHEN ((gi.recipient_type)::text = 'PRIMARY'::text) THEN i."UUID" ELSE NULL::uuid END) AS total_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS twa_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS twa_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_mutwa'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS twa_beneficiaries, count(DISTINCT CASE WHEN (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'handicap'::text) = 'OUI'::text)))) THEN g."UUID" ELSE NULL::uuid END) AS disabled_households, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'handicap'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS disabled_members, count(DISTINCT CASE WHEN (((i."Json_ext" ->> 'handicap'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS disabled_beneficiaries, count(DISTINCT CASE WHEN (EXISTS ( SELECT 1 FROM (individual_groupindividual gi2 JOIN individual_individual i2 ON ((gi2.individual_id = i2."UUID"))) WHERE ((gi2.group_id = g."UUID") AND ((i2."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text)))) THEN g."UUID" ELSE NULL::uuid END) AS chronic_illness_households, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS chronic_illness_members, count(DISTINCT CASE WHEN (((i."Json_ext" ->> 'maladie_chro'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS chronic_illness_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS refugee_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS refugee_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_refugie'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS refugee_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS returnee_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS returnee_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_rapatrie'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS returnee_beneficiaries, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) THEN g."UUID" ELSE NULL::uuid END) AS displaced_households, count(DISTINCT CASE WHEN ((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) THEN i."UUID" ELSE NULL::uuid END) AS displaced_members, count(DISTINCT CASE WHEN (((g."Json_ext" ->> 'menage_deplace'::text) = 'OUI'::text) AND ((gi.recipient_type)::text = 'PRIMARY'::text)) THEN i."UUID" ELSE NULL::uuid END) AS displaced_beneficiaries, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%physique%'::text) THEN i."UUID" ELSE NULL::uuid END) AS physical_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%mental%'::text) THEN i."UUID" ELSE NULL::uuid END) AS mental_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%visuel%'::text) THEN i."UUID" ELSE NULL::uuid END) AS visual_disability_count, count(DISTINCT CASE WHEN ((i."Json_ext" ->> 'type_handicap'::text) ~~ '%auditif%'::text) THEN i."UUID" ELSE NULL::uuid END) AS hearing_disability_count, CURRENT_DATE AS report_date FROM (((((((social_protection_groupbeneficiary gb JOIN social_protection_benefitplan bp ON ((gb.benefit_plan_id = bp."UUID"))) JOIN individual_group g ON ((gb.group_id = g."UUID"))) JOIN individual_groupindividual gi ON ((gi.group_id = g."UUID"))) JOIN individual_individual i ON ((gi.individual_id = i."UUID"))) LEFT JOIN "tblLocations" l1 ON ((g.location_id = l1."LocationId"))) LEFT JOIN "tblLocations" l2 ON ((l1."ParentLocationId" = l2."LocationId"))) LEFT JOIN "tblLocations" l3 ON ((l2."ParentLocationId" = l3."LocationId"))) WHERE ((gb."isDeleted" = false) AND ((gb.status)::text = 'ACTIVE'::text)) GROUP BY l3."LocationName", l3."LocationId", (g."Json_ext" ->> 'type_menage'::text), bp."UUID", bp.code, bp.name, CURRENT_DATE''',
        'indexes': [
        ]
    },
}
