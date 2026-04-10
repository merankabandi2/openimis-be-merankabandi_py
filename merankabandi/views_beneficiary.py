"""
Beneficiary Materialized Views
Uses PostgreSQL ROLLUP for multi-level location aggregation instead of
repetitive UNION ALL branches.

Location hierarchy: Province > Commune > Colline
Plan dimension: per-plan + all-plans (computed separately due to cross-plan uniqueness)
"""

BENEFICIARY_VIEWS = {
    'dashboard_individual_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_individual_summary AS
WITH
-- Configuration constants (single source of truth)
constants AS (
    SELECT
        'OUI'::text AS yes_value,
        'M'::text AS male_value,
        'F'::text AS female_value,
        'true'::text AS true_value,
        'menage_mutwa'::text AS twa_household_field,
        'is_twa'::text AS twa_individual_field,
        'sexe'::text AS sex_field,
        'RECONCILED'::text AS reconciled_status,
        '00000000-0000-0000-0000-000000000000'::uuid AS all_plans_uuid,
        'ALL'::text AS all_value,
        'ALL PLANS'::text AS all_plans_label
),

-- Base data: all non-deleted groups with their location hierarchy
base_groups AS (
    SELECT
        ig."UUID" AS group_id,
        ig."Json_ext",
        l1."LocationId" AS colline_id,
        l1."LocationName" AS colline,
        l2."LocationId" AS commune_id,
        l2."LocationName" AS commune,
        l3."LocationId" AS province_id,
        l3."LocationName" AS province
    FROM individual_group ig
    JOIN "tblLocations" l1 ON l1."LocationId" = ig.location_id
    LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    WHERE ig."isDeleted" = false
),

-- Individuals linked to groups with demographic info
-- Gender normalization: handles both short codes (M/F) and full French words (Masculin/Féminin)
individuals_data AS (
    SELECT
        gi.group_id,
        i."UUID" AS individual_id,
        CASE
            WHEN UPPER(LEFT(i."Json_ext"->>(SELECT sex_field FROM constants), 1)) = 'M'
                THEN (SELECT male_value FROM constants)
            WHEN UPPER(LEFT(i."Json_ext"->>(SELECT sex_field FROM constants), 1)) = 'F'
                THEN (SELECT female_value FROM constants)
            ELSE i."Json_ext"->>(SELECT sex_field FROM constants)
        END AS sex,
        CASE
            WHEN i."Json_ext"->>(SELECT twa_individual_field FROM constants)
                 = (SELECT true_value FROM constants) THEN true
            ELSE false
        END AS is_twa_individual
    FROM individual_groupindividual gi
    JOIN individual_individual i ON i."UUID" = gi.individual_id AND i."isDeleted" = false
    WHERE gi."isDeleted" = false
),

-- Group beneficiaries with their benefit plan info
group_beneficiaries AS (
    SELECT
        gb."UUID" AS beneficiary_id,
        gb.group_id,
        bp."UUID" AS plan_uuid,
        bp.code AS plan_code,
        bp.name AS plan_name
    FROM social_protection_groupbeneficiary gb
    JOIN social_protection_benefitplan bp
        ON bp."UUID" = gb.benefit_plan_id AND bp."isDeleted" = false
    WHERE gb."isDeleted" = false
),

-- Raw payment rows (kept un-aggregated for correct COUNT(DISTINCT) under ROLLUP)
payment_data AS (
    SELECT
        bg.colline_id,
        bg.commune_id,
        bg.province_id,
        gb.plan_uuid AS benefit_plan_id,
        p."UUID" AS payroll_id,
        bc."Amount"::numeric AS amount,
        bc.status
    FROM payroll_benefitconsumption bc
    JOIN payroll_payrollbenefitconsumption pbc
        ON pbc.benefit_id = bc."UUID" AND pbc."isDeleted" = false
    JOIN payroll_payroll p
        ON p."UUID" = pbc.payroll_id AND p."isDeleted" = false
    JOIN individual_individual i
        ON i."UUID" = bc.individual_id AND i."isDeleted" = false
    JOIN individual_groupindividual gi
        ON gi.individual_id = i."UUID" AND gi."isDeleted" = false
    JOIN base_groups bg ON bg.group_id = gi.group_id
    LEFT JOIN group_beneficiaries gb ON gb.group_id = gi.group_id
    WHERE bc."isDeleted" = false
),

-- Province count (computed once)
active_provinces_count AS (
    SELECT COUNT(DISTINCT province_id) AS active_provinces
    FROM base_groups
    WHERE province_id IS NOT NULL
),

-- =====================================================================
-- DEMOGRAPHICS: Per-plan with location ROLLUP
-- ROLLUP(province, commune, colline) produces 4 levels automatically:
--   (province, commune, colline) → colline detail
--   (province, commune)          → commune rollup (colline = NULL)
--   (province)                   → province rollup
--   ()                           → global rollup
-- =====================================================================
demo_per_plan AS (
    SELECT
        bg.province_id, bg.province,
        bg.commune_id, bg.commune,
        bg.colline_id, bg.colline,
        gb.plan_uuid AS benefit_plan_id,
        gb.plan_code AS benefit_plan_code,
        gb.plan_name AS benefit_plan_name,
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual
               OR bg."Json_ext"->>c.twa_household_field = c.yes_value
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
    GROUP BY ROLLUP(
        (bg.province_id, bg.province),
        (bg.commune_id, bg.commune),
        (bg.colline_id, bg.colline)
    ), gb.plan_uuid, gb.plan_code, gb.plan_name
),

-- DEMOGRAPHICS: All-plans with location ROLLUP
-- Computed separately because COUNT(DISTINCT individual_id) across plans
-- cannot be derived from per-plan counts (individuals may appear in multiple plans)
-- NOTE: Only groups that have at least one beneficiary record are included,
-- so total_individuals counts members of beneficiary households only.
demo_all_plans AS (
    SELECT
        bg.province_id, bg.province,
        bg.commune_id, bg.commune,
        bg.colline_id, bg.colline,
        c.all_plans_uuid AS benefit_plan_id,
        c.all_value AS benefit_plan_code,
        c.all_plans_label AS benefit_plan_name,
        COUNT(DISTINCT id.individual_id) AS total_individuals,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.male_value) AS total_male,
        COUNT(DISTINCT id.individual_id) FILTER (WHERE id.sex = c.female_value) AS total_female,
        COUNT(DISTINCT id.individual_id) FILTER (
            WHERE id.is_twa_individual
               OR bg."Json_ext"->>c.twa_household_field = c.yes_value
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
    INNER JOIN group_beneficiaries gb ON gb.group_id = bg.group_id
    GROUP BY ROLLUP(
        (bg.province_id, bg.province),
        (bg.commune_id, bg.commune),
        (bg.colline_id, bg.colline)
    ), c.all_plans_uuid, c.all_value, c.all_plans_label
),

demographics AS (
    SELECT * FROM demo_per_plan
    UNION ALL
    SELECT * FROM demo_all_plans
),

-- =====================================================================
-- PAYMENTS: Per-plan with location ROLLUP
-- Uses raw payment rows so COUNT(DISTINCT payroll_id) is correct at
-- every rollup level (a single payroll can span multiple collines)
-- =====================================================================
pay_per_plan AS (
    SELECT
        province_id, commune_id, colline_id,
        benefit_plan_id,
        COUNT(DISTINCT payroll_id) AS transfer_count,
        COALESCE(SUM(amount) FILTER (WHERE status = 'RECONCILED'), 0) AS amount_paid,
        COALESCE(SUM(amount) FILTER (WHERE status <> 'RECONCILED'), 0) AS amount_unpaid,
        COALESCE(SUM(amount), 0) AS amount_total
    FROM payment_data
    WHERE benefit_plan_id IS NOT NULL
    GROUP BY ROLLUP(province_id, commune_id, colline_id), benefit_plan_id
),

pay_all_plans AS (
    SELECT
        province_id, commune_id, colline_id,
        (SELECT all_plans_uuid FROM constants) AS benefit_plan_id,
        COUNT(DISTINCT payroll_id) AS transfer_count,
        COALESCE(SUM(amount) FILTER (WHERE status = 'RECONCILED'), 0) AS amount_paid,
        COALESCE(SUM(amount) FILTER (WHERE status <> 'RECONCILED'), 0) AS amount_unpaid,
        COALESCE(SUM(amount), 0) AS amount_total
    FROM payment_data
    GROUP BY ROLLUP(province_id, commune_id, colline_id)
),

payments AS (
    SELECT * FROM pay_per_plan
    UNION ALL
    SELECT * FROM pay_all_plans
)

-- =====================================================================
-- FINAL: Join demographics + payments at matching rollup level
-- IS NOT DISTINCT FROM handles NULL = NULL matching for rolled-up levels
-- =====================================================================
SELECT
    d.province_id, d.province,
    d.commune_id, d.commune,
    d.colline_id, d.colline,
    d.benefit_plan_id, d.benefit_plan_code, d.benefit_plan_name,
    EXTRACT(year FROM CURRENT_DATE) AS year,
    date_trunc('month', CURRENT_DATE) AS month,
    date_trunc('quarter', CURRENT_DATE) AS quarter,

    d.total_individuals, d.total_male, d.total_female, d.total_twa,

    CASE WHEN d.total_individuals > 0
        THEN ROUND((d.total_male::numeric / d.total_individuals::numeric * 100), 2)
        ELSE 0 END AS male_percentage,
    CASE WHEN d.total_individuals > 0
        THEN ROUND((d.total_female::numeric / d.total_individuals::numeric * 100), 2)
        ELSE 0 END AS female_percentage,
    CASE WHEN d.total_individuals > 0
        THEN ROUND((d.total_twa::numeric / d.total_individuals::numeric * 100), 2)
        ELSE 0 END AS twa_percentage,

    d.total_households, d.total_beneficiaries,
    d.male_beneficiaries, d.female_beneficiaries, d.twa_beneficiaries,

    COALESCE(p.transfer_count, 0) AS total_transfers,
    COALESCE(p.amount_paid, 0) AS total_amount_paid,
    COALESCE(p.amount_unpaid, 0) AS total_amount_unpaid,
    COALESCE(p.amount_total, 0) AS total_amount,

    0 AS total_grievances,
    0 AS resolved_grievances,

    apc.active_provinces,
    CURRENT_TIMESTAMP AS last_updated
FROM demographics d
CROSS JOIN active_provinces_count apc
LEFT JOIN payments p ON
    d.colline_id IS NOT DISTINCT FROM p.colline_id
    AND d.commune_id IS NOT DISTINCT FROM p.commune_id
    AND d.province_id IS NOT DISTINCT FROM p.province_id
    AND d.benefit_plan_id IS NOT DISTINCT FROM p.benefit_plan_id''',
        'indexes': [
            """CREATE INDEX idx_individual_summary_location ON dashboard_individual_summary USING btree (province_id, commune_id, colline_id);""",
            """CREATE INDEX idx_individual_summary_plan_location ON dashboard_individual_summary USING btree (benefit_plan_id, colline_id);""",
            """CREATE INDEX idx_individual_summary_temporal ON dashboard_individual_summary USING btree (year, month);""",
            """CREATE INDEX idx_individual_summary_covering ON dashboard_individual_summary USING btree (colline_id, benefit_plan_id) INCLUDE (total_individuals, total_households, total_beneficiaries, total_amount);""",
            """CREATE INDEX idx_individual_summary_benefit_plan ON dashboard_individual_summary USING btree (benefit_plan_id) WHERE benefit_plan_id != '00000000-0000-0000-0000-000000000000'::uuid;""",
            """CREATE INDEX idx_individual_summary_detail ON dashboard_individual_summary USING btree (province_id, benefit_plan_id) WHERE province_id IS NOT NULL AND benefit_plan_id != '00000000-0000-0000-0000-000000000000'::uuid;""",
            """CREATE INDEX idx_individual_summary_quarter ON dashboard_individual_summary USING btree (year, quarter);""",
        ]
    },
    'dashboard_master_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_master_summary AS
WITH
config AS (
    SELECT
        'ACTIVE'::text AS active_status,
        'RECONCILED'::text AS reconciled_status,
        'M'::text AS male_gender,
        'F'::text AS female_gender,
        'OUI'::text AS twa_indicator
),
beneficiary_stats AS (
    SELECT
        COUNT(DISTINCT gb."UUID") AS total_beneficiaries,
        COUNT(DISTINCT gb."UUID") FILTER (WHERE gb.status = c.active_status) AS active_beneficiaries,
        COUNT(DISTINCT gb."UUID") FILTER (WHERE UPPER(LEFT(i."Json_ext"->>'sexe', 1)) = 'M') AS male_beneficiaries,
        COUNT(DISTINCT gb."UUID") FILTER (WHERE UPPER(LEFT(i."Json_ext"->>'sexe', 1)) = 'F') AS female_beneficiaries,
        COUNT(DISTINCT gb."UUID") FILTER (WHERE (gb."Json_ext" ->> 'menage_mutwa') = c.twa_indicator) AS twa_beneficiaries
    FROM social_protection_groupbeneficiary gb
    CROSS JOIN config c
    LEFT JOIN individual_groupindividual gi ON gi.group_id = gb.group_id AND gi."recipient_type" = 'PRIMARY'
    LEFT JOIN individual_individual i ON i."UUID" = gi.individual_id AND i."isDeleted" = false
    WHERE gb."isDeleted" = false
),
household_stats AS (
    SELECT
        COUNT(DISTINCT ig."UUID") AS total_households,
        COUNT(DISTINCT ig."UUID") FILTER (WHERE (ig."Json_ext" ->> 'menage_mutwa') = c.twa_indicator) AS total_twa
    FROM individual_group ig
    CROSS JOIN config c
    WHERE ig."isDeleted" = false
),
individual_demographics AS (
    SELECT
        COUNT(DISTINCT i."UUID") AS total_individuals,
        COUNT(DISTINCT i."UUID") FILTER (WHERE UPPER(LEFT(i."Json_ext"->>'sexe', 1)) = 'M') AS total_male,
        COUNT(DISTINCT i."UUID") FILTER (WHERE UPPER(LEFT(i."Json_ext"->>'sexe', 1)) = 'F') AS total_female
    FROM individual_individual i
    CROSS JOIN config c
    WHERE i."isDeleted" = false
),
payment_summary AS (
    SELECT
        COUNT(DISTINCT pp."UUID") AS total_transfers,
        COALESCE(SUM(bc."Amount"::numeric) FILTER (WHERE bc.status = c.reconciled_status), 0) AS total_amount_paid,
        COALESCE(SUM(bc."Amount"::numeric) FILTER (WHERE bc.status <> c.reconciled_status), 0) AS total_amount_unpaid,
        COALESCE(SUM(bc."Amount"::numeric), 0) AS total_amount
    FROM payroll_payroll pp
    CROSS JOIN config c
    LEFT JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = pp."UUID" AND pbc."isDeleted" = false
    LEFT JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
    WHERE pp."isDeleted" = false
),
geographic_coverage AS (
    SELECT COUNT(DISTINCT l3."LocationId") AS active_provinces
    FROM individual_group ig
    INNER JOIN "tblLocations" l1 ON ig.location_id = l1."LocationId"
    INNER JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
    INNER JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
    WHERE ig."isDeleted" = false
),
grievance_stats AS (
    SELECT
        COUNT(*) AS total_grievances,
        COUNT(*) FILTER (WHERE g.status = 'RESOLVED') AS resolved_grievances
    FROM grievance_social_protection_ticket g
    WHERE g."isDeleted" = false
)
SELECT
    bs.total_beneficiaries,
    bs.active_beneficiaries,
    bs.male_beneficiaries,
    bs.female_beneficiaries,
    bs.twa_beneficiaries,
    hs.total_households,
    hs.total_twa,
    id.total_individuals,
    id.total_male,
    id.total_female,
    ps.total_transfers,
    ps.total_amount_paid,
    gs.total_grievances,
    gs.resolved_grievances,
    gc.active_provinces,
    EXTRACT(year FROM CURRENT_DATE)::integer AS year,
    date_trunc('month', CURRENT_DATE)::date AS month,
    date_trunc('quarter', CURRENT_DATE)::date AS quarter,
    CURRENT_TIMESTAMP AS last_updated
FROM beneficiary_stats bs
CROSS JOIN household_stats hs
CROSS JOIN individual_demographics id
CROSS JOIN payment_summary ps
CROSS JOIN geographic_coverage gc
CROSS JOIN grievance_stats gs''',
        'indexes': []
    },
    'dashboard_vulnerable_groups_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_vulnerable_groups_summary AS
SELECT
    l3."LocationName" AS province,
    l3."LocationId" AS province_id,
    (g."Json_ext" ->> 'type_menage') AS household_type,
    bp."UUID" AS benefit_plan_id,
    bp.code AS benefit_plan_code,
    bp.name AS benefit_plan_name,
    COUNT(DISTINCT g."UUID") AS total_households,
    COUNT(DISTINCT i."UUID") AS total_members,
    COUNT(DISTINCT CASE WHEN gi.recipient_type = 'PRIMARY' THEN i."UUID" END) AS total_beneficiaries,
    -- TWA
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_mutwa') = 'OUI' THEN g."UUID" END) AS twa_households,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_mutwa') = 'OUI' THEN i."UUID" END) AS twa_members,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_mutwa') = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) AS twa_beneficiaries,
    -- Disabled
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM individual_groupindividual gi2
        JOIN individual_individual i2 ON gi2.individual_id = i2."UUID"
        WHERE gi2.group_id = g."UUID" AND (i2."Json_ext" ->> 'handicap') = 'OUI'
    ) THEN g."UUID" END) AS disabled_households,
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'handicap') = 'OUI' THEN i."UUID" END) AS disabled_members,
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'handicap') = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) AS disabled_beneficiaries,
    -- Chronic illness
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM individual_groupindividual gi2
        JOIN individual_individual i2 ON gi2.individual_id = i2."UUID"
        WHERE gi2.group_id = g."UUID" AND (i2."Json_ext" ->> 'maladie_chro') = 'OUI'
    ) THEN g."UUID" END) AS chronic_illness_households,
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'maladie_chro') = 'OUI' THEN i."UUID" END) AS chronic_illness_members,
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'maladie_chro') = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) AS chronic_illness_beneficiaries,
    -- Refugee
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_refugie') = 'OUI' THEN g."UUID" END) AS refugee_households,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_refugie') = 'OUI' THEN i."UUID" END) AS refugee_members,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_refugie') = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) AS refugee_beneficiaries,
    -- Returnee
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_rapatrie') = 'OUI' THEN g."UUID" END) AS returnee_households,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_rapatrie') = 'OUI' THEN i."UUID" END) AS returnee_members,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_rapatrie') = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) AS returnee_beneficiaries,
    -- Displaced
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_deplace') = 'OUI' THEN g."UUID" END) AS displaced_households,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_deplace') = 'OUI' THEN i."UUID" END) AS displaced_members,
    COUNT(DISTINCT CASE WHEN (g."Json_ext" ->> 'menage_deplace') = 'OUI' AND gi.recipient_type = 'PRIMARY' THEN i."UUID" END) AS displaced_beneficiaries,
    -- Disability types
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'type_handicap') LIKE '%physique%' THEN i."UUID" END) AS physical_disability_count,
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'type_handicap') LIKE '%mental%' THEN i."UUID" END) AS mental_disability_count,
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'type_handicap') LIKE '%visuel%' THEN i."UUID" END) AS visual_disability_count,
    COUNT(DISTINCT CASE WHEN (i."Json_ext" ->> 'type_handicap') LIKE '%auditif%' THEN i."UUID" END) AS hearing_disability_count,
    CURRENT_DATE AS report_date
FROM social_protection_groupbeneficiary gb
JOIN social_protection_benefitplan bp ON gb.benefit_plan_id = bp."UUID"
JOIN individual_group g ON gb.group_id = g."UUID"
JOIN individual_groupindividual gi ON gi.group_id = g."UUID"
JOIN individual_individual i ON gi.individual_id = i."UUID"
LEFT JOIN "tblLocations" l1 ON g.location_id = l1."LocationId"
LEFT JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
LEFT JOIN "tblLocations" l3 ON l2."ParentLocationId" = l3."LocationId"
WHERE gb."isDeleted" = false AND gb.status = 'ACTIVE'
GROUP BY l3."LocationName", l3."LocationId",
    (g."Json_ext" ->> 'type_menage'), bp."UUID", bp.code, bp.name, CURRENT_DATE''',
        'indexes': []
    },
}
