"""
Payment Materialized Views
All views related to payments, transfers, and financial reporting
"""

PAYMENT_VIEWS = {
    'payment_reporting_monetary_transfers': {
        'sql': '''CREATE MATERIALIZED VIEW payment_reporting_monetary_transfers AS
SELECT EXTRACT(year FROM mt.transfer_date) AS year, EXTRACT(month FROM mt.transfer_date) AS month, EXTRACT(quarter FROM mt.transfer_date) AS quarter, mt.transfer_date, loc."LocationId" AS location_id, loc."LocationName" AS location_name, loc."LocationType" AS location_type, com."LocationId" AS commune_id, com."LocationName" AS commune_name, prov."LocationId" AS province_id, prov."LocationName" AS province_name, mt.programme_id, bp.code AS programme_code, bp.name AS programme_name, bp.ceiling_per_beneficiary AS amount_per_beneficiary, mt.payment_agency_id, pp.name AS payment_agency_name, mt.planned_women, mt.planned_men, mt.planned_twa, ((mt.planned_women + mt.planned_men) + mt.planned_twa) AS total_planned, mt.paid_women, mt.paid_men, mt.paid_twa, ((mt.paid_women + mt.paid_men) + mt.paid_twa) AS total_paid, (COALESCE(bp.ceiling_per_beneficiary, (0)::numeric) * (((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric) AS total_amount_paid, CASE WHEN (((mt.paid_women + mt.paid_men) + mt.paid_twa) > 0) THEN (((mt.paid_women)::numeric / (((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric) * (100)::numeric) ELSE (0)::numeric END AS female_percentage, CASE WHEN (((mt.paid_women + mt.paid_men) + mt.paid_twa) > 0) THEN (((mt.paid_twa)::numeric / (((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric) * (100)::numeric) ELSE (0)::numeric END AS twa_percentage, CASE WHEN (((mt.planned_women + mt.planned_men) + mt.planned_twa) > 0) THEN (((((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric / (((mt.planned_women + mt.planned_men) + mt.planned_twa))::numeric) * (100)::numeric) ELSE (0)::numeric END AS completion_rate FROM (((((merankabandi_monetarytransfer mt LEFT JOIN social_protection_benefitplan bp ON ((bp."UUID" = mt.programme_id))) LEFT JOIN payroll_paymentpoint pp ON ((pp."UUID" = mt.payment_agency_id))) LEFT JOIN "tblLocations" loc ON ((loc."LocationId" = mt.location_id))) LEFT JOIN "tblLocations" com ON ((com."LocationId" = loc."ParentLocationId"))) LEFT JOIN "tblLocations" prov ON ((prov."LocationId" = com."ParentLocationId"))) WHERE (mt.transfer_date IS NOT NULL)''',
        'indexes': [
        ]
    },
    'payment_reporting_unified_summary': {
        'sql': '''CREATE MATERIALIZED VIEW payment_reporting_unified_summary AS
WITH combined_payments AS (
    -- Benefit consumption payments (from the system)
    SELECT 
        EXTRACT(year FROM bc."DateDue") AS year,
        EXTRACT(month FROM bc."DateDue") AS month,
        EXTRACT(quarter FROM bc."DateDue") AS quarter,
        bc."DateDue" AS payment_date,
        grp.location_id AS location_id,
        loc."LocationName" AS location_name,
        loc."LocationType" AS location_type,
        com."LocationId" AS commune_id,
        com."LocationName" AS commune_name,
        prov."LocationId" AS province_id,
        prov."LocationName" AS province_name,
        -- Colline level (location_id when it's a colline)
        CASE WHEN loc."LocationType" = 'S' THEN loc."LocationId" ELSE NULL END AS colline_id,
        CASE WHEN loc."LocationType" = 'S' THEN loc."LocationName" ELSE NULL END AS colline_name,
        bp."UUID" AS programme_id,
        bp.code AS programme_code,
        bp.name AS programme_name,
        'BENEFIT_CONSUMPTION' AS payment_source,
        bc."Amount" AS amount_paid,
        1 AS beneficiary_count,
        CASE WHEN ind."Json_ext"->>'sexe' = 'F' THEN 1 ELSE 0 END AS female_count,
        CASE WHEN grp."Json_ext"->>'menage_mutwa' = 'OUI' THEN 1 ELSE 0 END AS twa_count,
        bc.status AS payment_status,
        pp.name AS payment_point_name
    FROM payroll_benefitconsumption bc
    INNER JOIN individual_individual ind ON ind."UUID" = bc.individual_id
    INNER JOIN individual_groupindividual gi ON gi.individual_id = ind."UUID" AND gi."isDeleted" = false
    INNER JOIN individual_group grp ON grp."UUID" = gi.group_id AND grp."isDeleted" = false
    INNER JOIN social_protection_groupbeneficiary gb ON gb.group_id = grp."UUID" AND gb."isDeleted" = false
    INNER JOIN social_protection_benefitplan bp ON bp."UUID" = gb.benefit_plan_id
    LEFT JOIN "tblLocations" loc ON loc."LocationId" = grp.location_id
    LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
    LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
    LEFT JOIN payroll_payrollbenefitconsumption pbc ON pbc.benefit_id = bc."UUID" AND pbc."isDeleted" = false
    LEFT JOIN payroll_payroll p ON p."UUID" = pbc.payroll_id AND p."isDeleted" = false
    LEFT JOIN payroll_paymentpoint pp ON pp."UUID" = p.payment_point_id
    WHERE bc."isDeleted" = false
    
    UNION ALL
    
    -- Monetary transfers (external payments)
    SELECT 
        EXTRACT(year FROM mt.transfer_date) AS year,
        EXTRACT(month FROM mt.transfer_date) AS month,
        EXTRACT(quarter FROM mt.transfer_date) AS quarter,
        mt.transfer_date AS payment_date,
        mt.location_id,
        loc."LocationName" AS location_name,
        loc."LocationType" AS location_type,
        com."LocationId" AS commune_id,
        com."LocationName" AS commune_name,
        prov."LocationId" AS province_id,
        prov."LocationName" AS province_name,
        -- Colline level (location_id when it's a colline)
        CASE WHEN loc."LocationType" = 'S' THEN loc."LocationId" ELSE NULL END AS colline_id,
        CASE WHEN loc."LocationType" = 'S' THEN loc."LocationName" ELSE NULL END AS colline_name,
        mt.programme_id,
        bp.code AS programme_code,
        bp.name AS programme_name,
        'MONETARY_TRANSFER' AS payment_source,
        (COALESCE(bp.ceiling_per_beneficiary, 0)::numeric * (mt.paid_women + mt.paid_men + mt.paid_twa)::numeric) AS amount_paid,
        (mt.paid_women + mt.paid_men + mt.paid_twa) AS beneficiary_count,
        mt.paid_women AS female_count,
        mt.paid_twa AS twa_count,
        'PAID' AS payment_status,
        pp.name AS payment_point_name
    FROM merankabandi_monetarytransfer mt
    LEFT JOIN social_protection_benefitplan bp ON bp."UUID" = mt.programme_id
    LEFT JOIN payroll_paymentpoint pp ON pp."UUID" = mt.payment_agency_id
    LEFT JOIN "tblLocations" loc ON loc."LocationId" = mt.location_id
    LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
    LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
    WHERE mt.transfer_date IS NOT NULL
)
SELECT 
    year,
    month,
    quarter,
    payment_date,
    location_id,
    location_name,
    location_type,
    commune_id,
    commune_name,
    province_id,
    province_name,
    colline_id,
    colline_name,
    programme_id,
    programme_code,
    programme_name,
    payment_source,
    payment_status,
    payment_point_name,
    COUNT(*) AS payment_count,
    SUM(amount_paid) AS total_amount_paid,
    SUM(beneficiary_count) AS total_beneficiaries,
    SUM(female_count) AS total_female,
    SUM(beneficiary_count - female_count) AS total_male,
    SUM(twa_count) AS total_twa,
    CASE WHEN SUM(beneficiary_count) > 0 THEN (SUM(female_count)::numeric / SUM(beneficiary_count)::numeric * 100) ELSE 0 END AS female_percentage,
    CASE WHEN SUM(beneficiary_count) > 0 THEN (SUM(twa_count)::numeric / SUM(beneficiary_count)::numeric * 100) ELSE 0 END AS twa_percentage,
    AVG(amount_paid / NULLIF(beneficiary_count, 0)) AS avg_amount_per_beneficiary,
    CURRENT_DATE AS last_updated
FROM combined_payments
GROUP BY 
    year, month, quarter, payment_date, location_id, location_name, location_type,
    commune_id, commune_name, province_id, province_name,
    colline_id, colline_name,
    programme_id, programme_code, programme_name,
    payment_source, payment_status, payment_point_name''',
        'indexes': [
            """CREATE INDEX idx_unified_summary_year_quarter ON payment_reporting_unified_summary (year, quarter);""",
            """CREATE INDEX idx_unified_summary_programme ON payment_reporting_unified_summary (programme_id);""",
            """CREATE INDEX idx_unified_summary_province ON payment_reporting_unified_summary (province_id);""",
            """CREATE INDEX idx_unified_summary_commune ON payment_reporting_unified_summary (commune_id);""",
            """CREATE INDEX idx_unified_summary_colline ON payment_reporting_unified_summary (colline_id);""",
            """CREATE INDEX idx_unified_summary_source ON payment_reporting_unified_summary (payment_source);""",
            """CREATE INDEX idx_unified_summary_date ON payment_reporting_unified_summary (payment_date);""",
            """CREATE INDEX idx_unified_summary_location_type ON payment_reporting_unified_summary (location_type);""",
        ]
    },
    'payment_reporting_unified_quarterly': {
        'sql': '''CREATE MATERIALIZED VIEW payment_reporting_unified_quarterly AS
SELECT 
    programme_name AS transfer_type,
    programme_id,
    year,
    payment_source,
    payment_status,
    -- Quarterly aggregates for amounts
    SUM(CASE WHEN quarter = 1 THEN total_amount_paid ELSE 0 END) AS q1_amount,
    SUM(CASE WHEN quarter = 2 THEN total_amount_paid ELSE 0 END) AS q2_amount,
    SUM(CASE WHEN quarter = 3 THEN total_amount_paid ELSE 0 END) AS q3_amount,
    SUM(CASE WHEN quarter = 4 THEN total_amount_paid ELSE 0 END) AS q4_amount,
    -- Quarterly aggregates for beneficiaries
    SUM(CASE WHEN quarter = 1 THEN total_beneficiaries ELSE 0 END) AS q1_beneficiaries,
    SUM(CASE WHEN quarter = 2 THEN total_beneficiaries ELSE 0 END) AS q2_beneficiaries,
    SUM(CASE WHEN quarter = 3 THEN total_beneficiaries ELSE 0 END) AS q3_beneficiaries,
    SUM(CASE WHEN quarter = 4 THEN total_beneficiaries ELSE 0 END) AS q4_beneficiaries,
    -- Overall totals
    SUM(total_beneficiaries) AS total_beneficiaries,
    SUM(total_amount_paid) AS total_amount,
    AVG(female_percentage) AS avg_female_percentage,
    AVG(twa_percentage) AS avg_twa_percentage,
    COUNT(DISTINCT province_id) AS provinces_covered,
    CURRENT_DATE AS last_updated
FROM payment_reporting_unified_summary
GROUP BY programme_name, programme_id, year, payment_source, payment_status''',
        'indexes': [
            """CREATE INDEX idx_unified_quarterly_year ON payment_reporting_unified_quarterly (year);""",
            """CREATE INDEX idx_unified_quarterly_programme ON payment_reporting_unified_quarterly (programme_id);""",
            """CREATE INDEX idx_unified_quarterly_source ON payment_reporting_unified_quarterly (payment_source);""",
        ]
    },
    'payment_reporting_unified_by_location': {
        'sql': '''CREATE MATERIALIZED VIEW payment_reporting_unified_by_location AS
SELECT 
    year,
    month,
    quarter,
    province_id,
    province_name,
    commune_id,
    commune_name,
    colline_id,
    colline_name,
    programme_id,
    programme_name,
    payment_source,
    payment_status,
    COUNT(DISTINCT payment_date) AS payment_days,
    SUM(payment_count) AS total_payments,
    SUM(total_amount_paid) AS total_amount,
    SUM(total_beneficiaries) AS total_beneficiaries,
    SUM(total_female) AS total_female,
    SUM(total_male) AS total_male,
    SUM(total_twa) AS total_twa,
    AVG(female_percentage) AS avg_female_percentage,
    AVG(twa_percentage) AS avg_twa_percentage,
    -- Quarterly breakdowns at location level
    SUM(CASE WHEN quarter = 1 THEN total_amount_paid ELSE 0 END) AS q1_amount,
    SUM(CASE WHEN quarter = 2 THEN total_amount_paid ELSE 0 END) AS q2_amount,
    SUM(CASE WHEN quarter = 3 THEN total_amount_paid ELSE 0 END) AS q3_amount,
    SUM(CASE WHEN quarter = 4 THEN total_amount_paid ELSE 0 END) AS q4_amount,
    SUM(CASE WHEN quarter = 1 THEN total_beneficiaries ELSE 0 END) AS q1_beneficiaries,
    SUM(CASE WHEN quarter = 2 THEN total_beneficiaries ELSE 0 END) AS q2_beneficiaries,
    SUM(CASE WHEN quarter = 3 THEN total_beneficiaries ELSE 0 END) AS q3_beneficiaries,
    SUM(CASE WHEN quarter = 4 THEN total_beneficiaries ELSE 0 END) AS q4_beneficiaries,
    CURRENT_DATE AS last_updated
FROM payment_reporting_unified_summary
GROUP BY 
    year, month, quarter,
    province_id, province_name,
    commune_id, commune_name,
    colline_id, colline_name,
    programme_id, programme_name,
    payment_source, payment_status''',
        'indexes': [
            """CREATE INDEX idx_unified_by_location_year ON payment_reporting_unified_by_location (year);""",
            """CREATE INDEX idx_unified_by_location_quarter ON payment_reporting_unified_by_location (quarter);""",
            """CREATE INDEX idx_unified_by_location_province ON payment_reporting_unified_by_location (province_id);""",
            """CREATE INDEX idx_unified_by_location_commune ON payment_reporting_unified_by_location (commune_id);""",
            """CREATE INDEX idx_unified_by_location_colline ON payment_reporting_unified_by_location (colline_id);""",
            """CREATE INDEX idx_unified_by_location_programme ON payment_reporting_unified_by_location (programme_id);""",
            """CREATE INDEX idx_unified_by_location_source ON payment_reporting_unified_by_location (payment_source);""",
        ]
    },
}
