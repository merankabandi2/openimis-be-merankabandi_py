"""
Payment Materialized Views
All views related to payments, transfers, and financial reporting
"""

PAYMENT_VIEWS = {
    'dashboard_monetary_transfers': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_monetary_transfers AS
SELECT mt.id, mt.transfer_date, EXTRACT(year FROM mt.transfer_date) AS year, EXTRACT(month FROM mt.transfer_date) AS month, EXTRACT(quarter FROM mt.transfer_date) AS quarter, mt.location_id, loc."LocationName" AS location_name, com."LocationId" AS commune_id, com."LocationName" AS commune_name, prov."LocationId" AS province_id, prov."LocationName" AS province_name, mt.programme_id, bp.code AS programme_code, bp.name AS programme_name, mt.payment_agency_id, pp.name AS payment_agency_name, mt.planned_women, mt.planned_men, mt.planned_twa, ((mt.planned_women + mt.planned_men) + mt.planned_twa) AS total_planned, mt.paid_women, mt.paid_men, mt.paid_twa, ((mt.paid_women + mt.paid_men) + mt.paid_twa) AS total_paid, (COALESCE(bp.ceiling_per_beneficiary, (0)::numeric) * (((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric) AS total_amount, CASE WHEN (((mt.planned_women + mt.planned_men) + mt.planned_twa) > 0) THEN (((((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric / (((mt.planned_women + mt.planned_men) + mt.planned_twa))::numeric) * (100)::numeric) ELSE (0)::numeric END AS completion_rate, CURRENT_DATE AS last_updated FROM (((((merankabandi_monetarytransfer mt LEFT JOIN social_protection_benefitplan bp ON ((bp."UUID" = mt.programme_id))) LEFT JOIN payroll_paymentpoint pp ON ((pp."UUID" = mt.payment_agency_id))) LEFT JOIN "tblLocations" loc ON ((loc."LocationId" = mt.location_id))) LEFT JOIN "tblLocations" com ON ((com."LocationId" = loc."ParentLocationId"))) LEFT JOIN "tblLocations" prov ON ((prov."LocationId" = com."ParentLocationId")))''',
        'indexes': [
            """CREATE INDEX idx_transfers_month ON dashboard_monetary_transfers USING btree (month);""",
        ]
    },
    'dashboard_transfers_by_province': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_transfers_by_province AS
SELECT dashboard_monetary_transfers.province_id, dashboard_monetary_transfers.province_name, dashboard_monetary_transfers.year, dashboard_monetary_transfers.month, dashboard_monetary_transfers.programme_id, dashboard_monetary_transfers.programme_name, count(*) AS transfer_count, sum(dashboard_monetary_transfers.total_planned) AS total_planned, sum(dashboard_monetary_transfers.total_paid) AS total_paid, sum(dashboard_monetary_transfers.paid_women) AS women_paid, sum(dashboard_monetary_transfers.paid_men) AS men_paid, sum(dashboard_monetary_transfers.paid_twa) AS twa_paid, sum(dashboard_monetary_transfers.total_amount) AS total_amount, avg(dashboard_monetary_transfers.completion_rate) AS avg_completion_rate FROM dashboard_monetary_transfers GROUP BY dashboard_monetary_transfers.province_id, dashboard_monetary_transfers.province_name, dashboard_monetary_transfers.year, dashboard_monetary_transfers.month, dashboard_monetary_transfers.programme_id, dashboard_monetary_transfers.programme_name''',
        'indexes': [
            """CREATE INDEX idx_transfers_province_amount ON dashboard_transfers_by_province USING btree (total_amount);""",
            """CREATE INDEX idx_transfers_province_id ON dashboard_transfers_by_province USING btree (province_id);""",
        ]
    },
    'dashboard_transfers_by_time': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_transfers_by_time AS
SELECT dashboard_monetary_transfers.year, dashboard_monetary_transfers.month, dashboard_monetary_transfers.quarter, count(*) AS transfer_count, sum(dashboard_monetary_transfers.total_planned) AS total_planned, sum(dashboard_monetary_transfers.total_paid) AS total_paid, sum(dashboard_monetary_transfers.paid_women) AS women_paid, sum(dashboard_monetary_transfers.paid_men) AS men_paid, sum(dashboard_monetary_transfers.paid_twa) AS twa_paid, sum(dashboard_monetary_transfers.total_amount) AS total_amount, avg(dashboard_monetary_transfers.completion_rate) AS avg_completion_rate FROM dashboard_monetary_transfers GROUP BY dashboard_monetary_transfers.year, dashboard_monetary_transfers.month, dashboard_monetary_transfers.quarter ORDER BY dashboard_monetary_transfers.year, dashboard_monetary_transfers.month''',
        'indexes': [
            """CREATE INDEX idx_transfers_time_month ON dashboard_transfers_by_time USING btree (month);""",
            """CREATE INDEX idx_transfers_time_year ON dashboard_transfers_by_time USING btree (year);""",
        ]
    },
    'payment_reporting_monetary_transfers': {
        'sql': '''CREATE MATERIALIZED VIEW payment_reporting_monetary_transfers AS
SELECT EXTRACT(year FROM mt.transfer_date) AS year, EXTRACT(month FROM mt.transfer_date) AS month, EXTRACT(quarter FROM mt.transfer_date) AS quarter, mt.transfer_date, loc."LocationId" AS location_id, loc."LocationName" AS location_name, loc."LocationType" AS location_type, com."LocationId" AS commune_id, com."LocationName" AS commune_name, prov."LocationId" AS province_id, prov."LocationName" AS province_name, mt.programme_id, bp.code AS programme_code, bp.name AS programme_name, bp.ceiling_per_beneficiary AS amount_per_beneficiary, mt.payment_agency_id, pp.name AS payment_agency_name, mt.planned_women, mt.planned_men, mt.planned_twa, ((mt.planned_women + mt.planned_men) + mt.planned_twa) AS total_planned, mt.paid_women, mt.paid_men, mt.paid_twa, ((mt.paid_women + mt.paid_men) + mt.paid_twa) AS total_paid, (COALESCE(bp.ceiling_per_beneficiary, (0)::numeric) * (((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric) AS total_amount_paid, CASE WHEN (((mt.paid_women + mt.paid_men) + mt.paid_twa) > 0) THEN (((mt.paid_women)::numeric / (((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric) * (100)::numeric) ELSE (0)::numeric END AS female_percentage, CASE WHEN (((mt.paid_women + mt.paid_men) + mt.paid_twa) > 0) THEN (((mt.paid_twa)::numeric / (((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric) * (100)::numeric) ELSE (0)::numeric END AS twa_percentage, CASE WHEN (((mt.planned_women + mt.planned_men) + mt.planned_twa) > 0) THEN (((((mt.paid_women + mt.paid_men) + mt.paid_twa))::numeric / (((mt.planned_women + mt.planned_men) + mt.planned_twa))::numeric) * (100)::numeric) ELSE (0)::numeric END AS completion_rate FROM (((((merankabandi_monetarytransfer mt LEFT JOIN social_protection_benefitplan bp ON ((bp."UUID" = mt.programme_id))) LEFT JOIN payroll_paymentpoint pp ON ((pp."UUID" = mt.payment_agency_id))) LEFT JOIN "tblLocations" loc ON ((loc."LocationId" = mt.location_id))) LEFT JOIN "tblLocations" com ON ((com."LocationId" = loc."ParentLocationId"))) LEFT JOIN "tblLocations" prov ON ((prov."LocationId" = com."ParentLocationId"))) WHERE (mt.transfer_date IS NOT NULL)''',
        'indexes': [
        ]
    },
}
