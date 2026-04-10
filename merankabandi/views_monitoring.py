"""
Monitoring & Evaluation Materialized Views

Removed dashboard_activities_by_type (was just a GROUP BY on dashboard_activities_summary;
the service layer can do this directly since the base view has indexes on activity_type).

Kept:
  - dashboard_activities_summary: Activity data by location/type/status/date
  - dashboard_indicators: Achievement tracking by indicator/section/time
"""

MONITORING_VIEWS = {
    'dashboard_activities_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_activities_summary AS
WITH
-- Events: all training sessions (additive — each session is a distinct activity)
training_events AS (
    SELECT
        'SensitizationTraining'::text AS activity_type,
        sensitization_date AS activity_date,
        location_id, validation_status,
        (male_participants + female_participants + twa_participants) AS total_participants,
        male_participants, female_participants, twa_participants,
        0 AS agriculture_beneficiaries, 0 AS livestock_beneficiaries, 0 AS commerce_services_beneficiaries
    FROM merankabandi_sensitizationtraining
),
-- Snapshots: latest per colline only (periodic assessment, NOT additive across time)
latest_bcp AS (
    SELECT DISTINCT ON (location_id)
        'BehaviorChangePromotion'::text AS activity_type,
        report_date AS activity_date,
        location_id, validation_status,
        (male_participants + female_participants + twa_participants) AS total_participants,
        male_participants, female_participants, twa_participants,
        0 AS agriculture_beneficiaries, 0 AS livestock_beneficiaries, 0 AS commerce_services_beneficiaries
    FROM merankabandi_behaviorchangepromotion
    WHERE validation_status = 'VALIDATED'
    ORDER BY location_id, report_date DESC
),
latest_mp AS (
    SELECT DISTINCT ON (location_id)
        'MicroProject'::text AS activity_type,
        report_date AS activity_date,
        location_id, validation_status,
        (male_participants + female_participants + twa_participants) AS total_participants,
        male_participants, female_participants, twa_participants,
        agriculture_beneficiaries, livestock_beneficiaries, commerce_services_beneficiaries
    FROM merankabandi_microproject
    WHERE validation_status = 'VALIDATED'
    ORDER BY location_id, report_date DESC
),
all_activities AS (
    SELECT * FROM training_events
    UNION ALL
    SELECT * FROM latest_bcp
    UNION ALL
    SELECT * FROM latest_mp
)
SELECT
    loc."LocationId" AS location_id,
    loc."LocationName" AS location_name,
    com."LocationId" AS commune_id,
    com."LocationName" AS commune_name,
    prov."LocationId" AS province_id,
    prov."LocationName" AS province_name,
    a.activity_type,
    a.validation_status,
    EXTRACT(year FROM a.activity_date) AS year,
    EXTRACT(month FROM a.activity_date) AS month,
    COUNT(*) AS activity_count,
    SUM(a.total_participants) AS total_participants,
    SUM(a.male_participants) AS male_participants,
    SUM(a.female_participants) AS female_participants,
    SUM(a.twa_participants) AS twa_participants,
    SUM(a.agriculture_beneficiaries) AS agriculture_beneficiaries,
    SUM(a.livestock_beneficiaries) AS livestock_beneficiaries,
    SUM(a.commerce_services_beneficiaries) AS commerce_services_beneficiaries
FROM all_activities a
    LEFT JOIN "tblLocations" loc ON loc."LocationId" = a.location_id
    LEFT JOIN "tblLocations" com ON com."LocationId" = loc."ParentLocationId"
    LEFT JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
GROUP BY
    loc."LocationId", loc."LocationName",
    com."LocationId", com."LocationName",
    prov."LocationId", prov."LocationName",
    a.activity_type, a.validation_status,
    EXTRACT(year FROM a.activity_date),
    EXTRACT(month FROM a.activity_date)''',
        'indexes': [
            """CREATE INDEX idx_activities_month ON dashboard_activities_summary USING btree (month);""",
            """CREATE INDEX idx_activities_type ON dashboard_activities_summary USING btree (activity_type);""",
            """CREATE INDEX idx_activities_status ON dashboard_activities_summary USING btree (validation_status);""",
            """CREATE INDEX idx_activities_province ON dashboard_activities_summary USING btree (province_id);""",
            """CREATE INDEX idx_activities_year_type ON dashboard_activities_summary USING btree (year, activity_type);""",
        ]
    },
    'dashboard_indicators': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_indicators AS
SELECT
    date_trunc('month', ia.date::timestamp with time zone) AS month,
    date_trunc('quarter', ia.date::timestamp with time zone) AS quarter,
    EXTRACT(year FROM ia.date) AS year,
    i.name AS indicator_name,
    i.id AS indicator_code,
    i.target AS indicator_target,
    s.name AS section_name,
    SUM(ia.achieved) AS total_achieved,
    AVG(ia.achieved) AS avg_achieved,
    MAX(ia.achieved) AS max_achieved,
    MIN(ia.achieved) AS min_achieved,
    CASE WHEN i.target > 0
        THEN (SUM(ia.achieved) / i.target::numeric * 100)
        ELSE NULL END AS achievement_percentage,
    COUNT(*) AS achievement_records,
    COUNT(CASE WHEN ia.achieved >= i.target THEN 1 END) AS target_met_count
FROM merankabandi_indicatorachievement ia
JOIN merankabandi_indicator i ON ia.indicator_id = i.id
JOIN merankabandi_section s ON i.section_id = s.id
GROUP BY
    date_trunc('month', ia.date::timestamp with time zone),
    date_trunc('quarter', ia.date::timestamp with time zone),
    EXTRACT(year FROM ia.date),
    i.name, i.id, i.target, s.name''',
        'indexes': [
            """CREATE INDEX idx_indicators_month ON dashboard_indicators USING btree (month);""",
            """CREATE INDEX idx_indicators_name ON dashboard_indicators USING btree (indicator_name);""",
        ]
    },
}
