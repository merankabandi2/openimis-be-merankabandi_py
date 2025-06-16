"""
Monitoring & Evaluation Materialized Views
All views related to M&E reporting, KPIs, and performance tracking
"""

MONITORING_VIEWS = {
    'dashboard_activities_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_activities_summary AS
WITH all_activities AS (
    SELECT 
        'BehaviorChangePromotion'::text AS activity_type,
        merankabandi_behaviorchangepromotion.report_date AS activity_date,
        merankabandi_behaviorchangepromotion.location_id,
        merankabandi_behaviorchangepromotion.validation_status,
        ((merankabandi_behaviorchangepromotion.male_participants + merankabandi_behaviorchangepromotion.female_participants) + merankabandi_behaviorchangepromotion.twa_participants) AS total_participants,
        merankabandi_behaviorchangepromotion.male_participants,
        merankabandi_behaviorchangepromotion.female_participants,
        merankabandi_behaviorchangepromotion.twa_participants,
        0 AS agriculture_beneficiaries,
        0 AS livestock_beneficiaries,
        0 AS commerce_services_beneficiaries
    FROM merankabandi_behaviorchangepromotion
    
    UNION ALL
    
    SELECT 
        'SensitizationTraining'::text AS activity_type,
        merankabandi_sensitizationtraining.sensitization_date AS activity_date,
        merankabandi_sensitizationtraining.location_id,
        merankabandi_sensitizationtraining.validation_status,
        ((merankabandi_sensitizationtraining.male_participants + merankabandi_sensitizationtraining.female_participants) + merankabandi_sensitizationtraining.twa_participants) AS total_participants,
        merankabandi_sensitizationtraining.male_participants,
        merankabandi_sensitizationtraining.female_participants,
        merankabandi_sensitizationtraining.twa_participants,
        0 AS agriculture_beneficiaries,
        0 AS livestock_beneficiaries,
        0 AS commerce_services_beneficiaries
    FROM merankabandi_sensitizationtraining
    
    UNION ALL
    
    SELECT 
        'MicroProject'::text AS activity_type,
        merankabandi_microproject.report_date AS activity_date,
        merankabandi_microproject.location_id,
        merankabandi_microproject.validation_status,
        ((merankabandi_microproject.male_participants + merankabandi_microproject.female_participants) + merankabandi_microproject.twa_participants) AS total_participants,
        merankabandi_microproject.male_participants,
        merankabandi_microproject.female_participants,
        merankabandi_microproject.twa_participants,
        merankabandi_microproject.agriculture_beneficiaries,
        merankabandi_microproject.livestock_beneficiaries,
        merankabandi_microproject.commerce_services_beneficiaries
    FROM merankabandi_microproject
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
    count(*) AS activity_count,
    sum(a.total_participants) AS total_participants,
    sum(a.male_participants) AS male_participants,
    sum(a.female_participants) AS female_participants,
    sum(a.twa_participants) AS twa_participants,
    sum(a.agriculture_beneficiaries) AS agriculture_beneficiaries,
    sum(a.livestock_beneficiaries) AS livestock_beneficiaries,
    sum(a.commerce_services_beneficiaries) AS commerce_services_beneficiaries
FROM all_activities a
    LEFT JOIN "tblLocations" loc ON (loc."LocationId" = a.location_id)
    LEFT JOIN "tblLocations" com ON (com."LocationId" = loc."ParentLocationId")
    LEFT JOIN "tblLocations" prov ON (prov."LocationId" = com."ParentLocationId")
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
        ]
    },
    'dashboard_activities_by_type': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_activities_by_type AS
SELECT 
    activity_type,
    validation_status,
    year,
    month,
    sum(activity_count) AS total_activities,
    sum(total_participants) AS total_participants,
    sum(male_participants) AS male_participants,
    sum(female_participants) AS female_participants,
    sum(twa_participants) AS twa_participants,
    sum(agriculture_beneficiaries) AS agriculture_beneficiaries,
    sum(livestock_beneficiaries) AS livestock_beneficiaries,
    sum(commerce_services_beneficiaries) AS commerce_services_beneficiaries,
    count(DISTINCT location_id) AS locations_covered
FROM dashboard_activities_summary
GROUP BY activity_type, validation_status, year, month''',
        'indexes': [
            """CREATE INDEX idx_activities_by_type_year_month ON dashboard_activities_by_type USING btree (year, month);""",
        ]
    },
    'dashboard_indicators': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_indicators AS
SELECT date_trunc('month'::text, (ia.date)::timestamp with time zone) AS month, date_trunc('quarter'::text, (ia.date)::timestamp with time zone) AS quarter, EXTRACT(year FROM ia.date) AS year, i.name AS indicator_name, i.id AS indicator_code, i.target AS indicator_target, s.name AS section_name, sum(ia.achieved) AS total_achieved, avg(ia.achieved) AS avg_achieved, max(ia.achieved) AS max_achieved, min(ia.achieved) AS min_achieved, CASE WHEN (i.target > (0)::numeric) THEN ((sum(ia.achieved) / (i.target)::numeric) * (100)::numeric) ELSE NULL::numeric END AS achievement_percentage, count(*) AS achievement_records, count( CASE WHEN (ia.achieved >= i.target) THEN 1 ELSE NULL::integer END) AS target_met_count FROM ((merankabandi_indicatorachievement ia JOIN merankabandi_indicator i ON ((ia.indicator_id = i.id))) JOIN merankabandi_section s ON ((i.section_id = s.id))) GROUP BY (date_trunc('month'::text, (ia.date)::timestamp with time zone)), (date_trunc('quarter'::text, (ia.date)::timestamp with time zone)), (EXTRACT(year FROM ia.date)), i.name, i.id, i.target, s.name''',
        'indexes': [
            """CREATE INDEX idx_indicators_month ON dashboard_indicators USING btree (month);""",
            """CREATE INDEX idx_indicators_name ON dashboard_indicators USING btree (indicator_name);""",
        ]
    },
}
