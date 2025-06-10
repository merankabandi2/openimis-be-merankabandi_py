"""
Monitoring & Evaluation Materialized Views
All views related to M&E reporting, KPIs, and performance tracking
"""

MONITORING_VIEWS = {
    'dashboard_activities_summary': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_activities_summary AS
WITH all_activities AS ( SELECT 'BehaviorChangePromotion'::text AS activity_type, merankabandi_behaviorchangepromotion.report_date AS activity_date, merankabandi_behaviorchangepromotion.location_id, ((merankabandi_behaviorchangepromotion.male_participants + merankabandi_behaviorchangepromotion.female_participants) + merankabandi_behaviorchangepromotion.twa_participants) AS total_participants, merankabandi_behaviorchangepromotion.male_participants, merankabandi_behaviorchangepromotion.female_participants, merankabandi_behaviorchangepromotion.twa_participants FROM merankabandi_behaviorchangepromotion WHERE ((merankabandi_behaviorchangepromotion.validation_status)::text = 'VALIDATED'::text) UNION ALL SELECT 'SensitizationTraining'::text AS activity_type, merankabandi_sensitizationtraining.sensitization_date AS activity_date, merankabandi_sensitizationtraining.location_id, ((merankabandi_sensitizationtraining.male_participants + merankabandi_sensitizationtraining.female_participants) + merankabandi_sensitizationtraining.twa_participants) AS total_participants, merankabandi_sensitizationtraining.male_participants, merankabandi_sensitizationtraining.female_participants, merankabandi_sensitizationtraining.twa_participants FROM merankabandi_sensitizationtraining WHERE ((merankabandi_sensitizationtraining.validation_status)::text = 'VALIDATED'::text) ) SELECT loc."LocationId" AS location_id, loc."LocationName" AS location_name, com."LocationId" AS commune_id, com."LocationName" AS commune_name, prov."LocationId" AS province_id, prov."LocationName" AS province_name, a.activity_type, EXTRACT(year FROM a.activity_date) AS year, EXTRACT(month FROM a.activity_date) AS month, count(*) AS activity_count, sum(a.total_participants) AS total_participants, sum(a.male_participants) AS male_participants, sum(a.female_participants) AS female_participants, sum(a.twa_participants) AS twa_participants FROM (((all_activities a LEFT JOIN "tblLocations" loc ON ((loc."LocationId" = a.location_id))) LEFT JOIN "tblLocations" com ON ((com."LocationId" = loc."ParentLocationId"))) LEFT JOIN "tblLocations" prov ON ((prov."LocationId" = com."ParentLocationId"))) GROUP BY loc."LocationId", loc."LocationName", com."LocationId", com."LocationName", prov."LocationId", prov."LocationName", a.activity_type, (EXTRACT(year FROM a.activity_date)), (EXTRACT(month FROM a.activity_date))''',
        'indexes': [
            """CREATE INDEX idx_activities_month ON dashboard_activities_summary USING btree (month);""",
            """CREATE INDEX idx_activities_type ON dashboard_activities_summary USING btree (activity_type);""",
        ]
    },
    'dashboard_activities_by_type': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_activities_by_type AS
SELECT dashboard_activities_summary.activity_type, dashboard_activities_summary.year, dashboard_activities_summary.month, sum(dashboard_activities_summary.activity_count) AS total_activities, sum(dashboard_activities_summary.total_participants) AS total_participants, sum(dashboard_activities_summary.male_participants) AS male_participants, sum(dashboard_activities_summary.female_participants) AS female_participants, sum(dashboard_activities_summary.twa_participants) AS twa_participants, count(DISTINCT dashboard_activities_summary.location_id) AS locations_covered FROM dashboard_activities_summary GROUP BY dashboard_activities_summary.activity_type, dashboard_activities_summary.year, dashboard_activities_summary.month''',
        'indexes': [
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
