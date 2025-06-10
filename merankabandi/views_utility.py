"""
Utility Materialized Views
Supporting views for field mappings and other utilities
"""

UTILITY_VIEWS = {
    'dashboard_field_mappings': {
        'sql': '''CREATE MATERIALIZED VIEW dashboard_field_mappings AS
SELECT 'beneficiary_summary'::text AS view_name, 'total_individuals'::text AS new_field, 'beneficiary_count'::text AS legacy_field, 'Alias for beneficiary count'::text AS description UNION ALL SELECT 'beneficiary_summary'::text AS view_name, 'total_male'::text AS new_field, 'male_count'::text AS legacy_field, 'Alias for male count'::text AS description UNION ALL SELECT 'beneficiary_summary'::text AS view_name, 'total_female'::text AS new_field, 'female_count'::text AS legacy_field, 'Alias for female count'::text AS description UNION ALL SELECT 'beneficiary_summary'::text AS view_name, 'total_twa'::text AS new_field, 'twa_count'::text AS legacy_field, 'Alias for TWA count'::text AS description UNION ALL SELECT 'payment_summary'::text AS view_name, 'plannedWomen'::text AS new_field, 'planned_women'::text AS legacy_field, 'Camel case alias for planned women'::text AS description UNION ALL SELECT 'payment_summary'::text AS view_name, 'plannedMen'::text AS new_field, 'planned_men'::text AS legacy_field, 'Camel case alias for planned men'::text AS description UNION ALL SELECT 'payment_summary'::text AS view_name, 'transferredAmount'::text AS new_field, 'transferred_amount'::text AS legacy_field, 'Camel case alias for transferred amount'::text AS description''',
        'indexes': [
            """CREATE INDEX idx_dashboard_field_mappings_field ON dashboard_field_mappings USING btree (new_field);""",
            """CREATE INDEX idx_dashboard_field_mappings_view ON dashboard_field_mappings USING btree (view_name);""",
        ]
    },
}
