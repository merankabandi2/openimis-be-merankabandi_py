"""
Utility Views - REMOVED

dashboard_field_mappings was a materialized view storing 7 static rows of
field name aliases. This is now a Python dict (FIELD_MAPPINGS below).

No materialized views remain in this category.
"""

UTILITY_VIEWS = {}

# Field name mappings (replaces the former dashboard_field_mappings materialized view)
FIELD_MAPPINGS = {
    'beneficiary_summary': {
        'total_individuals': 'beneficiary_count',
        'total_male': 'male_count',
        'total_female': 'female_count',
        'total_twa': 'twa_count',
    },
    'payment_summary': {
        'plannedWomen': 'planned_women',
        'plannedMen': 'planned_men',
        'transferredAmount': 'transferred_amount',
    },
}
