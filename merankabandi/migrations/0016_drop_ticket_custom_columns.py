"""
Drop 35 custom columns from grievance_social_protection_ticket that were
migrated to json_ext in migration 0015.

Uses raw SQL because we cannot create migrations in the upstream
grievance_social_protection module.
"""
from django.db import migrations

CUSTOM_COLUMNS = [
    'reporter_name', 'reporter_phone', 'gender', 'is_beneficiary',
    'beneficiary_type', 'other_beneficiary_type', 'is_anonymous',
    'is_batwa', 'cni_number', 'non_beneficiary_details',
    'gps_location', 'colline',
    'is_project_related',
    'vbg_type', 'vbg_detail', 'viol_hospital', 'viol_complaint', 'viol_support',
    'exclusion_type', 'exclusion_detail',
    'payment_type', 'payment_detail',
    'phone_type', 'phone_detail',
    'account_type', 'account_detail',
    'receiver_name', 'receiver_function', 'receiver_phone',
    'other_channel', 'form_id',
    'is_resolved', 'resolver_name', 'resolver_function', 'resolution_details',
]

TABLES = [
    'grievance_social_protection_ticket',
    'grievance_social_protection_historicalticket',
]


def drop_columns(apps, schema_editor):
    from django.db import connection
    cursor = connection.cursor()
    for table in TABLES:
        for col in CUSTOM_COLUMNS:
            # Use savepoint so a failed drop doesn't poison the transaction
            cursor.execute('SAVEPOINT drop_col')
            try:
                cursor.execute(
                    f'ALTER TABLE {table} DROP COLUMN IF EXISTS "{col}" CASCADE'
                )
                cursor.execute('RELEASE SAVEPOINT drop_col')
            except Exception as e:
                cursor.execute('ROLLBACK TO SAVEPOINT drop_col')
                print(f"  Warning: Could not drop {table}.{col}: {e}")
    print(f"  Dropped columns from {len(TABLES)} tables")


def noop(apps, schema_editor):
    pass  # Cannot reverse column drops


class Migration(migrations.Migration):
    dependencies = [
        ('merankabandi', '0015_migrate_ticket_json_ext'),
    ]

    operations = [
        migrations.RunPython(drop_columns, noop),
    ]
