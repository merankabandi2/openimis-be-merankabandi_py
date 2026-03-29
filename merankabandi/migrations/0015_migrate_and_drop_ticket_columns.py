"""
Combined data migration + column drop for grievance ticket custom fields.
1. Copies 35 custom column values into Ticket.json_ext
2. Drops the 35 custom columns from ticket and historicalticket tables
"""
import json
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

DROP_TABLES = [
    'grievance_social_protection_ticket',
    'grievance_social_protection_historicalticket',
]


def migrate_to_json_ext(apps, schema_editor):
    cursor = schema_editor.connection.cursor()
    cursor.execute("""
        SELECT "UUID", "Json_ext",
            reporter_name, reporter_phone, gender, is_beneficiary,
            beneficiary_type, other_beneficiary_type, is_anonymous,
            is_batwa, cni_number, non_beneficiary_details,
            gps_location, colline, is_project_related,
            vbg_type, vbg_detail, viol_hospital, viol_complaint, viol_support,
            exclusion_type, exclusion_detail,
            payment_type, payment_detail, phone_type, phone_detail,
            account_type, account_detail,
            receiver_name, receiver_function, receiver_phone,
            other_channel, form_id,
            is_resolved, resolver_name, resolver_function, resolution_details,
            channel
        FROM grievance_social_protection_ticket
        WHERE "isDeleted" = false
    """)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    for row in rows:
        data = dict(zip(columns, row))
        ticket_id = data['UUID']
        existing = data.get('Json_ext') or {}
        if isinstance(existing, str):
            try:
                existing = json.loads(existing)
            except (json.JSONDecodeError, TypeError):
                existing = {}

        new_ext = {
            "form_version": "2025_v1",
            "form_id": data.get('form_id') or 'aeAgbxjy7d6rD8jtUdMD9Z',
            "case_type": "cas_de_r_clamation",
            "reporter": {
                "is_beneficiary": data.get('is_beneficiary'),
                "beneficiary_type": data.get('beneficiary_type'),
                "other_beneficiary_type": data.get('other_beneficiary_type'),
                "is_batwa": data.get('is_batwa'),
                "is_anonymous": data.get('is_anonymous'),
                "name": data.get('reporter_name'),
                "phone": data.get('reporter_phone'),
                "cni_number": data.get('cni_number'),
                "gender": data.get('gender'),
                "non_beneficiary_details": data.get('non_beneficiary_details'),
            },
            "location": {
                "colline": data.get('colline'),
                "gps": data.get('gps_location'),
            },
            "categorization": {
                "is_project_related": data.get('is_project_related'),
            },
            "submission": {
                "channels": data.get('channel'),
                "other_channel": data.get('other_channel'),
                "receiver_name": data.get('receiver_name'),
                "receiver_function": data.get('receiver_function'),
                "receiver_phone": data.get('receiver_phone'),
            },
            "resolution_initial": {
                "is_resolved": data.get('is_resolved'),
                "resolver_name": data.get('resolver_name'),
                "resolver_function": data.get('resolver_function'),
                "resolution_details": data.get('resolution_details'),
            },
            "legacy": {
                "vbg_type": data.get('vbg_type'),
                "vbg_detail": data.get('vbg_detail'),
                "viol_hospital": data.get('viol_hospital'),
                "viol_complaint": data.get('viol_complaint'),
                "viol_support": data.get('viol_support'),
                "exclusion_type": data.get('exclusion_type'),
                "exclusion_detail": data.get('exclusion_detail'),
                "payment_type": data.get('payment_type'),
                "payment_detail": data.get('payment_detail'),
                "phone_type": data.get('phone_type'),
                "phone_detail": data.get('phone_detail'),
                "account_type": data.get('account_type'),
                "account_detail": data.get('account_detail'),
            },
        }

        merged = {**existing, **new_ext}
        cursor.execute(
            'UPDATE grievance_social_protection_ticket SET "Json_ext" = %s WHERE "UUID" = %s',
            [json.dumps(merged), ticket_id],
        )

    print(f"  Migrated {len(rows)} tickets to json_ext")


def drop_custom_columns(apps, schema_editor):
    cursor = schema_editor.connection.cursor()
    for table in DROP_TABLES:
        for col in CUSTOM_COLUMNS:
            cursor.execute('SAVEPOINT drop_col')
            try:
                cursor.execute(f'ALTER TABLE {table} DROP COLUMN IF EXISTS "{col}" CASCADE')
                cursor.execute('RELEASE SAVEPOINT drop_col')
            except Exception as e:
                cursor.execute('ROLLBACK TO SAVEPOINT drop_col')
                print(f"  Warning: {table}.{col}: {e}")
    print(f"  Dropped columns from {len(DROP_TABLES)} tables")


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ('merankabandi', '0014_grievance_workflow_engine'),
    ]

    operations = [
        migrations.RunPython(migrate_to_json_ext, noop),
        migrations.RunPython(drop_custom_columns, noop),
    ]
