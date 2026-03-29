import json

from django.db import migrations


def migrate_ticket_fields_to_json_ext(apps, schema_editor):
    with schema_editor.connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                "UUID",
                "Json_ext",
                reporter_name,
                reporter_phone,
                gender,
                is_beneficiary,
                beneficiary_type,
                other_beneficiary_type,
                is_anonymous,
                is_batwa,
                cni_number,
                non_beneficiary_details,
                gps_location,
                colline,
                is_project_related,
                vbg_type,
                vbg_detail,
                viol_hospital,
                viol_complaint,
                viol_support,
                exclusion_type,
                exclusion_detail,
                payment_type,
                payment_detail,
                phone_type,
                phone_detail,
                account_type,
                account_detail,
                receiver_name,
                receiver_function,
                receiver_phone,
                other_channel,
                form_id,
                is_resolved,
                resolver_name,
                resolver_function,
                resolution_details,
                channel
            FROM grievance_social_protection_ticket
            WHERE "isDeleted" = false
            """
        )
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        updated = 0
        for row in rows:
            data = dict(zip(columns, row))
            ticket_id = data['UUID']

            existing_json_ext = data.get('Json_ext') or {}
            if isinstance(existing_json_ext, str):
                try:
                    existing_json_ext = json.loads(existing_json_ext)
                except (json.JSONDecodeError, TypeError):
                    existing_json_ext = {}

            new_json_ext = {
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

            # Merge: new migrated data wins over any stale existing keys
            merged = {**existing_json_ext, **new_json_ext}

            cursor.execute(
                'UPDATE grievance_social_protection_ticket SET "Json_ext" = %s WHERE "UUID" = %s',
                [json.dumps(merged), ticket_id],
            )
            updated += 1

        print(f"  Migrated {updated} tickets to json_ext structure.")


def reverse_migrate_ticket_fields(apps, schema_editor):
    # Reversing a data migration: clear the json_ext fields we wrote.
    # This only clears the keys we added; it cannot restore data to columns
    # if those columns have already been dropped (by a subsequent migration).
    keys_to_remove = [
        'form_version', 'form_id', 'case_type',
        'reporter', 'location', 'categorization',
        'submission', 'resolution_initial', 'legacy',
    ]
    with schema_editor.connection.cursor() as cursor:
        cursor.execute(
            'SELECT "UUID", "Json_ext" FROM grievance_social_protection_ticket WHERE "isDeleted" = false'
        )
        rows = cursor.fetchall()
        for ticket_id, json_ext in rows:
            if not json_ext:
                continue
            if isinstance(json_ext, str):
                try:
                    json_ext = json.loads(json_ext)
                except (json.JSONDecodeError, TypeError):
                    continue
            for key in keys_to_remove:
                json_ext.pop(key, None)
            cursor.execute(
                'UPDATE grievance_social_protection_ticket SET "Json_ext" = %s WHERE "UUID" = %s',
                [json.dumps(json_ext), ticket_id],
            )


class Migration(migrations.Migration):

    dependencies = [
        ('merankabandi', '0014_workflow_models'),
        # grievance_social_protection 0019 is already applied (added custom columns)
        # Dependency removed because editable install doesn't expose migration files
    ]

    operations = [
        migrations.RunPython(
            migrate_ticket_fields_to_json_ext,
            reverse_migrate_ticket_fields,
        ),
    ]
