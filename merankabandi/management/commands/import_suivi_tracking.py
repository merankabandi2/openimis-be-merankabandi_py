"""
Import tracking/follow-up data from the Fiche de Suivi Excel file.

Reads the 'Suivi' sheet and updates existing tickets with:
- Status (oui → RESOLVED, en cours* → IN_PROGRESS)
- json_ext.tracking: responsible, dates, observation
- attending_staff from RoleAssignment (if mapped)

Usage:
    python manage.py import_suivi_tracking /path/to/Fiche_de_Suivi-2025.xlsx
    python manage.py import_suivi_tracking /path/to/file.xlsx --dry-run
"""
import json
import logging
from datetime import datetime

import openpyxl
from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Import tracking data from Fiche de Suivi Excel into existing tickets'

    def add_arguments(self, parser):
        parser.add_argument('file', help='Path to Fiche de Suivi Excel file')
        parser.add_argument('--dry-run', action='store_true', help='Preview changes without saving')
        parser.add_argument('--sheet', default='Suivi', help='Sheet name (default: Suivi)')

    def handle(self, *args, **options):
        file_path = options['file']
        dry_run = options['dry_run']
        sheet_name = options['sheet']

        self.stdout.write(f"Loading {file_path} sheet '{sheet_name}'...")
        wb = openpyxl.load_workbook(file_path, data_only=True)
        ws = wb[sheet_name]

        # Find column indices from headers
        col_map = {}
        for c in ws[1]:
            if c.value:
                col_map[str(c.value).strip()] = c.column

        uuid_col = col_map.get('_uuid')
        if not uuid_col:
            self.stderr.write('ERROR: No _uuid column found')
            return

        resolved_col = col_map.get('Plainte résolue')
        resp_col = col_map.get('Responsable')
        date_sub_col = col_map.get('Date de soumission')
        date_res_col = col_map.get('Date de résolution')
        delay_col = col_map.get('Délai de résolution(jours)')
        obs_col = col_map.get('Observation')

        self.stdout.write(f"Columns: uuid={uuid_col} resolved={resolved_col} "
                          f"resp={resp_col} date_sub={date_sub_col} "
                          f"date_res={date_res_col} delay={delay_col} obs={obs_col}")

        # Read all rows
        rows = []
        for row_idx in range(2, ws.max_row + 1):
            uuid_val = ws.cell(row=row_idx, column=uuid_col).value
            if not uuid_val:
                continue

            resolved = ws.cell(row=row_idx, column=resolved_col).value if resolved_col else None
            responsible = ws.cell(row=row_idx, column=resp_col).value if resp_col else None
            date_sub = ws.cell(row=row_idx, column=date_sub_col).value if date_sub_col else None
            date_res = ws.cell(row=row_idx, column=date_res_col).value if date_res_col else None
            delay = ws.cell(row=row_idx, column=delay_col).value if delay_col else None
            observation = ws.cell(row=row_idx, column=obs_col).value if obs_col else None

            rows.append({
                'uuid': str(uuid_val).strip(),
                'resolved': str(resolved).strip().lower() if resolved else None,
                'responsible': str(responsible).strip() if responsible else None,
                'date_submitted': _parse_date(date_sub),
                'date_resolved': _parse_date(date_res),
                'resolution_delay_days': _parse_int(delay),
                'observation': str(observation).strip() if observation else None,
            })

        self.stdout.write(f"Read {len(rows)} rows from Excel")

        # Match against DB
        cursor = connection.cursor()
        cursor.execute("""
            SELECT "UUID"::text, status, "Json_ext"
            FROM grievance_social_protection_ticket
            WHERE "isDeleted" = false
        """)
        db_tickets = {}
        for row in cursor.fetchall():
            ext = row[2]
            if isinstance(ext, str):
                try:
                    ext = json.loads(ext)
                except (json.JSONDecodeError, TypeError):
                    ext = {}
            db_tickets[row[0]] = {'status': row[1], 'json_ext': ext or {}}

        self.stdout.write(f"DB tickets: {len(db_tickets)}")

        # Process updates
        updated = 0
        status_changed = 0
        not_found = 0
        skipped = 0

        for row in rows:
            uuid = row['uuid']
            if uuid not in db_tickets:
                not_found += 1
                if not_found <= 5:
                    self.stdout.write(f"  NOT FOUND: {uuid}")
                continue

            db = db_tickets[uuid]
            existing_ext = db['json_ext']

            # Skip if already has tracking data with a resolved date (idempotent)
            existing_tracking = existing_ext.get('tracking', {})
            if existing_tracking.get('date_resolved') and existing_tracking.get('responsible'):
                skipped += 1
                continue

            # Build tracking section
            tracking = {
                'responsible': row['responsible'],
                'date_submitted': row['date_submitted'],
                'date_resolved': row['date_resolved'],
                'resolution_delay_days': row['resolution_delay_days'],
                'observation': row['observation'],
                'resolved_raw': row['resolved'],
            }

            # Merge into json_ext
            new_ext = {**existing_ext, 'tracking': tracking}

            # Determine new status
            new_status = db['status']
            if row['resolved'] in ('oui', 'résolu'):
                new_status = 'RESOLVED'
            elif row['resolved'] and 'en cours' in row['resolved']:
                new_status = 'IN_PROGRESS'

            status_will_change = new_status != db['status']

            if not dry_run:
                cursor.execute("""
                    UPDATE grievance_social_protection_ticket
                    SET "Json_ext" = %s,
                        status = %s,
                        "DateUpdated" = NOW()
                    WHERE "UUID" = %s
                """, [json.dumps(new_ext), new_status, uuid])

            updated += 1
            if status_will_change:
                status_changed += 1

            if updated <= 5:
                self.stdout.write(
                    f"  {'[DRY] ' if dry_run else ''}UPDATE {uuid[:20]}... "
                    f"status={db['status']}→{new_status} "
                    f"resp={row['responsible']} "
                    f"date_res={row['date_resolved']}"
                )

        self.stdout.write(self.style.SUCCESS(
            f"\n{'[DRY RUN] ' if dry_run else ''}"
            f"Updated: {updated}, Status changed: {status_changed}, "
            f"Skipped (already tracked): {skipped}, Not found: {not_found}"
        ))


def _parse_date(val):
    """Parse date from Excel — could be datetime, string, or None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d')
    s = str(val).strip()
    if not s or s.lower() == 'none':
        return None
    # Try common formats
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    # If it's a free text date like "rendez-vous semaine prochaine", keep as-is
    return s


def _parse_int(val):
    """Parse integer from Excel — could be int, float, string, or None."""
    if val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None
