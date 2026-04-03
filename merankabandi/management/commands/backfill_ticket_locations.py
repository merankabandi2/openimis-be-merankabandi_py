"""
Backfill ticket json_ext.location with resolved colline_code and location_id.

Converts old KoBo numeric codes (province/commune/colline) stored in json_ext
to openIMIS colline_code + location_id, removing redundant province/commune fields.
"""
from django.core.management.base import BaseCommand
from grievance_social_protection.models import Ticket
from location.models import Location


def _kobo_to_openimis_code(kobo_code):
    if not kobo_code:
        return None
    padded = str(kobo_code).strip().zfill(7)
    return padded[:4] + padded[5:]


def _resolve_colline(colline_val):
    """Try to resolve a colline value to an openIMIS Location."""
    if not colline_val:
        return None

    val = str(colline_val).strip()

    # Numeric → KoBo code conversion
    if val.isdigit():
        imis_code = _kobo_to_openimis_code(val)
        loc = Location.objects.filter(code=imis_code, type='V').first()
        if loc:
            return loc
        loc = Location.objects.filter(code=val, type='V').first()
        if loc:
            return loc

    # Name match
    loc = Location.objects.filter(name__iexact=val, type='V').first()
    if loc:
        return loc

    return None


class Command(BaseCommand):
    help = 'Backfill ticket json_ext.location with resolved colline_code and location_id'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without saving')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        updated = 0
        skipped = 0
        not_found = 0

        tickets = Ticket.objects.filter(json_ext__isnull=False, is_deleted=False)
        total = tickets.count()
        self.stdout.write(f'Processing {total} tickets...')

        for ticket in tickets.iterator():
            ext = ticket.json_ext or {}
            location = ext.get('location', {})

            # Skip if already resolved
            if location.get('colline_code') and location.get('location_id'):
                skipped += 1
                continue

            # Try to resolve from various fields
            colline_val = (
                location.get('colline')
                or location.get('kobo_colline')
                or ext.get('legacy', {}).get('colline')
            )

            colline_loc = _resolve_colline(colline_val)

            if not colline_loc:
                not_found += 1
                if not dry_run:
                    self.stdout.write(f'  NOT FOUND: ticket={ticket.id} colline={colline_val!r}')
                continue

            # Build clean location
            new_location = {
                'colline_code': colline_loc.code,
                'location_id': str(colline_loc.id),
            }
            # Preserve non-location fields
            if location.get('milieu_residence'):
                new_location['milieu_residence'] = location['milieu_residence']
            if location.get('gps'):
                new_location['gps'] = location['gps']

            if dry_run:
                commune = colline_loc.parent
                province = commune.parent if commune else None
                self.stdout.write(
                    f'  WOULD UPDATE: ticket={ticket.id} '
                    f'colline={colline_val!r} -> {colline_loc.name} ({colline_loc.code}) '
                    f'commune={commune.name if commune else "?"} '
                    f'province={province.name if province else "?"}'
                )
            else:
                ext['location'] = new_location
                ticket.json_ext = ext
                ticket.save_history = False
                Ticket.objects.filter(id=ticket.id).update(json_ext=ext)

            updated += 1

        self.stdout.write(self.style.SUCCESS(
            f'\n{"DRY RUN - " if dry_run else ""}Done: {updated} updated, {skipped} already resolved, {not_found} not found (total: {total})'
        ))
