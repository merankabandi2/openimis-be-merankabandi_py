"""
Normalize grievance ticket categories to match module config.

Problem: Ticket categories are stored in 3 formats:
  1. JSON arrays: '["paiement"]', '["autre", "paiement"]' (new KoBo form)
  2. Legacy slug strings: 'remplacement', 'probl_me_de_paiement__non_r_ception__mon' (old KoBo form)
  3. NULL (no category assigned)

Solution: Convert all to plain single-value strings matching the grievance module's
configured categories. For multi-value arrays, save the most restrictive category
as the main one and store the others in json_ext.additional_categories.

Restrictiveness order (most → least):
  violence_vbg > corruption > accident_negligence > discrimination_ethnie_religion
  > maladie_mentale > erreur_exclusion > erreur_inclusion > paiement > telephone
  > compte > information > uncategorized

Usage:
  python manage.py normalize_ticket_categories --dry-run
  python manage.py normalize_ticket_categories
"""
import json
import logging

from django.core.management.base import BaseCommand
from django.db import transaction

logger = logging.getLogger(__name__)

# Restrictiveness ranking: lower number = more restrictive = takes priority
RESTRICTIVENESS = {
    'violence_vbg': 1,
    'corruption': 2,
    'accident_negligence': 3,
    'discrimination_ethnie_religion': 4,
    'maladie_mentale': 5,
    'erreur_exclusion': 6,
    'erreur_inclusion': 7,
    'paiement': 8,
    'telephone': 9,
    'compte': 10,
    'information': 11,
    'uncategorized': 99,
}

# All valid module config categories (flat, without sub-categories)
VALID_CATEGORIES = set(RESTRICTIVENESS.keys())

# Map old KoBo slug strings → module config category
LEGACY_KOBO_MAP = {
    # Old form (atpoVbHXZCdLD9ETHTv6z4) — sensitive
    'eas_hs__exploitation__abus_sexuel___harc': 'violence_vbg',
    'pr_l_vements_de_fonds': 'corruption',
    'd_tournement_de_fonds___corruption': 'corruption',
    'conflit_familial': 'violence_vbg',
    'accident_grave_ou_n_gligence_professionn': 'accident_negligence',
    # Old form — special
    'erreur_d_inclusion_potentielle': 'erreur_inclusion',
    'cibl__mais_pas_collect': 'erreur_exclusion',
    'cibl__et_collect': 'erreur_exclusion',
    'migration': 'erreur_exclusion',
    # Old form — non-sensitive
    'probl_me_de_paiement__non_r_ception__mon': 'paiement',
    'carte_sim__bloqu_e__vol_e__perdue__etc': 'telephone',
    'probl_mes_de_t_l_phone__vol__endommag__n': 'telephone',
    'incoh_rence_des_donn_es_personnelles__nu': 'information',
    'probl_mes_de_compte_mobile_money__ecocas': 'compte',
    # Workflow types (these are action types, not categories)
    'remplacement': 'uncategorized',
    'suppression': 'uncategorized',
}

# Map sub-category values (from new KoBo with parent > child format) to parent
SUB_CATEGORY_PARENT = {
    'viol': 'violence_vbg',
    'mariage_force_precoce': 'violence_vbg',
    'violence_abus': 'violence_vbg',
    'sante_maternelle': 'violence_vbg',
    'demande_insertion': 'erreur_exclusion',
    'probleme_identification': 'erreur_exclusion',
    'paiement_pas_recu': 'paiement',
    'paiement_en_retard': 'paiement',
    'paiement_incomplet': 'paiement',
    'vole': 'paiement',
    'perdu': 'telephone',
    'pas_de_reseau': 'telephone',
    'allume_pas_batterie': 'telephone',
    'recoit_pas_tm': 'telephone',
    'mot_de_passe_oublie': 'telephone',
    'non_active': 'compte',
    'bloque': 'compte',
}


def resolve_single_value(val):
    """Resolve a single category value string to a valid module config category."""
    val = val.strip()

    # Already a valid config category?
    if val in VALID_CATEGORIES:
        return val

    # Known legacy KoBo slug?
    if val in LEGACY_KOBO_MAP:
        return LEGACY_KOBO_MAP[val]

    # Known sub-category?
    if val in SUB_CATEGORY_PARENT:
        return SUB_CATEGORY_PARENT[val]

    # 'autre' from any context → uncategorized
    if val in ('autre', 'autre_'):
        return 'uncategorized'

    # Unknown → uncategorized
    return 'uncategorized'


def pick_most_restrictive(categories):
    """Given a list of resolved category strings, return the most restrictive one."""
    if not categories:
        return 'uncategorized'
    return min(categories, key=lambda c: RESTRICTIVENESS.get(c, 50))


def normalize_category(raw_value):
    """
    Normalize a raw category value from the DB.

    Returns: (main_category, additional_categories, original_value)
      - main_category: single string matching module config
      - additional_categories: list of other categories (if multi-value) or None
      - original_value: preserved for audit trail
    """
    if raw_value is None or raw_value.strip() == '':
        return None, None, raw_value

    original = raw_value

    # Try parsing as JSON array
    if raw_value.startswith('['):
        try:
            values = json.loads(raw_value)
            if isinstance(values, list):
                resolved = []
                for v in values:
                    # Handle space-separated values inside a single array element
                    # e.g. '["paiement compte"]' or '["erreur_exclusion erreur_inclusion"]'
                    for part in str(v).split():
                        resolved.append(resolve_single_value(part))

                # Deduplicate while preserving order
                seen = set()
                unique = []
                for r in resolved:
                    if r not in seen:
                        seen.add(r)
                        unique.append(r)

                main = pick_most_restrictive(unique)
                others = [c for c in unique if c != main]
                return main, others if others else None, original
        except (json.JSONDecodeError, TypeError):
            pass

    # Plain string — legacy KoBo or direct value
    resolved = resolve_single_value(raw_value)
    return resolved, None, original


class Command(BaseCommand):
    help = 'Normalize ticket categories to match grievance module config (single plain string)'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true',
                            help='Show what would change without modifying data')

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        from grievance_social_protection.models import Ticket

        tickets = Ticket.objects.all()
        total = tickets.count()

        # Collect stats
        stats = {
            'already_valid': 0,
            'json_array_single': 0,
            'json_array_multi': 0,
            'legacy_kobo': 0,
            'null_unchanged': 0,
            'errors': 0,
        }
        changes = []

        for ticket in tickets.iterator():
            raw = ticket.category
            main, additional, original = normalize_category(raw)

            if raw is None or raw.strip() == '':
                stats['null_unchanged'] += 1
                continue

            if raw == main:
                stats['already_valid'] += 1
                continue

            # Determine change type for stats
            if raw.startswith('['):
                if additional:
                    stats['json_array_multi'] += 1
                else:
                    stats['json_array_single'] += 1
            else:
                stats['legacy_kobo'] += 1

            changes.append({
                'id': ticket.id,
                'old': raw,
                'new': main,
                'additional': additional,
                'original': original,
            })

        # Report
        self.stdout.write(f'\n=== Category Normalization Report ===')
        self.stdout.write(f'Total tickets: {total}')
        self.stdout.write(f'Already valid: {stats["already_valid"]}')
        self.stdout.write(f'NULL/empty (unchanged): {stats["null_unchanged"]}')
        self.stdout.write(f'JSON array → single value: {stats["json_array_single"]}')
        self.stdout.write(f'JSON array → most restrictive (multi): {stats["json_array_multi"]}')
        self.stdout.write(f'Legacy KoBo slug → mapped: {stats["legacy_kobo"]}')
        self.stdout.write(f'Total changes: {len(changes)}')

        if not changes:
            self.stdout.write(self.style.SUCCESS('No changes needed.'))
            return

        # Show sample changes
        self.stdout.write(f'\n--- Sample changes (first 20) ---')
        for ch in changes[:20]:
            extra = f' + {ch["additional"]}' if ch['additional'] else ''
            self.stdout.write(f'  {ch["old"]!r:50s} → {ch["new"]!r}{extra}')

        # Show category distribution after normalization
        from collections import Counter
        new_dist = Counter()
        for ticket in tickets.iterator():
            raw = ticket.category
            main, _, _ = normalize_category(raw)
            if main:
                new_dist[main] += 1
            else:
                new_dist['(null)'] += 1

        self.stdout.write(f'\n--- Distribution after normalization ---')
        for cat, cnt in new_dist.most_common():
            self.stdout.write(f'  {cat:40s} {cnt:5d}')

        if dry_run:
            self.stdout.write(self.style.WARNING('\nDry run — no changes made.'))
            return

        # Apply changes via raw SQL to avoid HistoryBusinessModel.save() user requirement
        self.stdout.write(f'\nApplying {len(changes)} changes...')
        updated = 0
        from django.db import connection
        with transaction.atomic():
            cursor = connection.cursor()
            for ch in changes:
                # Update category directly
                ext_update = {}
                ext_update['original_category'] = ch['original']
                if ch['additional']:
                    ext_update['additional_categories'] = ch['additional']

                # Merge into existing json_ext
                cursor.execute("""
                    UPDATE grievance_social_protection_ticket
                    SET category = %s,
                        "Json_ext" = COALESCE("Json_ext", '{}'::jsonb) || %s::jsonb
                    WHERE "UUID" = %s
                """, [ch['new'], json.dumps(ext_update), str(ch['id'])])
                updated += 1

        self.stdout.write(self.style.SUCCESS(
            f'Done. Updated {updated} tickets. '
            f'Original values preserved in json_ext.original_category'
        ))
