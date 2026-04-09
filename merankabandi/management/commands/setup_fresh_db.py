"""
Setup a fresh openIMIS database with all Merankabandi-specific tables, data, and configuration.

Idempotent — safe to rerun. Each step checks if work is already done before acting.

Usage:
    # 1. Restore prod dump
    createdb imis080426
    pg_restore -d imis080426 -j 4 --no-owner --no-acl dump_file.dump

    # 2. Update .env to point to new DB
    # PSQL_DB_NAME=imis080426 / DB_NAME=imis080426

    # 3. Run setup
    python manage.py setup_fresh_db
    python manage.py setup_fresh_db --dry-run     # preview
    python manage.py setup_fresh_db --skip-views   # faster, create views later
"""
import os
import time
import json as _json
from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.db import connection


# ── Constants ──────────────────────────────────────────────────────────

# Vague assignments from RTM presentation Nov 2025
VAGUE_PROVINCES = {
    1: ['Kirundo', 'Gitega', 'Karuzi', 'Ruyigi'],
    2: ['Ngozi', 'Muyinga', 'Muramvya', 'Mwaro'],
    3: ['Bujumbura Mairie', 'Bubanza', 'Cibitoke', 'Rumonge'],
    4: ['Kayanza', 'Bujumbura Rural', 'Makamba', 'Cankuzo', 'Bururi', 'Rutana'],
}

PROVINCE_ALLOCATION = {
    'Bubanza': 4200, 'Bujumbura Rural': 5800, 'Bururi': 3900,
    'Cankuzo': 2800, 'Cibitoke': 5700, 'Gitega': 9000,
    'Karuzi': 5500, 'Kayanza': 8584, 'Kirundo': 7800,
    'Makamba': 5400, 'Muramvya': 3600, 'Muyinga': 9586,
    'Mwaro': 3500, 'Ngozi': 7200, 'Rumonge': 3500,
    'Rutana': 5000, 'Ruyigi': 5000, 'Bujumbura Mairie': 3932,
}

# Known migration inconsistencies in prod dumps — fake these if applied out of order
MIGRATION_FIXES = [
    ('social_protection', '0015_historicalactivity_activity'),
    ('social_protection', '0016_project_historicalproject'),
    ('social_protection', '0017_add_activity_project_rights_to_admin'),
]

SOLUTION_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'fixtures'
)


class Command(BaseCommand):
    help = 'Setup a fresh openIMIS database with all Merankabandi configuration (idempotent)'

    def add_arguments(self, parser):
        parser.add_argument('--skip-views', action='store_true', help='Skip materialized view creation')
        parser.add_argument('--skip-indexes', action='store_true', help='Skip index creation')
        parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
        parser.add_argument('--fresh-grievances', action='store_true',
                            help='Delete existing grievance tickets and refetch from KoBo')
        parser.add_argument('--suivi-file', default=None,
                            help='Path to Fiche de Suivi Excel file for grievance tracking import')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        self._suivi_file = options.get('suivi_file')
        t0 = time.time()

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 60))
        self.stdout.write(self.style.SUCCESS('MERANKABANDI FRESH DB SETUP (idempotent)'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        db = connection.settings_dict['NAME']
        self.stdout.write(f'  Database: {db}')

        # Step 0: Fix migration inconsistencies
        self._step('0. Fixing migration inconsistencies', dry_run, self._fix_migrations)

        # Step 1: Run ALL migrations
        self._step('1. Running all migrations', dry_run,
                   lambda: call_command('migrate', verbosity=0))

        # Step 2: Load solution fixtures (roles, permissions, module config)
        self._step('2. Loading solution fixtures (roles, rights, config)', dry_run,
                   self._load_solution_fixtures)

        # Step 3: Seed PMT formula
        self._step('3. Seeding PMT formula', dry_run,
                   lambda: call_command('seed_pmt_formula'))

        # Step 4: Seed programme targets
        self._step('4. Seeding programme targets', dry_run,
                   lambda: call_command('seed_programme_targets'))

        # Step 5: Seed workflow templates
        self._step('5. Seeding workflow templates', dry_run,
                   lambda: call_command('seed_workflow_templates'))

        # Step 6: Seed selection quotas
        self._step('6. Seeding selection quotas (4 vagues)', dry_run, self._seed_quotas)

        # Step 7: Backfill CommunePaymentSchedule
        self._step('7. Backfilling CommunePaymentSchedule', dry_run,
                   self._backfill_payment_schedule)

        # Step 8: Clean up known bad payrolls
        self._step('8. Cleaning up rejected payrolls (Oct 2024 SIM failures)', dry_run,
                   self._cleanup_rejected_payrolls)

        # Step 9: Backfill grievance task order
        self._step('9. Backfilling grievance task order', dry_run, self._backfill_task_order)

        # Step 10: Pull grievance data from KoBo (always — idempotent, skips existing)
        self._step('10. Pulling grievance data from KoBo', dry_run,
                   lambda: call_command('pullkobodata'))

        # Step 10b: Fresh grievances from KoBo (optional — deletes first, then re-pulls)
        if options['fresh_grievances']:
            self._step('10b. Refreshing grievances from KoBo (fresh)', dry_run,
                       self._refresh_grievances)

        # Step 11: Normalize ticket categories
        self._step('11. Normalizing ticket categories', dry_run,
                   lambda: call_command('normalize_ticket_categories'))

        # Step 12: Backfill ticket locations
        self._step('12. Backfilling ticket locations', dry_run,
                   lambda: call_command('backfill_ticket_locations'))

        # Step 13: Import suivi tracking (if file provided)
        self._step('13. Importing suivi tracking data', dry_run,
                   self._import_suivi_tracking)

        # Step 14: Normalize json_ext fields (individuals, groups, beneficiaries)
        self._step('14. Normalizing json_ext fields', dry_run,
                   lambda: call_command('normalize_json_ext'))

        # Step 15: Migrate respondent data (group → individual)
        self._step('15. Migrating respondent data to individuals', dry_run,
                   lambda: call_command('normalize_json_ext', migrate_respondent=True))

        # Step 16: Seed notification templates
        self._step('16. Seeding notification templates', dry_run,
                   lambda: call_command('seed_notification_templates'))

        # Step 17: Seed payment agencies (from PaymentPoint migration)
        self._step('17. Seeding payment agencies', dry_run, self._seed_payment_agencies)

        # Step 18: Seed analytics dashboards
        self._step('18. Seeding analytics dashboards', dry_run,
                   lambda: call_command('seed_analytics_dashboards'))

        # Step 19: Create indexes
        if not options['skip_indexes']:
            self._step('19. Creating database indexes', dry_run,
                       lambda: call_command('create_indexes'))

        # Step 20: Create materialized views
        if not options['skip_views']:
            self._step('20. Creating materialized views', dry_run,
                       lambda: call_command('manage_views', action='create'))

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 60}'))
        self.stdout.write(self.style.SUCCESS(f'DONE in {elapsed:.0f}s on database "{db}"'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 60}'))

    def _step(self, label, dry_run, func):
        self.stdout.write(f'\n{label}...')
        if dry_run:
            self.stdout.write(self.style.WARNING('  [DRY RUN] skipped'))
            return
        try:
            func()
            self.stdout.write(self.style.SUCCESS(f'  ✓ done'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'  ✗ ERROR: {e}'))

    # ── Step 0: Fix migration inconsistencies ──

    def _fix_migrations(self):
        """Fix known migration ordering issues in prod dumps."""
        with connection.cursor() as c:
            for app, name in MIGRATION_FIXES:
                c.execute(
                    "SELECT COUNT(*) FROM django_migrations WHERE app = %s AND name = %s",
                    [app, name]
                )
                if c.fetchone()[0] == 0:
                    c.execute(
                        "INSERT INTO django_migrations (app, name, applied) VALUES (%s, %s, NOW())",
                        [app, name]
                    )
                    self.stdout.write(f'  Faked: {app}.{name}')
                else:
                    self.stdout.write(f'  OK: {app}.{name} already applied')

    # ── Step 2: Load solution fixtures ──

    def _load_solution_fixtures(self):
        """Load roles, role rights, and module configuration from solution fixtures."""
        fixtures = [
            'core_language.json',
            'core_role.json',
            'core_roleright.json',
            'module-configuration-core.json',
        ]
        for fixture_name in fixtures:
            fixture_path = os.path.join(SOLUTION_DIR, fixture_name)
            if os.path.exists(fixture_path):
                # Clear existing records and reload
                data = _json.load(open(fixture_path))
                if not data:
                    continue
                model_label = data[0].get('model', '')
                self.stdout.write(f'  Loading {fixture_name} ({len(data)} records, model={model_label})')
                try:
                    call_command('loaddata', fixture_path, verbosity=0)
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f'  ⚠ {fixture_name}: {e}'))
            else:
                self.stdout.write(self.style.WARNING(f'  ⚠ Not found: {fixture_path}'))

    # ── Step 6: Seed selection quotas ──

    def _seed_quotas(self):
        from social_protection.models import BenefitPlan
        from merankabandi.models import SelectionQuota
        from location.models import Location

        bp = BenefitPlan.objects.filter(code='1.2', is_deleted=False).first()
        if not bp:
            self.stdout.write(self.style.WARNING('  No BenefitPlan 1.2 found'))
            return

        # Idempotent: delete and recreate
        deleted = SelectionQuota.objects.filter(benefit_plan=bp).delete()[0]
        if deleted:
            self.stdout.write(f'  Cleared {deleted} existing quotas')

        created = 0
        for vague_num, province_names in VAGUE_PROVINCES.items():
            for prov_name in province_names:
                province = Location.objects.filter(name__icontains=prov_name, type='D').first()
                if not province:
                    continue
                alloc = PROVINCE_ALLOCATION.get(prov_name, 0)
                if not alloc:
                    for k, v in PROVINCE_ALLOCATION.items():
                        if k in prov_name or prov_name in k:
                            alloc = v
                            break
                communes = Location.objects.filter(parent=province, type='W').order_by('name')
                if not communes.exists():
                    continue
                per_commune = alloc // communes.count()
                remainder = alloc % communes.count()
                for i, commune in enumerate(communes):
                    SelectionQuota.objects.create(
                        benefit_plan=bp, location=commune,
                        targeting_round=vague_num,
                        quota=per_commune + (1 if i < remainder else 0),
                        collect_multiplier=2.0,
                    )
                    created += 1
        self.stdout.write(f'  Created {created} quotas across 4 vagues')

    # ── Step 7: Backfill CommunePaymentSchedule ──

    def _backfill_payment_schedule(self):
        import uuid
        import re
        from merankabandi.models import CommunePaymentSchedule, CommunePaymentScheduleStatus
        from social_protection.models import BenefitPlan
        from location.models import Location

        bp = BenefitPlan.objects.filter(code='1.2', is_deleted=False).first()
        if not bp:
            return

        # Idempotent: delete and recreate
        deleted = CommunePaymentSchedule.objects.filter(benefit_plan=bp).delete()[0]
        if deleted:
            self.stdout.write(f'  Cleared {deleted} existing schedules')

        commune_map = {loc.code: loc for loc in Location.objects.filter(type='W')}
        name_map = {loc.name.lower(): loc for loc in Location.objects.filter(type='W')}
        status_map = {
            'APPROVE_FOR_PAYMENT': CommunePaymentScheduleStatus.APPROVED,
            'RECONCILED': CommunePaymentScheduleStatus.RECONCILED,
            'PENDING_APPROVAL': CommunePaymentScheduleStatus.PENDING,
            'PENDING_VERIFICATION': CommunePaymentScheduleStatus.PENDING,
            'REJECTED': CommunePaymentScheduleStatus.REJECTED,
        }

        created = 0
        with connection.cursor() as c:
            # Old format payrolls (niveau2_code in json_ext)
            c.execute("""
                SELECT p."UUID", p.status, p."Json_ext"->>'niveau2_code' AS code
                FROM payroll_payroll p
                WHERE p."isDeleted" = false AND p."Json_ext"->>'niveau2_code' IS NOT NULL
                ORDER BY code, p.name
            """)
            commune_payrolls = {}
            for payroll_id, status, code in c.fetchall():
                commune_payrolls.setdefault(code, []).append((payroll_id, status))

            for code, payrolls in commune_payrolls.items():
                commune = commune_map.get(code)
                if not commune:
                    continue
                for round_num, (payroll_id, status) in enumerate(payrolls, 1):
                    CommunePaymentSchedule.objects.create(
                        id=uuid.uuid4(), benefit_plan=bp, commune=commune,
                        round_number=round_num, payroll_id=payroll_id, is_retry=False,
                        status=status_map.get(status, CommunePaymentScheduleStatus.PENDING),
                        amount_per_beneficiary=72000,
                    )
                    created += 1

            # New format payrolls (commune name in payroll name)
            c.execute("""
                SELECT p."UUID", p.name, p.status
                FROM payroll_payroll p
                WHERE p."isDeleted" = false AND p."Json_ext"->>'niveau2_code' IS NULL
                  AND p.name LIKE '%%commune de%%'
                ORDER BY p.name
            """)
            for payroll_id, name, status in c.fetchall():
                match = re.search(r'commune de (.+)$', name, re.IGNORECASE)
                if not match:
                    continue
                commune = name_map.get(match.group(1).strip().lower())
                if not commune:
                    continue
                existing = CommunePaymentSchedule.objects.filter(
                    benefit_plan=bp, commune=commune, is_retry=False
                ).count()
                CommunePaymentSchedule.objects.create(
                    id=uuid.uuid4(), benefit_plan=bp, commune=commune,
                    round_number=existing + 1, payroll_id=payroll_id, is_retry=False,
                    status=status_map.get(status, CommunePaymentScheduleStatus.PENDING),
                    amount_per_beneficiary=72000,
                )
                created += 1

        self.stdout.write(f'  Created {created} schedules')

    # ── Step 8: Backfill task order ──

    def _cleanup_rejected_payrolls(self):
        """Soft-delete payrolls where ALL benefits are REJECTED.
        Known case: Oct 2024 V1 payrolls — mass SIM failure caused 100% rejection.
        Idempotent: only deletes if not already deleted.
        """
        with connection.cursor() as c:
            # Find payrolls where every benefit is REJECTED
            c.execute("""
                SELECT p."UUID"::text, p.name, COUNT(bc."UUID") AS total
                FROM payroll_payroll p
                JOIN payroll_payrollbenefitconsumption pbc ON pbc.payroll_id = p."UUID" AND pbc."isDeleted" = false
                JOIN payroll_benefitconsumption bc ON bc."UUID" = pbc.benefit_id AND bc."isDeleted" = false
                WHERE p."isDeleted" = false
                GROUP BY p."UUID", p.name
                HAVING COUNT(CASE WHEN bc.status != 'REJECTED' THEN 1 END) = 0
                ORDER BY p.name
            """)
            payrolls = c.fetchall()

            if not payrolls:
                self.stdout.write('  No all-rejected payrolls found')
                return

            total_bc = 0
            for pid, name, ben_count in payrolls:
                # Soft-delete benefits
                c.execute("""
                    UPDATE payroll_benefitconsumption SET "isDeleted" = true
                    WHERE "UUID" IN (
                        SELECT pbc.benefit_id FROM payroll_payrollbenefitconsumption pbc
                        WHERE pbc.payroll_id = %s AND pbc."isDeleted" = false
                    ) AND "isDeleted" = false
                """, [pid])
                # Soft-delete links
                c.execute("""
                    UPDATE payroll_payrollbenefitconsumption SET "isDeleted" = true
                    WHERE payroll_id = %s AND "isDeleted" = false
                """, [pid])
                # Soft-delete payroll
                c.execute('UPDATE payroll_payroll SET "isDeleted" = true WHERE "UUID" = %s', [pid])
                total_bc += ben_count
                self.stdout.write(f'  Soft-deleted: {name} ({ben_count} rejected benefits)')

            self.stdout.write(f'  Total: {len(payrolls)} payrolls, {total_bc} benefits soft-deleted')

    def _backfill_task_order(self):
        from merankabandi.workflow_models import GrievanceTask
        updated = 0
        for task in GrievanceTask.objects.select_related('step_template').filter(order=0):
            if task.step_template:
                task.order = task.step_template.order
                task.save(update_fields=['order'])
                updated += 1
        self.stdout.write(f'  Updated {updated} tasks')

    # ── Step 13: Import suivi tracking ──

    def _import_suivi_tracking(self):
        """Import grievance tracking data from Fiche de Suivi Excel."""
        if not self._suivi_file:
            self.stdout.write('  Skipped — no --suivi-file provided')
            self.stdout.write('  Usage: python manage.py setup_fresh_db --suivi-file /path/to/Fiche_de_Suivi.xlsx')
            return
        if not os.path.exists(self._suivi_file):
            self.stdout.write(self.style.WARNING(f'  File not found: {self._suivi_file}'))
            return
        call_command('import_suivi_tracking', self._suivi_file)

    # ── Step 17: Seed payment agencies ──

    def _seed_payment_agencies(self):
        """Ensure payment agencies exist — migrate from PaymentPoint if needed."""
        from merankabandi.models import PaymentAgency, ProvincePaymentAgency
        from social_protection.models import BenefitPlan
        from location.models import Location

        # Default agencies if none exist
        AGENCIES = [
            ('LUMICASH', 'LUMICASH', 'StrategyOnlinePaymentPush', True),
            ('INTERBANK', 'INTERBANK', 'StrategyOnlinePaymentPush', True),
            ('FINBANK', 'FINBANK', 'StrategyOfflinePayment', True),
            ('BANCOBU', 'BANCOBU', 'StrategyOfflinePayment', True),
        ]

        if PaymentAgency.objects.exists():
            self.stdout.write(f'  Already has {PaymentAgency.objects.count()} agencies — skipping')
            return

        # Try to migrate from PaymentPoint first
        try:
            from payroll.models import PaymentPoint
            pp_count = PaymentPoint.objects.filter(is_deleted=False).count()
            if pp_count > 0:
                created = 0
                for pp in PaymentPoint.objects.filter(is_deleted=False):
                    agency, was_created = PaymentAgency.objects.get_or_create(
                        code=pp.name[:20],
                        defaults={
                            'name': pp.name,
                            'is_active': True,
                            'payment_gateway': 'StrategyOfflinePayment',
                        },
                    )
                    if was_created:
                        created += 1
                self.stdout.write(f'  Migrated {created} agencies from {pp_count} PaymentPoints')
                return
        except Exception:
            pass

        # Fallback: seed defaults
        created = 0
        for code, name, gateway, active in AGENCIES:
            _, was_created = PaymentAgency.objects.get_or_create(
                code=code,
                defaults={'name': name, 'payment_gateway': gateway, 'is_active': active},
            )
            if was_created:
                created += 1
        self.stdout.write(f'  Created {created} default payment agencies')

        # Assign agencies to provinces for BenefitPlan 1.2
        bp = BenefitPlan.objects.filter(code='1.2', is_deleted=False).first()
        if bp:
            lumicash = PaymentAgency.objects.filter(code='LUMICASH').first()
            if lumicash:
                provinces = Location.objects.filter(type='D')
                assigned = 0
                for prov in provinces:
                    _, was_created = ProvincePaymentAgency.objects.get_or_create(
                        province=prov, benefit_plan=bp, payment_agency=lumicash,
                    )
                    if was_created:
                        assigned += 1
                self.stdout.write(f'  Assigned LUMICASH to {assigned} provinces')

    # ── Step 10: Refresh grievances ──

    def _refresh_grievances(self):
        """Delete existing tickets and refetch from KoBo."""
        from grievance_social_protection.models import Ticket
        count = Ticket.objects.count()
        if count > 0:
            self.stdout.write(f'  Deleting {count} existing tickets...')
            Ticket.objects.all().delete()
        self.stdout.write('  Pulling fresh grievance data from KoBo...')
        call_command('pullkobodata')
