"""
Seed test data for E2E testing.
Creates records directly in models (bypassing async MutationLog pipeline).
Usage: python manage.py seed_test_data
"""
import logging
from datetime import date, timedelta
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Seed test data for models that are empty (MonetaryTransfer, SelectionQuota, PreCollecte, RoleAssignment)'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Show what would be created without creating')
        parser.add_argument('--delete', action='store_true', help='Delete seeded test data')

    def handle(self, *args, **options):
        dry_run = options.get('dry_run', False)
        delete = options.get('delete', False)

        if delete:
            self._delete_test_data()
            return

        self._seed_monetary_transfers(dry_run)
        self._seed_selection_quotas(dry_run)
        self._seed_precollecte(dry_run)
        self._seed_role_assignments(dry_run)

        self.stdout.write(self.style.SUCCESS('Seed test data complete'))

    def _seed_monetary_transfers(self, dry_run):
        from merankabandi.models import MonetaryTransfer
        from social_protection.models import BenefitPlan
        from location.models import Location
        from merankabandi.models import PaymentAgency

        if MonetaryTransfer.objects.count() > 0:
            self.stdout.write('MonetaryTransfer: already has data, skipping')
            return

        plan = BenefitPlan.objects.first()
        province = Location.objects.filter(type='D').first()
        pp = PaymentAgency.objects.first()

        if not plan or not province:
            self.stdout.write(self.style.WARNING('MonetaryTransfer: missing BenefitPlan or Location'))
            return

        if dry_run:
            self.stdout.write(f'Would create MonetaryTransfer: plan={plan.code}, location={province.name}')
            return

        for i in range(3):
            MonetaryTransfer.objects.create(
                transfer_date=date.today() - timedelta(days=30 * i),
                location=province,
                programme=plan,
                payment_agency=pp,
                planned_women=100 + i * 10,
                paid_women=90 + i * 10,
                planned_men=80 + i * 10,
                paid_men=70 + i * 10,
                planned_twa=20 + i * 5,
                paid_twa=15 + i * 5,
            )
        self.stdout.write(self.style.SUCCESS(f'Created 3 MonetaryTransfers'))

    def _seed_selection_quotas(self, dry_run):
        from merankabandi.models import SelectionQuota
        from social_protection.models import BenefitPlan
        from location.models import Location

        if SelectionQuota.objects.count() > 0:
            self.stdout.write('SelectionQuota: already has data, skipping')
            return

        plan = BenefitPlan.objects.first()
        collines = Location.objects.filter(type='V')[:5]

        if not plan or not collines.exists():
            self.stdout.write(self.style.WARNING('SelectionQuota: missing BenefitPlan or Collines'))
            return

        if dry_run:
            self.stdout.write(f'Would create {collines.count()} SelectionQuotas for plan {plan.code}')
            return

        for colline in collines:
            SelectionQuota.objects.create(
                benefit_plan=plan,
                location=colline,
                targeting_round=1,
                quota=15,
                collect_multiplier=Decimal('3.0'),
            )
        self.stdout.write(self.style.SUCCESS(f'Created {collines.count()} SelectionQuotas'))

    def _seed_precollecte(self, dry_run):
        from merankabandi.models import PreCollecte
        from social_protection.models import BenefitPlan
        from location.models import Location

        if PreCollecte.objects.count() > 0:
            self.stdout.write('PreCollecte: already has data, skipping')
            return

        plan = BenefitPlan.objects.first()
        colline = Location.objects.filter(type='V').first()

        if not plan or not colline:
            self.stdout.write(self.style.WARNING('PreCollecte: missing BenefitPlan or Colline'))
            return

        if dry_run:
            self.stdout.write(f'Would create 5 PreCollecte records')
            return

        names = [
            ('Nshimirimana', 'Claudine', 'F'),
            ('Ndayisaba', 'Jean', 'M'),
            ('Irakoze', 'Marie', 'F'),
            ('Hakizimana', 'Pierre', 'M'),
            ('Niyonzima', 'Agnes', 'F'),
        ]

        for i, (nom, prenom, sexe) in enumerate(names):
            try:
                # Generate a simple social_id: YYPPRRNNNNNNNN
                province_code = str(colline.parent.parent.code if colline.parent and colline.parent.parent else '01').zfill(2)
                social_id = f"26{province_code}01{str(i+1).zfill(8)}"
                PreCollecte.objects.create(
                    benefit_plan=plan,
                    location=colline,
                    nom=nom,
                    prenom=prenom,
                    social_id=social_id,
                    targeting_round=1,
                    status='COLLECTED',
                )
            except Exception as e:
                self.stderr.write(f'Error creating PreCollecte {nom}: {e}')
        self.stdout.write(self.style.SUCCESS(f'Created 5 PreCollecte records'))

    def _seed_role_assignments(self, dry_run):
        from merankabandi.workflow_models import RoleAssignment

        if RoleAssignment.objects.count() > 0:
            self.stdout.write('RoleAssignment: already has data, skipping')
            return

        User = get_user_model()
        admin_user = User.objects.filter(username='Admin').first()
        if not admin_user:
            admin_user = User.objects.first()

        if not admin_user:
            self.stdout.write(self.style.WARNING('RoleAssignment: no users found'))
            return

        if dry_run:
            self.stdout.write(f'Would create 6 RoleAssignments for user {admin_user.username}')
            return

        roles = ['OT', 'RTM', 'RSI', 'RDO', 'RVBG', 'RIUIRCH']
        for role in roles:
            RoleAssignment.objects.create(
                role=role,
                user=admin_user,
                is_active=True,
            )
        self.stdout.write(self.style.SUCCESS(f'Created {len(roles)} RoleAssignments'))
