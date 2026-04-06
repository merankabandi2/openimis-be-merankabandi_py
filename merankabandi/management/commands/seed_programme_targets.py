"""
Seed programme targets from project document P175327 into BenefitPlan.json_ext.
"""
from django.core.management.base import BaseCommand
from social_protection.models import BenefitPlan


# Targets from project document (Merankabandi II — P175327)
PROGRAMME_TARGETS = {
    '1.2': {
        'target_households': 112000,  # 100K regular + 12K host community
        'max_rounds': 12,
        'amount_per_round': 72000,
        'total_per_beneficiary': 864000,
        'programme_type': 'REGULAR',
        'label': 'Transferts Monétaires Réguliers',
    },
    '1.1': {
        'target_households': 25000,
        'max_rounds': 2,
        'amount_per_round': 100000,
        'total_per_beneficiary': 200000,
        'programme_type': 'EMERGENCY',
        'label': "Transferts d'Urgence COVID-19",
    },
    '1.4': {
        'target_households': 8000,
        'max_rounds': 12,
        'amount_per_round': 72000,
        'total_per_beneficiary': 864000,
        'programme_type': 'REFUGEE',
        'label': 'Transferts Réfugiés',
    },
}


class Command(BaseCommand):
    help = 'Seed programme targets into BenefitPlan.json_ext from project document P175327'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without saving')

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        for code, targets in PROGRAMME_TARGETS.items():
            plans = BenefitPlan.objects.filter(code=code, is_deleted=False)
            if not plans.exists():
                self.stdout.write(self.style.WARNING(f'No BenefitPlan with code={code} found, skipping'))
                continue

            for plan in plans:
                json_ext = plan.json_ext or {}
                json_ext['programme_targets'] = targets

                if dry_run:
                    self.stdout.write(f'Would set programme_targets on {plan.code} "{plan.name}": {targets}')
                else:
                    # Use queryset update() to bypass HistoryBusinessModel.save() user requirement
                    BenefitPlan.objects.filter(pk=plan.pk).update(json_ext=json_ext)
                    self.stdout.write(self.style.SUCCESS(
                        f'Set programme_targets on {plan.code} "{plan.name}": '
                        f'{targets["target_households"]} households, {targets["max_rounds"]} rounds, '
                        f'{targets["amount_per_round"]} BIF/round'
                    ))

        if dry_run:
            self.stdout.write(self.style.WARNING('Dry run — no changes saved'))
