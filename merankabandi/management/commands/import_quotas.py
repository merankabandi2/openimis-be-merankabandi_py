import csv
from django.core.management.base import BaseCommand
from location.models import Location
from social_protection.models import BenefitPlan
from merankabandi.models import SelectionQuota


class Command(BaseCommand):
    help = 'Import selection quotas from CSV. Format: colline_code,quota[,collect_multiplier]'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str)
        parser.add_argument('benefit_plan_id', type=str)
        parser.add_argument('--round', type=int, default=1)

    def handle(self, *args, **options):
        benefit_plan = BenefitPlan.objects.get(id=options['benefit_plan_id'])
        targeting_round = options['round']
        created = 0
        updated = 0

        with open(options['csv_file'], 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                colline_code = row['colline_code'].strip()
                quota_value = int(row['quota'])
                multiplier = float(row.get('collect_multiplier', 2.0) or 2.0)

                location = Location.objects.filter(code=colline_code).first()
                if not location:
                    self.stderr.write(f"Location not found: {colline_code}")
                    continue

                obj, was_created = SelectionQuota.objects.update_or_create(
                    benefit_plan=benefit_plan,
                    location=location,
                    targeting_round=targeting_round,
                    defaults={'quota': quota_value, 'collect_multiplier': multiplier},
                )
                if was_created:
                    created += 1
                else:
                    updated += 1

        self.stdout.write(f"Done: {created} created, {updated} updated")
