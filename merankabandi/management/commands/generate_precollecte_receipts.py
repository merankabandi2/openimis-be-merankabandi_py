import os
from django.core.management.base import BaseCommand
from social_protection.models import BenefitPlan
from merankabandi.models import PreCollecte

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False


class Command(BaseCommand):
    help = 'Generate PDF receipts for pre-collecte records'

    def add_arguments(self, parser):
        parser.add_argument('benefit_plan_id', type=str)
        parser.add_argument('--location-id', type=str, default=None, help='Filter by colline location ID')
        parser.add_argument('--output-dir', type=str, default='/tmp/precollecte_receipts')

    def handle(self, *args, **options):
        if not HAS_REPORTLAB:
            self.stderr.write("reportlab is required. Install with: pip install reportlab")
            return

        benefit_plan = BenefitPlan.objects.get(id=options['benefit_plan_id'])
        output_dir = options['output_dir']
        os.makedirs(output_dir, exist_ok=True)

        qs = PreCollecte.objects.filter(benefit_plan=benefit_plan).select_related('location')
        if options['location_id']:
            qs = qs.filter(location_id=options['location_id'])

        qs = qs.order_by('location__name', 'social_id')

        if not qs.exists():
            self.stdout.write("No pre-collecte records found.")
            return

        output_path = os.path.join(output_dir, f"precollecte_{benefit_plan.code}.pdf")
        c = canvas.Canvas(output_path, pagesize=A4)
        width, height = A4

        records_per_page = 4
        y_start = height - 30 * mm
        receipt_height = (height - 60 * mm) / records_per_page

        count = 0
        for i, pc in enumerate(qs):
            slot = i % records_per_page
            if slot == 0 and i > 0:
                c.showPage()

            y = y_start - slot * receipt_height

            c.setFont("Helvetica-Bold", 12)
            c.drawString(20 * mm, y, f"FICHE DE PRÉ-COLLECTE - {benefit_plan.code}")
            c.setFont("Helvetica", 10)
            c.drawString(20 * mm, y - 15, f"ID Social: {pc.social_id or '-'}")
            c.drawString(20 * mm, y - 30, f"Nom: {pc.nom} {pc.prenom}")
            c.drawString(20 * mm, y - 45, f"Colline: {pc.location.name if pc.location else '-'}")
            c.drawString(20 * mm, y - 60, f"Téléphone: {pc.telephone or '-'}")
            c.drawString(20 * mm, y - 75, f"Tour: {pc.targeting_round}")

            # Separator line
            c.setStrokeColorRGB(0.7, 0.7, 0.7)
            c.line(15 * mm, y - 85, width - 15 * mm, y - 85)

            count += 1

        c.save()
        self.stdout.write(f"Generated {count} receipts in {output_path}")
