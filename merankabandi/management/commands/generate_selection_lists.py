import csv
import os
from django.core.management.base import BaseCommand
from individual.models import Group
from social_protection.models import BenefitPlan

try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False


class Command(BaseCommand):
    help = 'Generate selection lists (PDF/CSV) per colline for community validation'

    def add_arguments(self, parser):
        parser.add_argument('benefit_plan_id', type=str)
        parser.add_argument('--location-id', type=str, default=None, help='Filter by colline location ID')
        parser.add_argument('--format', type=str, default='both', choices=['pdf', 'csv', 'both'])
        parser.add_argument('--output-dir', type=str, default='/tmp/selection_lists')

    def handle(self, *args, **options):
        benefit_plan = BenefitPlan.objects.get(id=options['benefit_plan_id'])
        output_dir = options['output_dir']
        fmt = options['format']
        os.makedirs(output_dir, exist_ok=True)

        # Also include groups linked via json_ext that aren't yet GroupBeneficiary
        # (pre-beneficiary selection phase: groups with selection_status in json_ext)
        from django.db.models import Q
        all_groups = Group.objects.filter(
            Q(groupbeneficiary__benefit_plan=benefit_plan) |
            Q(json_ext__has_key='selection_status')
        ).distinct().select_related('head')

        if options['location_id']:
            all_groups = all_groups.filter(
                head__json_ext__location_id=options['location_id']
            )

        # Group by colline (location from head individual json_ext)
        collines = {}
        for group in all_groups:
            json_ext = group.json_ext or {}
            location_name = json_ext.get('colline_name', 'Unknown')
            if location_name not in collines:
                collines[location_name] = []
            collines[location_name].append({
                'social_id': json_ext.get('social_id', '-'),
                'head_name': f"{json_ext.get('nom', '')} {json_ext.get('prenom', '')}".strip() or str(group.head) if group.head else '-',
                'pmt_score': json_ext.get('pmt_score', '-'),
                'status': json_ext.get('selection_status', '-'),
            })

        if not collines:
            self.stdout.write("No groups found for this benefit plan.")
            return

        # Sort within each colline by PMT score (ascending = poorest first)
        for location_name in collines:
            collines[location_name].sort(
                key=lambda x: x['pmt_score'] if isinstance(x['pmt_score'], (int, float)) else 999999
            )

        if fmt in ('csv', 'both'):
            self._generate_csv(collines, benefit_plan, output_dir)

        if fmt in ('pdf', 'both'):
            if not HAS_REPORTLAB:
                self.stderr.write("reportlab is required for PDF. Install with: pip install reportlab")
            else:
                self._generate_pdf(collines, benefit_plan, output_dir)

    def _generate_csv(self, collines, benefit_plan, output_dir):
        output_path = os.path.join(output_dir, f"selection_{benefit_plan.code}.csv")
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Colline', 'Rang', 'ID Social', 'Chef de ménage', 'Score PMT', 'Statut'])
            for location_name, groups in sorted(collines.items()):
                for rank, g in enumerate(groups, 1):
                    writer.writerow([location_name, rank, g['social_id'], g['head_name'], g['pmt_score'], g['status']])
        self.stdout.write(f"CSV generated: {output_path}")

    def _generate_pdf(self, collines, benefit_plan, output_dir):
        output_path = os.path.join(output_dir, f"selection_{benefit_plan.code}.pdf")
        c = canvas.Canvas(output_path, pagesize=landscape(A4))
        width, height = landscape(A4)

        for location_name, groups in sorted(collines.items()):
            c.setFont("Helvetica-Bold", 14)
            c.drawString(20 * mm, height - 20 * mm, f"Liste de sélection - {benefit_plan.code}")
            c.setFont("Helvetica", 11)
            c.drawString(20 * mm, height - 30 * mm, f"Colline: {location_name}")
            c.drawString(20 * mm, height - 38 * mm, f"Nombre de ménages: {len(groups)}")

            # Table header
            y = height - 50 * mm
            c.setFont("Helvetica-Bold", 9)
            headers = ['Rang', 'ID Social', 'Chef de ménage', 'Score PMT', 'Statut']
            x_positions = [20, 40, 80, 160, 200]
            for col, header in enumerate(headers):
                c.drawString(x_positions[col] * mm, y, header)

            c.setFont("Helvetica", 9)
            for rank, g in enumerate(groups, 1):
                y -= 12
                if y < 20 * mm:
                    c.showPage()
                    y = height - 30 * mm
                    c.setFont("Helvetica", 9)

                c.drawString(x_positions[0] * mm, y, str(rank))
                c.drawString(x_positions[1] * mm, y, str(g['social_id']))
                c.drawString(x_positions[2] * mm, y, str(g['head_name'])[:40])
                c.drawString(x_positions[3] * mm, y, str(g['pmt_score']))
                c.drawString(x_positions[4] * mm, y, str(g['status']))

            c.showPage()

        c.save()
        self.stdout.write(f"PDF generated: {output_path}")
