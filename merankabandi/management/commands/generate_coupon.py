import csv
import os
import datetime
import logging
from django.core.management.base import BaseCommand
from docxtpl import DocxTemplate,InlineImage
from docx.shared import Mm
import barcode
from barcode.writer import ImageWriter
from django.conf import settings
from individual.models import  Individual


logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Generate pre-collection documents for selected individuals'

    def handle(self, *args, **options):
        racine = settings.DOCUMENTS_DIR

        preselected_individuals = Individual.objects.filter(
            json_ext__programme__isnull=False,
        ).prefetch_related('location', 'location__parent', 'location__parent__parent')

        # Load the template
        template_path = os.path.join(racine, 'templates', 'merankabandi-template-precollecte.docx')
        doc = DocxTemplate(template_path)

        # Prepare data for document
        data = {}
        for id, preselected_individual in enumerate(preselected_individuals, 1):
            pro = preselected_individual.location.parent.parent.name if preselected_individual.location and preselected_individual.location.parent and preselected_individual.location.parent.parent else ''
            com = preselected_individual.location.parent.name if preselected_individual.location and preselected_individual.location.parent else ''
            niveau3_label = preselected_individual.location.name if preselected_individual.location else ''
            # Create directories for output files
            barcode_dir = os.path.join(racine, 'documents-generes', 'precollecte', 'codebarres', 
                                    pro, com, niveau3_label)
            os.makedirs(barcode_dir, exist_ok=True)

            nombre_formate = f"{int(id):07d}"
            code = f"{preselected_individual.location.code.zfill(6)}{nombre_formate}"
            barcode_path = os.path.join(barcode_dir, f'codebarre_{code}.png')
            
            # Generate barcode if it doesn't exist``
            if not os.path.exists(barcode_path):
                code39 = barcode.get_barcode_class('code39')
                code39_instance = code39(code, writer=ImageWriter(), add_checksum=False)
                code39_instance.save(os.path.join(barcode_dir, f'codebarre_{code}'))
            
            # Check if fields need to be re-entered
            saisir = ""
            if not preselected_individual.json_ext.get('cni') or preselected_individual.json_ext.get('cni') == '-':
                saisir = " - SAISIR DE NOUVEAU"
            
            item = {
                'NOM': preselected_individual.last_name,
                'PRENOM': preselected_individual.first_name,
                'SEXE': preselected_individual.json_ext.get('sexe', ''),
                'CNI': preselected_individual.json_ext.get('cni', ''),
                'PRO': pro,
                'COM': com,
                'COL': niveau3_label,
                'NUM': id,
                'SAISIR': saisir,
                'CODEBARRE': InlineImage(doc, barcode_path, height=Mm(18))
            }
            
            if pro not in data:
                data[pro] = {}
            if com not in data[pro]:
                data[pro][com] = {}
            if niveau3_label not in data[pro][com]:
                data[pro][com][niveau3_label] = []
            data[pro][com][niveau3_label].append(item)

        for pro, items in data.items():
            for com, items in items.items():
                for niveau3_label, items in items.items():
                    lists_dir = os.path.join(racine, 'documents-generes', 'precollecte', 'listes',
                                        pro, com, niveau3_label)
                    os.makedirs(lists_dir, exist_ok=True)
                    # Set up the context for the template
                    date_gen = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    header_context = {
                        'DATE_GEN': date_gen,
                        'PRO': pro,
                        'COM': com,
                        'COL': niveau3_label
                    }
                    # Render the document
                    context = {**header_context, 'data': data[pro][com][niveau3_label]}
                    doc.render(context)
                    # Save the document
                    output_file = os.path.join(lists_dir, f'bi-merankabandi-precollecte-{niveau3_label}.docx')
                    doc.save(output_file)
                    self.stdout.write(self.style.SUCCESS(f'Successfully generated document at {output_file}'))
