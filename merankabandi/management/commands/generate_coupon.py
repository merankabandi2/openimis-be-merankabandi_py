import csv
import os
import datetime
import logging
from django.core.management.base import BaseCommand, CommandError
from docxtpl import DocxTemplate,InlineImage
from docx.shared import Mm
import barcode
from barcode.writer import ImageWriter
from django.conf import settings


logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Generate pre-collection documents for selected individuals'
    colline_map = {
        '90202': 'BURARA',
        '120504': 'CAGAKORI',
        '90503': 'CERU',
        '90204': 'GATARE',
        '90206': 'GATETE',
        '90207': 'GISENYI',
        '120507': 'GITERANYI',
        '90102': 'GITWE',
        '90213': 'KIBONDE',
        '90103': 'KIGINA',
        '90215': 'KIGOMA',
        '120514': 'KIJUMBURA',
        '90217': 'KIVO',
        '90106': 'KIYONZA',
        '120518': 'MASAKA',
        '90222': 'MUNAZI',
        '120523': 'MURAMA',
        '120525': 'NGOMO',
        '90228': 'NYABISINDU',
        '90108': 'NYAKARAMA',
        '90231': 'NYAKIZU',
        '90110': 'RUBUGA',
        '90233': 'RUGARAMA',
        '90112': 'RUHEHE',
        '120531': 'RUKUSHA',
        '90527': 'RUNYONZA',
        '90240': 'RWIBIKARA'
    }

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the CSV file')

    def handle(self, *args, **options):
        inverted_colline_map = {value: key for key, value in self.colline_map.items()}
        csv_file = options['csv_file']

        racine = settings.DOCUMENTS_DIR

        csv_file = options['csv_file']

        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            result_menages = csv.DictReader(file)

            # Load the template
            template_path = os.path.join(racine, 'templates', 'merankabandi-template-precollecte.docx')
            doc = DocxTemplate(template_path)
    
            # Prepare data for document
            data = {}
            for num, row_menages in enumerate(result_menages, start=2):  # Start at 2 to account for header
                pro = row_menages.get('Province', '')
                com = row_menages.get('Commune', '')
                niveau3_label = row_menages.get('Colline', '')
                # Create directories for output files
                barcode_dir = os.path.join(racine, 'documents-generes', 'precollecte', 'codebarres', 
                                        pro, com, niveau3_label)
                os.makedirs(barcode_dir, exist_ok=True)

                nombre_formate = f"{int(row_menages['_index']):07d}"
                code = f"{inverted_colline_map[row_menages['Colline']].zfill(6)}{nombre_formate}"
                barcode_path = os.path.join(barcode_dir, f'codebarre_{code}.png')
                
                # Generate barcode if it doesn't exist``
                if not os.path.exists(barcode_path):
                    code39 = barcode.get_barcode_class('code39')
                    code39_instance = code39(code, writer=ImageWriter(), add_checksum=False)
                    code39_instance.save(os.path.join(barcode_dir, f'codebarre_{code}'))
                
                # Check if fields need to be re-entered
                saisir = ""
                if not row_menages.get('cni') or row_menages.get('cni') == '-':
                    saisir = " - SAISIR DE NOUVEAU"
                
                item = {
                    'NOM1': row_menages.get('Nom du répondant', ''),
                    'PRENOM1': row_menages.get('Prénom du répondant', ''),
                    'SEXE1': row_menages.get('Sexe du répondant', ''),
                    'CNI1': row_menages.get('cni', ''),
                    'PRO1': pro,
                    'COM1': com,
                    'COL1': niveau3_label,
                    'NUM': num,
                    'SAISIR': saisir,
                    'CODEBARRE1': InlineImage(doc, barcode_path, height=Mm(18))
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
