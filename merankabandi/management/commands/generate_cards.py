from io import BytesIO
from django.core.management.base import BaseCommand, CommandError
from social_protection.models import GroupBeneficiary as Beneficiary
from individual.models import GroupIndividual
from pathlib import Path
import base64
from PIL import Image, UnidentifiedImageError  # Import Pillow for image processing
import os
from tqdm import tqdm  # For progress bar
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from django.template.loader import render_to_string
from django.conf import settings

class BeneficiaryCardGenerator:
    def __init__(self):
        self.font_config = FontConfiguration()
        self.css = self._get_card_css()

    def _get_card_css(self):
        """Define CSS for card styling"""
        return CSS(string='''
            @page {
                size: A4;
                margin: 1cm;
            }
            
            .card {
                height: 13cm;
                width: 18.5cm;
                margin: 0 0 0 0;
                page-break-inside: avoid;
                position: relative;
                font-family: Arial, sans-serif;
            }
            
            .card-header {
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                font-size: 12pt;
                font-weight: bold;
                margin-bottom: 0;
            }
            .header-text {
                flex-grow: 1;
                text-align: center;
            }
            .social-id-container {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 0.5cm;
            }
            .social-id {
                flex-grow: 1;
                font-weight: bold;
                text-align: center;
                font-size: 16pt;
                margin-top: 0.5cm
            }
            
            .logo {
                height: 2cm;
                object-fit: contain;
            }
            .photo {
                height: 3cm;
                object-fit: contain;
            }

            .field {
                margin-top: 0.3cm;
                margin-bottom: 0.3cm;
                display: flex;
                align-items: center;
            }
            
            .field-label {
                display: inline-block;
                font-size: 10pt;
                width: 6cm;
            }
            
            .field-value {
                font-weight: bold;
                font-size: 12pt;
            }
            
            .declaration {
                font-size: 10pt;
                margin-top: 0.6cm;
            }
    
            .card2 {
                margin-top: 1.3cm;
            }
        ''')

    def _get_image_data_url(self, image_path, target_height_cm=5):
        """Convert image to resized data URL for embedding in HTML"""
        if not image_path or not Path(image_path).exists():
            return ""  # Return an empty string if the file doesn't exist

        try:
            # Define DPI for conversion from cm to pixels
            dpi = 96  # Standard DPI for screen resolution
            target_height_px = int(target_height_cm * dpi / 2.54)  # Convert cm to pixels

            # Open and resize the image
            with Image.open(image_path) as img:
                # Check if the image is valid
                if img.format not in ['JPEG', 'PNG', 'GIF', 'BMP']:
                    raise ValueError(f"Unsupported image format: {img.format}")

                width, height = img.size
                aspect_ratio = width / height
                target_width_px = int(target_height_px * aspect_ratio)

                # Resize the image while maintaining aspect ratio using LANCZOS
                resized_img = img.resize((target_width_px, target_height_px), Image.LANCZOS)

                # Save the resized image to a temporary buffer
                with BytesIO() as buffer:
                    resized_img.save(buffer, format=img.format)
                    image_data = buffer.getvalue()

            # Convert the resized image to base64
            file_ext = os.path.splitext(image_path)[1].lower()
            mime_type = {
                '.png': 'image/png',
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.gif': 'image/gif',
                '.bmp': 'image/bmp'
            }.get(file_ext, 'application/octet-stream')

            return f'data:{mime_type};base64,{base64.b64encode(image_data).decode("utf-8")}'

        except UnidentifiedImageError:
            print(f"Error: Unable to identify image file at {image_path}. Skipping...")
            return ""  # Return an empty string if the image cannot be identified
        except Exception as e:
            print(f"Error processing image at {image_path}: {str(e)}")
            return ""  # Return an empty string for any other exception

    def generate_card_html(self, beneficiary):
        """Generate HTML for beneficiary card"""
        individual = beneficiary.group.groupindividuals.get(recipient_type=GroupIndividual.RecipientType.PRIMARY).individual
        household = individual.groupindividuals.get().group
        base_dir = os.path.join(settings.PHOTOS_BASE_PATH, str(household.json_ext.get('deviceid', '')), str(household.json_ext.get('date_collecte', '')).replace('-', ''))
        clean_path = f"photo_repondant_{str(individual.json_ext.get('social_id', ''))}.jpg"
        photo_path = os.path.join(base_dir, clean_path)
        moyen_telecom = beneficiary.json_ext.get('moyen_telecom', '')
        colline = beneficiary.group.location
        
        logo_path = os.path.join(settings.STATIC_ROOT, 'merankabandi/logo.png')
        context = {
            'logo_url': self._get_image_data_url(logo_path),
            'photo_url': self._get_image_data_url(photo_path),
            'social_id': beneficiary.group.code,
            'individual': individual,
            'telephone': moyen_telecom.get('msisdn', '') if moyen_telecom else '',
            'date_enregistrement': moyen_telecom.get('responseDate', '') if moyen_telecom else '',
            'province': colline.parent.parent.name,
            'commune': colline.parent.name,
            'colline': colline.name,
        }
        
        return render_to_string('beneficiary_card.html', context)

    def generate_beneficiary_cards(self, beneficiaries, output_path):
        """Generate PDF with front and back cards for multiple beneficiaries"""
        all_cards_html = []
        
        for beneficiary in beneficiaries:
            all_cards_html.append(self.generate_card_html(beneficiary))
        
        combined_html = '\n'.join(all_cards_html)
        
        html_doc = HTML(string=combined_html)
        pdf = html_doc.write_pdf(
            stylesheets=[self.css],
            font_config=self.font_config
        )
        
        with open(output_path, 'wb') as pdf_file:
            pdf_file.write(pdf)


class Command(BaseCommand):
    help = 'Generate beneficiary cards as PDF files'

    def add_arguments(self, parser):
        parser.add_argument('--commune', type=str, help='Name of the commune to generate cards for')
        parser.add_argument('--output', type=str, default='cards.pdf', help='Output PDF file name')

    def handle(self, *args, **options):
        commune_name = options['commune']
        output_file = options['output']
        output_file = f'commune_{commune_name}_cards.pdf'

        if not commune_name:
            raise CommandError("Please provide a commune name using --commune.")

        try:
            beneficiaries = Beneficiary.objects.filter(
                group__location__parent__name=commune_name,
                json_ext__moyen_telecom__status='SUCCESS'
            )
            if not beneficiaries.exists():
                raise CommandError(f"No beneficiaries found for commune: {commune_name}")

            generator = BeneficiaryCardGenerator()
            total_beneficiaries = beneficiaries.count()

            # Use tqdm for progress bar
            with tqdm(total=total_beneficiaries, desc="Generating Cards", unit="card") as pbar:
                generator.generate_beneficiary_cards(
                    beneficiaries.iterator(),
                    output_path=output_file
                )
                pbar.update(total_beneficiaries)

            self.stdout.write(self.style.SUCCESS(f"Cards generated successfully and saved to {output_file}"))

        except Exception as e:
            raise CommandError(f"Error generating cards: {str(e)}")