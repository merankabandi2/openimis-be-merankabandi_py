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
from datetime import date
import logging
import time
from threading import Lock
from PyPDF2 import PdfMerger
from functools import lru_cache

# Set up logging
logger = logging.getLogger(__name__)

class BeneficiaryCardGenerator:
    def __init__(self):
        self.font_config = FontConfiguration()
        self.css = self._get_card_css()
        self.image_cache = {}  # Cache for image data URLs
        self.image_cache_lock = Lock()  # Lock for thread-safe cache access

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

    @lru_cache(maxsize=100)
    def _get_image_data_url(self, image_path, target_height_cm=5):
        """Convert image to resized data URL for embedding in HTML with caching"""
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
            logger.warning(f"Unable to identify image file at {image_path}. Skipping...")
            return ""  # Return an empty string if the image cannot be identified
        except Exception as e:
            logger.error(f"Error processing image at {image_path}: {str(e)}")
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
        # Get current date for fallback
        current_date = date.today().strftime('%Y-%m-%d')

        logo_path = os.path.join(settings.STATIC_ROOT, 'merankabandi/logo.png')
        context = {
            'logo_url': self._get_image_data_url(logo_path),
            'photo_url': self._get_image_data_url(photo_path),
            'social_id': beneficiary.group.code,
            'individual': individual,
            'telephone': moyen_telecom.get('msisdn', '') if moyen_telecom else '',
            'date_enregistrement': moyen_telecom.get('responseDate', current_date) if moyen_telecom else current_date,
            'province': colline.parent.parent.name,
            'commune': colline.parent.name,
            'colline': colline.name,
        }
        
        return render_to_string('beneficiary_card.html', context)

    def generate_beneficiary_cards_batch(self, beneficiaries_queryset, output_path, batch_size=50):
        """Generate PDFs in batches and merge them"""
        start_time = time.time()
        total_beneficiaries = beneficiaries_queryset.count()
        logger.info(f"Starting generation of {total_beneficiaries} cards with batch size {batch_size}")
        
        # Create temp directory for batch PDFs
        import tempfile
        temp_dir = tempfile.mkdtemp()
        batch_files = []
        
        # Process in batches with a progress bar
        with tqdm(total=total_beneficiaries, desc="Generating Cards", unit="card") as pbar:
            offset = 0
            batch_num = 1
            
            while offset < total_beneficiaries:
                batch_start_time = time.time()
                # Get a batch of beneficiaries
                beneficiaries_batch = list(beneficiaries_queryset[offset:offset+batch_size])
                
                if not beneficiaries_batch:
                    break
                    
                # Generate batch HTML
                all_cards_html = []
                for beneficiary in beneficiaries_batch:
                    all_cards_html.append(self.generate_card_html(beneficiary))
                
                combined_html = '\n'.join(all_cards_html)
                
                # Generate batch PDF
                batch_file = os.path.join(temp_dir, f"batch_{batch_num}.pdf")
                html_doc = HTML(string=combined_html)
                pdf = html_doc.write_pdf(
                    stylesheets=[self.css],
                    font_config=self.font_config
                )
                
                with open(batch_file, 'wb') as pdf_file:
                    pdf_file.write(pdf)
                
                batch_files.append(batch_file)
                
                # Update progress and stats
                batch_size_actual = len(beneficiaries_batch)
                offset += batch_size_actual
                pbar.update(batch_size_actual)
                batch_time = time.time() - batch_start_time
                logger.info(f"Batch {batch_num} ({batch_size_actual} cards) completed in {batch_time:.2f} seconds")
                batch_num += 1
        
        # Merge all batch PDFs
        if batch_files:
            merger = PdfMerger()
            for pdf_file in batch_files:
                merger.append(pdf_file)
            
            merger.write(output_path)
            merger.close()
            
            # Clean up temp files
            for file in batch_files:
                try:
                    os.remove(file)
                except:
                    pass
            try:
                os.rmdir(temp_dir)
            except:
                pass
        
        total_time = time.time() - start_time
        logger.info(f"Total card generation time: {total_time:.2f} seconds for {total_beneficiaries} cards")

    def generate_beneficiary_cards(self, beneficiaries, output_path):
        """Generate PDF with front and back cards for multiple beneficiaries"""
        # For backward compatibility, convert iterator to queryset if needed
        if hasattr(beneficiaries, 'all'):
            # It's likely a queryset already
            return self.generate_beneficiary_cards_batch(beneficiaries, output_path)
        else:
            # Convert to list for the old method
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
        parser.add_argument('--province', type=str, help='Name of the province to generate cards for')
        parser.add_argument('--commune', type=str, help='Name of the commune to generate cards for')
        parser.add_argument('--colline', type=str, help='Name of the colline to generate cards for')
        parser.add_argument('--output', type=str, default='cards.pdf', help='Output PDF file name')
        parser.add_argument('--batch-size', type=int, default=50, 
                          help='Number of cards to process in each batch (default: 50)')
        parser.add_argument('--limit', type=int, help='Limit the number of cards to generate (for testing)')

    def handle(self, *args, **options):
        province_name = options.get('province')
        commune_name = options.get('commune')
        colline_name = options.get('colline')
        output_file = options['output']
        batch_size = options['batch_size']
        limit = options.get('limit')
        
        # Check that at least one location parameter is provided
        if not any([province_name, commune_name, colline_name]):
            raise CommandError("Please provide either a province, commune, or colline name.")
        
        # Determine the filter and output filename
        filter_params = {}
        location_type = ""
        location_name = ""
        
        if province_name:
            filter_params = {
                'group__location__parent__parent__name': province_name,
            }
            location_type = "province"
            location_name = province_name
        elif commune_name:
            filter_params = {
                'group__location__parent__name': commune_name,
            }
            location_type = "commune"
            location_name = commune_name
        elif colline_name:
            filter_params = {
                'group__location__name': colline_name,
            }
            location_type = "colline"
            location_name = colline_name
        
        # Set default output filename if not explicitly provided
        if output_file == 'cards.pdf':
            output_file = f'{location_type}_{location_name}_cards.pdf'

        try:
            # Add filter for beneficiaries with phone numbers
            filter_params['json_ext__moyen_telecom__status'] = 'SUCCESS'
            
            # Use select_related to optimize database queries
            beneficiaries = Beneficiary.objects.filter(**filter_params).select_related(
                'group', 
                'group__location', 
                'group__location__parent', 
                'group__location__parent__parent'
            )
            
            # Apply limit for testing if specified
            if limit:
                beneficiaries = beneficiaries[:limit]
                
            if not beneficiaries.exists():
                raise CommandError(f"No beneficiaries found for {location_type}: {location_name}")

            generator = BeneficiaryCardGenerator()
            total_beneficiaries = beneficiaries.count()

            self.stdout.write(f"Generating cards for {total_beneficiaries} beneficiaries in {location_type} {location_name}")
            self.stdout.write(f"Using batch size of {batch_size} cards")
            
            # Generate cards with batch processing
            generator.generate_beneficiary_cards_batch(
                beneficiaries,
                output_path=output_file,
                batch_size=batch_size
            )

            self.stdout.write(self.style.SUCCESS(f"Cards generated successfully and saved to {output_file}"))

        except Exception as e:
            logger.exception("Error in card generation")
            raise CommandError(f"Error generating cards: {str(e)}")