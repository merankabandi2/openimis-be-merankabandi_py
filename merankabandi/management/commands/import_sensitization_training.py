import csv
import uuid
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from merankabandi.models import SensitizationTraining
from location.models import Location


class Command(BaseCommand):
    help = 'Import sensitization/training data from Formation CSV file'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the Formation CSV file')
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Run without saving to database',
        )
        parser.add_argument(
            '--default-category',
            type=str,
            default=None,
            help='Default category if theme not in predefined choices (e.g., module_mip__mesures_d_inclusio)',
        )

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        dry_run = options['dry_run']
        default_category = options['default_category']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No data will be saved'))

        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                stats = {
                    'total': 0,
                    'created': 0,
                    'skipped': 0,
                    'errors': 0,
                    'location_not_found': [],
                    'unmapped_themes': set()
                }

                with transaction.atomic():
                    for row_num, row in enumerate(reader, start=2):
                        stats['total'] += 1
                        
                        try:
                            result = self.process_row(row, row_num, default_category)
                            
                            if result['status'] == 'success':
                                stats['created'] += 1
                                self.stdout.write(
                                    self.style.SUCCESS(
                                        f"Row {row_num}: Created training record for "
                                        f"{result['commune']} - {result['colline']}"
                                    )
                                )
                                if result.get('unmapped_theme'):
                                    stats['unmapped_themes'].add(result['unmapped_theme'])
                                    
                            elif result['status'] == 'location_not_found':
                                stats['skipped'] += 1
                                stats['location_not_found'].append(result['location_info'])
                                self.stdout.write(
                                    self.style.WARNING(
                                        f"Row {row_num}: Location not found - "
                                        f"{result['location_info']}"
                                    )
                                )
                            else:
                                stats['skipped'] += 1
                                self.stdout.write(
                                    self.style.WARNING(
                                        f"Row {row_num}: Skipped - {result.get('reason', 'Unknown')}"
                                    )
                                )
                                
                        except Exception as e:
                            stats['errors'] += 1
                            self.stdout.write(
                                self.style.ERROR(f"Row {row_num}: Error - {str(e)}")
                            )
                    
                    if dry_run:
                        raise CommandError("Dry run complete - rolling back transaction")

                # Print summary
                self.stdout.write('\n' + '='*60)
                self.stdout.write(self.style.SUCCESS('IMPORT SUMMARY'))
                self.stdout.write('='*60)
                self.stdout.write(f"Total rows processed: {stats['total']}")
                self.stdout.write(self.style.SUCCESS(f"Successfully created: {stats['created']}"))
                self.stdout.write(self.style.WARNING(f"Skipped: {stats['skipped']}"))
                self.stdout.write(self.style.ERROR(f"Errors: {stats['errors']}"))
                
                if stats['location_not_found']:
                    self.stdout.write('\n' + self.style.WARNING('Locations not found:'))
                    for loc in set(stats['location_not_found']):
                        self.stdout.write(f"  - {loc}")
                
                if stats['unmapped_themes']:
                    self.stdout.write('\n' + self.style.WARNING('Themes stored in observations (not in predefined categories):'))
                    for theme in stats['unmapped_themes']:
                        self.stdout.write(f"  - {theme}")
                    self.stdout.write('\nNote: Use --default-category option to assign a category, or themes will be stored in observations field')

        except FileNotFoundError:
            raise CommandError(f'File "{csv_file}" not found')
        except Exception as e:
            if not (dry_run and 'Dry run' in str(e)):
                raise CommandError(f'Error processing file: {str(e)}')
            else:
                self.stdout.write(self.style.SUCCESS('\nDry run completed successfully'))

    def process_row(self, row, row_num, default_category=None):
        """Process a single CSV row and create SensitizationTraining"""
        
        # Parse sensitization date (Date de la sensibilisation/Formation)
        date_str = row.get('Date de la sensibilisation/Formation', '').strip()
        if not date_str:
            return {'status': 'error', 'reason': 'Missing date'}
        
        try:
            # Try MM/DD/YY format (like 9/30/24)
            sensitization_date = datetime.strptime(date_str, '%m/%d/%y').date()
        except ValueError:
            try:
                # Try other common formats
                sensitization_date = datetime.strptime(date_str, '%m/%d/%Y').date()
            except ValueError:
                return {'status': 'error', 'reason': f'Invalid date format: {date_str}'}
        
        # Get location data
        commune = row.get('Commune', '').strip()
        colline = row.get('Colline', '').strip()
        
        if not commune:
            return {'status': 'error', 'reason': 'Missing commune'}
        
        # Find location by commune and colline
        location = self.find_location(commune, colline)
        
        if not location:
            location_info = f"Commune: {commune}, Colline: {colline or 'N/A'}"
            return {
                'status': 'location_not_found',
                'location_info': location_info
            }
        
        # Parse participant counts
        male_participants = self.parse_int(row.get('Homme', 0))
        female_participants = self.parse_int(row.get('Femme', 0))
        twa_participants = self.parse_int(row.get('Twa', 0))
        
        # Get theme from CSV
        theme_from_csv = row.get('Thème', '').strip()
        
        # Map theme to category (if it matches predefined categories)
        category = None
        unmapped_theme = None
        
        # Check if theme matches any predefined category
        theme_mapping = {
            'module mip': 'module_mip__mesures_d_inclusio',
            'mesures d\'inclusion productive': 'module_mip__mesures_d_inclusio',
            'mip': 'module_mip__mesures_d_inclusio',
            'module mach': 'module_mach__mesures_d_accompa',
            'capital humain': 'module_mach__mesures_d_accompa',
            'mach': 'module_mach__mesures_d_accompa',
        }
        
        theme_lower = theme_from_csv.lower()
        for key, value in theme_mapping.items():
            if key in theme_lower:
                category = value
                break
        
        # If no match found, use default_category or store in observations
        if not category:
            category = default_category
            unmapped_theme = theme_from_csv
        
        # Get facilitator and observations
        facilitator = row.get('Animateur', '').strip() or None
        observations = row.get('Observation', '').strip() or None
        
        # If theme wasn't mapped and we have it, prepend to observations
        if unmapped_theme and observations:
            observations = f"Thème: {unmapped_theme}\n\n{observations}"
        elif unmapped_theme:
            observations = f"Thème: {unmapped_theme}"
        
        # Create SensitizationTraining record
        training = SensitizationTraining(
            id=uuid.uuid4(),
            sensitization_date=sensitization_date,
            location=location,
            category=category,
            modules=None,  # CSV doesn't have module details
            facilitator=facilitator,
            male_participants=male_participants,
            female_participants=female_participants,
            twa_participants=twa_participants,
            observations=observations,
            validation_status='PENDING'
        )
        
        training.save()
        
        result = {
            'status': 'success',
            'commune': commune,
            'colline': colline
        }
        
        if unmapped_theme:
            result['unmapped_theme'] = unmapped_theme
        
        return result
    
    def find_location(self, commune, colline):
        """
        Find location by commune and colline names
        """
        if colline:
            # Try to find by both commune and colline
            location = Location.objects.filter(
                name__iexact=colline,
                parent__name__iexact=commune
            ).first()
            
            if location:
                return location
            
            # Try finding colline without commune filter
            location = Location.objects.filter(name__iexact=colline).first()
            if location:
                return location
        
        # If no colline or not found, try to find by commune name only
        location = Location.objects.filter(name__iexact=commune).first()
        return location
    
    def parse_int(self, value):
        """Safely parse integer value"""
        if value is None or value == '':
            return 0
        try:
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            return 0