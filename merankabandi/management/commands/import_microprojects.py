import csv
import re
import uuid
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from merankabandi.models import MicroProject, OtherProjectType
from location.models import Location


class Command(BaseCommand):
    help = 'Import microprojects from CSV file'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the CSV file')
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Run without saving to database',
        )

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        dry_run = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No data will be saved'))

        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
            # Skip first line (empty columns), second line has headers
            csv_data = ''.join(lines[1:])
            reader = csv.DictReader(csv_data.splitlines())
            
            stats = {
                'total': 0,
                'created': 0,
                'skipped': 0,
                'errors': 0,
                'location_not_found': []
            }

            with transaction.atomic():
                for row_num, row in enumerate(reader, start=2):
                    stats['total'] += 1
                    
                    try:
                        result = self.process_row(row, row_num)
                        
                        if result['status'] == 'success':
                            stats['created'] += 1
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"Row {row_num}: Created microproject for "
                                    f"{result['commune']} - {result['colline'] or 'N/A'}"
                                )
                            )
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
            self.stdout.write('\n' + '='*50)
            self.stdout.write(self.style.SUCCESS('IMPORT SUMMARY'))
            self.stdout.write('='*50)
            self.stdout.write(f"Total rows processed: {stats['total']}")
            self.stdout.write(self.style.SUCCESS(f"Successfully created: {stats['created']}"))
            self.stdout.write(self.style.WARNING(f"Skipped: {stats['skipped']}"))
            self.stdout.write(self.style.ERROR(f"Errors: {stats['errors']}"))
            
            if stats['location_not_found']:
                self.stdout.write('\n' + self.style.WARNING('Locations not found:'))
                for loc in set(stats['location_not_found']):
                    self.stdout.write(f"  - {loc}")

        except FileNotFoundError:
            raise CommandError(f'File "{csv_file}" not found')
        except Exception as e:
            if not (dry_run and 'Dry run' in str(e)):
                raise CommandError(f'Error processing file: {str(e)}')
            else:
                self.stdout.write(self.style.SUCCESS('\nDry run completed successfully'))

    def process_row(self, row, row_num):
        """Process a single CSV row and create MicroProject"""
        
        # Parse date (MM/DD/YY format)
        date_str = row.get('Date', '').strip()
        if not date_str:
            return {'status': 'error', 'reason': 'Missing date'}
        
        try:
            report_date = datetime.strptime(date_str, '%m/%d/%y').date()
        except ValueError:
            return {'status': 'error', 'reason': f'Invalid date format: {date_str}'}
        
        # Get location data
        commune = row.get('Communes', '').strip()
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
        male_participants = self.parse_int(row.get('H', 0))
        female_participants = self.parse_int(row.get('F', 0))
        twa_participants = self.parse_int(row.get('Twa', 0))
        
        # Parse beneficiary counts
        agriculture_beneficiaries = self.parse_int(row.get('Agriculture', 0))
        livestock_beneficiaries = self.parse_int(row.get('Elevage', 0))
        commerce_services_beneficiaries = self.parse_int(row.get('Commerce et services', 0))
        autres_count = self.parse_int(row.get('Autres (à préciser)', 0))
        
        # Create MicroProject
        microproject = MicroProject(
            id=uuid.uuid4(),
            report_date=report_date,
            location=location,
            male_participants=male_participants,
            female_participants=female_participants,
            twa_participants=twa_participants,
            agriculture_beneficiaries=agriculture_beneficiaries,
            livestock_beneficiaries=livestock_beneficiaries,
            livestock_goat_beneficiaries=0,
            livestock_pig_beneficiaries=0,
            livestock_rabbit_beneficiaries=0,
            livestock_poultry_beneficiaries=0,
            livestock_cattle_beneficiaries=0,
            commerce_services_beneficiaries=commerce_services_beneficiaries,
            validation_status='PENDING'
        )
        microproject.save()
        
        # Process "other projects" if specified
        other_projects_str = row.get('Préciser les autres types de microprojets', '').strip()
        if other_projects_str:
            self.create_other_project_types(microproject, other_projects_str)
        
        return {
            'status': 'success',
            'commune': commune,
            'colline': colline
        }
    
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
    
    def create_other_project_types(self, microproject, other_projects_str):
        """
        Parse and create OtherProjectType entries
        
        Examples:
        - "Atelier de couture (2)" -> name="Atelier de couture", count=2
        - "Apiculture" -> name="Apiculture", count=0
        - "Menuiserie (2)" -> name="Menuiserie", count=2
        """
        # Pattern to match "Name (count)" or just "Name"
        pattern = r'^(.+?)\s*(?:\((\d+)\))?\s*$'
        match = re.match(pattern, other_projects_str)
        
        if match:
            project_name = match.group(1).strip()
            count_str = match.group(2)
            count = int(count_str) if count_str else 0
            
            OtherProjectType.objects.create(
                microproject=microproject,
                name=project_name,
                beneficiary_count=count
            )
    
    def parse_int(self, value):
        """Safely parse integer value"""
        if value is None or value == '':
            return 0
        try:
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            return 0