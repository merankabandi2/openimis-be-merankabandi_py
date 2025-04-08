import csv
import datetime
import logging
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from individual.models import GroupIndividual, Individual
from social_protection.models import GroupBeneficiary

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Import phone number attribution data from a CSV file'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the CSV file')
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Validate the file without making changes to the database',
        )
        parser.add_argument(
            '--skip-errors',
            action='store_true',
            help='Continue processing even if some rows have errors',
        )

    def find_beneficiary(self, cni):
        """Find a beneficiary using CNI number"""
        try:
            # Query beneficiaries by joining through related models
            beneficiary = GroupBeneficiary.objects.raw("""
                SELECT gb.* FROM social_protection_groupbeneficiary gb
                JOIN individual_group g ON gb.group_id = g."UUID"
                JOIN individual_groupindividual gi ON gi.group_id = g."UUID"
                JOIN individual_individual i ON gi.individual_id = i."UUID"
                WHERE REGEXP_REPLACE(i."Json_ext"->>'ci', E'[\\n\\r\\s]', '', 'g') = REGEXP_REPLACE(%s, E'[\\n\\r\\s]', '', 'g')
                AND gi.recipient_type = 'PRIMARY'
                LIMIT 1
            """, [cni])
            
            # raw() returns an iterator, so get the first item if it exists
            return next(iter(beneficiary), None)
        except Exception as e:
            logger.error(f"Error finding beneficiary with CNI {cni}: {str(e)}")
            return None

    def parse_date(self, date_str):
        """Parse date from various formats"""
        try:
            # Try different date formats
            for fmt in ['%d/%m/%y', '%m/%d/%y', '%d/%m/%Y', '%m/%d/%Y']:
                try:
                    return datetime.datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            
            # If all formats fail, raise error
            raise ValueError(f"Unknown date format: {date_str}")
        except Exception as e:
            logger.error(f"Error parsing date {date_str}: {str(e)}")
            return None
    
    def parse_status(self, status_str):
        """Parse status string from telecom response"""
        if status_str.startswith('SUCC'):
            return 'SUCCESS', '', ''
        else:
            # Extract error code and message
            parts = status_str.split('|', 1)
            error_code = parts[0] if len(parts) > 0 else 'ERROR'
            error_message = parts[1] if len(parts) > 1 else status_str
            return 'REJECTED', error_code, error_message

    def process_row(self, row, dry_run=False):
        """Process a single row from the CSV file"""
        try:
            cni = row['cni'].strip()
            msisdn = row['MSISDN'].strip()
            status_str = row['CVBS_Response'].strip()
            
            # Find the beneficiary
            beneficiary = self.find_beneficiary(cni)
            if not beneficiary:
                return False, f"Beneficiary with CNI {cni} not found or not in valid state"
            
            # Parse status
            status, error_code, error_message = self.parse_status(status_str)
            
            if dry_run:
                return True, f"Would update beneficiary with CNI {cni} (dry run)"
            
            # Update phone number data in json_ext
            with transaction.atomic():
                json_ext = beneficiary.json_ext or {}
                
                # Ensure nested structure exists
                if 'moyen_telecom' not in json_ext or json_ext['moyen_telecom'] is None:
                    json_ext['moyen_telecom'] = {}
                    
                # Update phone number info
                json_ext['moyen_telecom']['msisdn'] = msisdn
                json_ext['moyen_telecom']['status'] = status
                json_ext['moyen_telecom']['iccid'] = row.get('ICCID', '')
                
                if status == 'REJECTED':
                    json_ext['moyen_telecom']['error_code'] = error_code
                    json_ext['moyen_telecom']['error_message'] = error_message
                
                beneficiary.json_ext = json_ext
                
                beneficiary.save(username='Admin')
                
            return True, f"Successfully updated beneficiary with CNI {cni}"
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return False, str(e)

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        dry_run = options['dry_run']
        skip_errors = options['skip_errors']
        
        try:
            with open(csv_file, 'r', newline='', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                total_rows = 0
                successful_rows = 0
                error_rows = 0
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header
                    total_rows += 1
                    success, message = self.process_row(row, dry_run)
                    
                    if success:
                        successful_rows += 1
                        self.stdout.write(self.style.SUCCESS(f"Row {row_num}: {message}"))
                    else:
                        error_rows += 1
                        self.stdout.write(self.style.ERROR(f"Row {row_num}: {message}"))
                        if not skip_errors:
                            raise CommandError(f"Error processing row {row_num}: {message}")
                
                # Print summary
                if dry_run:
                    self.stdout.write(self.style.WARNING(
                        f"DRY RUN: Would process {total_rows} rows "
                        f"({successful_rows} successful, {error_rows} with errors)"
                    ))
                else:
                    self.stdout.write(self.style.SUCCESS(
                        f"Processed {total_rows} rows "
                        f"({successful_rows} successful, {error_rows} with errors)"
                    ))
                    
        except FileNotFoundError:
            raise CommandError(f"File not found: {csv_file}")
        except Exception as e:
            raise CommandError(f"Error processing file: {str(e)}")