import csv
import datetime
import logging
import re
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
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Number of rows to process in a batch (default: 100)',
        )

    def extract_group_code_from_photo_url(self, photo_url):
        """Extract individual_group code from photo URL"""
        try:
            if not photo_url or not isinstance(photo_url, str):
                return None
                
            # Extract code from URL that matches pattern
            # .../photo_repondant_INDIVIDUAL_GROUP_CODE.jpg
            pattern = r'photo_repondant_([^.]+)\.jpg$'
            match = re.search(pattern, photo_url)
            
            if match:
                return match.group(1)
            return None
        except Exception as e:
            logger.error(f"Error extracting group code from photo URL {photo_url}: {str(e)}")
            return None

    def find_beneficiary(self, cni, photo_url=None):
        """Find a beneficiary using CNI number or group code extracted from photo URL"""
        try:
            # using the group code from photo URL
            if photo_url:
                group_code = self.extract_group_code_from_photo_url(photo_url)
                if group_code:
                    logger.info(f"Trying to find beneficiary using group code {group_code} from photo URL")
                    beneficiary = GroupBeneficiary.objects.raw("""
                        SELECT gb.* FROM social_protection_groupbeneficiary gb
                        JOIN individual_group g ON gb.group_id = g."UUID"
                        WHERE g.code = %s
                        LIMIT 1
                    """, [group_code])
                    
                    return next(iter(beneficiary), None)
            
            # If CNI is provided, try to find beneficiary by CNI
            if cni:
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
                result = next(iter(beneficiary), None)
                if result:
                    return result
            
            return None
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

    def process_row(self, row, dry_run=False, beneficiary_cache=None):
        """Process a single row from the CSV file"""
        try:
            cni = row.get('cni', '').strip()
            msisdn = row.get('MSISDN', '').strip()
            status_str = row.get('CVBS_Response', '').strip()
            photo_url = row.get('photo', '').strip()
            
            if not msisdn or not status_str:
                return False, f"Missing required fields in row: {row}"

            # Use cache to avoid redundant database lookups
            cache_key = f"cni:{cni}|photo:{photo_url}"
            if beneficiary_cache is not None and cache_key in beneficiary_cache:
                beneficiary = beneficiary_cache[cache_key]
            else:
                # Find the beneficiary using CNI or photo URL
                beneficiary = self.find_beneficiary(cni, photo_url)
                # Store in cache for future lookups
                if beneficiary_cache is not None:
                    beneficiary_cache[cache_key] = beneficiary

            if not beneficiary:
                lookup_info = f"CNI {cni}" if cni else f"photo URL {photo_url}"
                return False, f"Beneficiary with {lookup_info} not found or not in valid state"
            
            # Parse status
            status, error_code, error_message = self.parse_status(status_str)
            
            if dry_run:
                return True, f"Would update beneficiary with ID {beneficiary.id} (dry run)"
            
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
                
            return True, f"Successfully updated beneficiary with ID {beneficiary.id}"
            
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return False, str(e)

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        dry_run = options['dry_run']
        skip_errors = options['skip_errors']
        batch_size = options['batch_size']
        
        try:
            # First, count total rows for progress reporting
            total_rows = 0
            with open(csv_file, 'r', newline='', encoding='utf-8-sig') as file:
                total_rows = sum(1 for _ in csv.DictReader(file))
            
            self.stdout.write(f"Found {total_rows} rows to process")
            
            # Process rows in batches
            successful_rows = 0
            error_rows = 0
            # Cache to store beneficiary lookup results
            beneficiary_cache = {}
            
            with open(csv_file, 'r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                batch = []
                batch_count = 0
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header
                    batch.append((row_num, row))
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        batch_count += 1
                        self.stdout.write(f"Processing batch {batch_count} ({len(batch)} rows)")
                        
                        for b_row_num, b_row in batch:
                            success, message = self.process_row(b_row, dry_run, beneficiary_cache)
                            
                            if success:
                                successful_rows += 1
                                if successful_rows % 100 == 0 or successful_rows == total_rows:
                                    self.stdout.write(self.style.SUCCESS(
                                        f"Progress: {successful_rows + error_rows}/{total_rows} rows processed "
                                        f"({successful_rows} successful, {error_rows} errors)"
                                    ))
                            else:
                                error_rows += 1
                                self.stdout.write(self.style.ERROR(f"Row {b_row_num}: {message}"))
                                if not skip_errors:
                                    raise CommandError(f"Error processing row {b_row_num}: {message}")
                        
                        # Clear batch
                        batch = []
                
                # Process remaining rows in the last batch
                if batch:
                    self.stdout.write(f"Processing final batch ({len(batch)} rows)")
                    for b_row_num, b_row in batch:
                        success, message = self.process_row(b_row, dry_run, beneficiary_cache)
                        
                        if success:
                            successful_rows += 1
                        else:
                            error_rows += 1
                            self.stdout.write(self.style.ERROR(f"Row {b_row_num}: {message}"))
                            if not skip_errors:
                                raise CommandError(f"Error processing row {b_row_num}: {message}")
                
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