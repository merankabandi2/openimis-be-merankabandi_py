import json
import logging
import pandas as pd
import time
from contextlib import contextmanager
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction, connection, reset_queries
from django.utils import timezone
from django.db.models import Q

from core.models import User
from individual.models import Group
from social_protection.models import BenefitPlan, GroupBeneficiary

logger = logging.getLogger(__name__)


@contextmanager
def timer(name, stdout_writer=None):
    """Context manager to time code blocks and track database queries."""
    start_time = time.time()
    start_queries = len(connection.queries) if connection.queries_logged else 0

    try:
        yield
    finally:
        end_time = time.time()
        end_queries = len(connection.queries) if connection.queries_logged else 0
        elapsed = end_time - start_time
        query_count = end_queries - start_queries

        message = f"[TIMING] {name}: {elapsed:.3f}s"
        if connection.queries_logged:
            message += f" ({query_count} queries)"

        if stdout_writer:
            stdout_writer(message)
        logger.info(message)


class Command(BaseCommand):
    help = 'Import households from a CSV file and register them as beneficiaries to a benefit plan'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Path to the CSV file')
        parser.add_argument(
            'benefit_plan_id',
            type=str,
            help='UUID of the benefit plan to register households to'
        )
        parser.add_argument(
            '--column',
            type=str,
            default='socialid',
            help='Column name containing household codes (default: "socialid")'
        )
        parser.add_argument(
            '--status',
            type=str,
            default='POTENTIAL',
            choices=['ACTIVE', 'POTENTIAL', 'SUSPENDED', 'VALIDATED'],
            help='Status for the beneficiaries (default: POTENTIAL)'
        )
        parser.add_argument(
            '--user',
            type=str,
            default='Admin',
            help='Username to attribute the changes to (default: Admin)'
        )
        parser.add_argument(
            '--delimiter',
            type=str,
            default=',',
            help='CSV delimiter character (default: ,)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Validate the file without making changes to the database',
        )
        parser.add_argument(
            '--skip-errors',
            action='store_true',
            default=True,
            help='Continue processing even if some rows have errors',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Number of households to process in a single transaction (default: 100)',
        )
        parser.add_argument(
            '--json-ext',
            type=str,
            help='Additional JSON fields to add to json_ext in format {"key": "value", "key2": "value2"}',
        )
        parser.add_argument(
            '--update-existing',
            action='store_true',
            help='Update the status of existing beneficiaries (default: False)',
        )
        parser.add_argument(
            '--profile',
            action='store_true',
            help='Enable detailed performance profiling',
        )

    def validate_benefit_plan(self, benefit_plan_id):
        """Validate that the benefit plan exists and is of GROUP type."""
        try:
            benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
            if benefit_plan.type != BenefitPlan.BenefitPlanType.GROUP_TYPE:
                raise CommandError(f"Benefit plan {benefit_plan_id} is not a GROUP type plan")
            return benefit_plan
        except BenefitPlan.DoesNotExist:
            raise CommandError(f"Benefit plan with ID {benefit_plan_id} does not exist")

    def parse_json_ext(self, json_ext_str):
        """Parse and validate the JSON extension string."""
        if not json_ext_str:
            return {}
        
        try:
            json_ext = json.loads(json_ext_str)
            if not isinstance(json_ext, dict):
                raise CommandError("--json-ext must be a valid JSON object/dictionary")
            return json_ext
        except json.JSONDecodeError as e:
            raise CommandError(f"Invalid JSON format in --json-ext: {str(e)}")

    def merge_json_ext(self, original_json_ext, additional_json_ext):
        """Merge the original json_ext with additional fields."""
        if not additional_json_ext:
            return original_json_ext
        
        # Parse original json_ext if it's a string, otherwise use as-is
        if isinstance(original_json_ext, str):
            try:
                merged = json.loads(original_json_ext) if original_json_ext else {}
            except json.JSONDecodeError:
                merged = {}
        elif isinstance(original_json_ext, dict):
            merged = original_json_ext.copy()
        else:
            merged = {}
        
        # Add/override with additional fields
        merged.update(additional_json_ext)
        
        return merged

    def read_csv_file(self, file_path, code_column, delimiter=','):
        """Read household codes from CSV file."""
        try:
            df = pd.read_csv(file_path, delimiter=delimiter)
            if code_column not in df.columns:
                raise CommandError(f"Column '{code_column}' not found in CSV file")
                
            # Clean and extract household codes
            codes = df[code_column].astype(str).str.strip().tolist()
            return codes
        except Exception as e:
            raise CommandError(f"Error reading CSV file: {str(e)}")

    def find_groups_by_codes(self, codes):
        """Find groups (households) by their codes."""
        return Group.objects.filter(
            code__in=codes,
            is_deleted=False
        )

    def process_households(self, codes, benefit_plan, status, user, additional_json_ext=None, dry_run=False, batch_size=100, skip_errors=True, update_existing=False, profile=False):
        """Process households and register them as beneficiaries."""
        results = {
            'total': len(codes),
            'found': 0,
            'already_registered': 0,
            'newly_registered': 0,
            'updated': 0,
            'not_found': 0,
            'errors': 0,
            'not_found_codes': [],
            'error_details': [],
            'timing': {
                'find_groups': 0,
                'check_existing': 0,
                'bulk_update': 0,
                'create_beneficiaries': 0,
                'total_batch': 0
            }
        }

        # Process in batches to avoid memory issues with large files
        for i in range(0, len(codes), batch_size):
            batch_codes = codes[i:i+batch_size]
            batch_num = i//batch_size + 1
            self.stdout.write(f"\nProcessing batch {batch_num} ({len(batch_codes)} households)")

            batch_start = time.time()

            # Find groups for this batch
            with timer(f"Batch {batch_num}: Find groups", self.stdout.write if profile else None):
                groups = self.find_groups_by_codes(batch_codes)
                found_codes = set(groups.values_list('code', flat=True))
                results['found'] += len(found_codes)

            # Track codes not found
            not_found_codes = set(batch_codes) - found_codes
            results['not_found'] += len(not_found_codes)
            results['not_found_codes'].extend(not_found_codes)

            # Find which groups are already registered as beneficiaries
            with timer(f"Batch {batch_num}: Check existing beneficiaries", self.stdout.write if profile else None):
                # Get the group IDs from the found groups
                group_ids = [g.id for g in groups]

                # Find existing beneficiaries for these specific groups
                existing_beneficiaries = GroupBeneficiary.objects.filter(
                    group_id__in=group_ids,
                    benefit_plan=benefit_plan,
                    is_deleted=False
                )

                # Force evaluation of the queryset to capture current state
                existing_beneficiaries_list = list(existing_beneficiaries)
                existing_group_ids = set(eb.group_id for eb in existing_beneficiaries_list)
                results['already_registered'] += len(existing_group_ids)

            self.stdout.write(f"  Found: {len(found_codes)}, Already registered: {len(existing_group_ids)}, To register: {len(found_codes) - len(existing_group_ids)}")

            if profile and existing_beneficiaries_list:
                # Show status breakdown of existing beneficiaries
                status_counts = {}
                for eb in existing_beneficiaries_list:
                    status_counts[eb.status] = status_counts.get(eb.status, 0) + 1
                self.stdout.write(f"  Existing beneficiary statuses: {status_counts}")

            # Skip actual registration if dry run
            if dry_run:
                continue

            # Process groups using the service
            try:
                with transaction.atomic():
                    newly_registered = 0
                    updated = 0

                    # If update_existing is True, update existing beneficiaries first
                    if update_existing and existing_group_ids:
                        with timer(f"Batch {batch_num}: Update existing beneficiaries", self.stdout.write if profile else None):
                            # Collect beneficiaries that need status updates
                            beneficiaries_to_update = []
                            for beneficiary in existing_beneficiaries_list:
                                if beneficiary.status != status:
                                    beneficiary.status = status
                                    beneficiary.save_history = False  # Prevent individual history saves
                                    beneficiaries_to_update.append(beneficiary)

                            # Perform bulk update if there are beneficiaries to update
                            if beneficiaries_to_update:
                                try:
                                    # Bulk update for performance
                                    GroupBeneficiary.objects.bulk_update(
                                        beneficiaries_to_update,
                                        ['status'],
                                        batch_size=100
                                    )
                                    updated += len(beneficiaries_to_update)

                                    # Log the bulk update for audit trail
                                    self.stdout.write(f"  Bulk updated {len(beneficiaries_to_update)} beneficiaries to status '{status}'")
                                except Exception as e:
                                    results['errors'] += 1
                                    results['error_details'].append(f"Error bulk updating beneficiaries: {str(e)}")
                                    if not skip_errors:
                                        raise
                            else:
                                self.stdout.write(f"  All {len(existing_beneficiaries_list)} existing beneficiaries already have status '{status}' - no updates needed")

                    # Process groups not already registered
                    groups_to_register = [g for g in groups if g.id not in existing_group_ids]
                    if groups_to_register:
                        self.stdout.write(f"  Creating {len(groups_to_register)} new beneficiaries...")

                        with timer(f"Batch {batch_num}: Create beneficiaries", self.stdout.write if profile else None):
                            # OPTIMIZED: Use bulk_create instead of service.create() loop
                            beneficiaries_to_create = []
                            now = timezone.now()
                            date_valid_from = now.date() if hasattr(now.date(), '__call__') else now

                            for group in groups_to_register:
                                try:
                                    # Merge original json_ext with additional fields
                                    merged_json_ext = self.merge_json_ext(group.json_ext, additional_json_ext)

                                    # Create GroupBeneficiary object directly with proper field names
                                    beneficiary = GroupBeneficiary(
                                        group_id=group.id,
                                        benefit_plan_id=benefit_plan.id,
                                        status=status,
                                        date_valid_from=date_valid_from,
                                        json_ext=merged_json_ext,
                                        user_created=user,
                                        user_updated=user,
                                        date_created=now,
                                        date_updated=now
                                    )
                                    # Set the UUID manually for bulk create
                                    beneficiary.set_pk()
                                    beneficiaries_to_create.append(beneficiary)
                                except Exception as e:
                                    results['errors'] += 1
                                    results['error_details'].append(f"Error preparing beneficiary for group {group.code}: {str(e)}")
                                    if skip_errors:
                                        continue
                                    else:
                                        raise

                            # Bulk create all beneficiaries at once
                            if beneficiaries_to_create:
                                try:
                                    created = GroupBeneficiary.objects.bulk_create(
                                        beneficiaries_to_create,
                                        batch_size=500
                                    )
                                    newly_registered = len(created)
                                    self.stdout.write(f"  Successfully created {newly_registered} beneficiaries")
                                except Exception as e:
                                    results['errors'] += 1
                                    error_msg = f"Error bulk creating beneficiaries: {str(e)}"
                                    results['error_details'].append(error_msg)
                                    self.stdout.write(self.style.ERROR(error_msg))
                                    if skip_errors:
                                        # Fall back to individual creation using save()
                                        self.stdout.write(self.style.WARNING(f"  Bulk create failed, falling back to individual creation..."))
                                        for idx, beneficiary in enumerate(beneficiaries_to_create):
                                            try:
                                                beneficiary.save(user=user)
                                                newly_registered += 1
                                            except Exception as e2:
                                                results['errors'] += 1
                                                results['error_details'].append(f"Error creating beneficiary: {str(e2)}")
                                    else:
                                        raise

                    results['newly_registered'] += newly_registered
                    results['updated'] += updated

            except CommandError:
                # Re-raise CommandError even with skip_errors=True to ensure intentional errors are seen
                raise
            except Exception as e:
                results['errors'] += 1
                results['error_details'].append(f"Error in batch transaction: {str(e)}")
                if not skip_errors:
                    raise CommandError(f"Batch processing failed: {str(e)}")
                else:
                    self.stdout.write(self.style.WARNING(f"Skipping failed batch due to error: {str(e)}"))

            batch_elapsed = time.time() - batch_start
            self.stdout.write(f"  Batch {batch_num} completed in {batch_elapsed:.2f}s")

            # Clear query log to prevent memory buildup
            if connection.queries_logged:
                reset_queries()

        return results

    def handle(self, *args, **options):
        csv_file = options['csv_file']
        benefit_plan_id = options['benefit_plan_id']
        code_column = options['column']
        status = options['status']
        username = options['user']
        delimiter = options['delimiter']
        dry_run = options['dry_run']
        skip_errors = options['skip_errors']
        batch_size = options['batch_size']
        json_ext_str = options.get('json_ext')
        update_existing = options['update_existing']
        profile = options['profile']

        # Enable query logging if profiling
        if profile:
            from django.conf import settings
            settings.DEBUG = True
            self.stdout.write(self.style.WARNING("PROFILE MODE - Detailed timing information will be displayed"))

        overall_start = time.time()

        try:
            # Parse and validate JSON extension
            with timer("Parse JSON extension", self.stdout.write if profile else None):
                additional_json_ext = self.parse_json_ext(json_ext_str)
                if additional_json_ext:
                    self.stdout.write(f"Additional JSON fields to add: {json.dumps(additional_json_ext)}")

            # Validate benefit plan
            with timer("Validate benefit plan", self.stdout.write if profile else None):
                benefit_plan = self.validate_benefit_plan(benefit_plan_id)
                self.stdout.write(f"Using benefit plan: {benefit_plan.code} - {benefit_plan.name}")

            # Get user
            with timer("Get user", self.stdout.write if profile else None):
                try:
                    user = User.objects.get(username=username)
                except User.DoesNotExist:
                    raise CommandError(f"User with username {username} does not exist")

            # Read CSV file
            with timer("Read CSV file", self.stdout.write if profile else None):
                self.stdout.write(f"Reading household codes from {csv_file}, column '{code_column}'...")
                codes = self.read_csv_file(csv_file, code_column, delimiter)
                self.stdout.write(f"Found {len(codes)} household codes in the file")

            # Process households
            if dry_run:
                self.stdout.write(self.style.WARNING("DRY RUN MODE - No changes will be made to the database"))

            if skip_errors:
                self.stdout.write(self.style.WARNING("SKIP ERRORS MODE - Will continue processing even when errors occur"))

            with timer("Process all households", self.stdout.write if profile else None):
                results = self.process_households(codes, benefit_plan, status, user, additional_json_ext, dry_run, batch_size, skip_errors, update_existing, profile)
            
            # Print results
            self.stdout.write("\nImport Results:")
            self.stdout.write(f"Total households in file: {results['total']}")
            self.stdout.write(f"Households found in system: {results['found']}")
            self.stdout.write(f"Already registered to benefit plan: {results['already_registered']}")
            
            if not dry_run:
                self.stdout.write(self.style.SUCCESS(f"Newly registered to benefit plan: {results['newly_registered']}"))
                if update_existing and results['updated'] > 0:
                    self.stdout.write(self.style.SUCCESS(f"Updated status for existing beneficiaries: {results['updated']}"))
            else:
                self.stdout.write(f"Would register to benefit plan: {results['found'] - results['already_registered']}")
                if update_existing:
                    self.stdout.write(f"Would update status for existing beneficiaries: Check individual beneficiary statuses")
                
            self.stdout.write(self.style.WARNING(f"Households not found in system: {results['not_found']}"))
            
            if results['not_found'] > 0 and results['not_found'] <= 20:
                self.stdout.write("Not found household codes:")
                for code in results['not_found_codes']:
                    self.stdout.write(f"  - {code}")
            
            if results['errors'] > 0:
                self.stdout.write(self.style.ERROR(f"Errors encountered: {results['errors']}"))
                for error in results['error_details']:
                    self.stdout.write(f"  - {error}")
                    
            if dry_run:
                self.stdout.write(self.style.WARNING("\nThis was a dry run. No changes were made to the database."))

            # Print overall timing
            overall_elapsed = time.time() - overall_start
            self.stdout.write(f"\n{'='*60}")
            self.stdout.write(f"TOTAL EXECUTION TIME: {overall_elapsed:.2f}s ({overall_elapsed/60:.2f} minutes)")
            if results['newly_registered'] > 0:
                self.stdout.write(f"Average time per beneficiary: {overall_elapsed/results['newly_registered']:.3f}s")
            self.stdout.write(f"{'='*60}")

        except CommandError as e:
            self.stderr.write(self.style.ERROR(str(e)))
            raise
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Unexpected error: {str(e)}"))
            raise CommandError(f"Import failed: {str(e)}")
