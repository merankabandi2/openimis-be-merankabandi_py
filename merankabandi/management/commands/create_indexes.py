"""
Management command to create optimized indexes for JSON fields
"""
from django.core.management.base import BaseCommand
from django.db import connection
import time

class Command(BaseCommand):
    help = 'Creates optimized indexes for fields in PostgreSQL'

    def add_arguments(self, parser):
        parser.add_argument(
            '--drop',
            action='store_true',
            help='Drop existing indexes before creating new ones',
        )

    def handle(self, *args, **options):
        indexes = [
            # Individual Group (Household) indexes
            {
                'name': 'idx_group_json_type_menage',
                'table': 'individual_group',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'type_menage\'))'
            },
            {
                'name': 'idx_group_json_vulnerable',
                'table': 'individual_group',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'vulnerable_ressenti\'))'
            },
            {
                'name': 'idx_group_json_location',
                'table': 'individual_group',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'province_code\'), ("Json_ext"->\'commune_code\'), ("Json_ext"->\'colline_code\'))'
            },
            {
                'name': 'idx_group_json_pmt_score',
                'table': 'individual_group',
                'type': 'BTREE',
                'columns': '(CAST(NULLIF("Json_ext"->>\'score_pmt_initial\', \'\') AS FLOAT))',
                'where': '"Json_ext"->>\'score_pmt_initial\' IS NOT NULL AND "Json_ext"->>\'score_pmt_initial\' != \'\''
            },
            {
                'name': 'idx_group_json_etat',
                'table': 'individual_group',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'etat\'))'
            },
            {
                'name': 'idx_group_json_social_id',
                'table': 'individual_group',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'social_id\'))'
            },
            {
                'name': 'idx_group_json_menage_special',
                'table': 'individual_group',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'menage_mutwa\'), ("Json_ext"->\'menage_refugie\'), ("Json_ext"->\'menage_deplace\'))'
            },
            {
                'name': 'idx_group_json_assets',
                'table': 'individual_group',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'a_terres\'), ("Json_ext"->\'a_elevage\'), ("Json_ext"->\'logement_electricite_a\'))'
            },
            {
                'name': 'idx_group_code',
                'table': 'individual_group',
                'type': 'BTREE',
                'columns': '(code)'
            },
            
            # Individual indexes
            {
                'name': 'idx_individual_json_sexe',
                'table': 'individual_individual',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'sexe\'))'
            },
            {
                'name': 'idx_individual_json_education',
                'table': 'individual_individual',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'va_ecole\'), ("Json_ext"->\'instruction\'), ("Json_ext"->\'lit\'))'
            },
            {
                'name': 'idx_individual_json_health',
                'table': 'individual_individual',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'handicap\'), ("Json_ext"->\'maladie_chro\'), ("Json_ext"->\'prob_sante\'))'
            },
            {
                'name': 'idx_individual_json_role',
                'table': 'individual_individual',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'est_chef\'), ("Json_ext"->\'lien\'))'
            },
            {
                'name': 'idx_individual_json_social_id',
                'table': 'individual_individual',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'social_id\'))'
            },
            {
                'name': 'idx_individual_json_ci',
                'table': 'individual_individual',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'ci\'))'
            },
            {
                'name': 'idx_individual_dob_btree',
                'table': 'individual_individual',
                'type': 'BTREE',
                'columns': '(dob)'
            },
            
            # GroupBeneficiary indexes
            {
                'name': 'idx_beneficiary_json_payment',
                'table': 'social_protection_groupbeneficiary',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'moyen_paiement\'))'
            },
            {
                'name': 'idx_beneficiary_json_payment_status',
                'table': 'social_protection_groupbeneficiary',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'moyen_paiement\'->\'etat\'), ("Json_ext"->\'moyen_paiement\'->\'status\'))'
            },
            {
                'name': 'idx_beneficiary_json_location',
                'table': 'social_protection_groupbeneficiary',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'province_code\'), ("Json_ext"->\'commune_code\'), ("Json_ext"->\'colline_code\'))'
            },
            {
                'name': 'idx_beneficiary_json_moyen_telecom_msisdn',
                'table': 'social_protection_groupbeneficiary',
                'type': 'BTREE',
                'columns': '(("Json_ext"->\'moyen_telecom\'->\'msisdn\'))'
            },
            {
                'name': 'idx_beneficiary_json_moyen_telecom_status',
                'table': 'social_protection_groupbeneficiary',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'moyen_telecom\'->\'status\'))'
            },
            {
                'name': 'idx_beneficiary_json_moyen_paiement_status',
                'table': 'social_protection_groupbeneficiary',
                'type': 'GIN',
                'columns': '(("Json_ext"->\'moyen_paiement\'->\'status\'))'
            },
            {
                'name': 'idx_benefit_consumption_json_provider_ref',
                'table': 'payroll_benefitconsumption',
                'type': 'BTREE',
                'columns': '(("Json_ext"->\'payment_provider\'->\'transaction_reference\'))'
            },
            {
                'name': 'idx_payroll_benefitconsumption_code',
                'table': 'payroll_benefitconsumption',
                'type': 'BTREE',
                'columns': '(code)'
            },
            {
                'name': 'idx_tblbill_code',
                'table': '"tblBill"',
                'type': 'BTREE',
                'columns': '("Code")'
            },
            {
                'name': 'idx_groupbeneficiary_plan_status_deleted',
                'table': 'social_protection_groupbeneficiary',
                'type': 'BTREE',
                'columns': '(benefit_plan_id, status, "isDeleted")',
            },
            {
                'name': 'idx_beneficiary_json_pmt',
                'table': 'social_protection_groupbeneficiary',
                'type': 'BTREE',
                'columns': '(CAST(NULLIF("Json_ext"->>\'score_pmt_initial\', \'\') AS FLOAT))',
                'where': '"Json_ext"->>\'score_pmt_initial\' IS NOT NULL AND "Json_ext"->>\'score_pmt_initial\' != \'\''
            },
            {
                'name': 'idx_beneficiary_status_active',
                'table': 'social_protection_groupbeneficiary',
                'type': 'BTREE',
                'columns': '(status)',
                'where': 'status = \'ACTIVE\''
            },
            
            # Composite indexes for common queries
            {
                'name': 'idx_group_active_inscrit',
                'table': 'individual_group',
                'type': 'BTREE',
                'columns': '("isDeleted", ("Json_ext"->>\'etat\'))',
                'where': '"isDeleted" = false AND "Json_ext"->>\'etat\' = \'INSCRIT\''
            },
            {
                'name': 'idx_beneficiary_active_notdeleted',
                'table': 'social_protection_groupbeneficiary',
                'type': 'BTREE',
                'columns': '("isDeleted", status)',
                'where': '"isDeleted" = false AND status = \'ACTIVE\''
            },
            
            # Payment point and location indexes
            {
                'name': 'idx_payment_point_name',
                'table': 'payroll_paymentpoint',
                'type': 'BTREE',
                'columns': '(name)'
            },
            {
                'name': 'idx_location_name_type',
                'table': '"tblLocations"',
                'type': 'BTREE',
                'columns': '("LocationName", "LocationType")'
            }
        ]
        
        with connection.cursor() as cursor:
            # Drop indexes if requested
            if options['drop']:
                self.stdout.write('Dropping existing indexes...')
                for index in indexes:
                    try:
                        cursor.execute(f"DROP INDEX IF EXISTS {index['name']}")
                        self.stdout.write(f"Dropped index {index['name']}")
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error dropping {index['name']}: {str(e)}"))
            
            # Create indexes
            self.stdout.write('\nCreating indexes...')
            for index in indexes:
                try:
                    start_time = time.time()
                    
                    # Build CREATE INDEX statement
                    sql = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index['name']} "
                    sql += f"ON {index['table']} "
                    sql += f"USING {index['type']} {index['columns']}"
                    
                    if 'where' in index:
                        sql += f" WHERE {index['where']}"
                    
                    cursor.execute(sql)
                    
                    elapsed = time.time() - start_time
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Created index {index['name']} in {elapsed:.2f} seconds"
                        )
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f"Error creating {index['name']}: {str(e)}")
                    )
            
            # Analyze tables to update statistics
            self.stdout.write('\nAnalyzing tables...')
            tables = [
                'individual_group', 
                'individual_individual', 
                'social_protection_groupbeneficiary',
                'payroll_benefitconsumption',
                'payroll_paymentpoint',
                '"tblBill"',
                '"tblLocations"'
            ]
            for table in tables:
                try:
                    cursor.execute(f"ANALYZE {table}")
                    self.stdout.write(f"Analyzed {table}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Error analyzing {table}: {str(e)}"))
        
        self.stdout.write(self.style.SUCCESS('\nIndex creation complete!'))