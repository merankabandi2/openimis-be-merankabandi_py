import graphene
import graphene_django_optimizer as gql_optimizer
from core.gql.export_mixin import ExportableQueryMixin
from decimal import Decimal
from gettext import gettext as _
from django.contrib.auth.models import AnonymousUser
from django.db.models import Q, Sum, Count

from core.schema import OrderedDjangoFilterConnectionField
from core.services import wait_for_mutation
from core.utils import append_validity_filter

from merankabandi.gql_mutations import CreateMonetaryTransferMutation, DeleteMonetaryTransferMutation, UpdateMonetaryTransferMutation
from merankabandi.gql_queries import BehaviorChangePromotionGQLType, MicroProjectGQLType, MonetaryTransferBeneficiaryDataGQLType, MonetaryTransferGQLType, MonetaryTransferQuarterlyDataGQLType, SensitizationTrainingGQLType
from merankabandi.models import BehaviorChangePromotion, MicroProject, MonetaryTransfer, SensitizationTraining
from payroll.models import BenefitConsumption, BenefitConsumptionStatus
from social_protection.models import BenefitPlan

class Query(ExportableQueryMixin, graphene.ObjectType):

    exportable_fields = ['sensitization_training', 'behavior_change_promotion', 'micro_project', 'monetary_transfer']

    sensitization_training = OrderedDjangoFilterConnectionField(
        SensitizationTrainingGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    behavior_change_promotion = OrderedDjangoFilterConnectionField(
        BehaviorChangePromotionGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    micro_project = OrderedDjangoFilterConnectionField(
        MicroProjectGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    monetary_transfer = OrderedDjangoFilterConnectionField(
        MonetaryTransferGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        client_mutation_id=graphene.String(),
    )
    monetary_transfer_quarterly_data = graphene.List(
        MonetaryTransferQuarterlyDataGQLType,
        year=graphene.Int(description="Filter by year")
    )
    monetary_transfer_beneficiary_data = graphene.List(
        MonetaryTransferBeneficiaryDataGQLType,
        year=graphene.Int(description="Filter by year")
    )

    def resolve_sensitization_training(self, info, **kwargs):
        return gql_optimizer.query(SensitizationTraining.objects.all(), info)

    def resolve_behavior_change_promotion(self, info, **kwargs):
        return gql_optimizer.query(BehaviorChangePromotion.objects.all(), info)

    def resolve_micro_project(self, info, **kwargs):
        return gql_optimizer.query(MicroProject.objects.all(), info)

    def resolve_monetary_transfer(self, info, **kwargs):
        #Query._check_permissions(info.context.user, PayrollConfig.gql_payroll_search_perms)
        filters = append_validity_filter(**kwargs)
        query = MonetaryTransfer.objects.filter(*filters)
        return gql_optimizer.query(query, info)


    def resolve_monetary_transfer_quarterly_data(self, info, year=None, **kwargs):
        # Start by getting all benefit consumption data through payrolls
        query = BenefitConsumption.objects.filter(
            status='ACCEPTED',
            payrollbenefitconsumption__payroll__isnull=False
        )
        
        # Filter by year if provided
        if year:
            query = query.filter(
                payrollbenefitconsumption__payroll__payment_cycle__start_date__year=year
            )
        
        # Join with necessary tables to get benefit plan type
        query = query.select_related(
            'payrollbenefitconsumption__payroll__payment_plan',
            'payrollbenefitconsumption__payroll__payment_cycle'
        )
        
        # Extract quarter and annotate data
        result = []
        
        # Dynamically build transfer types dictionary by grouping benefit plans by their prefix codes
        # First, get all benefit plans that have monetary transfers
        # benefit_plans = BenefitPlan.objects.filter(
        #     id__in=query.values_list(
        #         'payrollbenefitconsumption__payroll__payment_plan__benefit_plan_id',
        #         flat=True
        #     ).distinct()
        # )
        benefit_plans = BenefitPlan.objects.filter(is_deleted=False)
        
        # Group them by their code prefix or use a custom field that categorizes them
        transfer_types = {}
        
        # Option 1: Group by code prefix (first 3-4 chars typically represent the type)
        prefixes = {}
        for plan in benefit_plans:
            # Extract prefix (first part of the code before dash or first 3 chars)
            prefix = plan.code.split('-')[0] if '-' in plan.code else plan.code[:3]
            
            if prefix not in prefixes:
                prefixes[prefix] = {
                    'name': None,  # Will be set below
                    'plans': []
                }
            
            prefixes[prefix]['plans'].append(plan)
        
        # Set display name for each prefix group based on the first plan's description or name
        mapping = {
            'TMO': 'Transferts monetaires ordinaires',
            'TMR': 'Transferts monetaires aux ménages refugiés',
            'TMU-C': 'Transferts monetaires d\'urgence climatique',
            'TMU-CERC': 'Transferts monetaires d\'urgence CERC'
        }
        
        for prefix, data in prefixes.items():
            # Try to get name from mapping or use first plan's name as fallback
            name = None
            
            # First check if the prefix matches directly
            if prefix in mapping:
                name = mapping[prefix]
            else:
                # Check for partial matches
                for map_prefix, map_name in mapping.items():
                    if prefix.startswith(map_prefix):
                        name = map_name
                        break
            
            # Fallback to first benefit plan's name
            if not name and data['plans']:
                name = data['plans'][0].name
            
            if not name:
                name = f"Transferts type {prefix}"
            
            data['name'] = name
            
            # Add to transfer_types dictionary
            transfer_types[name] = BenefitPlan.objects.filter(id__in=[p.id for p in data['plans']])
        
        # If no benefit plans were found, use the default mapping as fallback
        if not transfer_types:
            transfer_types = {
                'Transferts monetaires ordinaires': BenefitPlan.objects.filter(code__startswith='TMO'),
                'Transferts monetaires aux ménages refugiés': BenefitPlan.objects.filter(code__startswith='TMR'),
                'Transferts monetaires d\'urgence climatique': BenefitPlan.objects.filter(code__startswith='TMU-C'),
                'Transferts monetaires d\'urgence CERC': BenefitPlan.objects.filter(code__startswith='TMU-CERC')
            }
            
        for type_name, benefit_plan_filter in transfer_types.items():
            # Convert UUIDs to strings to avoid type mismatch with character varying fields
            benefit_plan_ids = [str(id) for id in benefit_plan_filter.values_list('id', flat=True)]
            
            type_data = {
                'transfer_type': type_name,
                'q1_amount': 0,
                'q2_amount': 0,
                'q3_amount': 0,
                'q4_amount': 0,
                'q1_beneficiaries': 0,
                'q2_beneficiaries': 0,
                'q3_beneficiaries': 0,
                'q4_beneficiaries': 0
            }

            # For each benefit plan in this type, get quarterly amounts and beneficiary counts
            for quarter in [1, 2, 3, 4]:
                # Get the total amount for the quarter
                quarter_amount = query.filter(
                    payrollbenefitconsumption__payroll__payment_plan__benefit_plan_id__in=benefit_plan_ids,
                    payrollbenefitconsumption__payroll__payment_cycle__start_date__quarter=quarter
                ).aggregate(total=Sum('amount'))['total'] or 0
                
                # Get the count of unique beneficiaries paid in this quarter
                beneficiary_count = query.filter(
                    payrollbenefitconsumption__payroll__payment_plan__benefit_plan_id__in=benefit_plan_ids,
                    payrollbenefitconsumption__payroll__payment_cycle__start_date__quarter=quarter
                ).aggregate(total=Count('amount'))['total'] or 0
                
                # Store both the amount and beneficiary count in the type_data dictionary
                type_data[f'q{quarter}_amount'] = Decimal(quarter_amount)
                type_data[f'q{quarter}_beneficiaries'] = beneficiary_count

            result.append(type_data)
        
        return result

    def resolve_monetary_transfer_beneficiary_data(self, info, year=None, **kwargs):
        # Start by getting all benefit consumption data
        query = BenefitConsumption.objects.filter(~Q(status__in= [BenefitConsumptionStatus.PENDING_DELETION, BenefitConsumptionStatus.DUPLICATE]))
        # Filter by year if provided
        if year:
            query = query.filter(
                payrollbenefitconsumption__payroll__payment_cycle__start_date__year=year
            )
        
        # Join with necessary tables
        query = query.select_related('individual')
        
        result = []
        
        # Get all benefit plans
        benefit_plans = BenefitPlan.objects.filter(is_deleted=False)
        
        # Group them by their code prefix
        prefixes = {}
        for plan in benefit_plans:
            prefix = plan.code.split('-')[0] if '-' in plan.code else plan.code[:3]
            
            if prefix not in prefixes:
                prefixes[prefix] = {
                    'name': None,
                    'plans': []
                }
            
            prefixes[prefix]['plans'].append(plan)
        
        # Set display name for each prefix group
        mapping = {
            'TMO': 'Transferts monetaires ordinaires',
            'TMR': 'Transferts monetaires aux ménages refugiés',
            'TMU-C': 'Transferts monetaires d\'urgence climatique',
            'TMU-CERC': 'Transferts monetaires d\'urgence CERC'
        }
        
        transfer_types = {}
        for prefix, data in prefixes.items():
            name = None
            
            if prefix in mapping:
                name = mapping[prefix]
            else:
                for map_prefix, map_name in mapping.items():
                    if prefix.startswith(map_prefix):
                        name = map_name
                        break
            
            if not name and data['plans']:
                name = data['plans'][0].name
            
            if not name:
                name = f"Transferts type {prefix}"
            
            data['name'] = name
            transfer_types[name] = BenefitPlan.objects.filter(id__in=[p.id for p in data['plans']])
        
        # If no benefit plans were found, use the default mapping as fallback
        if not transfer_types:
            transfer_types = {
                'Transferts monetaires ordinaires': BenefitPlan.objects.filter(code__startswith='TMO'),
                'Transferts monetaires aux ménages refugiés': BenefitPlan.objects.filter(code__startswith='TMR'),
                'Transferts monetaires d\'urgence climatique': BenefitPlan.objects.filter(code__startswith='TMU-C'),
                'Transferts monetaires d\'urgence CERC': BenefitPlan.objects.filter(code__startswith='TMU-CERC')
            }
            
        for type_name, benefit_plan_filter in transfer_types.items():
            benefit_plan_ids = [str(id) for id in benefit_plan_filter.values_list('id', flat=True)]
            
            type_data = {
                'transfer_type': type_name,
                'male_paid': 0,
                'male_unpaid': 0,
                'female_paid': 0,
                'female_unpaid': 0,
                'total_paid': 0,
                'total_unpaid': 0
            }
            
            # Get data for paid beneficiaries
            paid_query = query.filter(
                status=BenefitConsumptionStatus.RECONCILED,
                payrollbenefitconsumption__payroll__payment_plan__benefit_plan_id__in=benefit_plan_ids,
            )
            
            # Count male paid beneficiaries
            male_paid_count = paid_query.filter(individual__json_ext__sexe='M').count()
            type_data['male_paid'] = male_paid_count
            
            # Count female paid beneficiaries
            female_paid_count = paid_query.filter(individual__json_ext__sexe='F').count()
            type_data['female_paid'] = female_paid_count
            
            # Total paid beneficiaries
            type_data['total_paid'] = male_paid_count + female_paid_count
            
            # Get data for unpaid beneficiaries (eligible but not paid)
            unpaid_query = query.filter(
                status__ne=BenefitConsumptionStatus.RECONCILED,
                payrollbenefitconsumption__payroll__payment_plan__benefit_plan_id__in=benefit_plan_ids,
            )
            
            # Count male unpaid beneficiaries
            male_unpaid_count = unpaid_query.filter(individual__json_ext__sexe='M').count()
            type_data['male_unpaid'] = male_unpaid_count
            
            # Count female unpaid beneficiaries
            female_unpaid_count = unpaid_query.filter(individual__json_ext__sexe='F').count()
            type_data['female_unpaid'] = female_unpaid_count
            
            # Total unpaid beneficiaries
            type_data['total_unpaid'] = male_unpaid_count + female_unpaid_count
            
            result.append(type_data)
        
        return result

class Mutation(graphene.ObjectType):
    create_monetary_transfer = CreateMonetaryTransferMutation.Field()
    update_monetary_transfer = UpdateMonetaryTransferMutation.Field()
    delete_monetary_transfer = DeleteMonetaryTransferMutation.Field()
