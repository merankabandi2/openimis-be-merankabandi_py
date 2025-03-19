import graphene
import graphene_django_optimizer as gql_optimizer
from core.gql.export_mixin import ExportableQueryMixin
from decimal import Decimal
from gettext import gettext as _
from django.contrib.auth.models import AnonymousUser
from django.db.models import Q, Sum, Count, OuterRef, Subquery, F, Value

from core.custom_filters import CustomFilterWizardStorage
from core.schema import OrderedDjangoFilterConnectionField
from core.services import wait_for_mutation
from core.utils import append_validity_filter

from merankabandi.gql_mutations import CreateMonetaryTransferMutation, DeleteMonetaryTransferMutation, UpdateMonetaryTransferMutation
from merankabandi.gql_queries import BehaviorChangePromotionGQLType, MicroProjectGQLType, MonetaryTransferBeneficiaryDataGQLType, MonetaryTransferGQLType, MonetaryTransferQuarterlyDataGQLType, SensitizationTrainingGQLType, TicketResolutionStatusGQLType, BenefitConsumptionByProvinceGQLType
from merankabandi.models import BehaviorChangePromotion, MicroProject, MonetaryTransfer, SensitizationTraining
from payroll.models import BenefitConsumption, BenefitConsumptionStatus
from social_protection.models import BenefitPlan, GroupBeneficiary
from payment_cycle.gql_queries import PaymentCycleGQLType
from payment_cycle.models import PaymentCycle
from payment_cycle.apps import PaymentCycleConfig
from payroll.apps import PayrollConfig
from payroll.gql_queries import BenefitsSummaryGQLType
from social_protection.gql_queries import GroupBeneficiaryGQLType
from social_protection.apps import SocialProtectionConfig
from grievance_social_protection.models import Ticket

from location.apps import LocationConfig
from individual.apps import IndividualConfig
from individual.gql_queries import IndividualGQLType, GroupGQLType
from individual.models import GroupIndividual

class Query(ExportableQueryMixin, graphene.ObjectType):

    exportable_fields = ['sensitization_training', 'behavior_change_promotion', 'micro_project', 'monetary_transfer']

    # Add the new query field
    benefit_consumption_by_province = graphene.List(
        BenefitConsumptionByProvinceGQLType,
        year=graphene.Int(description="Filter by year"),
        benefitPlan_Id=graphene.String(description="Filter by benefit plan ID"),
    )
    
    payment_cycle_filtered = OrderedDjangoFilterConnectionField(
        PaymentCycleGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        dateValidFrom__Gte=graphene.DateTime(),
        dateValidFrom__Lte=graphene.DateTime(),
        dateValidTo__Lte=graphene.DateTime(),
        applyDefaultValidityFilter=graphene.Boolean(),
        search=graphene.String(),
        client_mutation_id=graphene.String(),
        year=graphene.Int(description="Filter by year"),
        benefitPlanUuid=graphene.String(),
    )
    
    
    sensitization_training = OrderedDjangoFilterConnectionField(
        SensitizationTrainingGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )
    behavior_change_promotion = OrderedDjangoFilterConnectionField(
        BehaviorChangePromotionGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )
    micro_project = OrderedDjangoFilterConnectionField(
        MicroProjectGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )
    monetary_transfer = OrderedDjangoFilterConnectionField(
        MonetaryTransferGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        client_mutation_id=graphene.String(),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )
    monetary_transfer_quarterly_data = graphene.List(
        MonetaryTransferQuarterlyDataGQLType,
        year=graphene.Int(description="Filter by year"),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )
    monetary_transfer_beneficiary_data = graphene.List(
        MonetaryTransferBeneficiaryDataGQLType,
        year=graphene.Int(description="Filter by year"),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )

    benefits_summary_filtered = graphene.Field(
        BenefitsSummaryGQLType,
        individualId=graphene.String(),
        payrollId=graphene.String(),
        benefitPlanUuid=graphene.String(),
        paymentCycleUuid=graphene.String(),
        year=graphene.Int(description="Filter by year"),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )

    group_beneficiary_filtered = OrderedDjangoFilterConnectionField(
        GroupBeneficiaryGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        dateValidFrom__Gte=graphene.DateTime(),
        dateValidFrom__Lte=graphene.DateTime(),
        dateValidTo__Lte=graphene.DateTime(),
        applyDefaultValidityFilter=graphene.Boolean(),
        client_mutation_id=graphene.String(),
        year=graphene.Int(description="Filter by year"),
        customFilters=graphene.List(of_type=graphene.String),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )
    
    individual_filtered = OrderedDjangoFilterConnectionField(
        IndividualGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        applyDefaultValidityFilter=graphene.Boolean(),
        client_mutation_id=graphene.String(),
        groupId=graphene.String(),
        customFilters=graphene.List(of_type=graphene.String),
        benefitPlanToEnroll=graphene.String(),
        benefitPlanId=graphene.String(),
        filterNotAttachedToGroup=graphene.Boolean(),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )

    group_filtered = OrderedDjangoFilterConnectionField(
        GroupGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        applyDefaultValidityFilter=graphene.Boolean(),
        client_mutation_id=graphene.String(),
        first_name=graphene.String(),
        last_name=graphene.String(),
        customFilters=graphene.List(of_type=graphene.String),
        benefitPlanToEnroll=graphene.String(),
        benefitPlanId=graphene.String(),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
    )

    tickets_by_resolution = graphene.List(
        TicketResolutionStatusGQLType,
        year=graphene.Int(description="Filter by year"),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
        benefitPlan_Id=graphene.String(),
    )

    def resolve_payment_cycle_filtered(self, info, year=None, **kwargs):
        filters = append_validity_filter(**kwargs)

        client_mutation_id = kwargs.get("client_mutation_id")
        if client_mutation_id:
            filters.append(Q(mutations__mutation__client_mutation_id=client_mutation_id))

        Query._check_permissions(info.context.user, PaymentCycleConfig.gql_query_payment_cycle_perms)
        query = PaymentCycle.objects.filter(*filters)
        
        benefit_plan_uuid = kwargs.get("benefitPlanUuid", None)
        if benefit_plan_uuid:
            query = query.filter(
                payment_plan__benefit_plan_id=benefit_plan_uuid
            )

        # Filter by year if provided
        if year:
            query = query.filter(
                start_date__year=year
            )
        
        return gql_optimizer.query(query, info)

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
            status__in=['ACCEPTED', 'RECONCILED'],
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
        Query._check_permissions(info.context.user,
                                 PayrollConfig.gql_payroll_search_perms)
        filters = append_validity_filter(**kwargs)

        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_individual_location_filters(parent_location, parent_location_level, prefix='individual__'))

        # Start by getting all benefit consumption data
        query = BenefitConsumption.objects.filter(~Q(status__in= [BenefitConsumptionStatus.PENDING_DELETION, BenefitConsumptionStatus.DUPLICATE]))
        # Filter by year if provided
        if year:
            query = query.filter(*filters,
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


    def resolve_benefits_summary_filtered(self, info, year=None, **kwargs):
        Query._check_permissions(info.context.user,
                                 PayrollConfig.gql_payroll_search_perms)
        filters = append_validity_filter(**kwargs)

        individual_id = kwargs.get("individualId", None)
        payroll_id = kwargs.get("payrollId", None)
        benefit_plan_uuid = kwargs.get("benefitPlanUuid", None)
        payment_cycle_uuid = kwargs.get("paymentCycleUuid", None)

        # Filter by year if provided
        if year:
            filters.append(Q(payrollbenefitconsumption__payroll__payment_cycle__start_date__year=year))
        
        if individual_id:
            filters.append(Q(individual__id=individual_id))

        if payroll_id:
            filters.append(Q(payrollbenefitconsumption__payroll_id=payroll_id))

        if benefit_plan_uuid:
            filters.append(Q(payrollbenefitconsumption__payroll__payment_plan__benefit_plan_id=benefit_plan_uuid))

        if payment_cycle_uuid:
            filters.append(Q(payrollbenefitconsumption__payroll__payment_cycle_id=payment_cycle_uuid))

        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_individual_location_filters(parent_location, parent_location_level, prefix='individual__'))

        amount_received = BenefitConsumption.objects.filter(
            *filters,
            is_deleted=False,
            payrollbenefitconsumption__is_deleted=False,
            status=BenefitConsumptionStatus.RECONCILED
        ).aggregate(total_received=Sum('amount'))['total_received'] or 0

        amount_due = BenefitConsumption.objects.filter(
            *filters,
            is_deleted=False,
            payrollbenefitconsumption__is_deleted=False
        ).exclude(status=BenefitConsumptionStatus.RECONCILED).aggregate(total_due=Sum('amount'))['total_due'] or 0

        return BenefitsSummaryGQLType(
            total_amount_received=amount_received,
            total_amount_due=amount_due,
        )

    def resolve_group_beneficiary_filtered(self, info, **kwargs):
        def _build_filters(info, **kwargs):
            filters = append_validity_filter(**kwargs)

            client_mutation_id = kwargs.get("client_mutation_id")
            if client_mutation_id:
                filters.append(Q(mutations__mutation__client_mutation_id=client_mutation_id))

            Query._check_permissions(
                info.context.user,
                SocialProtectionConfig.gql_beneficiary_search_perms
            )
            return filters

        def _apply_custom_filters(query, **kwargs):
            custom_filters = kwargs.get("customFilters")
            if custom_filters:
                query = CustomFilterWizardStorage.build_custom_filters_queryset(
                    Query.module_name,
                    Query.object_type,
                    custom_filters,
                    query,
                    "group__groupindividuals__individual",
                )
            return query
    

        filters = _build_filters(info, **kwargs)
        
        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_location_filters(parent_location, parent_location_level, prefix='group__'))

        query = GroupBeneficiary.get_queryset(None, info.context.user)
        query = _apply_custom_filters(query.filter(*filters), **kwargs)

        return gql_optimizer.query(query, info)

    def resolve_individual_filtered(self, info, **kwargs):
        Query._check_permissions(info.context.user,
                                 IndividualConfig.gql_individual_search_perms)

        filters = append_validity_filter(**kwargs)

        client_mutation_id = kwargs.get("client_mutation_id")
        if client_mutation_id:
            wait_for_mutation(client_mutation_id)
            filters.append(Q(mutations__mutation__client_mutation_id=client_mutation_id))

        group_id = kwargs.get("groupId")
        if group_id:
            filters.append(Q(groupindividuals__group__id=group_id))

        benefit_plan_to_enroll = kwargs.get("benefitPlanToEnroll")
        if benefit_plan_to_enroll:
            filters.append(
                Q(is_deleted=False) &
                ~Q(beneficiary__benefit_plan_id=benefit_plan_to_enroll)
            )

        benefit_plan_id = kwargs.get("benefitPlanId")
        if benefit_plan_id:
            filters.append(
                Q(is_deleted=False) &
                Q(beneficiary__benefit_plan_id=benefit_plan_id)
            )

        filter_not_attached_to_group = kwargs.get("filterNotAttachedToGroup")
        if filter_not_attached_to_group:
            subquery = GroupIndividual.objects.filter(individual=OuterRef('pk')).values('individual')
            filters.append(~Q(pk__in=Subquery(subquery)))

        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_individual_location_filters(parent_location, parent_location_level))

        query = IndividualGQLType.get_queryset(None, info)
        query = query.filter(*filters)

        custom_filters = kwargs.get("customFilters", None)
        if custom_filters:
            query = CustomFilterWizardStorage.build_custom_filters_queryset(
                Query.module_name,
                Query.object_type,
                custom_filters,
                query,
            )

        return gql_optimizer.query(query, info)

    def resolve_group_filtered(self, info, **kwargs):
        Query._check_permissions(
            info.context.user,
            IndividualConfig.gql_group_search_perms
        )
        filters = append_validity_filter(**kwargs)
        client_mutation_id = kwargs.get("client_mutation_id", None)
        if client_mutation_id:
            wait_for_mutation(client_mutation_id)
            filters.append(Q(mutations__mutation__client_mutation_id=client_mutation_id))

        first_name = kwargs.get("first_name", None)
        if first_name:
            filters.append(Q(groupindividuals__individual__first_name__icontains=first_name))

        last_name = kwargs.get("last_name", None)
        if last_name:
            filters.append(Q(groupindividuals__individual__last_name__icontains=last_name))

        benefit_plan_to_enroll = kwargs.get("benefitPlanToEnroll")
        if benefit_plan_to_enroll:
            filters.append(
                Q(is_deleted=False) &
                ~Q(groupbeneficiary__benefit_plan_id=benefit_plan_to_enroll)
            )

        benefit_plan_id = kwargs.get("benefitPlanId")
        if benefit_plan_id:
            filters.append(
                Q(is_deleted=False) &
                Q(groupbeneficiary__benefit_plan_id=benefit_plan_to_enroll)
            )

        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_location_filters(parent_location, parent_location_level))

        query = GroupGQLType.get_queryset(None, info)
        query = query.filter(*filters).distinct()

        custom_filters = kwargs.get("customFilters", None)
        if custom_filters:
            query = CustomFilterWizardStorage.build_custom_filters_queryset(
                Query.module_name,
                "Group",
                custom_filters,
                query
            )
        return gql_optimizer.query(query, info)

    def resolve_tickets_by_resolution(self, info, **kwargs):
        # Check permissions - adjust as needed based on your application's permission model
        # Query._check_permissions(info.context.user, SocialProtectionConfig.gql_query_beneficiaries_perms)
        
        # Start with all tickets
        query = Ticket.objects.filter(is_deleted=False)
        
        # Apply year filter if provided
        year = kwargs.get('year')
        if year:
            query = query.filter(
                date_of_incident__year=year
            )
        
        # Apply location filter if provided
        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters = Query._get_individual_location_filters(parent_location, parent_location_level)
            query = query.filter(*filters)
        
        # Apply benefit plan filter if provided
        benefit_plan_id = kwargs.get('benefitPlan_Id')
        if benefit_plan_id:
            query = query.filter(benefit_plan_id=benefit_plan_id)
        
        status_counts = query.values('status').annotate(count=Count('id'))
        
        result = []
        for item in status_counts:
            status_code = item['status']
            status_name =  Ticket.TicketStatus(status_code).label
            
            result.append({
                'status': status_name,
                'count': item['count']
            })
        
        return result

    def resolve_benefit_consumption_by_province(self, info, **kwargs):
        Query._check_permissions(info.context.user, PayrollConfig.gql_payroll_search_perms)
        
        # Filter by benefit plan if provided
        benefit_plan_id = kwargs.get('benefitPlan_Id')
        benefit_plan_filter = Q()
        if benefit_plan_id:
            benefit_plan_filter = Q(payrollbenefitconsumption__payroll__payment_plan__benefit_plan_id=benefit_plan_id)
        
        # Filter by year if provided
        year = kwargs.get('year')
        year_filter = Q()
        if year:
            year_filter = Q(payrollbenefitconsumption__payroll__payment_cycle__start_date__year=year)
        
        # Get the location information from Individual -> Group -> Location hierarchy
        from location.models import Location
        
        # Start by finding all provinces with benefit consumption
        provinces = Location.objects.filter(type='D')
        
        result = []
        for province in provinces:
            # Get all locations under this province
            location_query = Q()
            for i in range(len(LocationConfig.location_types) - 1):
                parent_field = "parent" + "__parent" * i
                location_query |= Q(**{f"{parent_field}__id": province.id})
            
            # Find groups in these locations
            groups_in_province = Q(individual__groupindividuals__group__location__id__in=Location.objects.filter(
                Q(id=province.id) | location_query
            ).values_list('id', flat=True))
            
            # Get benefit consumption data for individuals in these groups
            benefit_data = BenefitConsumption.objects.filter(
                groups_in_province,
                benefit_plan_filter,
                year_filter,
                is_deleted=False
            )
            
            # Calculate totals
            total_amount = benefit_data.filter(
                status=BenefitConsumptionStatus.RECONCILED
            ).aggregate(total=Sum('amount'))['total'] or 0
            
            total_paid = benefit_data.filter(
                status=BenefitConsumptionStatus.RECONCILED
            ).values('individual_id').distinct().count()
            
            # Get beneficiary status counts from groupbeneficiary
            from social_protection.models import GroupBeneficiary
            
            beneficiaries = GroupBeneficiary.objects.filter(
                group__location__id__in=Location.objects.filter(Q(id=province.id) | location_query).values_list('id', flat=True)
            )
            
            if benefit_plan_id:
                beneficiaries = beneficiaries.filter(benefit_plan_id=benefit_plan_id)
                
            if year:
                # Filter by valid date range in the specified year
                start_date = f"{year}-01-01"
                end_date = f"{year}-12-31"
                beneficiaries = beneficiaries.filter(
                    Q(date_valid_to__isnull=True) | Q(date_valid_to__gte=start_date),
                    date_valid_from__lte=end_date
                )
            
            active_count = beneficiaries.filter(status='ACTIVE').count()
            suspended_count = beneficiaries.filter(status='SUSPENDED').count()
            selected_count = beneficiaries.filter(status='SELECTED').count()
            
            result.append({
                'province_id': str(province.id),
                'province_name': province.name,
                'province_code': province.code,
                'total_paid': total_paid,
                'total_amount': total_amount,
                'beneficiaries_active': active_count,
                'beneficiaries_suspended': suspended_count,
                'beneficiaries_selected': selected_count,
            })
        
        return result

    @staticmethod
    def _get_location_filters(parent_location, parent_location_level, prefix=""):
        query_key = "uuid"
        for i in range(len(LocationConfig.location_types) - parent_location_level - 1):
            query_key = "parent__" + query_key
        query_key = prefix + "location__" + query_key
        return Q(**{query_key: parent_location})

    @staticmethod
    def _get_individual_location_filters(parent_location, parent_location_level, prefix=""):
        query_key = "uuid"
        for i in range(len(LocationConfig.location_types) - parent_location_level - 1):
            query_key = "parent__" + query_key
        query_key = prefix + "groupindividuals__group__location__" + query_key
        return Q(**{query_key: parent_location})


    @staticmethod
    def _check_permissions(user, perms):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(perms):
            raise PermissionError("Unauthorized")

class Mutation(graphene.ObjectType):
    create_monetary_transfer = CreateMonetaryTransferMutation.Field()
    update_monetary_transfer = UpdateMonetaryTransferMutation.Field()
    delete_monetary_transfer = DeleteMonetaryTransferMutation.Field()
