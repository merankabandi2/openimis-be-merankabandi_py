import graphene
import graphene_django_optimizer as gql_optimizer
from core.gql.export_mixin import ExportableQueryMixin
from decimal import Decimal
from django.contrib.auth.models import AnonymousUser
from django.db.models import Q, Sum, Count, OuterRef, Subquery, Exists
from location.models import UserDistrict, Location

# Import optimized dashboard queries and mutations
from .optimized_gql_queries import OptimizedDashboardQuery
from .optimized_gql_mutations import DashboardMutations
from .payment_reporting_gql import PaymentReportingQuery
from .vulnerable_groups_gql_queries import VulnerableGroupsQuery
from .geography_gql import GeographyQuery

from core.custom_filters import CustomFilterWizardStorage
from core.schema import OrderedDjangoFilterConnectionField
from core.services import wait_for_mutation
from core.utils import append_validity_filter
from merankabandi.gql_mutations import get_merankabandi_config

from merankabandi.gql_mutations import (
    CreateMonetaryTransferMutation, DeleteMonetaryTransferMutation, UpdateMonetaryTransferMutation,
    CreateSectionMutation, UpdateSectionMutation, DeleteSectionMutation,
    CreateIndicatorMutation, UpdateIndicatorMutation, DeleteIndicatorMutation,
    CreateIndicatorAchievementMutation, UpdateIndicatorAchievementMutation, DeleteIndicatorAchievementMutation,
    GenerateProvincePayrollMutation,
    CreatePaymentAgencyMutation, UpdatePaymentAgencyMutation, DeletePaymentAgencyMutation,
    CreateProvincePaymentAgencyMutation, UpdateProvincePaymentAgencyMutation, DeleteProvincePaymentAgencyMutation,
    CreateAgencyFeeConfigMutation, UpdateAgencyFeeConfigMutation, DeleteAgencyFeeConfigMutation,
    CreateTicketCommentMutation,
    CreateTicketWithExtMutation, UpdateTicketWithExtMutation,
    ValidateSensitizationTrainingMutation, ValidateBehaviorChangeMutation, ValidateMicroProjectMutation,
    ImportSurveyDataMutation, TriggerPMTCalculationMutation,
    BulkUpdateGroupBeneficiaryStatusMutation,
    CreatePmtFormulaMutation, UpdatePmtFormulaMutation, DeletePmtFormulaMutation,
    CreateSelectionQuotaMutation, UpdateSelectionQuotaMutation, DeleteSelectionQuotaMutation,
    CreatePreCollecteMutation, UpdatePreCollecteMutation, DeletePreCollecteMutation,
    ApplyQuotaSelectionMutation, ApplyCriteriaSelectionMutation, SelectAllMutation,
    PromoteToBeneficiaryMutation, PromoteFromWaitingListMutation,
)
from merankabandi.gql_queries import (
    BehaviorChangePromotionGQLType, MicroProjectGQLType, MonetaryTransferBeneficiaryDataGQLType,
    MonetaryTransferGQLType, MonetaryTransferQuarterlyDataGQLType, SensitizationTrainingGQLType,
    TicketResolutionStatusGQLType, BenefitConsumptionByProvinceGQLType, BenefitPlanLocationGQLType,
    SectionGQLType, IndicatorGQLType, IndicatorAchievementGQLType,
    PaymentAgencyGQLType, ProvincePaymentAgencyGQLType, AgencyFeeConfigGQLType, PaymentGatewayConnectorGQLType,
    PmtFormulaGQLType, SelectionQuotaGQLType, PreCollecteGQLType,
    CalculateIndicatorValueResultType, IndicatorBreakdownGQLType,
)
from merankabandi.payment_schedule_gql import (
    PaymentScheduleQuery,
    CreateCommunePaymentRoundMutation,
    CreateRetryPaymentRoundMutation,
    SyncPaymentScheduleMutation,
    InitializeCycleCommunesMutation,
    UpdateCommuneDatesBulkMutation,
    BatchGeneratePayrollsMutation,
)

from merankabandi.models import (
    BehaviorChangePromotion, MicroProject, MonetaryTransfer,
    SensitizationTraining, Section, Indicator, IndicatorAchievement,
    PaymentAgency, ProvincePaymentAgency, AgencyFeeConfig,
    PmtFormula, SelectionQuota, PreCollecte,
)
from payroll.models import BenefitConsumption, BenefitConsumptionStatus
from social_protection.models import BenefitPlan, GroupBeneficiary, BeneficiaryStatus
from payment_cycle.gql_queries import PaymentCycleGQLType
from payment_cycle.models import PaymentCycle
from payment_cycle.apps import PaymentCycleConfig
from payroll.apps import PayrollConfig
from payroll.gql_queries import BenefitsSummaryGQLType
from social_protection.gql_queries import GroupBeneficiaryGQLType
from social_protection.apps import SocialProtectionConfig
from grievance_social_protection.models import Ticket

# Workflow engine imports
from merankabandi.workflow_gql_queries import (
    WorkflowTemplateGQLType, WorkflowStepTemplateGQLType,
    GrievanceWorkflowGQLType, GrievanceTaskGQLType,
    ReplacementRequestGQLType, RoleAssignmentGQLType,
)
from merankabandi.workflow_gql_mutations import (
    CompleteGrievanceTaskMutation,
    SkipGrievanceTaskMutation,
    ReassignGrievanceTaskMutation,
    AddTaskToWorkflowMutation,
    ApproveReplacementRequestMutation,
    RejectReplacementRequestMutation,
    CreateWorkflowTemplateMutation,
    UpdateWorkflowTemplateMutation,
    DeleteWorkflowTemplateMutation,
    CreateRoleAssignmentMutation,
    UpdateRoleAssignmentMutation,
    DeleteRoleAssignmentMutation,
    CreateWorkflowStepTemplateMutation,
    UpdateWorkflowStepTemplateMutation,
    DeleteWorkflowStepTemplateMutation,
)
from merankabandi.result_framework_mutations import (
    CreateResultFrameworkSnapshotMutation,
    FinalizeSnapshotMutation,
    GenerateResultFrameworkDocumentMutation,
)
from location.apps import LocationConfig
from individual.apps import IndividualConfig
from individual.gql_queries import IndividualGQLType, GroupGQLType
from individual.models import GroupIndividual


class Query(ExportableQueryMixin, OptimizedDashboardQuery, PaymentReportingQuery,
            VulnerableGroupsQuery, GeographyQuery, PaymentScheduleQuery,
            graphene.ObjectType):

    exportable_fields = ['sensitization_training', 'behavior_change_promotion', 'micro_project', 'monetary_transfer',
                         'section', 'indicator', 'indicator_achievement']

    # Add the new query field
    benefit_consumption_by_province = graphene.List(
        BenefitConsumptionByProvinceGQLType,
        year=graphene.Int(description="Filter by year"),
        benefitPlan_Id=graphene.String(description="Filter by benefit plan ID"),
    )

    # Define the monetary transfers dashboard queries
    benefits_summary_filtered = graphene.Field(
        BenefitsSummaryGQLType,
        year=graphene.Int(description="Filter by year"),
        benefitPlanUuid=graphene.String(description="Filter by benefit plan ID"),
        parentLocation=graphene.String(description="Filter by location UUID"),
        parentLocationLevel=graphene.Int(description="Location level for filtering"),
    )

    group_beneficiary_filtered = OrderedDjangoFilterConnectionField(
        GroupBeneficiaryGQLType,
        year=graphene.Int(description="Filter by year"),
        parentLocation=graphene.String(description="Filter by location UUID"),
        parentLocationLevel=graphene.Int(description="Location level for filtering"),
    )

    group_filtered = OrderedDjangoFilterConnectionField(
        GroupGQLType,
        parentLocation=graphene.String(description="Filter by location UUID"),
        parentLocationLevel=graphene.Int(description="Location level for filtering"),
    )

    location_by_benefit_plan = OrderedDjangoFilterConnectionField(
        BenefitPlanLocationGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        applyDefaultValidityFilter=graphene.Boolean(),
        client_mutation_id=graphene.String(),
        benefit_plan__id=graphene.UUID(required=False),
        search=graphene.String(),
        sort_alphabetically=graphene.Boolean(),
    )

    individual_filtered = OrderedDjangoFilterConnectionField(
        IndividualGQLType,
        parentLocation=graphene.String(description="Filter by location UUID"),
        parentLocationLevel=graphene.Int(description="Location level for filtering"),
    )

    monetary_transfer_beneficiary_data = graphene.List(
        MonetaryTransferBeneficiaryDataGQLType,
        year=graphene.Int(description="Filter by year"),
        parentLocation=graphene.String(description="Filter by location UUID"),
        parentLocationLevel=graphene.Int(description="Location level for filtering"),
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
        modules_contains=graphene.String(description="Filter by module/theme (JSON contains)"),
        category_filter=graphene.String(description="Filter by category (string match, bypasses enum)"),
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
        jsonExt_Icontains=graphene.String(description="Filter by JSON extension field content"),
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
        jsonExt_Icontains=graphene.String(description="Filter by JSON extension field content"),
    )

    tickets_by_resolution = graphene.List(
        TicketResolutionStatusGQLType,
        year=graphene.Int(description="Filter by year"),
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
        benefitPlan_Id=graphene.String(),
    )

    # Add dedicated fields for gender and minority data
    individual_male = OrderedDjangoFilterConnectionField(
        IndividualGQLType,
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
        dateCreated_Gte=graphene.DateTime(),
        dateCreated_Lte=graphene.DateTime(),
    )

    individual_female = OrderedDjangoFilterConnectionField(
        IndividualGQLType,
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
        dateCreated_Gte=graphene.DateTime(),
        dateCreated_Lte=graphene.DateTime(),
    )

    minority_households = OrderedDjangoFilterConnectionField(
        GroupGQLType,
        parent_location=graphene.String(),
        parent_location_level=graphene.Int(),
        dateCreated_Gte=graphene.DateTime(),
        dateCreated_Lte=graphene.DateTime(),
    )

    section = OrderedDjangoFilterConnectionField(
        SectionGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    indicator = OrderedDjangoFilterConnectionField(
        IndicatorGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        section_id=graphene.Int(description="Filter by section ID"),
    )

    indicator_achievement = OrderedDjangoFilterConnectionField(
        IndicatorAchievementGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        indicator_id=graphene.Int(description="Filter by indicator ID"),
        date_from=graphene.Date(description="Filter by date from"),
        date_to=graphene.Date(description="Filter by date to"),
    )

    calculate_indicator_value = graphene.Field(
        CalculateIndicatorValueResultType,
        indicator_id=graphene.Int(required=True),
        date_from=graphene.Date(),
        date_to=graphene.Date(),
        location_id=graphene.ID(),
    )

    payment_agency = OrderedDjangoFilterConnectionField(
        PaymentAgencyGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    payment_gateway_connectors = graphene.List(
        PaymentGatewayConnectorGQLType,
        description="List of available payment gateway connectors",
    )

    province_payment_agency = OrderedDjangoFilterConnectionField(
        ProvincePaymentAgencyGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    pmt_formula = OrderedDjangoFilterConnectionField(
        PmtFormulaGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    selection_quota = OrderedDjangoFilterConnectionField(
        SelectionQuotaGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    def resolve_pmt_formula(self, info, **kwargs):
        Query._check_permissions(info.context.user, SocialProtectionConfig.gql_benefit_plan_search_perms)
        return gql_optimizer.query(PmtFormula.objects.all(), info)

    pre_collecte = OrderedDjangoFilterConnectionField(
        PreCollecteGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    def resolve_selection_quota(self, info, **kwargs):
        Query._check_permissions(info.context.user, SocialProtectionConfig.gql_benefit_plan_search_perms)
        return gql_optimizer.query(SelectionQuota.objects.all(), info)

    def resolve_pre_collecte(self, info, **kwargs):
        Query._check_permissions(info.context.user, SocialProtectionConfig.gql_benefit_plan_search_perms)
        return gql_optimizer.query(PreCollecte.objects.all(), info)

    # Workflow engine queries
    workflow_templates = OrderedDjangoFilterConnectionField(
        WorkflowTemplateGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    workflow_step_templates = OrderedDjangoFilterConnectionField(
        WorkflowStepTemplateGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    grievance_workflows = OrderedDjangoFilterConnectionField(
        GrievanceWorkflowGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    grievance_tasks = OrderedDjangoFilterConnectionField(
        GrievanceTaskGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    replacement_requests = OrderedDjangoFilterConnectionField(
        ReplacementRequestGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    role_assignments = OrderedDjangoFilterConnectionField(
        RoleAssignmentGQLType,
        orderBy=graphene.List(of_type=graphene.String),
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
        queryset = Query._apply_user_location_filter(
            SensitizationTraining.objects.all(), info.context.user
        )
        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            queryset = queryset.filter(Query._get_location_filters(parent_location, parent_location_level))
        modules_contains = kwargs.get('modules_contains')
        if modules_contains:
            queryset = queryset.filter(modules__contains=[modules_contains])
        category_filter = kwargs.get('category_filter')
        if category_filter:
            queryset = queryset.filter(category__icontains=category_filter)
        return gql_optimizer.query(queryset, info)

    def resolve_behavior_change_promotion(self, info, **kwargs):
        queryset = Query._apply_user_location_filter(
            BehaviorChangePromotion.objects.all(), info.context.user
        )
        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            queryset = queryset.filter(Query._get_location_filters(parent_location, parent_location_level))
        return gql_optimizer.query(queryset, info)

    def resolve_micro_project(self, info, **kwargs):
        queryset = Query._apply_user_location_filter(
            MicroProject.objects.all(), info.context.user
        )
        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            queryset = queryset.filter(Query._get_location_filters(parent_location, parent_location_level))
        return gql_optimizer.query(queryset, info)

    def resolve_monetary_transfer(self, info, **kwargs):
        # Query._check_permissions(info.context.user, PayrollConfig.gql_payroll_search_perms)
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

        parent_location = kwargs.get('parentLocation')
        parent_location_level = kwargs.get('parentLocationLevel')
        if parent_location is not None and parent_location_level is not None:
            filters.append(
                Query._get_individual_location_filters(
                    parent_location,
                    parent_location_level,
                    prefix='individual__'))

        # Start by getting all benefit consumption data
        query = BenefitConsumption.objects.filter(
            ~Q(status__in=[BenefitConsumptionStatus.PENDING_DELETION, BenefitConsumptionStatus.DUPLICATE]))
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

        parent_location = kwargs.get('parentLocation')
        parent_location_level = kwargs.get('parentLocationLevel')
        if parent_location is not None and parent_location_level is not None:
            filters.append(
                Query._get_individual_location_filters(
                    parent_location,
                    parent_location_level,
                    prefix='individual__'))

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

        parent_location = kwargs.get('parentLocation')
        parent_location_level = kwargs.get('parentLocationLevel')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_location_filters(parent_location, parent_location_level, prefix='group__'))

        # Handle year filter
        year = kwargs.get('year')
        if year:
            filters.append(Q(date_valid_from__year__lte=year) & Q(
                Q(date_valid_to__year__gte=year) | Q(date_valid_to__isnull=True)))

        # Handle benefit plan filter
        benefit_plan_id = kwargs.get('benefitPlan_Id')
        if benefit_plan_id:
            filters.append(Q(benefit_plan_id=benefit_plan_id))

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

        parent_location = kwargs.get('parentLocation')
        parent_location_level = kwargs.get('parentLocationLevel')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_individual_location_filters(parent_location, parent_location_level))

        # Handle JSON extension field filtering
        json_ext_contains = kwargs.get("jsonExt_Icontains")
        if json_ext_contains:
            filters.append(Q(json_ext__icontains=json_ext_contains))

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

        parent_location = kwargs.get('parentLocation')
        parent_location_level = kwargs.get('parentLocationLevel')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_location_filters(parent_location, parent_location_level))

        # Handle JSON extension field filtering
        json_ext_contains = kwargs.get("jsonExt_Icontains")
        if json_ext_contains:
            filters.append(Q(json_ext__icontains=json_ext_contains))

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

    def resolve_individual_male(self, info, **kwargs):
        Query._check_permissions(info.context.user, IndividualConfig.gql_individual_search_perms)

        filters = append_validity_filter(**kwargs)

        # Always filter for male individuals
        filters.append(Q(json_ext__sexe='M'))

        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_individual_location_filters(parent_location, parent_location_level))

        query = IndividualGQLType.get_queryset(None, info)
        query = query.filter(*filters)

        return gql_optimizer.query(query, info)

    def resolve_individual_female(self, info, **kwargs):
        Query._check_permissions(info.context.user, IndividualConfig.gql_individual_search_perms)

        filters = append_validity_filter(**kwargs)

        # Always filter for female individuals
        filters.append(Q(json_ext__sexe='F'))

        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_individual_location_filters(parent_location, parent_location_level))

        query = IndividualGQLType.get_queryset(None, info)
        query = query.filter(*filters)

        return gql_optimizer.query(query, info)

    def resolve_minority_households(self, info, **kwargs):
        Query._check_permissions(info.context.user, IndividualConfig.gql_group_search_perms)

        filters = append_validity_filter(**kwargs)

        # Always filter for Mutwa households
        filters.append(Q(json_ext__menage_mutwa='OUI'))

        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')
        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_location_filters(parent_location, parent_location_level))

        query = GroupGQLType.get_queryset(None, info)
        query = query.filter(*filters).distinct()

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
            status_name = Ticket.TicketStatus(status_code).label

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
            beneficiaries = GroupBeneficiary.objects.filter(
                group__location__id__in=Location.objects.filter(
                    Q(id=province.id) | location_query).values_list('id', flat=True)
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
                'total_amount': float(total_amount) if total_amount else None,
                'beneficiaries_active': active_count,
                'beneficiaries_suspended': suspended_count,
                'beneficiaries_selected': selected_count,
            })

        return result

    def resolve_section(self, info, **kwargs):
        Query._check_permissions(info.context.user, get_merankabandi_config().gql_section_search_perms)
        return gql_optimizer.query(Section.objects.all(), info)

    def resolve_indicator(self, info, **kwargs):
        Query._check_permissions(info.context.user, get_merankabandi_config().gql_indicator_search_perms)
        query = Indicator.objects.all()

        section_id = kwargs.get("section_id")
        if section_id:
            query = query.filter(section_id=section_id)

        return gql_optimizer.query(query, info)

    def resolve_indicator_achievement(self, info, **kwargs):
        Query._check_permissions(info.context.user, get_merankabandi_config().gql_indicator_achievement_search_perms)
        query = IndicatorAchievement.objects.all()

        indicator_id = kwargs.get("indicator_id")
        if indicator_id:
            query = query.filter(indicator_id=indicator_id)

        date_from = kwargs.get("date_from")
        if date_from:
            query = query.filter(date__gte=date_from)

        date_to = kwargs.get("date_to")
        if date_to:
            query = query.filter(date__lte=date_to)

        return gql_optimizer.query(query, info)

    def resolve_calculate_indicator_value(self, info, indicator_id, date_from=None, date_to=None, location_id=None):
        from merankabandi.result_framework_service import ResultFrameworkService

        location = None
        if location_id:
            location = Location.objects.filter(id=location_id).first()

        service = ResultFrameworkService()
        result = service.calculate_indicator_value(indicator_id, date_from, date_to, location)

        breakdowns = result.get('breakdowns', [])
        breakdown_objects = [
            IndicatorBreakdownGQLType(key=b['key'], label=b['label'], value=b['value'])
            for b in breakdowns
        ] if breakdowns else []

        return CalculateIndicatorValueResultType(
            value=result.get('value', 0),
            calculation_type=result.get('calculation_type'),
            system_value=result.get('system_value'),
            manual_value=result.get('manual_value'),
            error=result.get('error'),
            date=result.get('date'),
            gender_breakdown=result.get('gender_breakdown'),
            breakdowns=breakdown_objects,
        )

    def resolve_payment_agency(self, info, **kwargs):
        return gql_optimizer.query(PaymentAgency.objects.all(), info)

    def resolve_payment_gateway_connectors(self, info, **kwargs):
        from merankabandi.payment_gateway import PaymentGatewayRegistry
        return [
            PaymentGatewayConnectorGQLType(key=c['key'], label=c['label'], class_path=c['classPath'])
            for c in PaymentGatewayRegistry.get_all()
        ]

    def resolve_province_payment_agency(self, info, **kwargs):
        return gql_optimizer.query(ProvincePaymentAgency.objects.all(), info)

    agency_fee_config = OrderedDjangoFilterConnectionField(
        AgencyFeeConfigGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    def resolve_agency_fee_config(self, info, **kwargs):
        return gql_optimizer.query(AgencyFeeConfig.objects.all(), info)

    def resolve_location_by_benefit_plan(self, info, **kwargs):
        def _build_filters(info, **kwargs):
            filters = append_validity_filter(**kwargs)
            client_mutation_id = kwargs.get("client_mutation_id")
            if client_mutation_id:
                filters.append(Q(mutations__mutation__client_mutation_id=client_mutation_id))

            benefit_plan_id = kwargs.get("benefit_plan__id")
            location_type = kwargs.get("type", "D")

            if benefit_plan_id:
                if location_type == "D":
                    filters.append(Exists(
                        GroupBeneficiary.objects.filter(
                            group__location__parent__parent=OuterRef('pk'),
                            benefit_plan_id=benefit_plan_id
                        )
                    ))
                elif location_type == "W":
                    filters.append(Exists(
                        GroupBeneficiary.objects.filter(
                            group__location__parent=OuterRef('pk'),
                            benefit_plan_id=benefit_plan_id
                        )
                    ))
                elif location_type == "V":
                    filters.append(Exists(
                        GroupBeneficiary.objects.filter(
                            group__location=OuterRef('pk'),
                            benefit_plan_id=benefit_plan_id
                        )
                    ))
            else:
                if location_type == "D":
                    filters.append(Exists(
                        GroupBeneficiary.objects.filter(
                            group__location__parent__parent=OuterRef('pk')
                        )
                    ))
                elif location_type == "W":
                    filters.append(Exists(
                        GroupBeneficiary.objects.filter(
                            group__location__parent=OuterRef('pk')
                        )
                    ))
                elif location_type == "V":
                    filters.append(Exists(
                        GroupBeneficiary.objects.filter(
                            group__location=OuterRef('pk')
                        )
                    ))

            Query._check_permissions(
                info.context.user,
                SocialProtectionConfig.gql_beneficiary_search_perms
            )
            return filters

        def _annotate_stats(query, **kwargs):
            benefit_plan_id = kwargs.get("benefit_plan__id")
            location_type = kwargs.get("type", "D")

            annotations = {}

            if location_type == "D":
                path_prefix = 'children__children__groups__groupbeneficiary'
            elif location_type == "W":
                path_prefix = 'children__groups__groupbeneficiary'
            elif location_type == "V":
                path_prefix = 'groups__groupbeneficiary'
            else:
                path_prefix = 'children__children__groups__groupbeneficiary'

            for status_name, status_value in [
                ('count_selected', BeneficiaryStatus.POTENTIAL),
                ('count_active', BeneficiaryStatus.ACTIVE),
                ('count_suspended', BeneficiaryStatus.SUSPENDED)
            ]:
                annotations[status_name] = Count(
                    path_prefix,
                    filter=Q(**{
                        f'{path_prefix}__status': status_value
                    })
                )

            annotations['count_all'] = Count(path_prefix)
            if benefit_plan_id:
                query = query.filter(**{f'{path_prefix}__benefit_plan_id': benefit_plan_id})
            return query.annotate(**annotations)

        filters = _build_filters(info, **kwargs)
        parent_location = kwargs.get('parent_location')
        parent_location_level = kwargs.get('parent_location_level')

        if parent_location is not None and parent_location_level is not None:
            filters.append(Query._get_location_filters(parent_location, parent_location_level))

        query = Location.get_queryset(None, info.context.user)
        query = query.filter(*filters)
        query = _annotate_stats(query, **kwargs)

        return gql_optimizer.query(query, info)

    @staticmethod
    def _get_user_province_ids(user):
        """
        Returns the list of province (type 'D') location IDs assigned to the user
        via UserDistrict. Returns None if the user is a superuser (no filtering needed).
        """
        if hasattr(user, '_u'):
            interactive_user = user._u
        else:
            interactive_user = user
        if interactive_user.is_superuser:
            return None
        districts = UserDistrict.get_user_districts(user)
        return [d.location_id for d in districts]

    @staticmethod
    def _apply_user_location_filter(queryset, user):
        """
        Filters a queryset of activities (with a `location` FK pointing to colline level)
        to only include records within the user's assigned provinces.
        Superusers bypass this filter.
        """
        province_ids = Query._get_user_province_ids(user)
        if province_ids is None:
            return queryset
        if not province_ids:
            return queryset.none()
        return queryset.filter(location__parent__parent__id__in=province_ids)

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


class Mutation(DashboardMutations, graphene.ObjectType):
    create_monetary_transfer = CreateMonetaryTransferMutation.Field()
    update_monetary_transfer = UpdateMonetaryTransferMutation.Field()
    delete_monetary_transfer = DeleteMonetaryTransferMutation.Field()

    # Add new mutations for Section, Indicator, and IndicatorAchievement
    create_section = CreateSectionMutation.Field()
    update_section = UpdateSectionMutation.Field()
    delete_section = DeleteSectionMutation.Field()

    create_indicator = CreateIndicatorMutation.Field()
    update_indicator = UpdateIndicatorMutation.Field()
    delete_indicator = DeleteIndicatorMutation.Field()

    create_indicator_achievement = CreateIndicatorAchievementMutation.Field()
    update_indicator_achievement = UpdateIndicatorAchievementMutation.Field()
    delete_indicator_achievement = DeleteIndicatorAchievementMutation.Field()

    # Add province payroll generation mutation
    generate_province_payroll = GenerateProvincePayrollMutation.Field()

    # PaymentAgency CRUD
    create_payment_agency = CreatePaymentAgencyMutation.Field()
    update_payment_agency = UpdatePaymentAgencyMutation.Field()
    delete_payment_agency = DeletePaymentAgencyMutation.Field()

    # ProvincePaymentAgency CRUD
    create_province_payment_agency = CreateProvincePaymentAgencyMutation.Field()
    update_province_payment_agency = UpdateProvincePaymentAgencyMutation.Field()
    delete_province_payment_agency = DeleteProvincePaymentAgencyMutation.Field()

    # AgencyFeeConfig CRUD
    create_agency_fee_config = CreateAgencyFeeConfigMutation.Field()
    update_agency_fee_config = UpdateAgencyFeeConfigMutation.Field()
    delete_agency_fee_config = DeleteAgencyFeeConfigMutation.Field()

    # Add validation mutations for KoboToolbox data
    validate_sensitization_training = ValidateSensitizationTrainingMutation.Field()
    validate_behavior_change = ValidateBehaviorChangeMutation.Field()
    validate_micro_project = ValidateMicroProjectMutation.Field()

    import_survey_data = ImportSurveyDataMutation.Field()
    trigger_pmt_calculation = TriggerPMTCalculationMutation.Field()
    bulk_update_group_beneficiary_status = BulkUpdateGroupBeneficiaryStatusMutation.Field()

    # PmtFormula CRUD
    create_pmt_formula = CreatePmtFormulaMutation.Field()
    update_pmt_formula = UpdatePmtFormulaMutation.Field()
    delete_pmt_formula = DeletePmtFormulaMutation.Field()

    # SelectionQuota CRUD
    create_selection_quota = CreateSelectionQuotaMutation.Field()
    update_selection_quota = UpdateSelectionQuotaMutation.Field()
    delete_selection_quota = DeleteSelectionQuotaMutation.Field()

    # PreCollecte CRUD
    create_pre_collecte = CreatePreCollecteMutation.Field()
    update_pre_collecte = UpdatePreCollecteMutation.Field()
    delete_pre_collecte = DeletePreCollecteMutation.Field()

    # Selection lifecycle
    apply_quota_selection = ApplyQuotaSelectionMutation.Field()
    apply_criteria_selection = ApplyCriteriaSelectionMutation.Field()
    select_all = SelectAllMutation.Field()
    promote_to_beneficiary = PromoteToBeneficiaryMutation.Field()
    promote_from_waiting_list = PromoteFromWaitingListMutation.Field()

    # Workflow engine mutations
    complete_grievance_task = CompleteGrievanceTaskMutation.Field()
    skip_grievance_task = SkipGrievanceTaskMutation.Field()
    reassign_grievance_task = ReassignGrievanceTaskMutation.Field()
    add_task_to_workflow = AddTaskToWorkflowMutation.Field()
    approve_replacement_request = ApproveReplacementRequestMutation.Field()
    reject_replacement_request = RejectReplacementRequestMutation.Field()
    create_workflow_template = CreateWorkflowTemplateMutation.Field()
    update_workflow_template = UpdateWorkflowTemplateMutation.Field()
    delete_workflow_template = DeleteWorkflowTemplateMutation.Field()
    create_role_assignment = CreateRoleAssignmentMutation.Field()
    update_role_assignment = UpdateRoleAssignmentMutation.Field()
    delete_role_assignment = DeleteRoleAssignmentMutation.Field()

    # Enhanced comment with json_ext (tags, actions)
    create_ticket_comment = CreateTicketCommentMutation.Field()

    create_workflow_step_template = CreateWorkflowStepTemplateMutation.Field()
    update_workflow_step_template = UpdateWorkflowStepTemplateMutation.Field()
    delete_workflow_step_template = DeleteWorkflowStepTemplateMutation.Field()

    # Result framework mutations
    create_result_framework_snapshot = CreateResultFrameworkSnapshotMutation.Field()
    finalize_snapshot = FinalizeSnapshotMutation.Field()
    generate_result_framework_document = GenerateResultFrameworkDocumentMutation.Field()

    # Payment schedule mutations
    create_commune_payment_round = CreateCommunePaymentRoundMutation.Field()
    create_retry_payment_round = CreateRetryPaymentRoundMutation.Field()
    sync_payment_schedule = SyncPaymentScheduleMutation.Field()

    # Batch operations (PaymentCycle workspace)
    initialize_cycle_communes = InitializeCycleCommunesMutation.Field()
    update_commune_dates_bulk = UpdateCommuneDatesBulkMutation.Field()
    batch_generate_payrolls = BatchGeneratePayrollsMutation.Field()

    # Enhanced grievance mutations with json_ext (avoids modifying upstream)
    create_ticket_comment = CreateTicketCommentMutation.Field()
    create_ticket_with_ext = CreateTicketWithExtMutation.Field()
    update_ticket_with_ext = UpdateTicketWithExtMutation.Field()
