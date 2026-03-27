"""
GraphQL Schema for Payment Reporting
Comprehensive payment analytics across MonetaryTransfer and BenefitConsumption
"""

import graphene
from .dashboard_service import DashboardService as PaymentReportingService


# GraphQL Types for Payment Reporting
class PaymentSummaryType(graphene.ObjectType):
    total_payments = graphene.Int()
    total_amount = graphene.Float()
    total_beneficiaries = graphene.Int()
    avg_payment_amount = graphene.Float()
    external_payments = graphene.Int()
    external_amount = graphene.Float()
    internal_payments = graphene.Int()
    internal_amount = graphene.Float()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()
    provinces_covered = graphene.Int()
    communes_covered = graphene.Int()
    collines_covered = graphene.Int()
    programs_active = graphene.Int()


class PaymentSourceBreakdownType(graphene.ObjectType):
    source = graphene.String()
    payment_count = graphene.Int()
    payment_amount = graphene.Float()
    beneficiary_count = graphene.Int()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()


class PaymentGenderBreakdownType(graphene.ObjectType):
    gender = graphene.String()
    payment_count = graphene.Int()
    payment_amount = graphene.Float()
    beneficiary_count = graphene.Int()


class PaymentCommunityBreakdownType(graphene.ObjectType):
    community_type = graphene.String()
    payment_count = graphene.Int()
    payment_amount = graphene.Float()
    beneficiary_count = graphene.Int()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()


class PaymentLocationDataType(graphene.ObjectType):
    province_id = graphene.Int()
    province_name = graphene.String()
    commune_id = graphene.Int()
    commune_name = graphene.String()
    colline_id = graphene.Int()
    colline_name = graphene.String()
    payment_count = graphene.Int()
    payment_amount = graphene.Float()
    beneficiary_count = graphene.Int()
    avg_payment = graphene.Float()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()


class PaymentProgramDataType(graphene.ObjectType):
    benefit_plan_id = graphene.String()
    benefit_plan_name = graphene.String()
    payment_count = graphene.Int()
    payment_amount = graphene.Float()
    beneficiary_count = graphene.Int()
    avg_payment = graphene.Float()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()
    provinces_covered = graphene.Int()


class PaymentTrendDataType(graphene.ObjectType):
    period = graphene.String()
    payment_count = graphene.Int()
    payment_amount = graphene.Float()
    beneficiary_count = graphene.Int()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()
    cumulative_amount = graphene.Float()
    cumulative_payments = graphene.Int()


class PaymentKPIType(graphene.ObjectType):
    total_disbursed = graphene.Float()
    beneficiaries_reached = graphene.Int()
    avg_payment = graphene.Float()
    female_inclusion = graphene.Float()
    twa_inclusion = graphene.Float()
    geographic_coverage = graphene.Int()
    active_programs = graphene.Int()
    external_percentage = graphene.Float()
    internal_percentage = graphene.Float()
    efficiency_score = graphene.Float()


class PaymentKPITargetsType(graphene.ObjectType):
    female_inclusion = graphene.Float()
    twa_inclusion = graphene.Float()
    efficiency_score = graphene.Float()


class PaymentReportSummaryType(graphene.ObjectType):
    summary = graphene.Field(PaymentSummaryType)
    breakdown_by_source = graphene.List(PaymentSourceBreakdownType)
    breakdown_by_gender = graphene.List(PaymentGenderBreakdownType)
    breakdown_by_community = graphene.List(PaymentCommunityBreakdownType)
    last_updated = graphene.String()


class PaymentLocationReportType(graphene.ObjectType):
    locations = graphene.List(PaymentLocationDataType)
    total = graphene.Field(lambda: PaymentTotalType)
    level = graphene.String()
    last_updated = graphene.String()


class PaymentProgramReportType(graphene.ObjectType):
    programs = graphene.List(PaymentProgramDataType)
    total = graphene.Field(lambda: PaymentTotalType)
    last_updated = graphene.String()


class PaymentTrendsReportType(graphene.ObjectType):
    trends = graphene.List(PaymentTrendDataType)
    granularity = graphene.String()
    last_updated = graphene.String()


class PaymentKPIReportType(graphene.ObjectType):
    kpis = graphene.Field(PaymentKPIType)
    targets = graphene.Field(PaymentKPITargetsType)
    last_updated = graphene.String()


class PaymentTotalType(graphene.ObjectType):
    payment_count = graphene.Int()
    payment_amount = graphene.Float()
    beneficiary_count = graphene.Int()


# Input Types for Filters
class PaymentReportFiltersInput(graphene.InputObjectType):
    # Location filters (hierarchy)
    province_id = graphene.Int(description="Filter by province ID")
    commune_id = graphene.Int(description="Filter by commune ID")
    colline_id = graphene.Int(description="Filter by colline ID")

    # Program filter
    benefit_plan_id = graphene.String(description="Filter by benefit plan UUID")

    # Time filters
    year = graphene.Int(description="Filter by year")
    month = graphene.Int(description="Filter by month (1-12)")
    start_date = graphene.String(description="Start date (YYYY-MM-DD)")
    end_date = graphene.String(description="End date (YYYY-MM-DD)")

    # Demographic filters
    gender = graphene.String(description="Filter by gender (M/F)")
    is_twa = graphene.Boolean(description="Filter TWA minority only")
    community_type = graphene.String(
        description="Filter by community type (HOST/REFUGEE)"
    )

    # Payment source filter
    payment_source = graphene.String(
        description="Filter by payment source (EXTERNAL/INTERNAL)"
    )


# GraphQL Queries for Payment Reporting
class PaymentReportingQuery(graphene.ObjectType):
    """
    High-performance payment reporting queries
    """

    # Comprehensive payment summary
    payment_report_summary = graphene.Field(
        PaymentReportSummaryType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        description="Comprehensive payment summary with all dimensions"
    )

    # Location-based analysis
    payment_by_location = graphene.Field(
        PaymentLocationReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        level=graphene.String(
            default_value="province",
            description="Location level: province, commune, or colline"
        ),
        description="Payment data aggregated by location"
    )

    # Program-based analysis
    payment_by_program = graphene.Field(
        PaymentProgramReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        description="Payment data by benefit plan/program"
    )

    # Time-based trends
    payment_trends = graphene.Field(
        PaymentTrendsReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        granularity=graphene.String(
            default_value="month",
            description="Time granularity: day, week, month, quarter, year"
        ),
        description="Payment trends over time"
    )

    # Key Performance Indicators
    payment_kpis = graphene.Field(
        PaymentKPIReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        description="Key performance indicators for payment reporting"
    )

    def resolve_payment_report_summary(self, info, filters=None):
        """Resolve comprehensive payment summary"""
        service_filters = {}
        if filters:
            service_filters = {
                'province_id': filters.get('province_id'),
                'commune_id': filters.get('commune_id'),
                'colline_id': filters.get('colline_id'),
                'benefit_plan_id': filters.get('benefit_plan_id'),
                'year': filters.get('year'),
                'month': filters.get('month'),
                'start_date': filters.get('start_date'),
                'end_date': filters.get('end_date'),
                'gender': filters.get('gender'),
                'is_twa': filters.get('is_twa'),
                'community_type': filters.get('community_type'),
                'payment_source': filters.get('payment_source'),
            }
            service_filters = {
                k: v for k, v in service_filters.items() if v is not None
            }

        data = PaymentReportingService.get_payment_summary(service_filters)

        summary = data.get('summary', {})
        result = PaymentReportSummaryType(
            summary=PaymentSummaryType(
                total_payments=summary.get('total_payments', 0),
                total_amount=summary.get('total_amount', 0),
                total_beneficiaries=summary.get('total_beneficiaries', 0),
                avg_payment_amount=summary.get('avg_payment_amount', 0),
                external_payments=summary.get('external_payments', 0),
                external_amount=summary.get('external_amount', 0),
                internal_payments=summary.get('internal_payments', 0),
                internal_amount=summary.get('internal_amount', 0),
                female_percentage=summary.get('female_percentage', 0),
                twa_percentage=summary.get('twa_percentage', 0),
                provinces_covered=summary.get('provinces_covered', 0),
                communes_covered=summary.get('communes_covered', 0),
                collines_covered=summary.get('collines_covered', 0),
                programs_active=summary.get('programs_active', 0),
            ),
            breakdown_by_source=[
                PaymentSourceBreakdownType(
                    source=item['source'],
                    payment_count=item['payment_count'],
                    payment_amount=item['payment_amount'],
                    beneficiary_count=item['beneficiary_count'],
                    female_percentage=item['female_percentage'],
                    twa_percentage=item['twa_percentage'],
                ) for item in data.get('breakdown_by_source', [])
            ],
            breakdown_by_gender=[
                PaymentGenderBreakdownType(
                    gender=item['gender'],
                    payment_count=item['payment_count'],
                    payment_amount=item['payment_amount'],
                    beneficiary_count=item['beneficiary_count'],
                ) for item in data.get('breakdown_by_gender', [])
            ],
            breakdown_by_community=[
                PaymentCommunityBreakdownType(
                    community_type=item['community_type'],
                    payment_count=item['payment_count'],
                    payment_amount=item['payment_amount'],
                    beneficiary_count=item['beneficiary_count'],
                    female_percentage=item['female_percentage'],
                    twa_percentage=item['twa_percentage'],
                ) for item in data.get('breakdown_by_community', [])
            ],
            last_updated=data.get('last_updated'),
        )

        return result

    def resolve_payment_by_location(self, info, level="province", filters=None):
        """Resolve payment data by location"""
        service_filters = {}
        if filters:
            service_filters = {
                'benefit_plan_id': filters.get('benefit_plan_id'),
                'year': filters.get('year'),
                'month': filters.get('month'),
                'payment_source': filters.get('payment_source'),
            }
            service_filters = {
                k: v for k, v in service_filters.items() if v is not None
            }

        data = PaymentReportingService.get_payment_by_location(
            service_filters, level
        )

        locations = []
        for loc in data.get('locations', []):
            location_data = PaymentLocationDataType(
                payment_count=loc['payment_count'],
                payment_amount=loc['payment_amount'],
                beneficiary_count=loc['beneficiary_count'],
                avg_payment=loc['avg_payment'],
                female_percentage=loc['female_percentage'],
                twa_percentage=loc['twa_percentage'],
            )

            if level == 'province':
                location_data.province_id = loc.get('province_id')
                location_data.province_name = loc.get('province_name')
            elif level == 'commune':
                location_data.commune_id = loc.get('commune_id')
                location_data.commune_name = loc.get('commune_name')
            elif level == 'colline':
                location_data.colline_id = loc.get('colline_id')
                location_data.colline_name = loc.get('colline_name')

            locations.append(location_data)

        total = data.get('total', {})
        result = PaymentLocationReportType(
            locations=locations,
            total=PaymentTotalType(
                payment_count=total.get('payment_count', 0),
                payment_amount=total.get('payment_amount', 0),
                beneficiary_count=total.get('beneficiary_count', 0),
            ),
            level=data.get('level', level),
            last_updated=data.get('last_updated'),
        )

        return result

    def resolve_payment_by_program(self, info, filters=None):
        """Resolve payment data by program"""
        service_filters = {}
        if filters:
            service_filters = {
                'province_id': filters.get('province_id'),
                'year': filters.get('year'),
                'month': filters.get('month'),
            }
            service_filters = {
                k: v for k, v in service_filters.items() if v is not None
            }

        data = PaymentReportingService.get_payment_by_program(service_filters)

        programs = [
            PaymentProgramDataType(
                benefit_plan_id=prog['benefit_plan_id'],
                benefit_plan_name=prog['benefit_plan_name'],
                payment_count=prog['payment_count'],
                payment_amount=prog['payment_amount'],
                beneficiary_count=prog['beneficiary_count'],
                avg_payment=prog['avg_payment'],
                female_percentage=prog['female_percentage'],
                twa_percentage=prog['twa_percentage'],
                provinces_covered=prog['provinces_covered'],
            ) for prog in data.get('programs', [])
        ]

        total = data.get('total', {})
        result = PaymentProgramReportType(
            programs=programs,
            total=PaymentTotalType(
                payment_count=total.get('payment_count', 0),
                payment_amount=total.get('payment_amount', 0),
                beneficiary_count=total.get('beneficiary_count', 0),
            ),
            last_updated=data.get('last_updated'),
        )

        return result

    def resolve_payment_trends(self, info, granularity="month", filters=None):
        """Resolve payment trends over time"""
        service_filters = {}
        if filters:
            service_filters = {
                'province_id': filters.get('province_id'),
                'benefit_plan_id': filters.get('benefit_plan_id'),
                'start_date': filters.get('start_date'),
                'end_date': filters.get('end_date'),
            }
            service_filters = {
                k: v for k, v in service_filters.items() if v is not None
            }

        data = PaymentReportingService.get_payment_trends(
            service_filters, granularity
        )

        trends = [
            PaymentTrendDataType(
                period=trend['period'],
                payment_count=trend['payment_count'],
                payment_amount=trend['payment_amount'],
                beneficiary_count=trend['beneficiary_count'],
                female_percentage=trend['female_percentage'],
                twa_percentage=trend['twa_percentage'],
                cumulative_amount=trend['cumulative_amount'],
                cumulative_payments=trend['cumulative_payments'],
            ) for trend in data.get('trends', [])
        ]

        result = PaymentTrendsReportType(
            trends=trends,
            granularity=data.get('granularity', granularity),
            last_updated=data.get('last_updated'),
        )

        return result

    def resolve_payment_kpis(self, info, filters=None):
        """Resolve payment KPIs"""
        service_filters = {}
        if filters:
            service_filters = {
                'year': filters.get('year'),
                'month': filters.get('month'),
            }
            service_filters = {
                k: v for k, v in service_filters.items() if v is not None
            }

        data = PaymentReportingService.get_payment_kpis(service_filters)

        kpis = data.get('kpis', {})
        targets = data.get('targets', {})

        result = PaymentKPIReportType(
            kpis=PaymentKPIType(
                total_disbursed=kpis.get('total_disbursed', 0),
                beneficiaries_reached=kpis.get('beneficiaries_reached', 0),
                avg_payment=kpis.get('avg_payment', 0),
                female_inclusion=kpis.get('female_inclusion', 0),
                twa_inclusion=kpis.get('twa_inclusion', 0),
                geographic_coverage=kpis.get('geographic_coverage', 0),
                active_programs=kpis.get('active_programs', 0),
                external_percentage=kpis.get('external_percentage', 0),
                internal_percentage=kpis.get('internal_percentage', 0),
                efficiency_score=kpis.get('efficiency_score', 0),
            ),
            targets=PaymentKPITargetsType(
                female_inclusion=targets.get('female_inclusion', 50.0),
                twa_inclusion=targets.get('twa_inclusion', 10.0),
                efficiency_score=targets.get('efficiency_score', 85.0),
            ),
            last_updated=data.get('last_updated'),
        )

        return result
