"""
GraphQL Schema for Payment Reporting
Comprehensive payment analytics across MonetaryTransfer and BenefitConsumption
"""

import graphene
from graphene import ObjectType, String, Int, Float, List, Field, Boolean
from django.core.cache import cache
from datetime import datetime
from .payment_reporting_service import PaymentReportingService


# GraphQL Types for Payment Reporting
class PaymentSummaryType(graphene.ObjectType):
    totalPayments = graphene.Int()
    totalAmount = graphene.Float()
    totalBeneficiaries = graphene.Int()
    avgPaymentAmount = graphene.Float()
    externalPayments = graphene.Int()
    externalAmount = graphene.Float()
    internalPayments = graphene.Int()
    internalAmount = graphene.Float()
    femalePercentage = graphene.Float()
    twaPercentage = graphene.Float()
    provincesCovered = graphene.Int()
    communesCovered = graphene.Int()
    collinesCovered = graphene.Int()
    programsActive = graphene.Int()


class PaymentSourceBreakdownType(graphene.ObjectType):
    source = graphene.String()
    paymentCount = graphene.Int()
    paymentAmount = graphene.Float()
    beneficiaryCount = graphene.Int()
    femalePercentage = graphene.Float()
    twaPercentage = graphene.Float()


class PaymentGenderBreakdownType(graphene.ObjectType):
    gender = graphene.String()
    paymentCount = graphene.Int()
    paymentAmount = graphene.Float()
    beneficiaryCount = graphene.Int()


class PaymentCommunityBreakdownType(graphene.ObjectType):
    communityType = graphene.String()
    paymentCount = graphene.Int()
    paymentAmount = graphene.Float()
    beneficiaryCount = graphene.Int()
    femalePercentage = graphene.Float()
    twaPercentage = graphene.Float()


class PaymentLocationDataType(graphene.ObjectType):
    provinceId = graphene.Int()
    provinceName = graphene.String()
    communeId = graphene.Int()
    communeName = graphene.String()
    collineId = graphene.Int()
    collineName = graphene.String()
    paymentCount = graphene.Int()
    paymentAmount = graphene.Float()
    beneficiaryCount = graphene.Int()
    avgPayment = graphene.Float()
    femalePercentage = graphene.Float()
    twaPercentage = graphene.Float()


class PaymentProgramDataType(graphene.ObjectType):
    benefitPlanId = graphene.String()
    benefitPlanName = graphene.String()
    paymentCount = graphene.Int()
    paymentAmount = graphene.Float()
    beneficiaryCount = graphene.Int()
    avgPayment = graphene.Float()
    femalePercentage = graphene.Float()
    twaPercentage = graphene.Float()
    provincesCovered = graphene.Int()


class PaymentTrendDataType(graphene.ObjectType):
    period = graphene.String()
    paymentCount = graphene.Int()
    paymentAmount = graphene.Float()
    beneficiaryCount = graphene.Int()
    femalePercentage = graphene.Float()
    twaPercentage = graphene.Float()
    cumulativeAmount = graphene.Float()
    cumulativePayments = graphene.Int()


class PaymentKPIType(graphene.ObjectType):
    totalDisbursed = graphene.Float()
    beneficiariesReached = graphene.Int()
    avgPayment = graphene.Float()
    femaleInclusion = graphene.Float()
    twaInclusion = graphene.Float()
    geographicCoverage = graphene.Int()
    activePrograms = graphene.Int()
    externalPercentage = graphene.Float()
    internalPercentage = graphene.Float()
    efficiencyScore = graphene.Float()


class PaymentKPITargetsType(graphene.ObjectType):
    femaleInclusion = graphene.Float()
    twaInclusion = graphene.Float()
    efficiencyScore = graphene.Float()


class PaymentReportSummaryType(graphene.ObjectType):
    summary = graphene.Field(PaymentSummaryType)
    breakdownBySource = graphene.List(PaymentSourceBreakdownType)
    breakdownByGender = graphene.List(PaymentGenderBreakdownType)
    breakdownByCommunity = graphene.List(PaymentCommunityBreakdownType)
    lastUpdated = graphene.String()


class PaymentLocationReportType(graphene.ObjectType):
    locations = graphene.List(PaymentLocationDataType)
    total = graphene.Field(lambda: PaymentTotalType)
    level = graphene.String()
    lastUpdated = graphene.String()


class PaymentProgramReportType(graphene.ObjectType):
    programs = graphene.List(PaymentProgramDataType)
    total = graphene.Field(lambda: PaymentTotalType)
    lastUpdated = graphene.String()


class PaymentTrendsReportType(graphene.ObjectType):
    trends = graphene.List(PaymentTrendDataType)
    granularity = graphene.String()
    lastUpdated = graphene.String()


class PaymentKPIReportType(graphene.ObjectType):
    kpis = graphene.Field(PaymentKPIType)
    targets = graphene.Field(PaymentKPITargetsType)
    lastUpdated = graphene.String()


class PaymentTotalType(graphene.ObjectType):
    paymentCount = graphene.Int()
    paymentAmount = graphene.Float()
    beneficiaryCount = graphene.Int()


# Input Types for Filters
class PaymentReportFiltersInput(graphene.InputObjectType):
    # Location filters (hierarchy)
    provinceId = graphene.Int(description="Filter by province ID")
    communeId = graphene.Int(description="Filter by commune ID")
    collineId = graphene.Int(description="Filter by colline ID")
    
    # Program filter
    benefitPlanId = graphene.String(description="Filter by benefit plan UUID")
    
    # Time filters
    year = graphene.Int(description="Filter by year")
    month = graphene.Int(description="Filter by month (1-12)")
    startDate = graphene.String(description="Start date (YYYY-MM-DD)")
    endDate = graphene.String(description="End date (YYYY-MM-DD)")
    
    # Demographic filters
    gender = graphene.String(description="Filter by gender (M/F)")
    isTwa = graphene.Boolean(description="Filter TWA minority only")
    communityType = graphene.String(description="Filter by community type (HOST/REFUGEE)")
    
    # Payment source filter
    paymentSource = graphene.String(description="Filter by payment source (EXTERNAL/INTERNAL)")


# GraphQL Queries for Payment Reporting
class PaymentReportingQuery(graphene.ObjectType):
    """
    High-performance payment reporting queries
    """
    
    # Comprehensive payment summary
    paymentReportSummary = graphene.Field(
        PaymentReportSummaryType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        description="Comprehensive payment summary with all dimensions"
    )
    
    # Location-based analysis
    paymentByLocation = graphene.Field(
        PaymentLocationReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        level=graphene.String(default_value="province", description="Location level: province, commune, or colline"),
        description="Payment data aggregated by location"
    )
    
    # Program-based analysis
    paymentByProgram = graphene.Field(
        PaymentProgramReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        description="Payment data by benefit plan/program"
    )
    
    # Time-based trends
    paymentTrends = graphene.Field(
        PaymentTrendsReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        granularity=graphene.String(default_value="month", description="Time granularity: day, week, month, quarter, year"),
        description="Payment trends over time"
    )
    
    # Key Performance Indicators
    paymentKPIs = graphene.Field(
        PaymentKPIReportType,
        filters=graphene.Argument(PaymentReportFiltersInput),
        description="Key performance indicators for payment reporting"
    )
    
    def resolve_paymentReportSummary(self, info, filters=None):
        """Resolve comprehensive payment summary"""
        # Convert GraphQL filters to service format
        service_filters = {}
        if filters:
            service_filters = {
                'province_id': filters.get('provinceId'),
                'commune_id': filters.get('communeId'),
                'colline_id': filters.get('collineId'),
                'benefit_plan_id': filters.get('benefitPlanId'),
                'year': filters.get('year'),
                'month': filters.get('month'),
                'start_date': filters.get('startDate'),
                'end_date': filters.get('endDate'),
                'gender': filters.get('gender'),
                'is_twa': filters.get('isTwa'),
                'community_type': filters.get('communityType'),
                'payment_source': filters.get('paymentSource'),
            }
            # Remove None values
            service_filters = {k: v for k, v in service_filters.items() if v is not None}
        
        # Get data from service
        data = PaymentReportingService.get_payment_summary(service_filters)
        
        # Convert to GraphQL types
        summary = data.get('summary', {})
        result = PaymentReportSummaryType(
            summary=PaymentSummaryType(
                totalPayments=summary.get('total_payments', 0),
                totalAmount=summary.get('total_amount', 0),
                totalBeneficiaries=summary.get('total_beneficiaries', 0),
                avgPaymentAmount=summary.get('avg_payment_amount', 0),
                externalPayments=summary.get('external_payments', 0),
                externalAmount=summary.get('external_amount', 0),
                internalPayments=summary.get('internal_payments', 0),
                internalAmount=summary.get('internal_amount', 0),
                femalePercentage=summary.get('female_percentage', 0),
                twaPercentage=summary.get('twa_percentage', 0),
                provincesCovered=summary.get('provinces_covered', 0),
                communesCovered=summary.get('communes_covered', 0),
                collinesCovered=summary.get('collines_covered', 0),
                programsActive=summary.get('programs_active', 0),
            ),
            breakdownBySource=[
                PaymentSourceBreakdownType(
                    source=item['source'],
                    paymentCount=item['payment_count'],
                    paymentAmount=item['payment_amount'],
                    beneficiaryCount=item['beneficiary_count'],
                    femalePercentage=item['female_percentage'],
                    twaPercentage=item['twa_percentage'],
                ) for item in data.get('breakdown_by_source', [])
            ],
            breakdownByGender=[
                PaymentGenderBreakdownType(
                    gender=item['gender'],
                    paymentCount=item['payment_count'],
                    paymentAmount=item['payment_amount'],
                    beneficiaryCount=item['beneficiary_count'],
                ) for item in data.get('breakdown_by_gender', [])
            ],
            breakdownByCommunity=[
                PaymentCommunityBreakdownType(
                    communityType=item['community_type'],
                    paymentCount=item['payment_count'],
                    paymentAmount=item['payment_amount'],
                    beneficiaryCount=item['beneficiary_count'],
                    femalePercentage=item['female_percentage'],
                    twaPercentage=item['twa_percentage'],
                ) for item in data.get('breakdown_by_community', [])
            ],
            lastUpdated=data.get('last_updated'),
        )
        
        return result
    
    def resolve_paymentByLocation(self, info, level="province", filters=None):
        """Resolve payment data by location"""
        # Convert filters
        service_filters = {}
        if filters:
            service_filters = {
                'benefit_plan_id': filters.get('benefitPlanId'),
                'year': filters.get('year'),
                'month': filters.get('month'),
                'payment_source': filters.get('paymentSource'),
            }
            service_filters = {k: v for k, v in service_filters.items() if v is not None}
        
        # Get data from service
        data = PaymentReportingService.get_payment_by_location(service_filters, level)
        
        # Convert to GraphQL types
        locations = []
        for loc in data.get('locations', []):
            location_data = PaymentLocationDataType(
                paymentCount=loc['payment_count'],
                paymentAmount=loc['payment_amount'],
                beneficiaryCount=loc['beneficiary_count'],
                avgPayment=loc['avg_payment'],
                femalePercentage=loc['female_percentage'],
                twaPercentage=loc['twa_percentage'],
            )
            
            # Set location IDs and names based on level
            if level == 'province':
                location_data.provinceId = loc.get('province_id')
                location_data.provinceName = loc.get('province_name')
            elif level == 'commune':
                location_data.communeId = loc.get('commune_id')
                location_data.communeName = loc.get('commune_name')
            elif level == 'colline':
                location_data.collineId = loc.get('colline_id')
                location_data.collineName = loc.get('colline_name')
            
            locations.append(location_data)
        
        total = data.get('total', {})
        result = PaymentLocationReportType(
            locations=locations,
            total=PaymentTotalType(
                paymentCount=total.get('payment_count', 0),
                paymentAmount=total.get('payment_amount', 0),
                beneficiaryCount=total.get('beneficiary_count', 0),
            ),
            level=data.get('level', level),
            lastUpdated=data.get('last_updated'),
        )
        
        return result
    
    def resolve_paymentByProgram(self, info, filters=None):
        """Resolve payment data by program"""
        # Convert filters
        service_filters = {}
        if filters:
            service_filters = {
                'province_id': filters.get('provinceId'),
                'year': filters.get('year'),
                'month': filters.get('month'),
            }
            service_filters = {k: v for k, v in service_filters.items() if v is not None}
        
        # Get data from service
        data = PaymentReportingService.get_payment_by_program(service_filters)
        
        # Convert to GraphQL types
        programs = [
            PaymentProgramDataType(
                benefitPlanId=prog['benefit_plan_id'],
                benefitPlanName=prog['benefit_plan_name'],
                paymentCount=prog['payment_count'],
                paymentAmount=prog['payment_amount'],
                beneficiaryCount=prog['beneficiary_count'],
                avgPayment=prog['avg_payment'],
                femalePercentage=prog['female_percentage'],
                twaPercentage=prog['twa_percentage'],
                provincesCovered=prog['provinces_covered'],
            ) for prog in data.get('programs', [])
        ]
        
        total = data.get('total', {})
        result = PaymentProgramReportType(
            programs=programs,
            total=PaymentTotalType(
                paymentCount=total.get('payment_count', 0),
                paymentAmount=total.get('payment_amount', 0),
                beneficiaryCount=total.get('beneficiary_count', 0),
            ),
            lastUpdated=data.get('last_updated'),
        )
        
        return result
    
    def resolve_paymentTrends(self, info, granularity="month", filters=None):
        """Resolve payment trends over time"""
        # Convert filters
        service_filters = {}
        if filters:
            service_filters = {
                'province_id': filters.get('provinceId'),
                'benefit_plan_id': filters.get('benefitPlanId'),
                'start_date': filters.get('startDate'),
                'end_date': filters.get('endDate'),
            }
            service_filters = {k: v for k, v in service_filters.items() if v is not None}
        
        # Get data from service
        data = PaymentReportingService.get_payment_trends(service_filters, granularity)
        
        # Convert to GraphQL types
        trends = [
            PaymentTrendDataType(
                period=trend['period'],
                paymentCount=trend['payment_count'],
                paymentAmount=trend['payment_amount'],
                beneficiaryCount=trend['beneficiary_count'],
                femalePercentage=trend['female_percentage'],
                twaPercentage=trend['twa_percentage'],
                cumulativeAmount=trend['cumulative_amount'],
                cumulativePayments=trend['cumulative_payments'],
            ) for trend in data.get('trends', [])
        ]
        
        result = PaymentTrendsReportType(
            trends=trends,
            granularity=data.get('granularity', granularity),
            lastUpdated=data.get('last_updated'),
        )
        
        return result
    
    def resolve_paymentKPIs(self, info, filters=None):
        """Resolve payment KPIs"""
        # Convert filters
        service_filters = {}
        if filters:
            service_filters = {
                'year': filters.get('year'),
                'month': filters.get('month'),
            }
            service_filters = {k: v for k, v in service_filters.items() if v is not None}
        
        # Get data from service
        data = PaymentReportingService.get_payment_kpis(service_filters)
        
        # Convert to GraphQL types
        kpis = data.get('kpis', {})
        targets = data.get('targets', {})
        
        result = PaymentKPIReportType(
            kpis=PaymentKPIType(
                totalDisbursed=kpis.get('total_disbursed', 0),
                beneficiariesReached=kpis.get('beneficiaries_reached', 0),
                avgPayment=kpis.get('avg_payment', 0),
                femaleInclusion=kpis.get('female_inclusion', 0),
                twaInclusion=kpis.get('twa_inclusion', 0),
                geographicCoverage=kpis.get('geographic_coverage', 0),
                activePrograms=kpis.get('active_programs', 0),
                externalPercentage=kpis.get('external_percentage', 0),
                internalPercentage=kpis.get('internal_percentage', 0),
                efficiencyScore=kpis.get('efficiency_score', 0),
            ),
            targets=PaymentKPITargetsType(
                femaleInclusion=targets.get('female_inclusion', 50.0),
                twaInclusion=targets.get('twa_inclusion', 10.0),
                efficiencyScore=targets.get('efficiency_score', 85.0),
            ),
            lastUpdated=data.get('last_updated'),
        )
        
        return result