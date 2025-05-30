"""
Optimized GraphQL Queries for Dashboard using Materialized Views
High-performance GraphQL resolvers that use pre-aggregated data
"""

import graphene
from graphene import ObjectType, String, Int, Float, List, Field, Boolean
from graphene_django import DjangoObjectType
from django.core.cache import cache
from datetime import datetime
from .optimized_dashboard_service import OptimizedDashboardService
from .materialized_views import MaterializedViewManager


# GraphQL Types for Dashboard Data
class GenderBreakdownType(graphene.ObjectType):
    male = graphene.Int()
    female = graphene.Int()
    twa = graphene.Int()
    total = graphene.Int()
    male_percentage = graphene.Float()
    female_percentage = graphene.Float()
    twa_percentage = graphene.Float()


class StatusBreakdownType(graphene.ObjectType):
    status = graphene.String()
    count = graphene.Int()
    percentage = graphene.Float()


class AgeBreakdownType(graphene.ObjectType):
    age_group = graphene.String()
    count = graphene.Int()
    percentage = graphene.Float()


class CommunityBreakdownType(graphene.ObjectType):
    community_type = graphene.String()
    count = graphene.Int()
    percentage = graphene.Float()


class LocationBreakdownType(graphene.ObjectType):
    province = graphene.String()
    province_id = graphene.Int()
    count = graphene.Int()
    percentage = graphene.Float()


class HouseholdBreakdownType(graphene.ObjectType):
    total_households = graphene.Int()
    total_beneficiaries = graphene.Int()


class BeneficiaryBreakdownType(graphene.ObjectType):
    gender_breakdown = graphene.Field(GenderBreakdownType)
    status_breakdown = graphene.List(StatusBreakdownType)
    age_breakdown = graphene.List(AgeBreakdownType)
    community_breakdown = graphene.List(CommunityBreakdownType)
    location_breakdown = graphene.List(LocationBreakdownType)
    household_breakdown = graphene.Field(HouseholdBreakdownType)
    last_updated = graphene.String()


class TransferMetricsType(graphene.ObjectType):
    total_planned_beneficiaries = graphene.Int()
    total_paid_beneficiaries = graphene.Int()
    total_amount_planned = graphene.Float()
    total_amount_paid = graphene.Float()
    avg_completion_rate = graphene.Float()
    avg_financial_completion_rate = graphene.Float()
    avg_female_percentage = graphene.Float()
    avg_twa_inclusion_rate = graphene.Float()


class TransferByTypeType(graphene.ObjectType):
    transfer_type = graphene.String()
    beneficiaries = graphene.Int()
    amount = graphene.Float()
    completion_rate = graphene.Float()


class TransferByLocationType(graphene.ObjectType):
    province = graphene.String()
    province_id = graphene.Int()
    beneficiaries = graphene.Int()
    amount = graphene.Float()
    completion_rate = graphene.Float()


class TransferByCommunityType(graphene.ObjectType):
    community_type = graphene.String()
    beneficiaries = graphene.Int()
    amount = graphene.Float()
    completion_rate = graphene.Float()


class TransferPerformanceType(graphene.ObjectType):
    overall_metrics = graphene.Field(TransferMetricsType)
    by_transfer_type = graphene.List(TransferByTypeType)
    by_location = graphene.List(TransferByLocationType)
    by_community = graphene.List(TransferByCommunityType)
    last_updated = graphene.String()


class QuarterlyTrendType(graphene.ObjectType):
    quarter = graphene.Int()
    year = graphene.Int()
    metric = graphene.String()
    value = graphene.Float()
    period = graphene.String()


class QuarterlyTrendsType(graphene.ObjectType):
    trends = graphene.List(QuarterlyTrendType)
    last_updated = graphene.String()


class GrievanceSummaryType(graphene.ObjectType):
    totalTickets = graphene.Int()
    openTickets = graphene.Int()
    inProgressTickets = graphene.Int()
    resolvedTickets = graphene.Int()
    closedTickets = graphene.Int()
    sensitiveTickets = graphene.Int()
    anonymousTickets = graphene.Int()
    avgResolutionDays = graphene.Float()


class GrievanceDistributionType(graphene.ObjectType):
    category = graphene.String()
    count = graphene.Int()
    percentage = graphene.Float()


class MonthlyTrendType(graphene.ObjectType):
    month = graphene.String()
    count = graphene.Int()


class RecentTicketType(graphene.ObjectType):
    id = graphene.String()
    dateOfIncident = graphene.String()
    channel = graphene.String()
    category = graphene.String()
    status = graphene.String()
    title = graphene.String()
    description = graphene.String()
    priority = graphene.String()
    flags = graphene.List(graphene.String)
    dateCreated = graphene.String()
    dateUpdated = graphene.String()
    reporterType = graphene.String()
    reporterId = graphene.String()
    reporterFirstName = graphene.String()
    reporterLastName = graphene.String()
    reporterTypeName = graphene.String()
    gender = graphene.String()


class GrievanceDashboardType(graphene.ObjectType):
    summary = graphene.Field(GrievanceSummaryType)
    status_distribution = graphene.List(GrievanceDistributionType)
    category_distribution = graphene.List(GrievanceDistributionType)
    channel_distribution = graphene.List(GrievanceDistributionType)
    priority_distribution = graphene.List(GrievanceDistributionType)
    gender_distribution = graphene.List(GrievanceDistributionType)
    age_distribution = graphene.List(GrievanceDistributionType)
    monthly_trend = graphene.List(MonthlyTrendType)
    recent_tickets = graphene.List(RecentTicketType)
    last_updated = graphene.String()


class CommunityDataType(graphene.ObjectType):
    community_type = graphene.String()
    total_beneficiaries = graphene.Int()
    active_beneficiaries = graphene.Int()
    male_beneficiaries = graphene.Int()
    female_beneficiaries = graphene.Int()
    twa_beneficiaries = graphene.Int()
    avg_female_percentage = graphene.Float()
    avg_twa_inclusion_rate = graphene.Float()
    total_transfers = graphene.Int()
    total_amount_paid = graphene.Float()
    avg_completion_rate = graphene.Float()
    total_activities = graphene.Int()
    total_activity_participants = graphene.Int()
    total_projects = graphene.Int()
    completed_projects = graphene.Int()
    total_grievances = graphene.Int()
    resolved_grievances = graphene.Int()
    avg_resolution_days = graphene.Float()
    provinces_covered = graphene.Int()
    latest_quarter = graphene.Int()
    latest_year = graphene.Int()


class DashboardSummaryType(graphene.ObjectType):
    total_beneficiaries = graphene.Int()
    total_transfers = graphene.Int()
    total_amount_paid = graphene.Float()
    avg_amount_per_beneficiary = graphene.Float()
    provinces_covered = graphene.Int()


class MasterDashboardType(graphene.ObjectType):
    summary = graphene.Field(DashboardSummaryType)
    community_breakdown = graphene.List(CommunityDataType)
    last_updated = graphene.String()


class ViewStatsType(graphene.ObjectType):
    view_name = graphene.String()
    row_count = graphene.Int()
    size_mb = graphene.Float()
    last_refresh = graphene.String()


class DashboardStatsType(graphene.ObjectType):
    views = graphene.List(ViewStatsType)
    total_views = graphene.Int()
    total_size_mb = graphene.Float()
    total_rows = graphene.Int()


class HealthCheckType(graphene.ObjectType):
    component = graphene.String()
    status = graphene.String()
    message = graphene.String()


class DashboardHealthType(graphene.ObjectType):
    status = graphene.String()
    timestamp = graphene.String()
    checks = graphene.List(HealthCheckType)


# Input Types for Filters
class DashboardFiltersInput(graphene.InputObjectType):
    start_date = graphene.String()
    end_date = graphene.String()
    province_id = graphene.Int()
    community_type = graphene.String()
    year = graphene.Int()
    benefit_plan_id = graphene.String(description="Filter by benefit plan UUID")


# Optimized GraphQL Queries
class OptimizedDashboardQuery(graphene.ObjectType):
    """
    High-performance dashboard queries using materialized views
    """
    
    # Master dashboard summary
    optimized_dashboard_summary = graphene.Field(
        MasterDashboardType,
        filters=graphene.Argument(DashboardFiltersInput),
        description="Fast dashboard summary using materialized views"
    )
    
    # Beneficiary breakdown
    optimized_beneficiary_breakdown = graphene.Field(
        BeneficiaryBreakdownType,
        filters=graphene.Argument(DashboardFiltersInput),
        description="Fast beneficiary breakdown by gender, age, location, etc."
    )
    
    # Transfer performance
    optimized_transfer_performance = graphene.Field(
        TransferPerformanceType,
        filters=graphene.Argument(DashboardFiltersInput),
        description="Fast transfer performance metrics"
    )
    
    # Quarterly trends
    optimized_quarterly_trends = graphene.Field(
        QuarterlyTrendsType,
        filters=graphene.Argument(DashboardFiltersInput),
        description="Fast quarterly trends across all programs"
    )
    
    # Grievance dashboard
    optimizedGrievanceDashboard = graphene.Field(
        GrievanceDashboardType,
        filters=graphene.Argument(DashboardFiltersInput),
        description="Fast grievance dashboard data"
    )
    
    # System stats and health
    dashboard_view_stats = graphene.Field(
        DashboardStatsType,
        description="Statistics for materialized views"
    )
    
    dashboard_health = graphene.Field(
        DashboardHealthType,
        description="Health check for dashboard system"
    )
    
    def resolve_optimized_dashboard_summary(self, info, filters=None):
        """Resolve optimized dashboard summary"""
        cache_key = f"gql_dashboard_summary_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        # Convert GraphQL filters to service format
        service_filters = {}
        if filters:
            if filters.get('start_date'):
                service_filters['start_date'] = filters['start_date']
            if filters.get('end_date'):
                service_filters['end_date'] = filters['end_date']
            if filters.get('province_id'):
                service_filters['province_id'] = filters['province_id']
            if filters.get('community_type'):
                service_filters['community_type'] = filters['community_type']
            if filters.get('year'):
                service_filters['year'] = filters['year']
        
        # Get data from optimized service
        data = OptimizedDashboardService.get_master_dashboard_summary(service_filters)
        
        # Convert to GraphQL types
        result = MasterDashboardType(
            summary=DashboardSummaryType(**data['summary']),
            community_breakdown=[
                CommunityDataType(**item) for item in data['community_breakdown']
            ],
            last_updated=data['last_updated']
        )
        
        # Cache for 5 minutes
        cache.set(cache_key, result, 300)
        return result
    
    def resolve_optimized_beneficiary_breakdown(self, info, filters=None):
        """Resolve optimized beneficiary breakdown"""
        cache_key = f"gql_beneficiary_breakdown_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        # Convert filters
        service_filters = {}
        if filters:
            if filters.get('start_date'):
                service_filters['start_date'] = filters['start_date']
            if filters.get('end_date'):
                service_filters['end_date'] = filters['end_date']
            if filters.get('province_id'):
                service_filters['province_id'] = filters['province_id']
        
        # Get data
        data = OptimizedDashboardService.get_beneficiary_breakdown(service_filters)
        
        # Convert to GraphQL types
        result = BeneficiaryBreakdownType(
            gender_breakdown=GenderBreakdownType(**data['gender_breakdown']),
            status_breakdown=[
                StatusBreakdownType(**item) for item in data['status_breakdown']
            ],
            age_breakdown=[
                AgeBreakdownType(**item) for item in data['age_breakdown']
            ],
            community_breakdown=[
                CommunityBreakdownType(**item) for item in data['community_breakdown']
            ],
            location_breakdown=[
                LocationBreakdownType(**item) for item in data['location_breakdown']
            ],
            household_breakdown=HouseholdBreakdownType(**data['household_breakdown']),
            last_updated=data['last_updated']
        )
        
        # Cache for 10 minutes
        cache.set(cache_key, result, 600)
        return result
    
    def resolve_optimized_transfer_performance(self, info, filters=None):
        """Resolve optimized transfer performance"""
        cache_key = f"gql_transfer_performance_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        # Convert filters
        service_filters = {}
        if filters:
            if filters.get('start_date'):
                service_filters['start_date'] = filters['start_date']
            if filters.get('end_date'):
                service_filters['end_date'] = filters['end_date']
            if filters.get('province_id'):
                service_filters['province_id'] = filters['province_id']
        
        # Get data
        data = OptimizedDashboardService.get_transfer_performance(service_filters)
        
        # Convert to GraphQL types
        result = TransferPerformanceType(
            overall_metrics=TransferMetricsType(**data.get('overall_metrics', {})),
            by_transfer_type=[
                TransferByTypeType(**item) for item in data.get('by_transfer_type', [])
            ],
            by_location=[
                TransferByLocationType(**item) for item in data.get('by_location', [])
            ],
            by_community=[
                TransferByCommunityType(**item) for item in data.get('by_community', [])
            ],
            last_updated=data['last_updated']
        )
        
        # Cache for 10 minutes
        cache.set(cache_key, result, 600)
        return result
    
    def resolve_optimized_quarterly_trends(self, info, filters=None):
        """Resolve optimized quarterly trends"""
        cache_key = f"gql_quarterly_trends_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        # Convert filters
        service_filters = {}
        if filters:
            if filters.get('start_date'):
                service_filters['start_date'] = filters['start_date']
            if filters.get('end_date'):
                service_filters['end_date'] = filters['end_date']
            if filters.get('province_id'):
                service_filters['province_id'] = filters['province_id']
        
        # Get data
        data = OptimizedDashboardService.get_quarterly_trends(service_filters)
        
        # Convert to GraphQL types
        result = QuarterlyTrendsType(
            trends=[
                QuarterlyTrendType(**item) for item in data['trends']
            ],
            last_updated=data['last_updated']
        )
        
        # Cache for 30 minutes
        cache.set(cache_key, result, 1800)
        return result
    
    def resolve_optimizedGrievanceDashboard(self, info, filters=None):
        """Resolve optimized grievance dashboard"""
        cache_key = f"gql_grievance_dashboard_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        # Convert filters
        service_filters = {}
        if filters:
            if filters.get('start_date'):
                service_filters['start_date'] = filters['start_date']
            if filters.get('end_date'):
                service_filters['end_date'] = filters['end_date']
        
        # Get data
        data = OptimizedDashboardService.get_grievance_dashboard(service_filters)
        
        # Convert snake_case to camelCase for summary
        summary_data = data.get('summary', {})
        summary_camel = {
            'totalTickets': summary_data.get('total_tickets', 0),
            'openTickets': summary_data.get('open_tickets', 0),
            'inProgressTickets': summary_data.get('in_progress_tickets', 0),
            'resolvedTickets': summary_data.get('resolved_tickets', 0),
            'closedTickets': summary_data.get('closed_tickets', 0),
            'sensitiveTickets': summary_data.get('sensitive_tickets', 0),
            'anonymousTickets': summary_data.get('anonymous_tickets', 0),
            'avgResolutionDays': summary_data.get('avg_resolution_days', 0.0),
        }
        
        # Convert to GraphQL types
        result = GrievanceDashboardType(
            summary=GrievanceSummaryType(**summary_camel),
            status_distribution=[
                GrievanceDistributionType(**item) for item in data.get('status_distribution', [])
            ],
            category_distribution=[
                GrievanceDistributionType(**item) for item in data.get('category_distribution', [])
            ],
            channel_distribution=[
                GrievanceDistributionType(**item) for item in data.get('channel_distribution', [])
            ],
            priority_distribution=[
                GrievanceDistributionType(**item) for item in data.get('priority_distribution', [])
            ],
            gender_distribution=[
                GrievanceDistributionType(**item) for item in data.get('gender_distribution', [])
            ],
            age_distribution=[
                GrievanceDistributionType(**item) for item in data.get('age_distribution', [])
            ],
            monthly_trend=[
                MonthlyTrendType(**item) for item in data.get('monthly_trend', [])
            ],
            recent_tickets=[
                RecentTicketType(**item) for item in data.get('recent_tickets', [])
            ],
            last_updated=data['last_updated']
        )
        
        # Cache for 5 minutes
        cache.set(cache_key, result, 300)
        return result
    
    def resolve_dashboard_view_stats(self, info):
        """Resolve dashboard view statistics"""
        cache_key = "gql_dashboard_stats"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        try:
            stats = MaterializedViewManager.get_view_stats()
            
            formatted_stats = []
            total_size = 0
            total_rows = 0
            
            for view_name, row_count, size_mb, last_refresh in stats:
                formatted_stats.append(ViewStatsType(
                    view_name=view_name,
                    row_count=row_count,
                    size_mb=float(size_mb) if size_mb else 0,
                    last_refresh=last_refresh.isoformat() if last_refresh else None
                ))
                total_size += size_mb or 0
                total_rows += row_count or 0
            
            result = DashboardStatsType(
                views=formatted_stats,
                total_views=len(formatted_stats),
                total_size_mb=float(total_size),
                total_rows=total_rows
            )
            
            # Cache for 1 hour
            cache.set(cache_key, result, 3600)
            return result
            
        except Exception as e:
            return DashboardStatsType(
                views=[],
                total_views=0,
                total_size_mb=0.0,
                total_rows=0
            )
    
    def resolve_dashboard_health(self, info):
        """Resolve dashboard health check"""
        cache_key = "gql_dashboard_health"
        cached_data = cache.get(cache_key)
        
        if cached_data:
            return cached_data
        
        try:
            # Perform health checks
            checks = []
            
            # Check materialized views
            try:
                stats = MaterializedViewManager.get_view_stats()
                if stats:
                    views_with_data = sum(1 for _, row_count, _, _ in stats if row_count and row_count > 0)
                    total_views = len(stats)
                    
                    if views_with_data == total_views:
                        checks.append(HealthCheckType(
                            component="materialized_views",
                            status="healthy",
                            message=f"All {total_views} views have data"
                        ))
                    else:
                        checks.append(HealthCheckType(
                            component="materialized_views",
                            status="degraded",
                            message=f"Only {views_with_data}/{total_views} views have data"
                        ))
                else:
                    checks.append(HealthCheckType(
                        component="materialized_views",
                        status="unhealthy",
                        message="No materialized views found"
                    ))
            except Exception as e:
                checks.append(HealthCheckType(
                    component="materialized_views",
                    status="unhealthy",
                    message=f"Error checking views: {e}"
                ))
            
            # Check cache
            try:
                test_key = 'gql_health_check'
                test_value = datetime.now().isoformat()
                cache.set(test_key, test_value, 60)
                cached_value = cache.get(test_key)
                
                if cached_value == test_value:
                    checks.append(HealthCheckType(
                        component="cache",
                        status="healthy",
                        message="Cache read/write successful"
                    ))
                else:
                    checks.append(HealthCheckType(
                        component="cache",
                        status="degraded",
                        message="Cache read/write failed"
                    ))
            except Exception as e:
                checks.append(HealthCheckType(
                    component="cache",
                    status="unhealthy",
                    message=f"Cache error: {e}"
                ))
            
            # Check database
            try:
                from django.db import connection
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    
                    if result and result[0] == 1:
                        checks.append(HealthCheckType(
                            component="database",
                            status="healthy",
                            message="Database connectivity OK"
                        ))
                    else:
                        checks.append(HealthCheckType(
                            component="database",
                            status="unhealthy",
                            message="Database query failed"
                        ))
            except Exception as e:
                checks.append(HealthCheckType(
                    component="database",
                    status="unhealthy",
                    message=f"Database error: {e}"
                ))
            
            # Overall status
            all_healthy = all(check.status == "healthy" for check in checks)
            overall_status = "healthy" if all_healthy else "degraded"
            
            result = DashboardHealthType(
                status=overall_status,
                timestamp=datetime.now().isoformat(),
                checks=checks
            )
            
            # Cache for 5 minutes
            cache.set(cache_key, result, 300)
            return result
            
        except Exception as e:
            return DashboardHealthType(
                status="unhealthy",
                timestamp=datetime.now().isoformat(),
                checks=[HealthCheckType(
                    component="system",
                    status="unhealthy",
                    message=f"System error: {e}"
                )]
            )