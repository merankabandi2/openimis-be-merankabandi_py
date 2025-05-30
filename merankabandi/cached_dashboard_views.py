from django.http import JsonResponse
from django.db.models import Count, Q, Sum, Avg, F, Value
from django.db.models.functions import TruncMonth, TruncQuarter
from django.views.decorators.http import cache_control, etag
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime, timedelta
import hashlib

from individual.models import Individual
from social_protection.models import BenefitPlan, Beneficiary
from .dashboard_views import *  # Import existing views
from dashboard_cache import dashboard_cache

# Apply caching to existing views
@dashboard_cache('beneficiary_breakdown', 'beneficiary_breakdown')
@cache_control(max_age=86400, must_revalidate=True)
def cached_beneficiary_breakdown_api(request):
    return beneficiary_breakdown_api(request)

@dashboard_cache('location_performance', 'location_performance')
@cache_control(max_age=7200, must_revalidate=True)
def cached_location_performance_api(request):
    return location_performance_api(request)

@dashboard_cache('dashboard_summary', 'dashboard_summary')
@cache_control(max_age=3600, must_revalidate=True)
def cached_dashboard_summary_api(request):
    return dashboard_summary_api(request)

@dashboard_cache('quarterly_rollup', 'quarterly_rollup')
@cache_control(max_age=86400, must_revalidate=True)
def cached_quarterly_rollup_api(request):
    return quarterly_rollup_api(request)

# Optimized query with select_related and prefetch_related
def get_optimized_beneficiaries_queryset():
    """
    Returns an optimized queryset for beneficiaries with related data pre-fetched
    """
    return Beneficiary.objects.select_related(
        'individual',
        'benefit_plan',
        'individual__family__location'
    ).prefetch_related(
        'individual__family__members'
    ).filter(
        is_deleted=False,
        status='ACTIVE'
    )

# Pre-aggregated dashboard data
@dashboard_cache('pre_aggregated_summary', 'dashboard_summary')
def get_pre_aggregated_summary(request):
    """
    Returns pre-aggregated summary data for dashboard
    """
    cache_key = 'dashboard_pre_aggregated_summary'
    cached_data = cache.get(cache_key)
    
    if cached_data:
        return JsonResponse(cached_data)
    
    # Use database aggregation for better performance
    summary = {
        'total_beneficiaries': Beneficiary.objects.filter(
            is_deleted=False,
            status='ACTIVE'
        ).count(),
        
        'gender_breakdown': list(
            Individual.objects.filter(
                beneficiary__is_deleted=False,
                beneficiary__status='ACTIVE'
            ).values('gender').annotate(
                count=Count('id')
            ).order_by('gender')
        ),
        
        'location_summary': list(
            Individual.objects.filter(
                beneficiary__is_deleted=False,
                beneficiary__status='ACTIVE'
            ).values(
                province=F('family__location__parent__parent__name'),
                commune=F('family__location__parent__name')
            ).annotate(
                count=Count('id')
            ).order_by('-count')[:10]  # Top 10 locations
        ),
        
        'recent_enrollments': Beneficiary.objects.filter(
            is_deleted=False,
            created_date__gte=timezone.now() - timedelta(days=30)
        ).count(),
        
        'timestamp': timezone.now().isoformat()
    }
    
    # Cache for 1 hour
    cache.set(cache_key, summary, 3600)
    
    return JsonResponse(summary)

# Cache warming task
def warm_dashboard_cache():
    """
    Pre-populate cache with dashboard data
    Can be called by a Celery task or management command
    """
    from django.test import RequestFactory
    
    factory = RequestFactory()
    request = factory.get('/api/dashboard/')
    
    # Warm up all dashboard endpoints
    endpoints = [
        cached_dashboard_summary_api,
        cached_beneficiary_breakdown_api,
        cached_location_performance_api,
        cached_quarterly_rollup_api,
        get_pre_aggregated_summary,
    ]
    
    for endpoint in endpoints:
        try:
            endpoint(request)
        except Exception as e:
            print(f"Error warming cache for {endpoint.__name__}: {str(e)}")
    
    return True