"""
Dashboard views using optimized JSON queries
"""
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods, cache_control
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from dashboard_json_queries import DashboardJSONQueries
from dashboard_cache import dashboard_cache
import logging

logger = logging.getLogger(__name__)

@require_http_methods(["GET"])
@dashboard_cache('household_statistics', 'dashboard_summary')
@cache_control(max_age=3600)
def household_statistics_view(request):
    """
    Get comprehensive household statistics from JSON data
    """
    try:
        stats = DashboardJSONQueries.get_household_statistics()
        
        # Calculate additional metrics
        if stats['total_households'] > 0:
            stats['vulnerability_rate'] = round(
                100 * stats['vulnerable_households'] / stats['total_households'], 2
            )
            stats['food_insecurity_rate'] = round(
                100 * stats['food_insecure'] / stats['total_households'], 2
            )
            stats['electricity_coverage'] = round(
                100 * stats['with_electricity'] / stats['total_households'], 2
            )
            stats['clean_water_access'] = round(
                100 * stats['with_tap_water'] / stats['total_households'], 2
            )
        
        return JsonResponse({
            'success': True,
            'data': stats
        })
    except Exception as e:
        logger.error(f"Error in household_statistics_view: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["GET"])
@dashboard_cache('individual_demographics', 'dashboard_summary')
@cache_control(max_age=3600)
def individual_demographics_view(request):
    """
    Get individual demographics from JSON data
    """
    try:
        demographics = DashboardJSONQueries.get_individual_demographics()
        
        # Calculate additional metrics
        if demographics['total_individuals'] > 0:
            demographics['gender_ratio'] = round(
                demographics['male_count'] / demographics['female_count'], 2
            ) if demographics['female_count'] > 0 else None
            
            demographics['literacy_rate'] = round(
                100 * demographics['literate'] / demographics['total_individuals'], 2
            )
            demographics['school_attendance_rate'] = round(
                100 * demographics['attending_school'] / demographics['total_individuals'], 2
            )
            demographics['disability_rate'] = round(
                100 * demographics['with_disability'] / demographics['total_individuals'], 2
            )
        
        return JsonResponse({
            'success': True,
            'data': demographics
        })
    except Exception as e:
        logger.error(f"Error in individual_demographics_view: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["GET"])
@dashboard_cache('payment_status', 'dashboard_summary')
@cache_control(max_age=1800)
def payment_status_view(request):
    """
    Get beneficiary payment status from JSON data
    """
    try:
        payment_stats = DashboardJSONQueries.get_beneficiary_payment_status()
        
        # Calculate success rates
        if payment_stats['has_payment_method'] > 0:
            payment_stats['payment_success_rate'] = round(
                100 * payment_stats['payment_successful'] / payment_stats['has_payment_method'], 2
            )
        
        if payment_stats['total_beneficiaries'] > 0:
            payment_stats['active_rate'] = round(
                100 * payment_stats['active_beneficiaries'] / payment_stats['total_beneficiaries'], 2
            )
            payment_stats['payment_coverage'] = round(
                100 * payment_stats['has_payment_method'] / payment_stats['total_beneficiaries'], 2
            )
        
        return JsonResponse({
            'success': True,
            'data': payment_stats
        })
    except Exception as e:
        logger.error(f"Error in payment_status_view: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["GET"])
@dashboard_cache('location_breakdown', 'location_performance')
@cache_control(max_age=7200)
def location_breakdown_view(request):
    """
    Get location-based breakdown with optional filtering
    """
    try:
        province_code = request.GET.get('province')
        commune_code = request.GET.get('commune')
        
        breakdown = DashboardJSONQueries.get_location_breakdown(
            province_code=province_code,
            commune_code=commune_code
        )
        
        # Aggregate summary
        summary = {
            'total_households': sum(item['households'] for item in breakdown),
            'total_individuals': sum(item['individuals'] for item in breakdown),
            'total_beneficiaries': sum(item['beneficiaries'] for item in breakdown),
            'total_vulnerable': sum(item['vulnerable'] for item in breakdown),
            'total_twa': sum(item['twa'] for item in breakdown),
            'locations': len(breakdown)
        }
        
        if summary['total_households'] > 0:
            summary['overall_coverage'] = round(
                100 * summary['total_beneficiaries'] / summary['total_households'], 2
            )
        
        return JsonResponse({
            'success': True,
            'summary': summary,
            'breakdown': breakdown
        })
    except Exception as e:
        logger.error(f"Error in location_breakdown_view: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)

@require_http_methods(["GET"])
@dashboard_cache('combined_dashboard', 'dashboard_summary')
@cache_control(max_age=3600)
def combined_dashboard_view(request):
    """
    Get all dashboard data in a single optimized call with filtering support
    """
    try:
        # Extract filters from query params
        filters = {
            'provinces': request.GET.get('provinces', '').split(',') if request.GET.get('provinces') else [],
            'communes': request.GET.get('communes', '').split(',') if request.GET.get('communes') else [],
            'vulnerability': request.GET.get('vulnerability', '').split(',') if request.GET.get('vulnerability') else [],
            'pmt_min': int(request.GET.get('pmt_min', 0)),
            'pmt_max': int(request.GET.get('pmt_max', 20000)),
            'date_start': request.GET.get('date_start'),
            'date_end': request.GET.get('date_end'),
            'gender': request.GET.get('gender', '').split(',') if request.GET.get('gender') else [],
            'age_groups': request.GET.get('age_groups', '').split(',') if request.GET.get('age_groups') else [],
            'status': request.GET.get('status', '').split(',') if request.GET.get('status') else [],
        }
        
        # Get all statistics with filters
        household_stats = DashboardJSONQueries.get_household_statistics(filters)
        demographics = DashboardJSONQueries.get_individual_demographics(filters)
        payment_stats = DashboardJSONQueries.get_beneficiary_payment_status(filters)
        
        # Combined summary
        dashboard_data = {
            'overview': {
                'total_households': household_stats['total_households'],
                'total_individuals': demographics['total_individuals'],
                'total_beneficiaries': payment_stats['total_beneficiaries'],
                'active_beneficiaries': payment_stats['active_beneficiaries'],
                'provinces_covered': payment_stats['provinces_covered'],
                'communes_covered': payment_stats['communes_covered']
            },
            'vulnerability': {
                'vulnerable_households': household_stats['vulnerable_households'],
                'twa_households': household_stats['twa_households'],
                'refugee_households': household_stats['refugee_households'],
                'displaced_households': household_stats['displaced_households'],
                'food_insecure': household_stats['food_insecure'],
                'avg_pmt_score': round(household_stats['avg_pmt_score'], 2) if household_stats['avg_pmt_score'] else None
            },
            'demographics': {
                'male_count': demographics['male_count'],
                'female_count': demographics['female_count'],
                'children_under_5': demographics['age_0_5'],
                'school_age': demographics['age_5_18'],
                'working_age': demographics['age_18_60'],
                'elderly': demographics['age_60_plus'],
                'with_disability': demographics['with_disability'],
                'literate': demographics['literate'],
                'attending_school': demographics['attending_school']
            },
            'living_conditions': {
                'avg_rooms': round(household_stats['avg_rooms'], 1) if household_stats['avg_rooms'] else None,
                'with_electricity': household_stats['with_electricity'],
                'with_tap_water': household_stats['with_tap_water'],
                'with_land': household_stats['with_land'],
                'with_livestock': household_stats['with_livestock'],
                'avg_meals_adults': round(household_stats['avg_meals_adults'], 1) if household_stats['avg_meals_adults'] else None
            },
            'payments': {
                'payment_assigned': payment_stats['payment_assigned'],
                'payment_successful': payment_stats['payment_successful'],
                'econet_users': payment_stats['econet_users'],
                'lumicash_users': payment_stats['lumicash_users'],
                'pmt_median': round(payment_stats['pmt_median'], 0) if payment_stats['pmt_median'] else None
            }
        }
        
        return JsonResponse({
            'success': True,
            'data': dashboard_data
        })
    except Exception as e:
        logger.error(f"Error in combined_dashboard_view: {str(e)}")
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)