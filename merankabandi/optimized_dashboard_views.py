"""
Optimized Dashboard Views using Materialized Views
High-performance API endpoints for dashboard data
"""

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from django.views.decorators.cache import cache_page
from django.views.decorators.vary import vary_on_headers
from django.utils.decorators import method_decorator
from django.views import View
from django.core.cache import cache
import json
from datetime import datetime, date
from .optimized_dashboard_service import OptimizedDashboardService
from .views_manager import MaterializedViewsManager


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


# Cache decorators
five_minute_cache = cache_page(60 * 5)  # 5 minutes
ten_minute_cache = cache_page(60 * 10)  # 10 minutes
thirty_minute_cache = cache_page(60 * 30)  # 30 minutes


def parse_filters(request):
    """Parse common filters from request parameters"""
    filters = {}
    
    if request.GET.get('start_date'):
        filters['start_date'] = request.GET.get('start_date')
    if request.GET.get('end_date'):
        filters['end_date'] = request.GET.get('end_date')
    if request.GET.get('province_id'):
        filters['province_id'] = int(request.GET.get('province_id'))
    if request.GET.get('community_type'):
        filters['community_type'] = request.GET.get('community_type')
    if request.GET.get('year'):
        filters['year'] = int(request.GET.get('year'))
    
    return filters


@api_view(['GET'])
@permission_classes([IsAuthenticated])
@five_minute_cache
@vary_on_headers('Authorization')
def optimized_dashboard_summary(request):
    """
    Fast dashboard summary using materialized views
    GET /api/merankabandi/dashboard/optimized/summary/
    """
    try:
        filters = parse_filters(request)
        data = OptimizedDashboardService.get_master_dashboard_summary(filters)
        
        return Response({
            'success': True,
            'data': data,
            'cached': True,
            'source': 'materialized_views'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e),
            'source': 'materialized_views'
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
@ten_minute_cache
@vary_on_headers('Authorization')
def optimized_beneficiary_breakdown(request):
    """
    Fast beneficiary breakdown using materialized views
    GET /api/merankabandi/dashboard/optimized/beneficiary-breakdown/
    """
    try:
        filters = parse_filters(request)
        data = OptimizedDashboardService.get_beneficiary_breakdown(filters)
        
        return Response({
            'success': True,
            'data': data,
            'cached': True,
            'source': 'materialized_views'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
@ten_minute_cache
@vary_on_headers('Authorization')
def optimized_transfer_performance(request):
    """
    Fast transfer performance metrics
    GET /api/merankabandi/dashboard/optimized/transfer-performance/
    """
    try:
        filters = parse_filters(request)
        data = OptimizedDashboardService.get_transfer_performance(filters)
        
        return Response({
            'success': True,
            'data': data,
            'cached': True,
            'source': 'materialized_views'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
@thirty_minute_cache
@vary_on_headers('Authorization')
def optimized_quarterly_trends(request):
    """
    Fast quarterly trends across all programs
    GET /api/merankabandi/dashboard/optimized/quarterly-trends/
    """
    try:
        filters = parse_filters(request)
        data = OptimizedDashboardService.get_quarterly_trends(filters)
        
        return Response({
            'success': True,
            'data': data,
            'cached': True,
            'source': 'materialized_views'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
@five_minute_cache
@vary_on_headers('Authorization')
def optimized_grievance_dashboard(request):
    """
    Fast grievance dashboard data
    GET /api/merankabandi/dashboard/optimized/grievances/
    """
    try:
        filters = parse_filters(request)
        data = OptimizedDashboardService.get_grievance_dashboard(filters)
        
        return Response({
            'success': True,
            'data': data,
            'cached': True,
            'source': 'materialized_views'
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def refresh_dashboard_views(request):
    """
    Manually refresh materialized views
    POST /api/merankabandi/dashboard/optimized/refresh/
    """
    try:
        view_name = request.data.get('view_name')
        concurrent = request.data.get('concurrent', True)
        
        if view_name:
            MaterializedViewsManager.refresh_single_view(view_name, concurrent)
            message = f"Refreshed view: {view_name}"
        else:
            MaterializedViewsManager.refresh_all_views(category=None, concurrent=concurrent)
            message = "Refreshed all dashboard views"
        
        # Clear cache after refresh
        OptimizedDashboardService.clear_cache()
        
        return Response({
            'success': True,
            'message': message,
            'timestamp': datetime.now().isoformat()
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def dashboard_view_stats(request):
    """
    Get statistics for dashboard materialized views
    GET /api/merankabandi/dashboard/optimized/stats/
    """
    try:
        stats = MaterializedViewsManager.get_view_stats()
        
        formatted_stats = []
        for view_name, row_count, size_mb, last_refresh in stats:
            formatted_stats.append({
                'view_name': view_name,
                'row_count': row_count,
                'size_mb': float(size_mb) if size_mb else 0,
                'last_refresh': last_refresh.isoformat() if last_refresh else None
            })
        
        return Response({
            'success': True,
            'data': {
                'views': formatted_stats,
                'total_views': len(formatted_stats),
                'total_size_mb': sum(s['size_mb'] for s in formatted_stats),
                'total_rows': sum(s['row_count'] for s in formatted_stats if s['row_count'])
            }
        }, status=status.HTTP_200_OK)
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(cache_page(60 * 60), name='get')  # 1 hour cache
class OptimizedDashboardHealthView(View):
    """
    Dashboard health check endpoint
    GET /api/merankabandi/dashboard/optimized/health/
    """
    
    def get(self, request):
        """Check health of dashboard system"""
        try:
            health_data = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'checks': {
                    'materialized_views': self.check_materialized_views(),
                    'cache': self.check_cache(),
                    'database': self.check_database()
                }
            }
            
            # Overall health status
            all_healthy = all(
                check['status'] == 'healthy' 
                for check in health_data['checks'].values()
            )
            health_data['status'] = 'healthy' if all_healthy else 'degraded'
            
            status_code = 200 if all_healthy else 503
            
            return JsonResponse(health_data, status=status_code, encoder=DateTimeEncoder)
            
        except Exception as e:
            return JsonResponse({
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }, status=500, encoder=DateTimeEncoder)
    
    def check_materialized_views(self):
        """Check materialized views health"""
        try:
            stats = MaterializedViewsManager.get_view_stats()
            
            if not stats:
                return {
                    'status': 'unhealthy',
                    'message': 'No materialized views found'
                }
            
            views_with_data = sum(1 for _, row_count, _, _ in stats if row_count and row_count > 0)
            total_views = len(stats)
            
            if views_with_data == total_views:
                return {
                    'status': 'healthy',
                    'message': f'All {total_views} views have data',
                    'views_count': total_views
                }
            else:
                return {
                    'status': 'degraded',
                    'message': f'Only {views_with_data}/{total_views} views have data',
                    'views_count': total_views
                }
                
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Error checking views: {e}'
            }
    
    def check_cache(self):
        """Check cache health"""
        try:
            # Test cache read/write
            test_key = 'dashboard_health_check'
            test_value = datetime.now().isoformat()
            
            cache.set(test_key, test_value, 60)
            cached_value = cache.get(test_key)
            
            if cached_value == test_value:
                return {
                    'status': 'healthy',
                    'message': 'Cache read/write successful'
                }
            else:
                return {
                    'status': 'degraded',
                    'message': 'Cache read/write failed'
                }
                
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Cache error: {e}'
            }
    
    def check_database(self):
        """Check database connectivity"""
        try:
            from django.db import connection
            
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                
                if result and result[0] == 1:
                    return {
                        'status': 'healthy',
                        'message': 'Database connectivity OK'
                    }
                else:
                    return {
                        'status': 'unhealthy',
                        'message': 'Database query failed'
                    }
                    
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Database error: {e}'
            }


# Legacy endpoint redirects for backwards compatibility
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def redirect_to_optimized_summary(request):
    """Redirect legacy summary endpoint to optimized version"""
    return optimized_dashboard_summary(request)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def redirect_to_optimized_breakdown(request):
    """Redirect legacy breakdown endpoint to optimized version"""
    return optimized_beneficiary_breakdown(request)