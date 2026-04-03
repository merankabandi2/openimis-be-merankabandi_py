"""
Optimized Dashboard Views using Materialized Views
High-performance API endpoints for dashboard data
"""

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from django.views import View
from django.core.cache import cache
import json
from datetime import datetime, date
from .dashboard_service import DashboardService as OptimizedDashboardService
from .views_manager import MaterializedViewsManager


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def parse_filters(request):
    """Parse common filters from request parameters"""
    filters = {}
    if request.GET.get('start_date'):
        filters['start_date'] = request.GET.get('start_date')
    if request.GET.get('end_date'):
        filters['end_date'] = request.GET.get('end_date')
    if request.GET.get('province_id'):
        filters['province_id'] = int(request.GET.get('province_id'))
    if request.GET.get('year'):
        filters['year'] = int(request.GET.get('year'))
    return filters


@api_view(['GET'])
@permission_classes([IsAuthenticated])
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
            valid_views = MaterializedViewsManager.get_all_view_names()
            if view_name not in valid_views:
                return Response({
                    'success': False,
                    'error': f"Invalid view name '{view_name}'. Allowed: {', '.join(sorted(valid_views))}"
                }, status=status.HTTP_400_BAD_REQUEST)
            MaterializedViewsManager.refresh_single_view(view_name, concurrent)
            message = f"Refreshed view: {view_name}"
        else:
            MaterializedViewsManager.refresh_all_views(category=None, concurrent=concurrent)
            message = "Refreshed all dashboard views"

        # Clear cache after refresh if available
        if hasattr(OptimizedDashboardService, 'clear_cache'):
            OptimizedDashboardService.clear_cache()
        else:
            # Fallback: clear dashboard-related cache keys
            cache.delete_pattern('dashboard_*') if hasattr(cache, 'delete_pattern') else None

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
        for view_name, view_info in stats.items():
            row_count = view_info.get('row_count', 0) or 0
            formatted_stats.append({
                'view_name': view_name,
                'row_count': row_count,
                'exists': view_info.get('exists', False),
                'size': view_info.get('size', '0 bytes'),
                'error': view_info.get('error'),
            })

        return Response({
            'success': True,
            'data': {
                'views': formatted_stats,
                'total_views': len(formatted_stats),
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
                    'status': 'degraded',
                    'message': 'No materialized views configured'
                }

            total_views = len(stats)
            views_existing = sum(
                1 for info in stats.values()
                if info.get('exists', False)
            )
            views_with_data = sum(
                1 for info in stats.values()
                if info.get('exists', False) and (info.get('row_count') or 0) > 0
            )

            if views_existing == 0:
                return {
                    'status': 'degraded',
                    'message': f'No materialized views created yet (0/{total_views}). Run manage_views --action=create to initialize.',
                    'views_count': total_views
                }
            elif views_with_data == total_views:
                return {
                    'status': 'healthy',
                    'message': f'All {total_views} views have data',
                    'views_count': total_views
                }
            else:
                return {
                    'status': 'degraded',
                    'message': f'{views_with_data}/{total_views} views have data ({views_existing} exist)',
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


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def optimized_refugee_host_breakdown(request):
    """
    Refugee vs host community breakdown from dashboard_vulnerable_groups_summary.
    GET /api/merankabandi/dashboard/optimized/refugee-host-breakdown/
    """
    try:
        filters = parse_filters(request)
        from django.db import connection

        conditions, params = [], []
        if filters.get('province_id'):
            conditions.append("province_id = %s")
            params.append(filters['province_id'])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
        SELECT
            SUM(total_households) AS total_households,
            SUM(total_members) AS total_members,
            SUM(total_beneficiaries) AS total_beneficiaries,
            SUM(refugee_households) AS refugee_households,
            SUM(refugee_members) AS refugee_members,
            SUM(refugee_beneficiaries) AS refugee_beneficiaries
        FROM dashboard_vulnerable_groups_summary
        {where}
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()
            r = dict(zip(columns, row)) if row else {}

        total_hh = int(r.get('total_households') or 0)
        total_ben = int(r.get('total_beneficiaries') or 0)
        refugee_hh = int(r.get('refugee_households') or 0)
        refugee_members = int(r.get('refugee_members') or 0)
        refugee_ben = int(r.get('refugee_beneficiaries') or 0)
        host_hh = total_hh - refugee_hh
        host_ben = total_ben - refugee_ben

        data = {
            'refugee_community': {
                'planned_households': refugee_hh,
                'planned_members': refugee_members,
                'planned_beneficiaries': refugee_ben,
            },
            'host_community': {
                'planned_households': host_hh,
                'planned_members': int(r.get('total_members') or 0) - refugee_members,
                'planned_beneficiaries': host_ben,
            },
            'totals': {
                'total_households': total_hh,
                'total_beneficiaries': total_ben,
                'refugee_percentage': round(refugee_ben / total_ben * 100, 2) if total_ben else 0,
                'host_percentage': round(host_ben / total_ben * 100, 2) if total_ben else 0,
            },
        }

        return Response({
            'success': True,
            'data': data,
            'source': 'materialized_views'
        }, status=status.HTTP_200_OK)

    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def optimized_twa_metrics(request):
    """
    Twa/Batwa inclusion metrics from dashboard_vulnerable_groups_summary.
    GET /api/merankabandi/dashboard/optimized/twa-metrics/
    """
    try:
        filters = parse_filters(request)
        from django.db import connection

        conditions, params = [], []
        if filters.get('province_id'):
            conditions.append("province_id = %s")
            params.append(filters['province_id'])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Overall Twa metrics
        query = f"""
        SELECT
            SUM(total_households) AS total_households,
            SUM(total_members) AS total_members,
            SUM(total_beneficiaries) AS total_beneficiaries,
            SUM(twa_households) AS twa_households,
            SUM(twa_members) AS twa_members,
            SUM(twa_beneficiaries) AS twa_beneficiaries
        FROM dashboard_vulnerable_groups_summary
        {where}
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()
            overall = dict(zip(columns, row)) if row else {}

        # By province
        query_prov = f"""
        SELECT
            province, province_id,
            SUM(total_households) AS total_households,
            SUM(total_beneficiaries) AS total_beneficiaries,
            SUM(twa_households) AS twa_households,
            SUM(twa_beneficiaries) AS twa_beneficiaries
        FROM dashboard_vulnerable_groups_summary
        {where}
        GROUP BY province, province_id
        ORDER BY SUM(twa_beneficiaries) DESC
        """
        with connection.cursor() as cursor:
            cursor.execute(query_prov, params)
            columns = [col[0] for col in cursor.description]
            prov_rows = [dict(zip(columns, r)) for r in cursor.fetchall()]

        total_hh = int(overall.get('total_households') or 0)
        total_ben = int(overall.get('total_beneficiaries') or 0)
        twa_hh = int(overall.get('twa_households') or 0)
        twa_members = int(overall.get('twa_members') or 0)
        twa_ben = int(overall.get('twa_beneficiaries') or 0)

        data = {
            'overall': {
                'twa_households': twa_hh,
                'twa_members': twa_members,
                'twa_beneficiaries': twa_ben,
                'total_households': total_hh,
                'total_beneficiaries': total_ben,
                'twa_household_percentage': round(twa_hh / total_hh * 100, 2) if total_hh else 0,
                'twa_beneficiary_percentage': round(twa_ben / total_ben * 100, 2) if total_ben else 0,
            },
            'by_province': [
                {
                    'province': r.get('province', ''),
                    'province_id': int(r.get('province_id') or 0),
                    'twa_households': int(r.get('twa_households') or 0),
                    'twa_beneficiaries': int(r.get('twa_beneficiaries') or 0),
                    'total_beneficiaries': int(r.get('total_beneficiaries') or 0),
                    'twa_percentage': round(
                        int(r.get('twa_beneficiaries') or 0) / int(r.get('total_beneficiaries') or 1) * 100, 2
                    ) if int(r.get('total_beneficiaries') or 0) > 0 else 0,
                }
                for r in prov_rows
            ],
        }

        return Response({
            'success': True,
            'data': data,
            'source': 'materialized_views'
        }, status=status.HTTP_200_OK)

    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


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
