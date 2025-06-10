from django.urls import path, include
from . import views
from . import dashboard_views
from . import optimized_dashboard_views
from . import analytics_views
from . import enhanced_analytics_views
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(
    r'phonenumber-attribution',
    views.PhoneNumberAttributionViewSet,
    basename='phonenumber-attribution'
)
router.register(
    r'paymentaccount-attribution',
    views.PaymentAccountAttributionViewSet,
    basename='paymentaccount-attribution'
)
router.register(
    r'monetary-transfers',
    views.MonetaryTransferViewSet,
    basename='monetary-transfers'
)
router.register(
    r'data-exploration',
    analytics_views.DataExplorationViewSet,
    basename='data-exploration'
)
router.register(
    r'enhanced-analytics',
    enhanced_analytics_views.EnhancedDataExplorationViewSet,
    basename='enhanced-analytics'
)

urlpatterns = [
    # Existing card generation endpoints
    path('card/<str:social_id>/', views.generate_beneficiary_card_view, 
         name='beneficiary_card'),
    path('commune-cards/<str:commune_name>/', views.generate_colline_cards_view, 
         name='colline_cards'),
    path('beneficiary-photo/<str:type>/<str:id>/', views.beneficiary_photo_view, name='beneficiary_photo'),
    path('location/<str:location_id>/cards/', views.generate_location_cards_view, name='generate_location_cards'),
    path('location/<str:location_id>/generate-cards-background/', views.trigger_background_card_generation, name='generate_cards_background'),
    path('location/<str:location_id>/generate-cards-background/<str:location_type>/', views.trigger_background_card_generation, name='generate_cards_background_with_type'),
    
    # M&E Dashboard API endpoints
    path('dashboard/beneficiary-breakdown/', dashboard_views.beneficiary_breakdown_api, name='dashboard_beneficiary_breakdown'),
    path('dashboard/refugee-host-breakdown/', dashboard_views.refugee_host_breakdown_api, name='dashboard_refugee_host_breakdown'),
    path('dashboard/quarterly-rollup/', dashboard_views.quarterly_rollup_api, name='dashboard_quarterly_rollup'),
    path('dashboard/twa-metrics/', dashboard_views.twa_minority_metrics_api, name='dashboard_twa_metrics'),
    path('dashboard/summary/', dashboard_views.dashboard_summary_api, name='dashboard_summary'),
    path('dashboard/location-performance/', dashboard_views.location_performance_api, name='dashboard_location_performance'),
    path('dashboard/activity-summary/', dashboard_views.activity_summary_api, name='dashboard_activity_summary'),
    
    # Excel export endpoints
    path('export/excel/<str:report_type>/', dashboard_views.excel_export_api, name='excel_export'),
    
    # Automated aggregation endpoints
    path('indicators/auto-aggregate/', dashboard_views.auto_aggregate_indicators_api, name='auto_aggregate_indicators'),
    
    # OPTIMIZED DASHBOARD ENDPOINTS (Materialized Views)
    path('dashboard/optimized/summary/', optimized_dashboard_views.optimized_dashboard_summary, name='optimized_dashboard_summary'),
    path('dashboard/optimized/beneficiary-breakdown/', optimized_dashboard_views.optimized_beneficiary_breakdown, name='optimized_beneficiary_breakdown'),
    path('dashboard/optimized/transfer-performance/', optimized_dashboard_views.optimized_transfer_performance, name='optimized_transfer_performance'),
    path('dashboard/optimized/quarterly-trends/', optimized_dashboard_views.optimized_quarterly_trends, name='optimized_quarterly_trends'),
    path('dashboard/optimized/grievances/', optimized_dashboard_views.optimized_grievance_dashboard, name='optimized_grievance_dashboard'),
    path('dashboard/optimized/refresh/', optimized_dashboard_views.refresh_dashboard_views, name='refresh_dashboard_views'),
    path('dashboard/optimized/stats/', optimized_dashboard_views.dashboard_view_stats, name='dashboard_view_stats'),
    path('dashboard/optimized/health/', optimized_dashboard_views.OptimizedDashboardHealthView.as_view(), name='dashboard_health'),
    
    # Legacy endpoint redirects (backwards compatibility)
    path('dashboard/fast/summary/', optimized_dashboard_views.redirect_to_optimized_summary, name='legacy_fast_summary'),
    path('dashboard/fast/breakdown/', optimized_dashboard_views.redirect_to_optimized_breakdown, name='legacy_fast_breakdown'),
    
    # REST API router
    path('', include(router.urls)),
]
