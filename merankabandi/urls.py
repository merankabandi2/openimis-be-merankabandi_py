from django.urls import path, include
from . import views
from . import dashboard_views
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
    
    # REST API router
    path('', include(router.urls)),
]
