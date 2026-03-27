from django.urls import path, include
from . import views
from . import optimized_dashboard_views
from . import analytics_views
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
    r'payment-request',
    views.PaymentRequestViewSet,
    basename='payment-request'
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
    r'group-beneficiary',
    views.GroupBeneficiaryCheckViewSet,
    basename='group-beneficiary'
)
router.register(
    r'communes',
    views.ProvincePaymentPointCommunesViewSet,
    basename='payment-communes'
)


urlpatterns = [
    # Existing card generation endpoints
    path('card/<str:social_id>/', views.generate_beneficiary_card_view, 
         name='beneficiary_card'),
    path('commune-cards/<str:commune_name>/', views.generate_colline_cards_view, 
         name='colline_cards'),
    path('beneficiary-photo/<str:type>/<str:id>/', views.beneficiary_photo_view, name='beneficiary_photo'),
    path('beneficiary-photos/<str:socialid>/', views.beneficiary_photos_view, name='beneficiary_photos'),
    path('location/<str:location_id>/cards/', views.generate_location_cards_view, name='generate_location_cards'),
    path('location/<str:location_id>/generate-cards-background/', views.trigger_background_card_generation, name='generate_cards_background'),
    path('location/<str:location_id>/generate-cards-background/<str:location_type>/', views.trigger_background_card_generation, name='generate_cards_background_with_type'),
    
    # DASHBOARD ENDPOINTS (Materialized Views)
    path('dashboard/optimized/summary/', optimized_dashboard_views.optimized_dashboard_summary, name='optimized_dashboard_summary'),
    path('dashboard/optimized/beneficiary-breakdown/', optimized_dashboard_views.optimized_beneficiary_breakdown, name='optimized_beneficiary_breakdown'),
    path('dashboard/optimized/transfer-performance/', optimized_dashboard_views.optimized_transfer_performance, name='optimized_transfer_performance'),
    path('dashboard/optimized/quarterly-trends/', optimized_dashboard_views.optimized_quarterly_trends, name='optimized_quarterly_trends'),
    path('dashboard/optimized/grievances/', optimized_dashboard_views.optimized_grievance_dashboard, name='optimized_grievance_dashboard'),
    path('dashboard/optimized/refresh/', optimized_dashboard_views.refresh_dashboard_views, name='refresh_dashboard_views'),
    path('dashboard/optimized/stats/', optimized_dashboard_views.dashboard_view_stats, name='dashboard_view_stats'),
    path('dashboard/optimized/health/', optimized_dashboard_views.OptimizedDashboardHealthView.as_view(), name='dashboard_health'),
    
    # REST API router
    path('', include(router.urls)),
]
