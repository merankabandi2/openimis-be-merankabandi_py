from django.urls import path
from .dashboard_json_views import (
    household_statistics_view,
    individual_demographics_view,
    payment_status_view,
    location_breakdown_view,
    combined_dashboard_view
)

urlpatterns = [
    path('api/dashboard/household-statistics/', household_statistics_view, name='dashboard-household-stats'),
    path('api/dashboard/demographics/', individual_demographics_view, name='dashboard-demographics'),
    path('api/dashboard/payment-status/', payment_status_view, name='dashboard-payment-status'),
    path('api/dashboard/location-breakdown/', location_breakdown_view, name='dashboard-location-breakdown'),
    path('api/dashboard/combined/', combined_dashboard_view, name='dashboard-combined'),
]