from django.urls import path, include
from . import views
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
    path('card/<str:social_id>/', views.generate_beneficiary_card_view, 
         name='beneficiary_card'),
    path('commune-cards/<str:commune_name>/', views.generate_colline_cards_view, 
         name='colline_cards'),
    path('beneficiary-photo/<str:type>/<str:id>/', views.beneficiary_photo_view, name='beneficiary_photo'),
    path('', include(router.urls)),
    path('location/<str:location_id>/cards/', views.generate_location_cards_view, name='generate_location_cards'),
    path('location/<str:location_id>/generate-cards-background/', views.trigger_background_card_generation, name='generate_cards_background'),
    path('location/<str:location_id>/generate-cards-background/<str:location_type>/', views.trigger_background_card_generation, name='generate_cards_background_with_type'),
]
