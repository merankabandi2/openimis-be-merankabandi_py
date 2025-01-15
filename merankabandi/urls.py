from django.urls import path
from . import views

urlpatterns = [
    path('card/<str:social_id>/', views.generate_beneficiary_card_view, 
         name='beneficiary_card'),
    path('colline-cards/<int:benefit_plan_id>/<int:colline_id>/', views.generate_colline_cards_view, 
         name='colline_cards'),
    path('beneficiary-photo/<str:type>/<str:id>/', views.beneficiary_photo_view, name='beneficiary_photo'),
]
