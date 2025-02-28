import graphene
from django.db.models import Sum, Q
from graphene_django import DjangoObjectType

from core import prefix_filterset, ExtendedConnection
from core.gql_queries import UserGQLType
from core.utils import DefaultStorageFileHandler

from location.gql_queries import LocationGQLType
from merankabandi.models import BehaviorChangePromotion, MicroProject, SensitizationTraining


class SensitizationTrainingGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')

    class Meta:
        model = SensitizationTraining
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "sensitization_date": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            "category": ["exact", "icontains"],
            "facilitator": ["exact", "icontains"],
            "male_participants": ["exact", "lt", "lte", "gt", "gte"],
            "female_participants": ["exact", "lt", "lte", "gt", "gte"],
            "twa_participants": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection


class BehaviorChangePromotionGQLType(DjangoObjectType):
    class Meta:
        model = BehaviorChangePromotion
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "report_date": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            "male_participants": ["exact", "lt", "lte", "gt", "gte"],
            "female_participants": ["exact", "lt", "lte", "gt", "gte"],
            "twa_participants": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection


class MicroProjectGQLType(DjangoObjectType):
    class Meta:
        model = MicroProject
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "report_date": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            "male_participants": ["exact", "lt", "lte", "gt", "gte"],
            "female_participants": ["exact", "lt", "lte", "gt", "gte"],
            "twa_participants": ["exact", "lt", "lte", "gt", "gte"],
            "agriculture_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "livestock_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "livestock_goat_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "livestock_pig_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "livestock_rabbit_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "livestock_poultry_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "livestock_cattle_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
            "commerce_services_beneficiaries": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection