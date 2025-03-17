import graphene
from django.db.models import Sum, Q
from graphene_django import DjangoObjectType

from core import prefix_filterset, ExtendedConnection
from core.gql_queries import UserGQLType
from core.utils import DefaultStorageFileHandler

from location.gql_queries import LocationGQLType
from merankabandi.models import BehaviorChangePromotion, MicroProject, MonetaryTransfer, SensitizationTraining
from payroll.gql_queries import PaymentPointGQLType
from social_protection.gql_queries import BenefitPlanGQLType


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


class MonetaryTransferGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')

    class Meta:
        model = MonetaryTransfer
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "transfer_date": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            **prefix_filterset("programme__", BenefitPlanGQLType._meta.filter_fields),
            **prefix_filterset("payment_agency__", PaymentPointGQLType._meta.filter_fields),
            "planned_women": ["exact", "lt", "lte", "gt", "gte"],
            "planned_men": ["exact", "lt", "lte", "gt", "gte"],
            "planned_twa": ["exact", "lt", "lte", "gt", "gte"],
            "paid_women": ["exact", "lt", "lte", "gt", "gte"],
            "paid_men": ["exact", "lt", "lte", "gt", "gte"],
            "paid_twa": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection


class MonetaryTransferQuarterlyDataGQLType(graphene.ObjectType):
    transfer_type = graphene.String(required=True)
    q1_amount = graphene.Decimal()
    q2_amount = graphene.Decimal()
    q3_amount = graphene.Decimal()
    q4_amount = graphene.Decimal()
    q1_beneficiaries = graphene.Int()
    q2_beneficiaries = graphene.Int()
    q3_beneficiaries = graphene.Int()
    q4_beneficiaries = graphene.Int()

    class Meta:
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "transfer_date": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            **prefix_filterset("programme__", BenefitPlanGQLType._meta.filter_fields),
            **prefix_filterset("payment_agency__", PaymentPointGQLType._meta.filter_fields),
        }


class MonetaryTransferBeneficiaryDataGQLType(graphene.ObjectType):
    transfer_type = graphene.String()
    male_paid = graphene.Int()
    male_unpaid = graphene.Int()
    female_paid = graphene.Int()
    female_unpaid = graphene.Int()
    total_paid = graphene.Int()
    total_unpaid = graphene.Int()

    class Meta:
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "transfer_date": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            **prefix_filterset("programme__", BenefitPlanGQLType._meta.filter_fields),
            **prefix_filterset("payment_agency__", PaymentPointGQLType._meta.filter_fields),
        }
    
