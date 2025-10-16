import graphene
from django.db.models import Sum, Q
from graphene_django import DjangoObjectType

from core import prefix_filterset, ExtendedConnection
from core.gql_queries import UserGQLType
from core.utils import DefaultStorageFileHandler

from location.gql_queries import LocationGQLType
from merankabandi.models import (
    BehaviorChangePromotion, MicroProject, MonetaryTransfer, SensitizationTraining, 
    Section, Indicator, IndicatorAchievement, ProvincePaymentPoint,
    ResultFrameworkSnapshot, IndicatorCalculationRule
)
from payroll.gql_queries import PaymentPointGQLType
from social_protection.gql_queries import BenefitPlanGQLType


class SensitizationTrainingGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')
    validation_status_display = graphene.String()
    
    def resolve_validation_status_display(self, info):
        return self.get_validation_status_display()

    class Meta:
        model = SensitizationTraining
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "sensitization_date": ["exact", "lt", "lte", "gt", "gte"],
            "location__id": ["exact", "in"],
            "location__parent__id": ["exact", "in"],
            "location__parent__parent__id": ["exact", "in"],
            "category": ["exact", "icontains"],
            "facilitator": ["exact", "icontains"],
            "male_participants": ["exact", "lt", "lte", "gt", "gte"],
            "female_participants": ["exact", "lt", "lte", "gt", "gte"],
            "twa_participants": ["exact", "lt", "lte", "gt", "gte"],
            "validation_status": ["exact", "in"],
            "validated_by": ["exact"],
            "validation_date": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection


class BehaviorChangePromotionGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')
    validation_status_display = graphene.String()
    
    def resolve_validation_status_display(self, info):
        return self.get_validation_status_display()
    
    class Meta:
        model = BehaviorChangePromotion
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "report_date": ["exact", "lt", "lte", "gt", "gte"],
            "location__id": ["exact", "in"],
            "location__parent__id": ["exact", "in"],
            "location__parent__parent__id": ["exact", "in"],
            "male_participants": ["exact", "lt", "lte", "gt", "gte"],
            "female_participants": ["exact", "lt", "lte", "gt", "gte"],
            "twa_participants": ["exact", "lt", "lte", "gt", "gte"],
            "validation_status": ["exact", "in"],
            "validated_by": ["exact"],
            "validation_date": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection


class MicroProjectGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')
    validation_status_display = graphene.String()
    
    def resolve_validation_status_display(self, info):
        return self.get_validation_status_display()
    
    class Meta:
        model = MicroProject
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "report_date": ["exact", "lt", "lte", "gt", "gte"],
            "location__id": ["exact", "in"],
            "location__parent__id": ["exact", "in"],
            "location__parent__parent__id": ["exact", "in"],
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
            "validation_status": ["exact", "in"],
            "validated_by": ["exact"],
            "validation_date": ["exact", "lt", "lte", "gt", "gte"],
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
            "planned_amount": ["exact", "lt", "lte", "gt", "gte"],
            "transferred_amount": ["exact", "lt", "lte", "gt", "gte"],
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
    

class TicketResolutionStatusGQLType(graphene.ObjectType):
    status = graphene.String(required=True)
    count = graphene.Int(required=True)

    class Meta:
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "title": ["iexact", "istartswith", "icontains"],
            "description": ["iexact", "istartswith", "icontains"],
            "code": ["iexact", "istartswith", "icontains"],
            "resolution": ["iexact", "istartswith", "icontains"],
            "date_of_incident": ["exact", "lt", "lte", "gt", "gte"],
            "due_date": ["exact", "lt", "lte", "gt", "gte"],
            "status": ["exact"],
            "priority": ["exact"],
            "category": ["exact"],
            "flags": ["exact"],
            "channel": ["exact"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
            **prefix_filterset("attending_staff__", UserGQLType._meta.filter_fields),
        }

class BenefitConsumptionByProvinceGQLType(graphene.ObjectType):
    province_id = graphene.String()
    province_name = graphene.String()
    province_code = graphene.String()
    total_paid = graphene.Int()
    total_amount = graphene.Float()
    beneficiaries_active = graphene.Int()
    beneficiaries_suspended = graphene.Int()
    beneficiaries_selected = graphene.Int()


class SectionGQLType(DjangoObjectType):
    class Meta:
        model = Section
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "name": ["exact", "icontains"],
        }
        connection_class = ExtendedConnection

class IndicatorGQLType(DjangoObjectType):
    class Meta:
        model = Indicator
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "name": ["exact", "icontains"],
            "pbc": ["exact", "icontains"],
            "section": ["exact"],
            "baseline": ["exact", "lt", "lte", "gt", "gte"],
            "target": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection

class IndicatorAchievementGQLType(DjangoObjectType):
    class Meta:
        model = IndicatorAchievement
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "indicator": ["exact"],
            "achieved": ["exact", "lt", "lte", "gt", "gte"],
            "timestamp": ["exact", "lt", "lte", "gt", "gte"],
            "date": ["exact", "lt", "lte", "gt", "gte"],
        }
        connection_class = ExtendedConnection


class ProvincePaymentPointGQLType(DjangoObjectType):
    class Meta:
        model = ProvincePaymentPoint
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "province": ["exact"],
            "payment_point": ["exact"],
            "payment_plan": ["exact", "isnull"],
            "created_date": ["exact", "lt", "lte", "gt", "gte"],
            "updated_date": ["exact", "lt", "lte", "gt", "gte"],
            **prefix_filterset("province__", LocationGQLType._meta.filter_fields),
            **prefix_filterset("payment_point__", PaymentPointGQLType._meta.filter_fields),
        }
        connection_class = ExtendedConnection


class ResultFrameworkSnapshotGQLType(DjangoObjectType):
    """GraphQL type for Result Framework Snapshots"""
    class Meta:
        model = ResultFrameworkSnapshot
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "name": ["exact", "icontains"],
            "status": ["exact", "in"],
            "snapshot_date": ["exact", "lt", "lte", "gt", "gte"],
            "created_by": ["exact"],
        }
        connection_class = ExtendedConnection
        
    def resolve_created_by(self, info):
        return self.created_by


class IndicatorCalculationRuleGQLType(DjangoObjectType):
    """GraphQL type for Indicator Calculation Rules"""
    class Meta:
        model = IndicatorCalculationRule
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "indicator": ["exact"],
            "calculation_type": ["exact", "in"],
            "is_active": ["exact"],
        }
        connection_class = ExtendedConnection


# Additional GraphQL types for result framework operations
class IndicatorCalculationResultType(graphene.ObjectType):
    """Result of indicator calculation"""
    value = graphene.Float()
    calculation_type = graphene.String()
    system_value = graphene.Float()
    manual_value = graphene.Float()
    error = graphene.String()
    date = graphene.Date()
    gender_breakdown = graphene.JSONString()
    
    
class DashboardFiltersInputType(graphene.InputObjectType):
    """Input filters for dashboard queries"""
    date_from = graphene.Date()
    date_to = graphene.Date()
    location_id = graphene.ID()
    indicator_ids = graphene.List(graphene.Int)
