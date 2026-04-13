"""
Extended Payroll GQL type for Merankabandi.

Adds meraLocation virtual field that resolves json_ext.location_uuid
into a full Location object with parent chain (commune → province).
"""
import graphene
from graphene_django import DjangoObjectType

from core.schema import OrderedDjangoFilterConnectionField
from location.gql_queries import LocationGQLType
from location.models import Location
from payroll.models import Payroll
from payroll.gql_queries import (
    PayrollGQLType,
    PaymentPointGQLType,
    PaymentPlanGQLType,
    BenefitConsumptionGQLType,
    ExtendedConnection,
)
from payment_cycle.gql_queries import PaymentCycleGQLType
from social_protection.models import BenefitPlan


class MeraPayrollGQLType(DjangoObjectType):
    """Extended Payroll type with Merankabandi-specific fields.

    Adds:
    - meraLocation: resolves json_ext.location_uuid → full Location with parent chain
    - benefitPlanNameCode: safe resolver that handles null payment_plan
    """
    uuid = graphene.String(source='uuid')
    mera_location = graphene.Field(LocationGQLType)
    benefit_plan_name_code = graphene.String()
    benefit_consumption = graphene.List(BenefitConsumptionGQLType)

    class Meta:
        model = Payroll
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "name": ["iexact", "istartswith", "icontains"],
            "status": ["exact", "startswith", "contains"],
            "payment_method": ["exact", "startswith", "contains"],
            **PayrollGQLType._meta.filter_fields,
        }
        connection_class = ExtendedConnection

    def resolve_mera_location(self, info):
        ext = self.json_ext if isinstance(self.json_ext, dict) else {}
        loc_uuid = ext.get('location_uuid')
        if not loc_uuid:
            return None
        try:
            return Location.objects.select_related(
                'parent', 'parent__parent'
            ).get(uuid=loc_uuid)
        except Location.DoesNotExist:
            return None

    def resolve_benefit_plan_name_code(self, info):
        try:
            if not self.payment_plan:
                return None
            bp_id = self.payment_plan.benefit_plan_id
            if not bp_id:
                return None
            bp = BenefitPlan.objects.get(id=bp_id, is_deleted=False)
            return f"{bp.code} - {bp.name}"
        except (BenefitPlan.DoesNotExist, AttributeError):
            return None

    def resolve_benefit_consumption(self, info):
        from payroll.models import BenefitConsumption
        return BenefitConsumption.objects.filter(
            payrollbenefitconsumption__payroll__id=self.id,
            is_deleted=False,
            payrollbenefitconsumption__is_deleted=False,
        )
