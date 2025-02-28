import graphene
import graphene_django_optimizer as gql_optimizer
from gettext import gettext as _
from django.contrib.auth.models import AnonymousUser
from django.db.models import Q, Sum

from core.schema import OrderedDjangoFilterConnectionField
from core.services import wait_for_mutation
from core.utils import append_validity_filter

from merankabandi.gql_queries import BehaviorChangePromotionGQLType, MicroProjectGQLType, SensitizationTrainingGQLType
from merankabandi.models import BehaviorChangePromotion, MicroProject, SensitizationTraining

class Query(graphene.ObjectType):
    sensitization_training = OrderedDjangoFilterConnectionField(
        SensitizationTrainingGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    behavior_change_promotion = OrderedDjangoFilterConnectionField(
        BehaviorChangePromotionGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )
    micro_project = OrderedDjangoFilterConnectionField(
        MicroProjectGQLType,
        orderBy=graphene.List(of_type=graphene.String),
    )

    def resolve_sensitization_training(self, info, **kwargs):
        return gql_optimizer.query(SensitizationTraining.objects.all(), info)

    def resolve_behavior_change_promotion(self, info, **kwargs):
        return gql_optimizer.query(BehaviorChangePromotion.objects.all(), info)

    def resolve_micro_project(self, info, **kwargs):
        return gql_optimizer.query(MicroProject.objects.all(), info)
