import graphene
import graphene_django_optimizer as gql_optimizer
from gettext import gettext as _
from django.contrib.auth.models import AnonymousUser
from django.db.models import Q, Sum

from core.schema import OrderedDjangoFilterConnectionField
from core.services import wait_for_mutation
from core.utils import append_validity_filter

from merankabandi.gql_mutations import CreateMonetaryTransferMutation, DeleteMonetaryTransferMutation, UpdateMonetaryTransferMutation
from merankabandi.gql_queries import BehaviorChangePromotionGQLType, MicroProjectGQLType, MonetaryTransferGQLType, SensitizationTrainingGQLType
from merankabandi.models import BehaviorChangePromotion, MicroProject, MonetaryTransfer, SensitizationTraining

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
    monetary_transfer = OrderedDjangoFilterConnectionField(
        MonetaryTransferGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        client_mutation_id=graphene.String(),
    )

    def resolve_sensitization_training(self, info, **kwargs):
        return gql_optimizer.query(SensitizationTraining.objects.all(), info)

    def resolve_behavior_change_promotion(self, info, **kwargs):
        return gql_optimizer.query(BehaviorChangePromotion.objects.all(), info)

    def resolve_micro_project(self, info, **kwargs):
        return gql_optimizer.query(MicroProject.objects.all(), info)

    def resolve_monetary_transfer(self, info, **kwargs):
        #Query._check_permissions(info.context.user, PayrollConfig.gql_payroll_search_perms)
        filters = append_validity_filter(**kwargs)
        query = MonetaryTransfer.objects.filter(*filters)
        return gql_optimizer.query(query, info)


class Mutation(graphene.ObjectType):
    create_monetary_transfer = CreateMonetaryTransferMutation.Field()
    update_monetary_transfer = UpdateMonetaryTransferMutation.Field()
    delete_monetary_transfer = DeleteMonetaryTransferMutation.Field()