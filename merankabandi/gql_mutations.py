import graphene
from gettext import gettext as _
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from django.db import transaction

from core.gql.gql_mutations.base_mutation import BaseHistoryModelCreateMutationMixin, BaseMutation, \
    BaseHistoryModelUpdateMutationMixin, BaseHistoryModelDeleteMutationMixin
from core.schema import OpenIMISMutation
from merankabandi.apps import MerankabandiConfig
from merankabandi.models import MonetaryTransfer
from merankabandi.services import MonetaryTransferService
from payroll.apps import PayrollConfig


class CreateMonetaryTransferInputType(OpenIMISMutation.Input):
    transfer_date = graphene.Date(required=True)
    location_id = graphene.Int(required=True)
    programme_id = graphene.UUID(required=True)
    payment_agency_id = graphene.UUID(required=True)
    planned_women = graphene.Int(required=False)
    planned_men = graphene.Int(required=False)
    planned_twa = graphene.Int(required=False)
    paid_women = graphene.Int(required=False)
    paid_men = graphene.Int(required=False)
    paid_twa = graphene.Int(required=False)
    json_ext = graphene.JSONString(required=False)


class UpdateMonetaryTransferInputType(CreateMonetaryTransferInputType):
    id = graphene.UUID(required=True)


class DeleteMonetaryTransferInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class CreateMonetaryTransferMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateMonetaryTransferMutation"
    _mutation_module = MerankabandiConfig.name
    _model = MonetaryTransfer

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = MonetaryTransferService(user)
        service.create(data)

    class Input(CreateMonetaryTransferInputType):
        pass


class UpdateMonetaryTransferMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateMonetaryTransferMutation"
    _mutation_module = PayrollConfig.name
    _model = MonetaryTransfer

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = MonetaryTransferService(user)
        service.update(data)

    class Input(UpdateMonetaryTransferInputType):
        pass


class DeleteMonetaryTransferMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteMonetaryTransferMutation"
    _mutation_module = PayrollConfig.name
    _model = MonetaryTransfer

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = MonetaryTransferService(user)

        ids = data.get('ids')
        if ids:
            with transaction.atomic():
                for id in ids:
                    service.delete({'id': id, 'user': user})

    class Input(DeleteMonetaryTransferInputType):
        pass