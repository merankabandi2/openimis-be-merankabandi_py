import graphene
from gettext import gettext as _
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from django.db import transaction

from django.apps import apps

from core.gql.gql_mutations.base_mutation import BaseHistoryModelCreateMutationMixin, BaseMutation, \
    BaseHistoryModelUpdateMutationMixin, BaseHistoryModelDeleteMutationMixin
from core.schema import OpenIMISMutation
from merankabandi.apps import MerankabandiConfig
from merankabandi.models import MonetaryTransfer, Section, Indicator, IndicatorAchievement, ProvincePaymentPoint
from merankabandi.services import (
    MonetaryTransferService, SectionService, IndicatorService, 
    IndicatorAchievementService, ProvincePaymentPointService
)
from payroll.apps import PayrollConfig


def get_merankabandi_config():
    """Get the MerankabandiConfig instance"""
    return apps.get_app_config('merankabandi')


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

# Section mutations
class CreateSectionInputType(OpenIMISMutation.Input):
    name = graphene.String(required=True)

class UpdateSectionInputType(CreateSectionInputType):
    id = graphene.Int(required=True)

class DeleteSectionInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.Int, required=True)

class CreateSectionMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateSectionMutation"
    _mutation_module = MerankabandiConfig.name
    _model = Section

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_section_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = SectionService(user)
        return service.create(data)

    class Input(CreateSectionInputType):
        pass

class UpdateSectionMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateSectionMutation"
    _mutation_module = MerankabandiConfig.name
    _model = Section

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_section_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = SectionService(user)
        return service.update(data)

    class Input(UpdateSectionInputType):
        pass

class DeleteSectionMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteSectionMutation"
    _mutation_module = MerankabandiConfig.name
    _model = Section

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_section_delete_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = SectionService(user)

        ids = data.get('ids')
        if ids:
            with transaction.atomic():
                for id in ids:
                    service.delete({'id': id, 'user': user})

    class Input(DeleteSectionInputType):
        pass

# Indicator mutations
class CreateIndicatorInputType(OpenIMISMutation.Input):
    section_id = graphene.Int(required=False)
    name = graphene.String(required=True)
    pbc = graphene.String(required=False)
    baseline = graphene.Decimal(required=False)
    target = graphene.Decimal(required=False)
    observation = graphene.String(required=False)

class UpdateIndicatorInputType(CreateIndicatorInputType):
    id = graphene.Int(required=True)

class DeleteIndicatorInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.Int, required=True)

class CreateIndicatorMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateIndicatorMutation"
    _mutation_module = MerankabandiConfig.name
    _model = Indicator

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_indicator_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = IndicatorService(user)
        return service.create(data)

    class Input(CreateIndicatorInputType):
        pass

class UpdateIndicatorMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateIndicatorMutation"
    _mutation_module = MerankabandiConfig.name
    _model = Indicator

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_indicator_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = IndicatorService(user)
        return service.update(data)

    class Input(UpdateIndicatorInputType):
        pass

class DeleteIndicatorMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteIndicatorMutation"
    _mutation_module = MerankabandiConfig.name
    _model = Indicator

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_indicator_delete_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = IndicatorService(user)

        ids = data.get('ids')
        if ids:
            with transaction.atomic():
                for id in ids:
                    service.delete({'id': id, 'user': user})

    class Input(DeleteIndicatorInputType):
        pass

# IndicatorAchievement mutations
class CreateIndicatorAchievementInputType(OpenIMISMutation.Input):
    indicator_id = graphene.Int(required=True)
    achieved = graphene.Decimal(required=True)
    comment = graphene.String(required=False)
    date = graphene.Date(required=False)

class UpdateIndicatorAchievementInputType(CreateIndicatorAchievementInputType):
    id = graphene.Int(required=True)

class DeleteIndicatorAchievementInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.Int, required=True)

class CreateIndicatorAchievementMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateIndicatorAchievementMutation"
    _mutation_module = MerankabandiConfig.name
    _model = IndicatorAchievement

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_indicator_achievement_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = IndicatorAchievementService(user)
        return service.create(data)

    class Input(CreateIndicatorAchievementInputType):
        pass

class UpdateIndicatorAchievementMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateIndicatorAchievementMutation"
    _mutation_module = MerankabandiConfig.name
    _model = IndicatorAchievement

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_indicator_achievement_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = IndicatorAchievementService(user)
        return service.update(data)

    class Input(UpdateIndicatorAchievementInputType):
        pass

class DeleteIndicatorAchievementMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteIndicatorAchievementMutation"
    _mutation_module = MerankabandiConfig.name
    _model = IndicatorAchievement

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                get_merankabandi_config().gql_indicator_achievement_delete_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = IndicatorAchievementService(user)

        ids = data.get('ids')
        if ids:
            with transaction.atomic():
                for id in ids:
                    service.delete({'id': id, 'user': user})

    class Input(DeleteIndicatorAchievementInputType):
        pass

# Province Payroll Generation mutation
class GenerateProvincePayrollInputType(OpenIMISMutation.Input):
    province_id = graphene.String(required=True, description="UUID of the province location")
    payment_plan_id = graphene.String(required=True, description="UUID of the payment plan")
    payment_date = graphene.Date(required=True, description="Payment date for the payroll")

class GenerateProvincePayrollResponseType(graphene.ObjectType):
    success = graphene.Boolean(required=True)
    error = graphene.String()
    payment_cycle_id = graphene.String()
    payment_cycle_code = graphene.String()
    payment_date = graphene.String()
    province_id = graphene.String()
    province_name = graphene.String()
    benefit_plan_id = graphene.String()
    benefit_plan_name = graphene.String()
    generated_payrolls = graphene.List(graphene.JSONString)
    total_payrolls = graphene.Int()
    total_beneficiaries = graphene.Int()

class GenerateProvincePayrollMutation(BaseMutation):
    _mutation_class = "GenerateProvincePayrollMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(GenerateProvincePayrollInputType):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.services import PayrollGenerationService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        province_id = data.get('province_id')
        payment_plan_id = data.get('payment_plan_id')
        payment_date = data.get('payment_date')

        service = PayrollGenerationService(user)
        result = service.generate_province_payroll(
            province_id=province_id,
            payment_plan_id=payment_plan_id,
            payment_date=payment_date
        )

        return result

# Add Province Payment Point mutation
class AddProvincePaymentPointInputType(OpenIMISMutation.Input):
    province_id = graphene.String(required=True, description="UUID of the province location")
    payment_point_id = graphene.String(required=True, description="UUID of the payment point")
    payment_plan_id = graphene.String(required=False, description="UUID of the payment plan (optional)")

class AddProvincePaymentPointResponseType(graphene.ObjectType):
    success = graphene.Boolean(required=True)
    error = graphene.String()
    province_id = graphene.String()
    province_name = graphene.String()
    payment_point_id = graphene.String()
    payment_point_name = graphene.String()
    benefit_plan_id = graphene.String()
    benefit_plan_name = graphene.String()

class AddProvincePaymentPointMutation(BaseMutation):
    _mutation_class = "AddProvincePaymentPointMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(AddProvincePaymentPointInputType):
        pass

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.services import ProvincePaymentPointService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        province_id = data.get('province_id')
        payment_point_id = data.get('payment_point_id')
        payment_plan_id = data.get('payment_plan_id')

        service = ProvincePaymentPointService(user)
        result = service.add_province_payment_point(
            province_id=province_id,
            payment_point_id=payment_point_id,
            payment_plan_id=payment_plan_id
        )

        return result

# ProvincePaymentPoint mutations
class CreateProvincePaymentPointInputType(OpenIMISMutation.Input):
    province_id = graphene.String(required=True, description="UUID of the province location")
    payment_point_id = graphene.String(required=True, description="UUID of the payment point")
    payment_plan_id = graphene.String(required=False, description="UUID of the payment plan (optional)")
    is_active = graphene.Boolean(required=False, description="Is the association active")

class UpdateProvincePaymentPointInputType(CreateProvincePaymentPointInputType):
    id = graphene.String(required=True)

class DeleteProvincePaymentPointInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.String, required=True)

class CreateProvincePaymentPointMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateProvincePaymentPointMutation"
    _mutation_module = MerankabandiConfig.name
    _model = ProvincePaymentPoint

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = ProvincePaymentPointService(user)
        return service.create(data)

    class Input(CreateProvincePaymentPointInputType):
        pass

class UpdateProvincePaymentPointMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateProvincePaymentPointMutation"
    _mutation_module = MerankabandiConfig.name
    _model = ProvincePaymentPoint

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = ProvincePaymentPointService(user)
        return service.update(data)

    class Input(UpdateProvincePaymentPointInputType):
        pass

class DeleteProvincePaymentPointMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteProvincePaymentPointMutation"
    _mutation_module = MerankabandiConfig.name
    _model = ProvincePaymentPoint

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                PayrollConfig.gql_payroll_create_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = ProvincePaymentPointService(user)

        ids = data.get('ids')
        if ids:
            with transaction.atomic():
                for id in ids:
                    service.delete({'id': id})

    class Input(DeleteProvincePaymentPointInputType):
        pass