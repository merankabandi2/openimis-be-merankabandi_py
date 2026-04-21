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
from merankabandi.models import MonetaryTransfer, Section, Indicator, IndicatorAchievement, PaymentAgency, ProvincePaymentAgency, AgencyFeeConfig, PmtFormula, SelectionQuota, PreCollecte, SensitizationTraining, BehaviorChangePromotion, MicroProject
from location.models import UserDistrict
from merankabandi.services import (
    MonetaryTransferService, SectionService, IndicatorService,
    IndicatorAchievementService
)
from django.core.management import call_command
from social_protection.models import BenefitPlan
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
    planned_amount = graphene.Decimal(required=False)
    transferred_amount = graphene.Decimal(required=False)
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
                    service.delete({'id': id})

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
                    # BaseService already holds `user` as self.user and forwards
                    # obj_data as **kwargs to validate_delete — do not pass
                    # user again here, it would collide with the positional arg.
                    service.delete({'id': id})

    class Input(DeleteSectionInputType):
        pass

# Survey & PMT Targeting Mutations


class ImportSurveyDataMutation(BaseMutation):
    _mutation_class = "ImportSurveyDataMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)
        csv_path = graphene.String(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))
        csv_path = data.get('csv_path', '')
        if not csv_path.startswith('/data/'):
            raise ValidationError(_("csv_path must start with /data/"))

    @classmethod
    def _mutate(cls, user, **data):
        call_command(
            'import_households_to_benefit_plan',
            data['csv_path'],
            str(data['benefit_plan_id']),
        )


class TriggerPMTCalculationMutation(BaseMutation):
    _mutation_class = "TriggerPMTCalculationMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.pmt_scoring_service import BurundiPMTScoringService
        benefit_plan = BenefitPlan.objects.get(id=data['benefit_plan_id'])
        BurundiPMTScoringService.score_beneficiaries(
            benefit_plan=benefit_plan, username=user.username,
        )

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
                    service.delete({'id': id})

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
                    service.delete({'id': id})

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



# PaymentAgency CRUD mutations

class CreatePaymentAgencyInputType(OpenIMISMutation.Input):
    code = graphene.String(required=True)
    name = graphene.String(required=True)
    payment_gateway = graphene.String(required=False)
    gateway_config = graphene.String(required=False)
    contact_name = graphene.String(required=False)
    contact_phone = graphene.String(required=False)
    contact_email = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)


class UpdatePaymentAgencyInputType(CreatePaymentAgencyInputType):
    id = graphene.String(required=True)


class DeletePaymentAgencyInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.String, required=True)


class CreatePaymentAgencyMutation(BaseMutation):
    _mutation_class = "CreatePaymentAgencyMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        PaymentAgency.objects.create(**data)

    class Input(CreatePaymentAgencyInputType):
        pass


class UpdatePaymentAgencyMutation(BaseMutation):
    _mutation_class = "UpdatePaymentAgencyMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        agency_id = data.pop('id')
        agency = PaymentAgency.objects.get(id=agency_id)
        agency.update(data)

    class Input(UpdatePaymentAgencyInputType):
        pass


class DeletePaymentAgencyMutation(BaseMutation):
    _mutation_class = "DeletePaymentAgencyMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ids = data.get('ids', [])
        PaymentAgency.objects.filter(id__in=ids).delete()

    class Input(DeletePaymentAgencyInputType):
        pass


# ProvincePaymentAgency CRUD mutations

class CreateProvincePaymentAgencyInputType(OpenIMISMutation.Input):
    province_id = graphene.String(required=True)
    benefit_plan_id = graphene.String(required=True)
    payment_agency_id = graphene.String(required=True)
    is_active = graphene.Boolean(required=False)


class UpdateProvincePaymentAgencyInputType(CreateProvincePaymentAgencyInputType):
    id = graphene.String(required=True)


class DeleteProvincePaymentAgencyInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.String, required=True)


class CreateProvincePaymentAgencyMutation(BaseMutation):
    _mutation_class = "CreateProvincePaymentAgencyMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ProvincePaymentAgency.objects.create(**data)

    class Input(CreateProvincePaymentAgencyInputType):
        pass


class UpdateProvincePaymentAgencyMutation(BaseMutation):
    _mutation_class = "UpdateProvincePaymentAgencyMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        obj_id = data.pop('id')
        obj = ProvincePaymentAgency.objects.get(id=obj_id)
        obj.update(data)

    class Input(UpdateProvincePaymentAgencyInputType):
        pass


class DeleteProvincePaymentAgencyMutation(BaseMutation):
    _mutation_class = "DeleteProvincePaymentAgencyMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ids = data.get('ids', [])
        ProvincePaymentAgency.objects.filter(id__in=ids).delete()

    class Input(DeleteProvincePaymentAgencyInputType):
        pass


# ── AgencyFeeConfig CRUD ──────────────────────────────────────────────

class CreateAgencyFeeConfigInputType(OpenIMISMutation.Input):
    payment_agency_id = graphene.UUID(required=True)
    benefit_plan_id = graphene.UUID(required=True)
    province_id = graphene.UUID(required=False)
    fee_rate = graphene.Decimal(required=True)
    fee_included = graphene.Boolean(required=False, default_value=False)
    is_active = graphene.Boolean(required=False, default_value=True)


class UpdateAgencyFeeConfigInputType(CreateAgencyFeeConfigInputType):
    id = graphene.UUID(required=True)


class DeleteAgencyFeeConfigInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class CreateAgencyFeeConfigMutation(BaseMutation):
    _mutation_class = "CreateAgencyFeeConfigMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        province_id = data.pop('province_id', None)
        if province_id:
            from location.models import Location
            data['province'] = Location.objects.get(uuid=province_id)
        data['payment_agency_id'] = data.pop('payment_agency_id')
        data['benefit_plan_id'] = data.pop('benefit_plan_id')
        AgencyFeeConfig.objects.create(**data)

    class Input(CreateAgencyFeeConfigInputType):
        pass


class UpdateAgencyFeeConfigMutation(BaseMutation):
    _mutation_class = "UpdateAgencyFeeConfigMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        config_id = data.pop('id')
        province_id = data.pop('province_id', None)
        config = AgencyFeeConfig.objects.get(id=config_id)
        if province_id:
            from location.models import Location
            data['province'] = Location.objects.get(uuid=province_id)
        elif 'province_id' in data:
            data['province'] = None
        for k, v in data.items():
            setattr(config, k, v)
        config.save()

    class Input(UpdateAgencyFeeConfigInputType):
        pass


class DeleteAgencyFeeConfigMutation(BaseMutation):
    _mutation_class = "DeleteAgencyFeeConfigMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ids = data.get('ids', [])
        AgencyFeeConfig.objects.filter(id__in=ids).delete()

    class Input(DeleteAgencyFeeConfigInputType):
        pass


# Validation mutations for KoboToolbox data
class ValidateKoboDataInputType(OpenIMISMutation.Input):
    id = graphene.UUID(required=True, description="ID of the record to validate")
    status = graphene.String(required=True, description="VALIDATED or REJECTED")
    comment = graphene.String(required=False, description="Validation comment")


def _check_user_province_access(user, activity):
    """
    Checks that the activity's location falls within one of the user's assigned provinces.
    Superusers bypass this check. Raises ValidationError if access is denied.
    """
    interactive_user = user._u if hasattr(user, '_u') else user
    if interactive_user.is_superuser:
        return
    districts = UserDistrict.get_user_districts(user)
    province_ids = [d.location_id for d in districts]
    if not province_ids:
        raise ValidationError(_("You have no province assignments. Cannot validate activities."))
    # Activity location is at colline level: colline -> commune -> province
    activity_province_id = activity.location.parent.parent_id if activity.location.parent else None
    if activity_province_id not in province_ids:
        raise ValidationError(_("You are not authorized to validate activities outside your assigned province."))


class ValidateSensitizationTrainingMutation(BaseMutation):
    _mutation_class = "ValidateSensitizationTrainingMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))
        training = SensitizationTraining.objects.select_related(
            'location__parent'
        ).get(id=data.get('id'))
        _check_user_province_access(user, training)

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.services_validation import KoboDataValidationService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        training_id = data.get('id')
        status = data.get('status')
        comment = data.get('comment')

        if status not in ['VALIDATED', 'REJECTED']:
            raise ValidationError(_("Invalid status. Must be VALIDATED or REJECTED"))

        success, training, error = KoboDataValidationService.validate_sensitization_training(
            user=user,
            training_id=training_id,
            status=status,
            comment=comment
        )

        if not success:
            raise ValidationError(error)

        return {"success": success}

    class Input(ValidateKoboDataInputType):
        pass


class ValidateBehaviorChangeMutation(BaseMutation):
    _mutation_class = "ValidateBehaviorChangeMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))
        behavior_change = BehaviorChangePromotion.objects.select_related(
            'location__parent'
        ).get(id=data.get('id'))
        _check_user_province_access(user, behavior_change)

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.services_validation import KoboDataValidationService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        behavior_change_id = data.get('id')
        status = data.get('status')
        comment = data.get('comment')

        if status not in ['VALIDATED', 'REJECTED']:
            raise ValidationError(_("Invalid status. Must be VALIDATED or REJECTED"))

        success, behavior_change, error = KoboDataValidationService.validate_behavior_change(
            user=user,
            behavior_change_id=behavior_change_id,
            status=status,
            comment=comment
        )

        if not success:
            raise ValidationError(error)

        return {"success": success}

    class Input(ValidateKoboDataInputType):
        pass


class ValidateMicroProjectMutation(BaseMutation):
    _mutation_class = "ValidateMicroProjectMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))
        microproject = MicroProject.objects.select_related(
            'location__parent'
        ).get(id=data.get('id'))
        _check_user_province_access(user, microproject)

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.services_validation import KoboDataValidationService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        microproject_id = data.get('id')
        status = data.get('status')
        comment = data.get('comment')

        if status not in ['VALIDATED', 'REJECTED']:
            raise ValidationError(_("Invalid status. Must be VALIDATED or REJECTED"))

        success, microproject, error = KoboDataValidationService.validate_microproject(
            user=user,
            microproject_id=microproject_id,
            status=status,
            comment=comment
        )

        if not success:
            raise ValidationError(error)

        return {"success": success}

    class Input(ValidateKoboDataInputType):
        pass


class BulkUpdateGroupBeneficiaryStatusMutation(BaseMutation):
    _mutation_class = "BulkUpdateGroupBeneficiaryStatusMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from social_protection.models import GroupBeneficiary, BeneficiaryStatus
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        ids = data.get('ids', [])
        status = data.get('status')
        benefit_plan_id = data.get('benefit_plan_id')
        current_status = data.get('current_status')
        json_ext_update = data.get('json_ext_update')

        valid_statuses = [s.value for s in BeneficiaryStatus]
        if status not in valid_statuses:
            raise ValidationError(_(f"Invalid status: {status}"))

        qs = GroupBeneficiary.objects.filter(
            benefit_plan_id=benefit_plan_id,
            is_deleted=False,
        )
        if ids:
            qs = qs.filter(id__in=ids)
        if current_status:
            qs = qs.filter(status=current_status)

        if json_ext_update:
            import json
            update_data = json.loads(json_ext_update) if isinstance(json_ext_update, str) else json_ext_update
            updated = 0
            for gb in qs:
                ext = gb.json_ext or {}
                ext.update(update_data)
                gb.json_ext = ext
                gb.status = status
                gb.save(username=user.username)
                updated += 1
        else:
            updated = qs.update(status=status)

        # Fire community validation notification if selection_status was set
        if json_ext_update:
            sel_status = update_data.get('selection_status', '') if isinstance(update_data, dict) else ''
            if sel_status in ('COMMUNITY_VALIDATED', 'COMMUNITY_REJECTED'):
                try:
                    from merankabandi.notification_signals import on_community_validation_completed
                    bp = BenefitPlan.objects.filter(id=benefit_plan_id).first()
                    validated = updated if sel_status == 'COMMUNITY_VALIDATED' else 0
                    rejected = updated if sel_status == 'COMMUNITY_REJECTED' else 0
                    on_community_validation_completed(
                        user=user,
                        result={
                            "program_name": (bp.name or bp.code) if bp else "",
                            "location": "",
                            "validated_count": validated,
                            "rejected_count": rejected,
                        },
                    )
                except Exception:
                    pass

        return {"success": True, "count": updated}

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)
        ids = graphene.List(graphene.UUID, required=False)
        status = graphene.String(required=True)
        current_status = graphene.String(required=False)
        json_ext_update = graphene.JSONString(required=False)


# PmtFormula mutations
class CreatePmtFormulaInputType(OpenIMISMutation.Input):
    name = graphene.String(required=True)
    description = graphene.String(required=False)
    base_score_urban = graphene.Decimal(required=False)
    base_score_rural = graphene.Decimal(required=False)
    variables = graphene.JSONString(required=False)
    geographic_adjustments = graphene.JSONString(required=False)
    is_active = graphene.Boolean(required=False)


class UpdatePmtFormulaInputType(CreatePmtFormulaInputType):
    id = graphene.UUID(required=True)


class DeletePmtFormulaInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class CreatePmtFormulaMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreatePmtFormulaMutation"
    _mutation_module = MerankabandiConfig.name
    _model = PmtFormula

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        import json
        if isinstance(data.get('variables'), str):
            data['variables'] = json.loads(data['variables'])
        if isinstance(data.get('geographic_adjustments'), str):
            data['geographic_adjustments'] = json.loads(data['geographic_adjustments'])
        PmtFormula.objects.create(**data)

    class Input(CreatePmtFormulaInputType):
        pass


class UpdatePmtFormulaMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdatePmtFormulaMutation"
    _mutation_module = MerankabandiConfig.name
    _model = PmtFormula

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        formula_id = data.pop('id')
        import json
        if isinstance(data.get('variables'), str):
            data['variables'] = json.loads(data['variables'])
        if isinstance(data.get('geographic_adjustments'), str):
            data['geographic_adjustments'] = json.loads(data['geographic_adjustments'])
        PmtFormula.objects.filter(id=formula_id).update(**data)

    class Input(UpdatePmtFormulaInputType):
        pass


class DeletePmtFormulaMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeletePmtFormulaMutation"
    _mutation_module = MerankabandiConfig.name
    _model = PmtFormula

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.id or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ids = data.get('ids')
        if ids:
            PmtFormula.objects.filter(id__in=ids).delete()

    class Input(DeletePmtFormulaInputType):
        pass


# SelectionQuota mutations
class CreateSelectionQuotaInputType(OpenIMISMutation.Input):
    benefit_plan_id = graphene.UUID(required=True)
    location_id = graphene.Int(required=True)
    targeting_round = graphene.Int(required=False)
    quota = graphene.Int(required=True)
    collect_multiplier = graphene.Decimal(required=False)


class UpdateSelectionQuotaInputType(CreateSelectionQuotaInputType):
    id = graphene.UUID(required=True)


class DeleteSelectionQuotaInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class CreateSelectionQuotaMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreateSelectionQuotaMutation"
    _mutation_module = MerankabandiConfig.name
    _model = SelectionQuota

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        SelectionQuota.objects.create(**data)

    class Input(CreateSelectionQuotaInputType):
        pass


class UpdateSelectionQuotaMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdateSelectionQuotaMutation"
    _mutation_module = MerankabandiConfig.name
    _model = SelectionQuota

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        quota_id = data.pop('id')
        SelectionQuota.objects.filter(id=quota_id).update(**data)

    class Input(UpdateSelectionQuotaInputType):
        pass


class DeleteSelectionQuotaMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeleteSelectionQuotaMutation"
    _mutation_module = MerankabandiConfig.name
    _model = SelectionQuota

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ids = data.get('ids')
        if ids:
            SelectionQuota.objects.filter(id__in=ids).delete()

    class Input(DeleteSelectionQuotaInputType):
        pass


# PreCollecte mutations
class CreatePreCollecteInputType(OpenIMISMutation.Input):
    benefit_plan_id = graphene.UUID(required=True)
    location_id = graphene.Int(required=True)
    origin_location_id = graphene.Int(required=False)
    nom = graphene.String(required=True)
    prenom = graphene.String(required=True)
    pere = graphene.String(required=False)
    mere = graphene.String(required=False)
    ci = graphene.String(required=False)
    telephone = graphene.String(required=False)
    sexe = graphene.String(required=True)
    mutwa = graphene.Boolean(required=False)
    rapatrie = graphene.Boolean(required=False)
    age_handicap = graphene.Boolean(required=False)
    targeting_round = graphene.Int(required=False)
    kobo_uuid = graphene.String(required=False)
    device_id = graphene.String(required=False)
    json_ext = graphene.JSONString(required=False)


class UpdatePreCollecteInputType(CreatePreCollecteInputType):
    id = graphene.UUID(required=True)


class DeletePreCollecteInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class CreatePreCollecteMutation(BaseHistoryModelCreateMutationMixin, BaseMutation):
    _mutation_class = "CreatePreCollecteMutation"
    _mutation_module = MerankabandiConfig.name
    _model = PreCollecte

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.social_id_service import generate_social_id
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        import json
        if isinstance(data.get('json_ext'), str):
            data['json_ext'] = json.loads(data['json_ext'])
        pc = PreCollecte(**data)
        generate_social_id(pc)
        pc.save()

    class Input(CreatePreCollecteInputType):
        pass


class UpdatePreCollecteMutation(BaseHistoryModelUpdateMutationMixin, BaseMutation):
    _mutation_class = "UpdatePreCollecteMutation"
    _mutation_module = MerankabandiConfig.name
    _model = PreCollecte

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        pc_id = data.pop('id')
        PreCollecte.objects.filter(id=pc_id).update(**data)

    class Input(UpdatePreCollecteInputType):
        pass


class DeletePreCollecteMutation(BaseHistoryModelDeleteMutationMixin, BaseMutation):
    _mutation_class = "DeletePreCollecteMutation"
    _mutation_module = MerankabandiConfig.name
    _model = PreCollecte

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ids = data.get('ids')
        if ids:
            PreCollecte.objects.filter(id__in=ids).delete()

    class Input(DeletePreCollecteInputType):
        pass


# Selection lifecycle mutations
class ApplyQuotaSelectionMutation(BaseMutation):
    _mutation_class = "ApplyQuotaSelectionMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)
        targeting_round = graphene.Int(required=False)

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.selection_service import SelectionService
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        result = SelectionService.apply_quota_selection(
            benefit_plan_id=data['benefit_plan_id'],
            targeting_round=data.get('targeting_round', 1),
        )
        return result


class ApplyCriteriaSelectionMutation(BaseMutation):
    _mutation_class = "ApplyCriteriaSelectionMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.selection_service import SelectionService
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        result = SelectionService.apply_criteria_selection(
            benefit_plan_id=data['benefit_plan_id'],
        )
        return result


class SelectAllMutation(BaseMutation):
    _mutation_class = "SelectAllMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.selection_service import SelectionService
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        result = SelectionService.select_all(
            benefit_plan_id=data['benefit_plan_id'],
        )
        return result


class PromoteToBeneficiaryMutation(BaseMutation):
    _mutation_class = "PromoteToBeneficiaryMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.selection_service import SelectionService
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        result = SelectionService.promote_to_beneficiary(
            benefit_plan_id=data['benefit_plan_id'],
            username=user.username,
        )
        return result


class PromoteFromWaitingListMutation(BaseMutation):
    _mutation_class = "PromoteFromWaitingListMutation"
    _mutation_module = MerankabandiConfig.name

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)
        colline_id = graphene.Int(required=True)
        count = graphene.Int(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        from social_protection.apps import SocialProtectionConfig
        if type(user) is AnonymousUser or not user.has_perms(
                SocialProtectionConfig.gql_beneficiary_update_perms):
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from merankabandi.selection_service import SelectionService
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        result = SelectionService.promote_from_waiting_list(
            benefit_plan_id=data['benefit_plan_id'],
            colline_id=data['colline_id'],
            count=data['count'],
            username=user.username,
        )
        return result


# Enhanced comment mutation with json_ext support (avoids modifying upstream grievance module)

class CreateTicketCommentInputType(OpenIMISMutation.Input):
    ticket_id = graphene.UUID(required=True)
    commenter_type = graphene.String(required=False)
    commenter_id = graphene.String(required=False)
    comment = graphene.String(required=True)
    json_ext = graphene.JSONString(required=False)


class CreateTicketCommentMutation(BaseMutation):
    _mutation_class = "CreateTicketCommentMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from grievance_social_protection.services import CommentService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        if 'commenter_type' in data:
            data['commenter_type'] = (data.get('commenter_type') or '').lower()

        service = CommentService(user)
        response = service.create(data)

        if not response.get('success', True):
            return response
        return None

    class Input(CreateTicketCommentInputType):
        pass


# Enhanced ticket mutations with json_ext support (avoids modifying upstream grievance module)

class CreateTicketWithExtInputType(OpenIMISMutation.Input):
    key = graphene.String(required=False)
    title = graphene.String(required=False)
    description = graphene.String(required=False)
    reporter_type = graphene.String(required=False)
    reporter_id = graphene.String(required=False)
    attending_staff_id = graphene.UUID(required=False)
    date_of_incident = graphene.Date(required=False)
    status = graphene.String(required=False)
    priority = graphene.String(required=False)
    due_date = graphene.Date(required=False)
    category = graphene.String(required=False)
    flags = graphene.String(required=False)
    channel = graphene.String(required=False)
    resolution = graphene.String(required=False)
    json_ext = graphene.JSONString(required=False)


class CreateTicketWithExtMutation(BaseMutation):
    _mutation_class = "CreateTicketWithExtMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from grievance_social_protection.services import TicketService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = TicketService(user)
        response = service.create(data)

        if not response.get('success', True):
            return response
        return None

    class Input(CreateTicketWithExtInputType):
        pass


class UpdateTicketWithExtInputType(CreateTicketWithExtInputType):
    id = graphene.UUID(required=True)


class UpdateTicketWithExtMutation(BaseMutation):
    _mutation_class = "UpdateTicketWithExtMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if type(user) is AnonymousUser or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        from grievance_social_protection.services import TicketService

        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = TicketService(user)
        response = service.update(data)

        if not response.get('success', True):
            return response
        return None

    class Input(UpdateTicketWithExtInputType):
        pass
