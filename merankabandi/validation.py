from django.core.exceptions import ValidationError
from django.utils.translation import gettext as _
from core.validation import BaseModelValidation
from merankabandi.models import MonetaryTransfer, Section, Indicator, IndicatorAchievement


class MonetaryTransferValidation(BaseModelValidation):
    OBJECT_TYPE = MonetaryTransfer

    @classmethod
    def validate_create(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_update(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_core_fields(cls, user, **data):
        if 'transfer_date' not in data or not data['transfer_date']:
            raise ValidationError(_("transfer_date is required"))
        if 'location_id' not in data or not data['location_id']:
            raise ValidationError(_("location_id is required"))
        if 'programme_id' not in data or not data['programme_id']:
            raise ValidationError(_("programme_id is required"))
        if 'payment_agency_id' not in data or not data['payment_agency_id']:
            raise ValidationError(_("payment_agency_id is required"))


class SectionValidation(BaseModelValidation):
    OBJECT_TYPE = Section

    @classmethod
    def validate_create(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_update(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_core_fields(cls, user, **data):
        if 'name' not in data or not data['name']:
            raise ValidationError(_("name is required"))


class IndicatorValidation(BaseModelValidation):
    OBJECT_TYPE = Indicator

    @classmethod
    def validate_create(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_update(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_core_fields(cls, user, **data):
        if 'name' not in data or not data['name']:
            raise ValidationError(_("name is required"))


class IndicatorAchievementValidation(BaseModelValidation):
    OBJECT_TYPE = IndicatorAchievement

    @classmethod
    def validate_create(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_update(cls, user, **data):
        cls.validate_core_fields(user, **data)

    @classmethod
    def validate_core_fields(cls, user, **data):
        if 'indicator_id' not in data or not data['indicator_id']:
            raise ValidationError(_("indicator_id is required"))
        if 'achieved' not in data or data['achieved'] is None:
            raise ValidationError(_("achieved is required"))