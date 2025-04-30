from django.core.exceptions import ValidationError
from django.utils.translation import gettext as _
from core.validation import BaseModelValidation
from merankabandi.models import MonetaryTransfer, Section, Indicator, IndicatorAchievement, ProvincePaymentPoint
from location.models import Location
from payroll.models import PaymentPoint
from contribution_plan.models import PaymentPlan


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


class ProvincePaymentPointValidation(BaseModelValidation):
    @classmethod
    def validate_create(cls, user, **data):
        if not data.get('province_id'):
            raise ValidationError('Province is required')
            
        if not data.get('payment_point_id'):
            raise ValidationError('Payment point is required')
            
        # Validate province exists and is a province (type='D')
        province = Location.objects.filter(id=data.get('province_id'), type='D').first()
        if not province:
            raise ValidationError('Invalid province')
            
        # Validate payment point exists
        payment_point = PaymentPoint.objects.filter(id=data.get('payment_point_id')).first()
        if not payment_point:
            raise ValidationError('Invalid payment point')
            
        # Validate payment plan if provided
        if data.get('payment_plan_id'):
            payment_plan = PaymentPlan.objects.filter(id=data.get('payment_plan_id')).first()
            if not payment_plan:
                raise ValidationError('Invalid payment plan')
                
        # Validate uniqueness
        existing = ProvincePaymentPoint.objects.filter(
            province_id=data.get('province_id'),
            payment_point_id=data.get('payment_point_id'),
            payment_plan_id=data.get('payment_plan_id')
        )
        if existing.exists():
            raise ValidationError('This province-payment point association already exists')
                
    @classmethod
    def validate_update(cls, user, **data):
        cls.validate_create(user, **data)
        
        # Validate object exists
        if not ProvincePaymentPoint.objects.filter(id=data.get('id')).exists():
            raise ValidationError('Invalid ProvincePaymentPoint')