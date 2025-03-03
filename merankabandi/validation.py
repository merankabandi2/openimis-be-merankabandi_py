from django.core.exceptions import ValidationError
from django.utils.translation import gettext as _

from core.validation import BaseModelValidation
from merankabandi.models import MonetaryTransfer
from payroll.models import PaymentPoint, Payroll, PayrollBill, BenefitConsumption

class MonetaryTransferValidation(BaseModelValidation):
    OBJECT_TYPE = MonetaryTransfer

    @classmethod
    def validate_create(cls, user, **data):
        super().validate_create(user, **data)

    @classmethod
    def validate_update(cls, user, **data):
        super().validate_update(user, **data)

    @classmethod
    def validate_delete(cls, user, **data):
        super().validate_delete(user, **data)