import os

from django.apps import AppConfig

from core.custom_filters import CustomFilterRegistryPoint
from payroll.payments_registry import PaymentsMethodRegistryPoint
from .strategies import StrategyOnlinePaymentPush, StrategyOnlinePaymentPull

MODULE_NAME = 'merankabandi'

DEFAULT_CONFIG = {}

class MerankabandiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = MODULE_NAME

    def ready(self):
        self.__register_filters_and_payment_methods()

    def __register_filters_and_payment_methods(cls):
        PaymentsMethodRegistryPoint.register_payment_method(
            payment_method_class_list=[
                StrategyOnlinePaymentPush(),
                StrategyOnlinePaymentPull(),
            ]
        )
