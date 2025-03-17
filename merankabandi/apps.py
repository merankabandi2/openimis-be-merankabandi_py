import os

from django.apps import AppConfig

from core.custom_filters import CustomFilterRegistryPoint
from payroll.payments_registry import PaymentsMethodRegistryPoint
from .strategies import StrategyOnlinePaymentPush, StrategyOnlinePaymentPull

MODULE_NAME = 'merankabandi'

DEFAULT_CONFIG = {
    "gql_payment_point_search_perms": ["201001"],
    "gql_payment_point_create_perms": ["201002"],
    "gql_payment_point_update_perms": ["201003"],
    "gql_payment_point_delete_perms": ["201004"],
    "gql_payroll_search_perms": ["202001"],
    "gql_payroll_create_perms": ["202002"],
    "gql_payroll_delete_perms": ["202004"]
    }

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
