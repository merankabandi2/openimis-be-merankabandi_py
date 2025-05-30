import os

from django.apps import AppConfig

from core.custom_filters import CustomFilterRegistryPoint
from payroll.payments_registry import PaymentsMethodRegistryPoint
from .strategies import StrategyOnlinePaymentPush, StrategyOnlinePaymentPull
from .gql_config import (
    GQL_SECTION_SEARCH_PERMS, GQL_SECTION_CREATE_PERMS, 
    GQL_SECTION_UPDATE_PERMS, GQL_SECTION_DELETE_PERMS,
    GQL_INDICATOR_SEARCH_PERMS, GQL_INDICATOR_CREATE_PERMS,
    GQL_INDICATOR_UPDATE_PERMS, GQL_INDICATOR_DELETE_PERMS,
    GQL_INDICATOR_ACHIEVEMENT_SEARCH_PERMS, GQL_INDICATOR_ACHIEVEMENT_CREATE_PERMS,
    GQL_INDICATOR_ACHIEVEMENT_UPDATE_PERMS, GQL_INDICATOR_ACHIEVEMENT_DELETE_PERMS
)

MODULE_NAME = 'merankabandi'

DEFAULT_CONFIG = {
    # Payment Point permissions (from Payroll module)
    "gql_payment_point_search_perms": ["201001"],
    "gql_payment_point_create_perms": ["201002"],
    "gql_payment_point_update_perms": ["201003"],
    "gql_payment_point_delete_perms": ["201004"],
    # Payroll permissions (from Payroll module)
    "gql_payroll_search_perms": ["202001"],
    "gql_payroll_create_perms": ["202002"],
    "gql_payroll_delete_perms": ["202004"],
    # Section permissions (Merankabandi specific)
    "gql_section_search_perms": GQL_SECTION_SEARCH_PERMS,
    "gql_section_create_perms": GQL_SECTION_CREATE_PERMS,
    "gql_section_update_perms": GQL_SECTION_UPDATE_PERMS,
    "gql_section_delete_perms": GQL_SECTION_DELETE_PERMS,
    # Indicator permissions (Merankabandi specific)
    "gql_indicator_search_perms": GQL_INDICATOR_SEARCH_PERMS,
    "gql_indicator_create_perms": GQL_INDICATOR_CREATE_PERMS,
    "gql_indicator_update_perms": GQL_INDICATOR_UPDATE_PERMS,
    "gql_indicator_delete_perms": GQL_INDICATOR_DELETE_PERMS,
    # Indicator Achievement permissions (Merankabandi specific)
    "gql_indicator_achievement_search_perms": GQL_INDICATOR_ACHIEVEMENT_SEARCH_PERMS,
    "gql_indicator_achievement_create_perms": GQL_INDICATOR_ACHIEVEMENT_CREATE_PERMS,
    "gql_indicator_achievement_update_perms": GQL_INDICATOR_ACHIEVEMENT_UPDATE_PERMS,
    "gql_indicator_achievement_delete_perms": GQL_INDICATOR_ACHIEVEMENT_DELETE_PERMS
    }

class MerankabandiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = MODULE_NAME
    
    # Section permissions
    gql_section_search_perms = None
    gql_section_create_perms = None
    gql_section_update_perms = None
    gql_section_delete_perms = None
    
    # Indicator permissions
    gql_indicator_search_perms = None
    gql_indicator_create_perms = None
    gql_indicator_update_perms = None
    gql_indicator_delete_perms = None
    
    # Indicator Achievement permissions
    gql_indicator_achievement_search_perms = None
    gql_indicator_achievement_create_perms = None
    gql_indicator_achievement_update_perms = None
    gql_indicator_achievement_delete_perms = None

    def ready(self):
        self.__register_filters_and_payment_methods()
        self.__load_config()
        self.__setup_dashboard_optimization()

    def __register_filters_and_payment_methods(self):
        PaymentsMethodRegistryPoint.register_payment_method(
            payment_method_class_list=[
                StrategyOnlinePaymentPush(),
                StrategyOnlinePaymentPull(),
            ]
        )
    
    def __load_config(self):
        """Load the module configuration including permissions"""
        # For now, use DEFAULT_CONFIG directly instead of ModuleConfiguration
        cfg = DEFAULT_CONFIG
        
        # Load section permissions
        self.gql_section_search_perms = cfg.get("gql_section_search_perms", GQL_SECTION_SEARCH_PERMS)
        self.gql_section_create_perms = cfg.get("gql_section_create_perms", GQL_SECTION_CREATE_PERMS)
        self.gql_section_update_perms = cfg.get("gql_section_update_perms", GQL_SECTION_UPDATE_PERMS)
        self.gql_section_delete_perms = cfg.get("gql_section_delete_perms", GQL_SECTION_DELETE_PERMS)
        
        # Load indicator permissions
        self.gql_indicator_search_perms = cfg.get("gql_indicator_search_perms", GQL_INDICATOR_SEARCH_PERMS)
        self.gql_indicator_create_perms = cfg.get("gql_indicator_create_perms", GQL_INDICATOR_CREATE_PERMS)
        self.gql_indicator_update_perms = cfg.get("gql_indicator_update_perms", GQL_INDICATOR_UPDATE_PERMS)
        self.gql_indicator_delete_perms = cfg.get("gql_indicator_delete_perms", GQL_INDICATOR_DELETE_PERMS)
        
        # Load indicator achievement permissions
        self.gql_indicator_achievement_search_perms = cfg.get("gql_indicator_achievement_search_perms", GQL_INDICATOR_ACHIEVEMENT_SEARCH_PERMS)
        self.gql_indicator_achievement_create_perms = cfg.get("gql_indicator_achievement_create_perms", GQL_INDICATOR_ACHIEVEMENT_CREATE_PERMS)
        self.gql_indicator_achievement_update_perms = cfg.get("gql_indicator_achievement_update_perms", GQL_INDICATOR_ACHIEVEMENT_UPDATE_PERMS)
        self.gql_indicator_achievement_delete_perms = cfg.get("gql_indicator_achievement_delete_perms", GQL_INDICATOR_ACHIEVEMENT_DELETE_PERMS)
    
    def __setup_dashboard_optimization(self):
        """Set up dashboard optimization on app ready"""
        from django.conf import settings
        
        # Only set up if optimization is enabled
        if getattr(settings, 'DASHBOARD_OPTIMIZATION', {}).get('ENABLED', True):
            from django.db import connection
            
            # Check if we're using PostgreSQL
            if 'postgresql' in connection.vendor:
                # The migration will handle creating the views
                # Just log that optimization is enabled
                import logging
                logger = logging.getLogger('merankabandi.dashboard')
                logger.info("Dashboard optimization enabled for PostgreSQL")
            else:
                import logging
                logger = logging.getLogger('merankabandi.dashboard')
                logger.warning("Dashboard optimization requires PostgreSQL, currently using: %s", connection.vendor)
