# Merankabandi module initialization
# Import performance optimization components

default_app_config = 'merankabandi.apps.MerankabandiConfig'

# Make key components available at module level
from .views_manager import MaterializedViewsManager
from .dashboard_service import DashboardService
from .optimized_gql_queries import OptimizedDashboardQuery
from .optimized_gql_mutations import DashboardMutations

__all__ = [
    'MaterializedViewsManager',
    'DashboardService',
    'OptimizedDashboardQuery',
    'DashboardMutations',
]