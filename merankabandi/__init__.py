# Merankabandi module initialization
# Import performance optimization components

from .optimized_gql_mutations import DashboardMutations
from .optimized_gql_queries import OptimizedDashboardQuery
from .dashboard_service import DashboardService
from .views_manager import MaterializedViewsManager
default_app_config = 'merankabandi.apps.MerankabandiConfig'

# Make key components available at module level

__all__ = [
    'MaterializedViewsManager',
    'DashboardService',
    'OptimizedDashboardQuery',
    'DashboardMutations',
]
