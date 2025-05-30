# Merankabandi module initialization
# Import performance optimization components

default_app_config = 'merankabandi.apps.MerankabandiConfig'

# Make key components available at module level
from .materialized_views import MaterializedViewManager
from .optimized_dashboard_service import OptimizedDashboardService
from .optimized_gql_queries import OptimizedDashboardQuery
from .optimized_gql_mutations import DashboardMutations

__all__ = [
    'MaterializedViewManager',
    'OptimizedDashboardService', 
    'OptimizedDashboardQuery',
    'DashboardMutations',
]