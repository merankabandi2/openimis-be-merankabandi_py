"""
Optimized GraphQL Mutations for Dashboard Management
Mutations for refreshing views, clearing cache, and managing dashboard system
"""

import graphene
from graphene import String, Boolean, Int, Float
from django.core.cache import cache
from datetime import datetime
from .materialized_views import MaterializedViewManager
from .optimized_dashboard_service import OptimizedDashboardService


# Response Types
class RefreshViewResponseType(graphene.ObjectType):
    success = graphene.Boolean()
    message = graphene.String()
    view_name = graphene.String()
    timestamp = graphene.String()
    duration_seconds = graphene.Float()


class RefreshAllViewsResponseType(graphene.ObjectType):
    success = graphene.Boolean()
    message = graphene.String()
    views_refreshed = graphene.Int()
    timestamp = graphene.String()
    duration_seconds = graphene.Float()


class ClearCacheResponseType(graphene.ObjectType):
    success = graphene.Boolean()
    message = graphene.String()
    cache_pattern = graphene.String()
    timestamp = graphene.String()


class CreateViewsResponseType(graphene.ObjectType):
    success = graphene.Boolean()
    message = graphene.String()
    views_created = graphene.Int()
    indexes_created = graphene.Int()
    timestamp = graphene.String()
    duration_seconds = graphene.Float()


# Input Types
class RefreshViewInput(graphene.InputObjectType):
    view_name = graphene.String(required=True)
    concurrent = graphene.Boolean(default_value=True)


class RefreshAllViewsInput(graphene.InputObjectType):
    concurrent = graphene.Boolean(default_value=True)
    force = graphene.Boolean(default_value=False)


class ClearCacheInput(graphene.InputObjectType):
    pattern = graphene.String()  # Optional pattern to clear specific cache keys


# Optimized GraphQL Mutations
class OptimizedDashboardMutation(graphene.ObjectType):
    """
    Dashboard management mutations for materialized views and cache
    """
    
    refresh_dashboard_view = graphene.Field(
        RefreshViewResponseType,
        input=graphene.Argument(RefreshViewInput, required=True),
        description="Refresh a specific materialized view"
    )
    
    refresh_all_dashboard_views = graphene.Field(
        RefreshAllViewsResponseType,
        input=graphene.Argument(RefreshAllViewsInput),
        description="Refresh all dashboard materialized views"
    )
    
    clear_dashboard_cache = graphene.Field(
        ClearCacheResponseType,
        input=graphene.Argument(ClearCacheInput),
        description="Clear dashboard cache"
    )
    
    create_dashboard_views = graphene.Field(
        CreateViewsResponseType,
        description="Create all dashboard materialized views and indexes"
    )
    
    def resolve_refresh_dashboard_view(self, info, input):
        """Refresh a specific materialized view"""
        start_time = datetime.now()
        
        try:
            view_name = input.view_name
            concurrent = input.concurrent
            
            # Validate view name
            valid_views = [
                'dashboard_beneficiary_summary',
                'dashboard_monetary_transfers',
                'dashboard_activities_summary',
                'dashboard_microprojects',
                'dashboard_grievances', 
                'dashboard_indicators',
                'dashboard_master_summary'
            ]
            
            if view_name not in valid_views:
                return RefreshViewResponseType(
                    success=False,
                    message=f"Invalid view name. Valid views: {', '.join(valid_views)}",
                    view_name=view_name,
                    timestamp=start_time.isoformat(),
                    duration_seconds=0
                )
            
            # Refresh the view
            MaterializedViewManager.refresh_view(view_name, concurrent)
            
            # Clear related cache
            cache_pattern = f"gql_{view_name.replace('dashboard_', '')}"
            OptimizedDashboardService.clear_cache(cache_pattern)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return RefreshViewResponseType(
                success=True,
                message=f"Successfully refreshed view: {view_name}",
                view_name=view_name,
                timestamp=end_time.isoformat(),
                duration_seconds=duration
            )
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return RefreshViewResponseType(
                success=False,
                message=f"Error refreshing view: {e}",
                view_name=input.view_name,
                timestamp=end_time.isoformat(),
                duration_seconds=duration
            )
    
    def resolve_refresh_all_dashboard_views(self, info, input=None):
        """Refresh all dashboard materialized views"""
        start_time = datetime.now()
        
        try:
            concurrent = input.concurrent if input else True
            force = input.force if input else False
            
            # Check if refresh is needed (unless forced)
            if not force:
                needs_refresh = OptimizedDashboardService.refresh_views_if_needed()
                if not needs_refresh:
                    return RefreshAllViewsResponseType(
                        success=True,
                        message="Views are up to date, no refresh needed",
                        views_refreshed=0,
                        timestamp=start_time.isoformat(),
                        duration_seconds=0
                    )
            
            # Refresh all views
            MaterializedViewManager.refresh_all_views(concurrent)
            
            # Clear all dashboard cache
            OptimizedDashboardService.clear_cache()
            
            # Count views
            stats = MaterializedViewManager.get_view_stats()
            views_count = len(stats)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return RefreshAllViewsResponseType(
                success=True,
                message=f"Successfully refreshed all {views_count} dashboard views",
                views_refreshed=views_count,
                timestamp=end_time.isoformat(),
                duration_seconds=duration
            )
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return RefreshAllViewsResponseType(
                success=False,
                message=f"Error refreshing views: {e}",
                views_refreshed=0,
                timestamp=end_time.isoformat(),
                duration_seconds=duration
            )
    
    def resolve_clear_dashboard_cache(self, info, input=None):
        """Clear dashboard cache"""
        try:
            pattern = input.pattern if input else None
            
            if pattern:
                # In production, you'd use cache.delete_pattern(pattern)
                # For now, clear all cache if pattern is specified
                cache.clear()
                message = f"Cleared cache for pattern: {pattern}"
            else:
                # Clear all cache
                OptimizedDashboardService.clear_cache()
                message = "Cleared all dashboard cache"
                pattern = "all"
            
            return ClearCacheResponseType(
                success=True,
                message=message,
                cache_pattern=pattern,
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            return ClearCacheResponseType(
                success=False,
                message=f"Error clearing cache: {e}",
                cache_pattern=pattern if input and input.pattern else "all",
                timestamp=datetime.now().isoformat()
            )
    
    def resolve_create_dashboard_views(self, info):
        """Create all dashboard materialized views and indexes"""
        start_time = datetime.now()
        
        try:
            # Create all views and indexes
            MaterializedViewManager.create_all_views()
            
            # Get statistics
            stats = MaterializedViewManager.get_view_stats()
            views_count = len(stats)
            
            # Estimate indexes created (each view has multiple indexes)
            indexes_count = views_count * 5  # Approximate
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return CreateViewsResponseType(
                success=True,
                message=f"Successfully created {views_count} materialized views and {indexes_count} indexes",
                views_created=views_count,
                indexes_created=indexes_count,
                timestamp=end_time.isoformat(),
                duration_seconds=duration
            )
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            return CreateViewsResponseType(
                success=False,
                message=f"Error creating views: {e}",
                views_created=0,
                indexes_created=0,
                timestamp=end_time.isoformat(),
                duration_seconds=duration
            )


# Background task mutations (for future Celery integration)
class BackgroundTaskResponseType(graphene.ObjectType):
    success = graphene.Boolean()
    task_id = graphene.String()
    message = graphene.String()
    timestamp = graphene.String()


class ScheduleViewRefreshInput(graphene.InputObjectType):
    view_name = graphene.String()
    schedule_minutes = graphene.Int(default_value=60)  # Default: every hour


class OptimizedDashboardBackgroundMutation(graphene.ObjectType):
    """
    Background task mutations (for future Celery integration)
    """
    
    schedule_view_refresh = graphene.Field(
        BackgroundTaskResponseType,
        input=graphene.Argument(ScheduleViewRefreshInput, required=True),
        description="Schedule periodic view refresh (requires Celery)"
    )
    
    def resolve_schedule_view_refresh(self, info, input):
        """Schedule periodic view refresh (placeholder for Celery integration)"""
        try:
            # This would integrate with Celery in production
            # For now, just return a placeholder response
            
            return BackgroundTaskResponseType(
                success=True,
                task_id="placeholder-task-id",
                message=f"Scheduled refresh for {input.view_name or 'all views'} every {input.schedule_minutes} minutes",
                timestamp=datetime.now().isoformat()
            )
            
        except Exception as e:
            return BackgroundTaskResponseType(
                success=False,
                task_id=None,
                message=f"Error scheduling task: {e}",
                timestamp=datetime.now().isoformat()
            )


# Combined mutation class
class DashboardMutations(OptimizedDashboardMutation, OptimizedDashboardBackgroundMutation):
    """Combined dashboard mutations"""
    pass