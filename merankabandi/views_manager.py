"""
Unified Materialized Views Manager
Single entry point for all dashboard materialized views
"""

from django.db import connection
import logging
import time
from typing import Dict, List, Optional

from .views_beneficiary import BENEFICIARY_VIEWS
from .views_grievance import GRIEVANCE_VIEWS
from .views_payment import PAYMENT_VIEWS
from .views_monitoring import MONITORING_VIEWS
from .views_utility import UTILITY_VIEWS

logger = logging.getLogger(__name__)


class MaterializedViewsManager:
    """
    Centralized manager for all materialized views in the Merankabandi dashboard
    """
    
    # Consolidated view registry
    ALL_VIEWS = {
        'beneficiary': BENEFICIARY_VIEWS,
        'grievance': GRIEVANCE_VIEWS,
        'payment': PAYMENT_VIEWS,
        'monitoring': MONITORING_VIEWS,
        'utility': UTILITY_VIEWS,
    }
    
    @classmethod
    def get_all_view_names(cls) -> List[str]:
        """Get all view names across all categories"""
        all_names = []
        for category_views in cls.ALL_VIEWS.values():
            all_names.extend(category_views.keys())
        return all_names
    
    @classmethod
    def get_views_by_category(cls, category: str) -> Dict:
        """Get views for a specific category"""
        return cls.ALL_VIEWS.get(category, {})
    
    @classmethod
    def create_all_views(cls, category: Optional[str] = None) -> Dict[str, bool]:
        """Create all views or views for a specific category"""
        results = {}
        
        if category:
            if category not in cls.ALL_VIEWS:
                raise ValueError(f"Unknown category: {category}")
            categories_to_process = {category: cls.ALL_VIEWS[category]}
        else:
            categories_to_process = cls.ALL_VIEWS
        
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = '30min'")
            for cat_name, views in categories_to_process.items():
                logger.info(f"Creating {cat_name} views...")
                for view_name, view_config in views.items():
                    try:
                        # Drop existing view
                        cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                        
                        # Create new view
                        cursor.execute(view_config['sql'])
                        
                        # Create indexes
                        if 'indexes' in view_config:
                            for index_sql in view_config['indexes']:
                                try:
                                    cursor.execute(index_sql)
                                except Exception as idx_e:
                                    logger.warning(f"Index creation warning for {view_name}: {str(idx_e)}")
                        
                        results[view_name] = True
                        logger.info(f"✓ Created view: {view_name}")
                        
                    except Exception as e:
                        results[view_name] = False
                        logger.error(f"✗ Failed to create view {view_name}: {str(e)}")
        
        return results
    
    @classmethod
    def refresh_all_views(cls, category: Optional[str] = None, concurrent: bool = True) -> Dict[str, bool]:
        """Refresh all views or views for a specific category"""
        results = {}
        
        if category:
            if category not in cls.ALL_VIEWS:
                raise ValueError(f"Unknown category: {category}")
            view_names = list(cls.ALL_VIEWS[category].keys())
        else:
            view_names = cls.get_all_view_names()
        
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = '30min'")
            for view_name in view_names:
                try:
                    refresh_sql = f"REFRESH MATERIALIZED VIEW {'CONCURRENTLY' if concurrent else ''} {view_name}"
                    cursor.execute(refresh_sql)
                    results[view_name] = True
                    logger.info(f"✓ Refreshed view: {view_name}")
                except Exception as e:
                    results[view_name] = False
                    logger.error(f"✗ Failed to refresh view {view_name}: {str(e)}")
        
        return results
    
    @classmethod
    def drop_all_views(cls, category: Optional[str] = None) -> Dict[str, bool]:
        """Drop all views or views for a specific category"""
        results = {}
        
        if category:
            if category not in cls.ALL_VIEWS:
                raise ValueError(f"Unknown category: {category}")
            view_names = list(cls.ALL_VIEWS[category].keys())
        else:
            view_names = cls.get_all_view_names()
        
        with connection.cursor() as cursor:
            for view_name in view_names:
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    results[view_name] = True
                    logger.info(f"✓ Dropped view: {view_name}")
                except Exception as e:
                    results[view_name] = False
                    logger.error(f"✗ Failed to drop view {view_name}: {str(e)}")
        
        return results
    
    @classmethod
    def get_view_stats(cls, category: Optional[str] = None) -> Dict:
        """Get statistics for all views or views for a specific category"""
        if category:
            if category not in cls.ALL_VIEWS:
                raise ValueError(f"Unknown category: {category}")
            view_names = list(cls.ALL_VIEWS[category].keys())
        else:
            view_names = cls.get_all_view_names()
        
        stats = {}
        
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = '30min'")
            for view_name in view_names:
                try:
                    # Check if view exists
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_name = %s AND table_type = 'MATERIALIZED VIEW'
                    """, [view_name])
                    
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists:
                        # Get row count
                        cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
                        row_count = cursor.fetchone()[0]
                        
                        # Get size
                        cursor.execute("""
                            SELECT pg_size_pretty(pg_total_relation_size(%s))
                        """, [view_name])
                        size = cursor.fetchone()[0]
                        
                        stats[view_name] = {
                            'exists': True,
                            'row_count': row_count,
                            'size': size
                        }
                    else:
                        stats[view_name] = {
                            'exists': False,
                            'row_count': 0,
                            'size': '0 bytes'
                        }
                        
                except Exception as e:
                    stats[view_name] = {
                        'exists': False,
                        'error': str(e)
                    }
        
        return stats
    
    @classmethod
    def create_single_view(cls, view_name: str) -> bool:
        """Create a single view by name"""
        # Find the view in all categories
        view_config = None
        for category_views in cls.ALL_VIEWS.values():
            if view_name in category_views:
                view_config = category_views[view_name]
                break
        
        if not view_config:
            raise ValueError(f"View '{view_name}' not found in any category")
        
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = '30min'")
                # Drop existing view
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                
                # Create new view
                cursor.execute(view_config['sql'])
                
                # Create indexes
                if 'indexes' in view_config:
                    for index_sql in view_config['indexes']:
                        try:
                            cursor.execute(index_sql)
                        except Exception as idx_e:
                            logger.warning(f"Index creation warning for {view_name}: {str(idx_e)}")
                
                logger.info(f"✓ Created view: {view_name}")
                return True
                
        except Exception as e:
            logger.error(f"✗ Failed to create view {view_name}: {str(e)}")
            return False
    
    @classmethod
    def refresh_single_view(cls, view_name: str, concurrent: bool = True) -> bool:
        """Refresh a single view by name"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = '30min'")
                refresh_sql = f"REFRESH MATERIALIZED VIEW {'CONCURRENTLY' if concurrent else ''} {view_name}"
                cursor.execute(refresh_sql)
                logger.info(f"✓ Refreshed view: {view_name}")
                return True
        except Exception as e:
            logger.error(f"✗ Failed to refresh view {view_name}: {str(e)}")
            return False
    
    @classmethod
    def get_all_views(cls) -> Dict[str, str]:
        """Get all view SQL definitions for migration compatibility"""
        all_views = {}
        for category_views in cls.ALL_VIEWS.values():
            for view_name, view_config in category_views.items():
                all_views[view_name] = view_config['sql']
        return all_views
    
    @classmethod
    def get_all_indexes(cls) -> Dict[str, List[str]]:
        """Get all index definitions for migration compatibility"""
        all_indexes = {}
        for category_views in cls.ALL_VIEWS.values():
            for view_name, view_config in category_views.items():
                if 'indexes' in view_config and view_config['indexes']:
                    all_indexes[view_name] = view_config['indexes']
        return all_indexes
    
    @classmethod
    def get_refresh_functions(cls) -> str:
        """Get refresh function SQL for migration compatibility"""
        return """
        -- Function to refresh all dashboard views
        CREATE OR REPLACE FUNCTION refresh_dashboard_views(concurrent_refresh BOOLEAN DEFAULT true)
        RETURNS VOID AS $$
        DECLARE
            view_name TEXT;
            view_names TEXT[] := ARRAY[
                'mv_household_dashboard',
                'mv_individual_beneficiary_dashboard', 
                'mv_payment_dashboard',
                'mv_transfer_dashboard',
                'mv_grievance_dashboard',
                'mv_grievance_channel_dashboard',
                'mv_kpi_dashboard',
                'mv_monitoring_dashboard',
                'mv_field_aliases'
            ];
        BEGIN
            FOREACH view_name IN ARRAY view_names LOOP
                BEGIN
                    IF concurrent_refresh THEN
                        EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || view_name;
                        RAISE NOTICE 'Refreshed view %% concurrently', view_name;
                    ELSE
                        EXECUTE 'REFRESH MATERIALIZED VIEW ' || view_name;
                        RAISE NOTICE 'Refreshed view %%', view_name;
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'Failed to refresh view %%: %%', view_name, SQLERRM;
                END;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;

        -- Function to refresh a single dashboard view
        CREATE OR REPLACE FUNCTION refresh_dashboard_view(view_name TEXT, concurrent_refresh BOOLEAN DEFAULT true)
        RETURNS VOID AS $$
        BEGIN
            BEGIN
                IF concurrent_refresh THEN
                    EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY ' || view_name;
                    RAISE NOTICE 'Refreshed view %% concurrently', view_name;
                ELSE
                    EXECUTE 'REFRESH MATERIALIZED VIEW ' || view_name;
                    RAISE NOTICE 'Refreshed view %%', view_name;
                END IF;
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Failed to refresh view %%: %%', view_name, SQLERRM;
            END;
        END;
        $$ LANGUAGE plpgsql;

        -- Function to get view statistics
        CREATE OR REPLACE FUNCTION get_dashboard_view_stats()
        RETURNS TABLE(view_name TEXT, row_count BIGINT, size_pretty TEXT) AS $$
        DECLARE
            view_names TEXT[] := ARRAY[
                'mv_household_dashboard',
                'mv_individual_beneficiary_dashboard', 
                'mv_payment_dashboard',
                'mv_transfer_dashboard',
                'mv_grievance_dashboard',
                'mv_grievance_channel_dashboard',
                'mv_kpi_dashboard',
                'mv_monitoring_dashboard',
                'mv_field_aliases'
            ];
            v TEXT;
        BEGIN
            FOREACH v IN ARRAY view_names LOOP
                BEGIN
                    EXECUTE 'SELECT COUNT(*) FROM ' || v INTO row_count;
                    SELECT pg_size_pretty(pg_total_relation_size(v)) INTO size_pretty;
                    view_name := v;
                    RETURN NEXT;
                EXCEPTION WHEN OTHERS THEN
                    view_name := v;
                    row_count := -1;
                    size_pretty := 'ERROR';
                    RETURN NEXT;
                END;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
        """
