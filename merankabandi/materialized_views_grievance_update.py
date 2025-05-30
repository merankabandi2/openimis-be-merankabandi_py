"""
Additional Grievance Materialized Views for Enhanced Dashboard
"""

from django.db import connection
import logging

logger = logging.getLogger(__name__)


class GrievanceMaterializedViewsUpdate:
    """
    Additional materialized views for comprehensive grievance analytics
    """
    
    @staticmethod
    def create_channel_distribution_view():
        """Create channel distribution view"""
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_channel CASCADE;
        
        CREATE MATERIALIZED VIEW dashboard_grievance_channel AS
        SELECT 
            channel,
            COUNT(*) as count,
            COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM grievance_social_protection_ticket 
        WHERE "isDeleted" = false AND channel IS NOT NULL
        GROUP BY channel, report_date
        ORDER BY count DESC;
        
        CREATE INDEX idx_grievance_channel_channel ON dashboard_grievance_channel(channel);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        logger.info("Created dashboard_grievance_channel view")
    
    @staticmethod
    def create_priority_distribution_view():
        """Create priority distribution view"""
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_priority CASCADE;
        
        CREATE MATERIALIZED VIEW dashboard_grievance_priority AS
        SELECT 
            priority,
            COUNT(*) as count,
            COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM grievance_social_protection_ticket 
        WHERE "isDeleted" = false AND priority IS NOT NULL
        GROUP BY priority, report_date
        ORDER BY count DESC;
        
        CREATE INDEX idx_grievance_priority_priority ON dashboard_grievance_priority(priority);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        logger.info("Created dashboard_grievance_priority view")
    
    @staticmethod
    def create_gender_distribution_view():
        """Create gender distribution view"""
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_gender CASCADE;
        
        CREATE MATERIALIZED VIEW dashboard_grievance_gender AS
        WITH reporter_gender AS (
            SELECT 
                id,
                CASE 
                    WHEN reporter IS NOT NULL AND reporter::text != '' 
                    THEN json_extract_path_text(reporter::json, 'gender')
                    ELSE NULL
                END as gender
            FROM grievance_social_protection_ticket
            WHERE "isDeleted" = false
        )
        SELECT 
            COALESCE(gender, 'Unknown') as gender,
            COUNT(*) as count,
            COUNT(*)::numeric / (SELECT COUNT(*) FROM reporter_gender)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM reporter_gender
        GROUP BY gender, report_date
        ORDER BY count DESC;
        
        CREATE INDEX idx_grievance_gender_gender ON dashboard_grievance_gender(gender);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        logger.info("Created dashboard_grievance_gender view")
    
    @staticmethod
    def create_age_distribution_view():
        """Create age distribution view"""
        sql = """
        DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_age CASCADE;
        
        CREATE MATERIALIZED VIEW dashboard_grievance_age AS
        WITH reporter_age AS (
            SELECT 
                id,
                CASE 
                    WHEN reporter IS NOT NULL AND reporter::text != '' 
                    THEN json_extract_path_text(reporter::json, 'age')::int
                    ELSE NULL
                END as age
            FROM grievance_social_protection_ticket
            WHERE "isDeleted" = false
        )
        SELECT 
            CASE 
                WHEN age < 18 THEN '0-17'
                WHEN age BETWEEN 18 AND 25 THEN '18-25'
                WHEN age BETWEEN 26 AND 35 THEN '26-35'
                WHEN age BETWEEN 36 AND 50 THEN '36-50'
                WHEN age BETWEEN 51 AND 65 THEN '51-65'
                WHEN age > 65 THEN '65+'
                ELSE 'Unknown'
            END as age_group,
            COUNT(*) as count,
            COUNT(*)::numeric / (SELECT COUNT(*) FROM reporter_age)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM reporter_age
        GROUP BY age_group, report_date
        ORDER BY 
            CASE age_group
                WHEN '0-17' THEN 1
                WHEN '18-25' THEN 2
                WHEN '26-35' THEN 3
                WHEN '36-50' THEN 4
                WHEN '51-65' THEN 5
                WHEN '65+' THEN 6
                ELSE 7
            END;
        
        CREATE INDEX idx_grievance_age_group ON dashboard_grievance_age(age_group);
        """
        
        with connection.cursor() as cursor:
            cursor.execute(sql)
        logger.info("Created dashboard_grievance_age view")
    
    @staticmethod
    def create_category_views():
        """Create category views that handle JSON arrays"""
        from .grievance_category_views import GRIEVANCE_CATEGORY_VIEWS
        
        with connection.cursor() as cursor:
            for view_name, view_sql in GRIEVANCE_CATEGORY_VIEWS.items():
                try:
                    # Drop existing view if it exists
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name} CASCADE")
                    
                    # Create new view
                    cursor.execute(view_sql)
                    
                    # Create indexes for better performance
                    if view_name == 'dashboard_grievance_category_summary':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_group ON {view_name}(category_group)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_status ON {view_name}(status)")
                    elif view_name == 'dashboard_grievance_category_details':
                        cursor.execute(f"CREATE INDEX idx_{view_name}_category ON {view_name}(individual_category)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_group ON {view_name}(category_group)")
                        cursor.execute(f"CREATE INDEX idx_{view_name}_status ON {view_name}(status)")
                    
                    logger.info(f"Created {view_name}")
                    
                except Exception as e:
                    logger.error(f"Error creating {view_name}: {e}")
                    raise
    
    @staticmethod
    def create_all_views():
        """Create all additional grievance views"""
        GrievanceMaterializedViewsUpdate.create_channel_distribution_view()
        GrievanceMaterializedViewsUpdate.create_priority_distribution_view()
        GrievanceMaterializedViewsUpdate.create_gender_distribution_view()
        GrievanceMaterializedViewsUpdate.create_age_distribution_view()
        GrievanceMaterializedViewsUpdate.create_category_views()
        logger.info("All additional grievance views created successfully")
    
    @staticmethod
    def refresh_all_views(concurrent=False):
        """Refresh all additional grievance views"""
        views = [
            'dashboard_grievance_channel',
            'dashboard_grievance_priority',
            'dashboard_grievance_gender',
            'dashboard_grievance_age',
            'dashboard_grievance_category_summary',
            'dashboard_grievance_category_details',
        ]
        
        concurrently = "CONCURRENTLY" if concurrent else ""
        
        with connection.cursor() as cursor:
            for view in views:
                try:
                    sql = f"REFRESH MATERIALIZED VIEW {concurrently} {view}"
                    cursor.execute(sql)
                    logger.info(f"Refreshed {view}")
                except Exception as e:
                    logger.error(f"Error refreshing {view}: {e}")
    
    @staticmethod
    def drop_all_views():
        """Drop all additional grievance views"""
        views = [
            'dashboard_grievance_category_details',
            'dashboard_grievance_category_summary',
            'dashboard_grievance_age',
            'dashboard_grievance_gender',
            'dashboard_grievance_priority',
            'dashboard_grievance_channel',
        ]
        
        with connection.cursor() as cursor:
            for view in views:
                try:
                    cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE")
                    logger.info(f"Dropped {view}")
                except Exception as e:
                    logger.error(f"Error dropping {view}: {e}")


# Export views as dict for backward compatibility with management commands
GRIEVANCE_MATERIALIZED_VIEWS_UPDATE = {
    'dashboard_grievance_channel': """
        CREATE MATERIALIZED VIEW dashboard_grievance_channel AS
        SELECT 
            channel,
            COUNT(*) as count,
            COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM grievance_social_protection_ticket 
        WHERE "isDeleted" = false AND channel IS NOT NULL
        GROUP BY channel, report_date
        ORDER BY count DESC;
    """,
    'dashboard_grievance_priority': """
        CREATE MATERIALIZED VIEW dashboard_grievance_priority AS
        SELECT 
            priority,
            COUNT(*) as count,
            COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM grievance_social_protection_ticket 
        WHERE "isDeleted" = false AND priority IS NOT NULL
        GROUP BY priority, report_date
        ORDER BY count DESC;
    """,
    'dashboard_grievance_gender': """
        CREATE MATERIALIZED VIEW dashboard_grievance_gender AS
        SELECT 
            i.gender,
            COUNT(DISTINCT t.id) as count,
            COUNT(DISTINCT t.id)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM grievance_social_protection_ticket t
        LEFT JOIN individual_individual i ON t.complainant_individual_id = i.id
        WHERE t."isDeleted" = false AND i.gender IS NOT NULL
        GROUP BY i.gender, report_date
        ORDER BY count DESC;
    """,
    'dashboard_grievance_age': """
        CREATE MATERIALIZED VIEW dashboard_grievance_age AS
        SELECT 
            CASE 
                WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '18 years' THEN '0-17'
                WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '35 years' THEN '18-34'
                WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '50 years' THEN '35-49'
                WHEN AGE(CURRENT_DATE, i.dob) < INTERVAL '65 years' THEN '50-64'
                ELSE '65+'
            END as age_group,
            COUNT(DISTINCT t.id) as count,
            COUNT(DISTINCT t.id)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
            CURRENT_DATE as report_date
        FROM grievance_social_protection_ticket t
        LEFT JOIN individual_individual i ON t.complainant_individual_id = i.id
        WHERE t."isDeleted" = false AND i.dob IS NOT NULL
        GROUP BY age_group, report_date
        ORDER BY age_group;
    """
}