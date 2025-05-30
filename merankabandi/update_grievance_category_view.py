"""
Update Grievance Category Materialized View to Handle JSON Array Categories
"""

from django.db import connection
import logging

logger = logging.getLogger(__name__)


def update_grievance_category_view():
    """
    Update the grievance category view to properly handle JSON array categories
    Categories are stored as JSON arrays like: ["erreur_exclusion", "telephone"]
    """
    
    # Drop the existing view
    drop_sql = """
    DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_category CASCADE;
    """
    
    # Create the updated view that handles JSON arrays
    create_sql = """
    CREATE MATERIALIZED VIEW dashboard_grievance_category AS
    WITH category_expanded AS (
        -- Extract individual categories from JSON arrays
        SELECT 
            id,
            CASE 
                -- Handle JSON array format
                WHEN category LIKE '[%' THEN
                    TRIM(BOTH '"' FROM json_array_elements_text(category::json))
                -- Handle single string values
                ELSE category
            END as individual_category
        FROM grievance_social_protection_ticket
        WHERE "isDeleted" = false 
            AND category IS NOT NULL 
            AND category != ''
            AND category != '[]'
    ),
    category_mapping AS (
        -- Map individual categories to their parent groups
        SELECT 
            id,
            individual_category,
            CASE 
                -- Sensitive categories
                WHEN individual_category IN ('violence_vbg', 'corruption', 'accident_negligence', 'discrimination_ethnie_religion') 
                    THEN 'cas_sensibles'
                -- Special categories  
                WHEN individual_category IN ('erreur_exclusion', 'erreur_inclusion', 'maladie_mentale')
                    THEN 'cas_speciaux'
                -- Non-sensitive categories
                WHEN individual_category IN ('probleme_de_paiement', 'retard_de_paiement', 'perte_de_coupon', 
                                           'telephone', 'assistance_information', 'probleme_distribution',
                                           'autre_cas_non_sensible')
                    THEN 'cas_non_sensibles'
                -- Keep original if not mapped
                ELSE individual_category
            END as category_group
        FROM category_expanded
    ),
    category_counts AS (
        -- Count by both individual category and group
        SELECT 
            category_group,
            COUNT(DISTINCT id) as ticket_count
        FROM category_mapping
        GROUP BY category_group
        
        UNION ALL
        
        -- Also include counts for individual categories
        SELECT 
            individual_category as category_group,
            COUNT(DISTINCT id) as ticket_count
        FROM category_expanded
        GROUP BY individual_category
    )
    SELECT 
        category_group as category,
        SUM(ticket_count) as count,
        SUM(ticket_count)::numeric / (
            SELECT COUNT(DISTINCT id) 
            FROM grievance_social_protection_ticket 
            WHERE "isDeleted" = false
        )::numeric * 100 as percentage,
        CURRENT_DATE as report_date
    FROM category_counts
    GROUP BY category_group
    ORDER BY count DESC;
    
    -- Create indexes for performance
    CREATE INDEX idx_dashboard_grievance_category_cat ON dashboard_grievance_category(category);
    CREATE INDEX idx_dashboard_grievance_category_date ON dashboard_grievance_category(report_date);
    """
    
    # Also create a separate view for tracking all categories (not just groups)
    create_detailed_sql = """
    CREATE MATERIALIZED VIEW dashboard_grievance_category_detailed AS
    WITH category_expanded AS (
        SELECT 
            id,
            date_created,
            status,
            CASE 
                WHEN category LIKE '[%' THEN
                    TRIM(BOTH '"' FROM json_array_elements_text(category::json))
                ELSE category
            END as individual_category
        FROM grievance_social_protection_ticket
        WHERE "isDeleted" = false 
            AND category IS NOT NULL 
            AND category != ''
            AND category != '[]'
    )
    SELECT 
        individual_category as category,
        COUNT(DISTINCT id) as count,
        COUNT(DISTINCT CASE WHEN status = 'OPEN' THEN id END) as open_count,
        COUNT(DISTINCT CASE WHEN status = 'IN_PROGRESS' THEN id END) as in_progress_count,
        COUNT(DISTINCT CASE WHEN status = 'RESOLVED' THEN id END) as resolved_count,
        COUNT(DISTINCT CASE WHEN status = 'CLOSED' THEN id END) as closed_count,
        COUNT(DISTINCT id)::numeric / (
            SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false
        )::numeric * 100 as percentage,
        CURRENT_DATE as report_date
    FROM category_expanded
    GROUP BY individual_category
    ORDER BY count DESC;
    
    CREATE INDEX idx_dashboard_grievance_cat_detailed ON dashboard_grievance_category_detailed(category);
    """
    
    try:
        with connection.cursor() as cursor:
            # Drop existing view
            cursor.execute(drop_sql)
            logger.info("Dropped existing dashboard_grievance_category view")
            
            # Create updated view
            cursor.execute(create_sql)
            logger.info("Created updated dashboard_grievance_category view")
            
            # Create detailed view
            cursor.execute(create_detailed_sql)
            logger.info("Created dashboard_grievance_category_detailed view")
            
            # Refresh the views
            cursor.execute("REFRESH MATERIALIZED VIEW dashboard_grievance_category")
            cursor.execute("REFRESH MATERIALIZED VIEW dashboard_grievance_category_detailed")
            logger.info("Refreshed category views")
            
    except Exception as e:
        logger.error(f"Error updating grievance category view: {e}")
        raise


def update_grievance_status_view():
    """
    Update the grievance status view to include the proper summary
    """
    
    update_sql = """
    DROP MATERIALIZED VIEW IF EXISTS dashboard_grievance_status CASCADE;
    
    CREATE MATERIALIZED VIEW dashboard_grievance_status AS
    SELECT 
        status,
        COUNT(*) as count,
        COUNT(*)::numeric / (SELECT COUNT(*) FROM grievance_social_protection_ticket WHERE "isDeleted" = false)::numeric * 100 as percentage,
        CURRENT_DATE as report_date
    FROM grievance_social_protection_ticket 
    WHERE "isDeleted" = false AND status IS NOT NULL
    GROUP BY status, report_date
    ORDER BY 
        CASE status
            WHEN 'OPEN' THEN 1
            WHEN 'IN_PROGRESS' THEN 2
            WHEN 'RESOLVED' THEN 3
            WHEN 'CLOSED' THEN 4
            ELSE 5
        END;
    
    CREATE INDEX idx_dashboard_grievance_status ON dashboard_grievance_status(status);
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(update_sql)
            cursor.execute("REFRESH MATERIALIZED VIEW dashboard_grievance_status")
            logger.info("Updated and refreshed dashboard_grievance_status view")
    except Exception as e:
        logger.error(f"Error updating grievance status view: {e}")
        raise


def get_category_statistics():
    """
    Get statistics about categories to verify the update
    """
    stats_sql = """
    WITH raw_categories AS (
        SELECT 
            category,
            COUNT(*) as count
        FROM grievance_social_protection_ticket
        WHERE "isDeleted" = false
        GROUP BY category
        ORDER BY count DESC
        LIMIT 20
    )
    SELECT * FROM raw_categories;
    """
    
    with connection.cursor() as cursor:
        cursor.execute(stats_sql)
        results = cursor.fetchall()
        
        print("\nTop 20 Raw Category Values:")
        print("-" * 60)
        for category, count in results:
            print(f"{category[:50]:<50} | {count:>5}")
        
        # Check the materialized view
        cursor.execute("SELECT * FROM dashboard_grievance_category ORDER BY count DESC")
        mv_results = cursor.fetchall()
        
        print("\n\nMaterialized View Results:")
        print("-" * 60)
        print(f"{'Category':<30} | {'Count':>8} | {'Percentage':>10}")
        print("-" * 60)
        for row in mv_results:
            print(f"{row[0]:<30} | {row[1]:>8} | {row[2]:>10.2f}%")


if __name__ == "__main__":
    # This can be run as a script or imported
    update_grievance_category_view()
    update_grievance_status_view()
    get_category_statistics()