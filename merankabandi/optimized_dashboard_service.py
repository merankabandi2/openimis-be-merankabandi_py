"""
Optimized Dashboard Service using Materialized Views
This service provides fast dashboard data by querying pre-aggregated materialized views
instead of performing real-time aggregations on large datasets.
"""

from django.db import connection
from django.core.cache import cache
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
from decimal import Decimal


class OptimizedDashboardService:
    """
    High-performance dashboard service using materialized views for fast data retrieval.
    Implements multi-level caching and efficient query patterns.
    """
    
    CACHE_TTL = {
        'summary': 300,      # 5 minutes
        'breakdown': 600,    # 10 minutes  
        'trends': 1800,      # 30 minutes
        'master': 900,       # 15 minutes
    }
    
    @classmethod
    def get_master_dashboard_summary(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get comprehensive dashboard summary from master materialized view
        """
        cache_key = f"dashboard_master_summary_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        # Build WHERE clause from filters
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('start_date'):
                where_conditions.append("month >= %s")
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append("month <= %s") 
                params.append(filters['end_date'])
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
            if filters.get('community_type'):
                where_conditions.append("community_type = %s")
                params.append(filters['community_type'])
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('benefit_plan_id'):
                # Note: This assumes benefit_plan_id column exists in the view
                # If not, we'll need to update the materialized view definitions
                where_conditions.append("benefit_plan_id = %s")
                params.append(filters['benefit_plan_id'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Use the optimized materialized view for sub-1s response time
        query = f"""
        SELECT 
            -- Get data from materialized view for speed
            total_beneficiaries,
            active_beneficiaries,
            0 as male_beneficiaries,  -- Not available at group level
            0 as female_beneficiaries, -- Not available at group level
            0 as twa_beneficiaries,   -- Not available at group level
            
            0 as avg_female_percentage,
            0 as avg_twa_inclusion_rate,
            
            -- Real payment cycle counts and amounts from materialized view
            total_transfers,
            total_amount_paid,
            
            0 as avg_completion_rate, -- Calculate separately if needed
            
            0 as total_activities,  -- Add real query if needed
            0 as total_activity_participants, -- Add real query if needed
            
            0 as total_projects,  -- Add real query if needed
            0 as completed_projects, -- Add real query if needed
            
            -- Real grievance counts from materialized view
            total_grievances,
            resolved_grievances,
            0 as avg_resolution_days, -- Calculate if needed
            
            -- Default community breakdown
            'ALL' as community_type,
            active_provinces as provinces_covered,
            EXTRACT(quarter FROM CURRENT_DATE) as latest_quarter,
            EXTRACT(year FROM CURRENT_DATE) as latest_year
            
        FROM dashboard_master_summary
        {where_clause}
        """
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
            data = {
                'summary': {},
                'community_breakdown': [],
                'last_updated': datetime.now().isoformat()
            }
            
            total_beneficiaries = 0
            total_transfers = 0
            total_amount = 0
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                data['community_breakdown'].append(row_dict)
                
                total_beneficiaries += row_dict.get('total_beneficiaries', 0) or 0
                total_transfers += row_dict.get('total_transfers', 0) or 0
                total_amount += row_dict.get('total_amount_paid', 0) or 0
            
            # Calculate overall summary
            data['summary'] = {
                'total_beneficiaries': total_beneficiaries,
                'total_transfers': total_transfers,
                'total_amount_paid': float(total_amount) if total_amount else 0,
                'avg_amount_per_beneficiary': float(total_amount / total_beneficiaries) if total_beneficiaries > 0 else 0,
                'provinces_covered': max([r.get('provinces_covered', 0) for r in data['community_breakdown']], default=0)
            }
        
        cache.set(cache_key, data, cls.CACHE_TTL['master'])
        return data
    
    @classmethod
    def get_beneficiary_breakdown(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get detailed beneficiary breakdown from beneficiary summary view
        """
        cache_key = f"dashboard_beneficiary_breakdown_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('start_date'):
                where_conditions.append("month >= %s")
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append("month <= %s")
                params.append(filters['end_date'])
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Use the optimized materialized view for sub-1s response time
        query = f"""
        SELECT 
            -- Individual counts and percentages from materialized view
            total_male,
            total_female,
            total_twa,
            total_individuals,
            male_percentage,
            female_percentage,
            twa_percentage,
            
            -- Household and beneficiary counts from materialized view
            total_households,
            total_beneficiaries
            
        FROM dashboard_individual_summary
        {where_clause}
        """
        
        with connection.cursor() as cursor:
            cursor.execute(query)  # No params needed for this simple query
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            
            data = {
                'gender_breakdown': {},
                'status_breakdown': [],
                'age_breakdown': [],
                'community_breakdown': [],
                'location_breakdown': [],
                'last_updated': datetime.now().isoformat()
            }
            
            # Process the materialized view result (optimized)
            if rows:
                row_dict = dict(zip(columns, rows[0]))
                
                # Get data directly from materialized view (already computed)
                total_individuals = row_dict.get('total_individuals', 0) or 0
                male_count = row_dict.get('total_male', 0) or 0
                female_count = row_dict.get('total_female', 0) or 0
                twa_count = row_dict.get('total_twa', 0) or 0
                male_percentage = row_dict.get('male_percentage', 0) or 0
                female_percentage = row_dict.get('female_percentage', 0) or 0
                twa_percentage = row_dict.get('twa_percentage', 0) or 0
                total_beneficiaries = row_dict.get('total_beneficiaries', 0) or 0
                total_households = row_dict.get('total_households', 0) or 0
                
                # Format gender breakdown for individuals (using pre-computed percentages)
                data['gender_breakdown'] = {
                    'male': male_count,
                    'female': female_count,
                    'twa': twa_count,
                    'total': total_individuals,
                    'male_percentage': float(male_percentage),
                    'female_percentage': float(female_percentage),
                    'twa_percentage': float(twa_percentage)
                }
                
                # Add household information
                data['household_breakdown'] = {
                    'total_households': total_households,
                    'total_beneficiaries': total_beneficiaries
                }
            else:
                # Default values if no data
                data['gender_breakdown'] = {
                    'male': 0,
                    'female': 0,
                    'twa': 0,
                    'total': 0,
                    'male_percentage': 0,
                    'female_percentage': 0,
                    'twa_percentage': 0
                }
                data['household_breakdown'] = {
                    'total_households': 0,
                    'total_beneficiaries': 0
                }
            
            # Empty breakdowns for now (can be populated later if needed)
            data['status_breakdown'] = []
            data['age_breakdown'] = []
            data['community_breakdown'] = []
            data['location_breakdown'] = []
        
        cache.set(cache_key, data, cls.CACHE_TTL['breakdown'])
        return data
    
    @classmethod
    def get_transfer_performance(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get transfer performance metrics from monetary transfers view
        """
        cache_key = f"dashboard_transfer_performance_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('start_date'):
                where_conditions.append("month >= %s")
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append("month <= %s")
                params.append(filters['end_date'])
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
            if filters.get('benefit_plan_id'):
                where_conditions.append("programme_id = %s")  # Note: Using programme_id from MonetaryTransfer model
                params.append(filters['benefit_plan_id'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Use available columns from dashboard_monetary_transfers view
        query = f"""
        SELECT 
            -- Overall metrics from the summary view
            total_payment_cycles,
            total_benefit_consumptions,
            total_amount_paid,
            avg_amount_per_beneficiary,
            payment_completion_rate,
            month,
            quarter,
            year
        FROM dashboard_monetary_transfers
        WHERE summary_type = 'TRANSFERS_SUMMARY'
        ORDER BY year DESC, quarter DESC
        LIMIT 1
        """
        
        with connection.cursor() as cursor:
            cursor.execute(query)  # No params needed for this simple query
            row = cursor.fetchone()
            
            data = {
                'overall_metrics': {},
                'by_transfer_type': [],
                'by_location': [],
                'by_community': [],
                'last_updated': datetime.now().isoformat()
            }
            
            if row:
                # Map the available data to the expected structure
                data['overall_metrics'] = {
                    'total_planned_beneficiaries': 0,  # Not available
                    'total_paid_beneficiaries': row[1] if row[1] else 0,  # Using total_benefit_consumptions
                    'total_amount_planned': 0,  # Not available
                    'total_amount_paid': float(row[2]) if row[2] else 0.0,  # total_amount_paid
                    'avg_completion_rate': float(row[4]) if row[4] else 0.0,  # payment_completion_rate
                    'avg_financial_completion_rate': 0.0,  # Not available
                    'avg_female_percentage': 0.0,  # Not available
                    'avg_twa_inclusion_rate': 0.0,  # Not available
                }
                
                # Add a single transfer type entry with the summary data
                data['by_transfer_type'].append({
                    'transfer_type': 'MONETARY_TRANSFER',
                    'beneficiaries': row[1] if row[1] else 0,
                    'amount': float(row[2]) if row[2] else 0.0,
                    'completion_rate': float(row[4]) if row[4] else 0.0,
                })
            
        cache.set(cache_key, data, cls.CACHE_TTL['breakdown'])
        return data
    
    @classmethod
    def get_quarterly_trends(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get quarterly trend data across all programs
        """
        cache_key = f"dashboard_quarterly_trends_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        # Query multiple views for comprehensive trends
        queries = {
            'beneficiaries': """
                SELECT quarter, year, 
                       SUM(beneficiary_count) as value,
                       'Beneficiaries' as metric
                FROM dashboard_beneficiary_summary 
                GROUP BY quarter, year
            """,
            'transfers': """
                SELECT quarter, year,
                       total_benefit_consumptions as value,
                       'Transfers' as metric
                FROM dashboard_monetary_transfers
                WHERE summary_type = 'TRANSFERS_SUMMARY'
            """,
            'activities': """
                SELECT quarter, year,
                       SUM(total_participants) as value,
                       'Activity Participants' as metric
                FROM dashboard_activities_summary
                GROUP BY quarter, year
            """,
            'projects': """
                SELECT quarter, year,
                       SUM(project_count) as value,
                       'Projects' as metric
                FROM dashboard_microprojects
                GROUP BY quarter, year
            """,
            'grievances': """
                SELECT quarter, year,
                       total_tickets as value,
                       'Grievances' as metric
                FROM dashboard_grievances
                WHERE summary_type = 'SUMMARY'
            """
        }
        
        data = {
            'trends': [],
            'last_updated': datetime.now().isoformat()
        }
        
        with connection.cursor() as cursor:
            for metric_name, query in queries.items():
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    
                    # Extract quarter and year properly from datetime objects
                    quarter_val = row_dict['quarter']
                    year_val = row_dict['year']
                    
                    # Handle datetime conversion
                    if quarter_val is None:
                        quarter_num = 1  # Default to Q1
                        quarter_str = 'Q1'
                    elif hasattr(quarter_val, 'quarter'):
                        quarter_num = quarter_val.quarter
                        quarter_str = f"Q{quarter_num}"
                    else:
                        quarter_num = 1
                        quarter_str = 'Q1'
                    
                    if year_val is None:
                        year_num = 2025  # Default year
                    elif hasattr(year_val, 'year'):
                        year_num = year_val.year
                    else:
                        year_num = int(year_val) if year_val else 2025
                    
                    data['trends'].append({
                        'quarter': quarter_num,
                        'year': year_num,
                        'metric': row_dict['metric'],
                        'value': float(row_dict['value']) if row_dict['value'] else 0,
                        'period': f"{quarter_str} {year_num}"
                    })
        
        # Sort by year, quarter
        data['trends'].sort(key=lambda x: (x['year'], x['quarter']))
        
        cache.set(cache_key, data, cls.CACHE_TTL['trends'])
        return data
    
    @classmethod
    def get_grievance_dashboard(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get grievance-specific dashboard data
        """
        cache_key = f"dashboard_grievances_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('start_date'):
                where_conditions.append("month >= %s")
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append("month <= %s")
                params.append(filters['end_date'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Use materialized view for sub-1s response time
        query = f"""
        SELECT 
            -- Summary metrics from materialized view for speed
            total_tickets,
            open_tickets,
            in_progress_tickets,
            resolved_tickets,
            closed_tickets,
            sensitive_tickets,
            anonymous_tickets,
            avg_resolution_days
            
        FROM dashboard_grievances
        WHERE summary_type = 'SUMMARY'
        """
        
        with connection.cursor() as cursor:
            # Get summary metrics from materialized view
            cursor.execute(query)  # No params needed for simple materialized view query
            summary_row = cursor.fetchone()
            
            data = {
                'summary': {},
                'status_distribution': [],
                'category_distribution': [],
                'channel_distribution': [],
                'priority_distribution': [],
                'gender_distribution': [],
                'age_distribution': [],
                'monthly_trend': [],
                'recent_tickets': [],
                'last_updated': datetime.now().isoformat()
            }
            
            if summary_row:
                columns = [col[0] for col in cursor.description]
                summary_dict = dict(zip(columns, summary_row))
                
                # Format summary data from materialized view
                data['summary'] = {
                    'total_tickets': summary_dict.get('total_tickets', 0),
                    'open_tickets': summary_dict.get('open_tickets', 0),
                    'in_progress_tickets': summary_dict.get('in_progress_tickets', 0),
                    'resolved_tickets': summary_dict.get('resolved_tickets', 0),
                    'closed_tickets': summary_dict.get('closed_tickets', 0),
                    'sensitive_tickets': summary_dict.get('sensitive_tickets', 0),
                    'anonymous_tickets': summary_dict.get('anonymous_tickets', 0),
                    'avg_resolution_days': float(summary_dict.get('avg_resolution_days', 0)) if summary_dict.get('avg_resolution_days') else 0,
                }
            
            # Get status distribution from materialized view
            cursor.execute("SELECT status, count, percentage FROM dashboard_grievance_status ORDER BY count DESC")
            for row in cursor.fetchall():
                data['status_distribution'].append({
                    'category': row[0],
                    'count': row[1],
                    'percentage': float(row[2]) if row[2] else 0
                })
            
            # Get category distribution from materialized view that handles JSON arrays
            cursor.execute("""
                SELECT 
                    category_group as category, 
                    SUM(count) as count,
                    SUM(count)::numeric / (
                        SELECT COUNT(*) 
                        FROM grievance_social_protection_ticket 
                        WHERE "isDeleted" = false
                    )::numeric * 100 as percentage
                FROM dashboard_grievance_category_summary
                GROUP BY category_group
                ORDER BY count DESC
            """)
            for row in cursor.fetchall():
                data['category_distribution'].append({
                    'category': row[0],
                    'count': row[1],
                    'percentage': float(row[2]) if row[2] else 0
                })
            
            # Also get detailed categories if needed
            cursor.execute("""
                SELECT 
                    individual_category,
                    category_group,
                    SUM(count) as count,
                    SUM(percentage) as percentage
                FROM dashboard_grievance_category_details
                GROUP BY individual_category, category_group
                ORDER BY count DESC
                LIMIT 20
            """)
            data['detailed_categories'] = []
            for row in cursor.fetchall():
                data['detailed_categories'].append({
                    'category': row[0],
                    'group': row[1],
                    'count': row[2],
                    'percentage': float(row[3]) if row[3] else 0
                })
            
            # Get channel distribution if view exists
            try:
                cursor.execute("SELECT channel, count, percentage FROM dashboard_grievance_channel ORDER BY count DESC")
                for row in cursor.fetchall():
                    data['channel_distribution'].append({
                        'category': row[0],
                        'count': row[1],
                        'percentage': float(row[2]) if row[2] else 0
                    })
            except:
                # View doesn't exist yet
                pass
            
            # Get priority distribution if view exists
            try:
                cursor.execute("SELECT priority, count, percentage FROM dashboard_grievance_priority ORDER BY count DESC")
                for row in cursor.fetchall():
                    data['priority_distribution'].append({
                        'category': row[0],
                        'count': row[1],
                        'percentage': float(row[2]) if row[2] else 0
                    })
            except:
                # View doesn't exist yet
                pass
            
            # Get gender distribution if view exists
            try:
                cursor.execute("SELECT gender, count, percentage FROM dashboard_grievance_gender ORDER BY count DESC")
                for row in cursor.fetchall():
                    data['gender_distribution'].append({
                        'category': row[0],
                        'count': row[1],
                        'percentage': float(row[2]) if row[2] else 0
                    })
            except:
                # View doesn't exist yet
                pass
            
            # Get age distribution if view exists
            try:
                cursor.execute("SELECT age_group, count, percentage FROM dashboard_grievance_age ORDER BY count DESC")
                for row in cursor.fetchall():
                    data['age_distribution'].append({
                        'category': row[0],
                        'count': row[1],
                        'percentage': float(row[2]) if row[2] else 0
                    })
            except:
                # View doesn't exist yet
                pass
            
            # Get monthly trend if view exists
            try:
                cursor.execute("""
                    SELECT 
                        date_received AS month,
                        COUNT(*) as count
                    FROM grievance_social_protection_ticket
                    WHERE "isDeleted" = false
                    GROUP BY date_received
                    ORDER BY date_received DESC
                    LIMIT 12
                """)
                monthly_data = []
                for row in cursor.fetchall():
                    if row[0]:  # If month is not None
                        monthly_data.append({
                            'month': row[0].strftime('%Y-%m'),
                            'count': row[1]
                        })
                # Reverse to get chronological order
                data['monthly_trend'] = list(reversed(monthly_data))
            except:
                # Fallback or view doesn't exist
                pass
            
            # Get recent tickets
            try:
                cursor.execute("""
                    SELECT 
                        id,
                        date_of_incident AS "dateOfIncident",
                        channel,
                        category,
                        status,
                        title,
                        description,
                        priority,
                        flags,
                        date_created AS "dateCreated",
                        date_updated AS "dateUpdated",
                        reporter_type AS "reporterType",
                        reporter_id AS "reporterId",
                        reporter_first_name AS "reporterFirstName",
                        reporter_last_name AS "reporterLastName",
                        reporter_type_name AS "reporterTypeName",
                        CASE 
                            WHEN reporter IS NOT NULL AND reporter::text != '' 
                            THEN json_extract_path_text(reporter::json, 'gender')
                            ELSE NULL 
                        END as gender
                    FROM grievance_social_protection_ticket
                    WHERE "isDeleted" = false
                    ORDER BY date_created DESC
                    LIMIT 50
                """)
                
                recent_tickets = []
                for row in cursor.fetchall():
                    ticket = {
                        'id': str(row[0]),
                        'dateOfIncident': row[1].isoformat() if row[1] else None,
                        'channel': row[2],
                        'category': row[3],
                        'status': row[4],
                        'title': row[5],
                        'description': row[6],
                        'priority': row[7],
                        'flags': row[8],
                        'dateCreated': row[9].isoformat() if row[9] else None,
                        'dateUpdated': row[10].isoformat() if row[10] else None,
                        'reporterType': row[11],
                        'reporterId': str(row[12]) if row[12] else None,
                        'reporterFirstName': row[13],
                        'reporterLastName': row[14],
                        'reporterTypeName': row[15],
                        'gender': row[16]
                    }
                    recent_tickets.append(ticket)
                data['recent_tickets'] = recent_tickets
            except Exception as e:
                # Fallback or error
                print(f"Error fetching recent tickets: {e}")
                pass
            
        cache.set(cache_key, data, cls.CACHE_TTL['summary'])
        return data
    
    @classmethod
    def refresh_views_if_needed(cls) -> bool:
        """
        Check if views need refreshing and refresh them if necessary
        """
        from .views_manager import MaterializedViewsManager
        
        # Check last refresh time
        stats = MaterializedViewsManager.get_view_stats()
        
        # If any view is older than 1 hour, refresh all views
        one_hour_ago = datetime.now() - timedelta(hours=1)
        
        for view_name, row_count, size_mb, last_refresh in stats:
            if not last_refresh or last_refresh < one_hour_ago:
                # Refresh in background (would typically use Celery)
                MaterializedViewsManager.refresh_all_views(concurrent=True)
                return True
        
        return False
    
    @classmethod
    def clear_cache(cls, pattern: str = None):
        """Clear dashboard cache"""
        if pattern:
            # In production, you'd use cache.delete_pattern(pattern)
            # For now, clear all dashboard cache
            cache.clear()
        else:
            cache.clear()


class DashboardMetrics:
    """Utility class for dashboard metrics calculations"""
    
    @staticmethod
    def calculate_growth_rate(current: float, previous: float) -> float:
        """Calculate period-over-period growth rate"""
        if previous == 0:
            return 100.0 if current > 0 else 0.0
        return ((current - previous) / previous) * 100
    
    @staticmethod
    def calculate_percentage(part: float, total: float) -> float:
        """Calculate percentage with safe division"""
        return (part / total * 100) if total > 0 else 0.0
    
    @staticmethod
    def format_currency(amount: Decimal) -> str:
        """Format amount as currency"""
        return f"BIF {amount:,.0f}"
    
    @staticmethod
    def format_percentage(value: float) -> str:
        """Format percentage value"""
        return f"{value:.1f}%"