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
        # Check if we have any location or benefit plan filters
        has_filters = bool(filters and any(
            filters.get(key) for key in ['province_id', 'commune_id', 'colline_id', 'benefit_plan_id', 'year']
        ))
        
        where_conditions = []
        params = []
        
        if has_filters:
            # Use dashboard_individual_summary when filtered
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
            if filters.get('commune_id'):
                where_conditions.append("commune_id = %s")
                params.append(filters['commune_id'])
            if filters.get('colline_id'):
                where_conditions.append("colline_id = %s")
                params.append(filters['colline_id'])
            
            # When no benefit plan filter, use the 'ALL' row
            if not filters.get('benefit_plan_id'):
                where_conditions.append("benefit_plan_code = 'ALL'")
            else:
                # When filtered by benefit plan, only count those enrolled in that plan
                where_conditions.append("benefit_plan_id = %s")
                params.append(filters['benefit_plan_id'])
                
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Select appropriate view based on filters
        if has_filters:
            # Query from dashboard_individual_summary for filtered data
            query = f"""
            SELECT 
                -- Aggregate data from materialized view
                SUM(total_beneficiaries) as total_beneficiaries,
                SUM(total_beneficiaries) as active_beneficiaries,
                SUM(total_male) as male_beneficiaries,
                SUM(total_female) as female_beneficiaries,
                SUM(total_twa) as twa_beneficiaries,
                
                CASE WHEN SUM(total_individuals) > 0 
                    THEN (SUM(total_female)::numeric / SUM(total_individuals)::numeric * 100) 
                    ELSE 0 END as avg_female_percentage,
                CASE WHEN SUM(total_individuals) > 0 
                    THEN (SUM(total_twa)::numeric / SUM(total_individuals)::numeric * 100) 
                    ELSE 0 END as avg_twa_inclusion_rate,
                
                -- Payment data from view
                SUM(total_transfers) as total_transfers,
                SUM(total_amount_paid) as total_amount_paid,
                
                -- Grievance data from view
                SUM(total_grievances) as total_grievances,
                SUM(resolved_grievances) as resolved_grievances,
                
                -- Community breakdown
                community_type,
                COUNT(DISTINCT province_id) as provinces_covered,
                MAX(EXTRACT(quarter FROM month)) as latest_quarter,
                MAX(EXTRACT(year FROM month)) as latest_year
                
            FROM dashboard_individual_summary
            {where_clause}
            GROUP BY community_type
            """
        else:
            # Use dashboard_master_summary for unfiltered global view
            query = """
            SELECT 
                -- Get global aggregates
                total_beneficiaries,
                active_beneficiaries,
                total_male as male_beneficiaries,
                total_female as female_beneficiaries,
                total_twa as twa_beneficiaries,
                
                CASE WHEN total_individuals > 0 
                    THEN (total_female::numeric / total_individuals::numeric * 100) 
                    ELSE 0 END as avg_female_percentage,
                CASE WHEN total_individuals > 0 
                    THEN (total_twa::numeric / total_individuals::numeric * 100) 
                    ELSE 0 END as avg_twa_inclusion_rate,
                
                total_transfers,
                total_amount_paid,
                
                total_grievances,
                resolved_grievances,
                
                community_type,
                active_provinces as provinces_covered,
                EXTRACT(quarter FROM month) as latest_quarter,
                EXTRACT(year FROM month) as latest_year
                
            FROM dashboard_master_summary
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
            provinces_covered = 0
            
            for row in rows:
                row_dict = dict(zip(columns, row))
                data['community_breakdown'].append(row_dict)
                
                total_beneficiaries += row_dict.get('total_beneficiaries', 0) or 0
                total_transfers += row_dict.get('total_transfers', 0) or 0
                total_amount += row_dict.get('total_amount_paid', 0) or 0
                provinces_covered = max(provinces_covered, row_dict.get('provinces_covered', 0) or 0)
            
            # Calculate overall summary
            data['summary'] = {
                'total_beneficiaries': total_beneficiaries,
                'total_transfers': total_transfers,
                'total_amount_paid': float(total_amount) if total_amount else 0,
                'avg_amount_per_beneficiary': float(total_amount / total_beneficiaries) if total_beneficiaries > 0 else 0,
                'provinces_covered': provinces_covered
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
            if filters.get('commune_id'):
                where_conditions.append("commune_id = %s")
                params.append(filters['commune_id'])
            if filters.get('colline_id'):
                where_conditions.append("colline_id = %s")
                params.append(filters['colline_id'])
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            
            # When no benefit plan filter, use the 'ALL' row
            if not filters.get('benefit_plan_id'):
                where_conditions.append("benefit_plan_code = 'ALL'")
            else:
                # When filtered by benefit plan, only count those enrolled in that plan
                where_conditions.append("benefit_plan_id = %s")
                params.append(filters['benefit_plan_id'])
        
        # Check if we have any actual filters
        has_filters = bool(filters and any(
            filters.get(key) for key in ['province_id', 'commune_id', 'colline_id', 'benefit_plan_id', 'year']
        ))
        
        if not has_filters:
            # When no filters, use the 'ALL' row from dashboard_master_summary
            query = """
            SELECT 
                total_male,
                total_female,
                total_twa,
                total_individuals,
                CASE WHEN total_individuals > 0 
                    THEN (total_male::numeric / total_individuals::numeric * 100) 
                    ELSE 0 END as male_percentage,
                CASE WHEN total_individuals > 0 
                    THEN (total_female::numeric / total_individuals::numeric * 100) 
                    ELSE 0 END as female_percentage,
                CASE WHEN total_individuals > 0 
                    THEN (total_twa::numeric / total_individuals::numeric * 100) 
                    ELSE 0 END as twa_percentage,
                total_households,
                total_beneficiaries
            FROM dashboard_master_summary
            WHERE benefit_plan_code = 'ALL'
            """
            params = []
        else:
            # With filters, use dashboard_individual_summary and aggregate
            where_clause = "WHERE " + " AND ".join(where_conditions)
            query = f"""
            SELECT 
                -- Aggregate individual counts and percentages from materialized view
                SUM(total_male) as total_male,
                SUM(total_female) as total_female,
                SUM(total_twa) as total_twa,
                SUM(total_individuals) as total_individuals,
                CASE WHEN SUM(total_individuals) > 0 
                    THEN (SUM(total_male)::numeric / SUM(total_individuals)::numeric * 100) 
                    ELSE 0 END as male_percentage,
                CASE WHEN SUM(total_individuals) > 0 
                    THEN (SUM(total_female)::numeric / SUM(total_individuals)::numeric * 100) 
                    ELSE 0 END as female_percentage,
                CASE WHEN SUM(total_individuals) > 0 
                    THEN (SUM(total_twa)::numeric / SUM(total_individuals)::numeric * 100) 
                    ELSE 0 END as twa_percentage,
                
                -- Household and beneficiary counts from materialized view
                SUM(total_households) as total_households,
                SUM(total_beneficiaries) as total_beneficiaries
                
            FROM dashboard_individual_summary
            {where_clause}
            """
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
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
        quarterly_conditions = []
        params = []
        quarterly_params = []
        
        # Determine aggregation level (default to programme level)
        aggregation_level = filters.get('aggregation_level', 'programme') if filters else 'programme'
        
        if filters:
            if filters.get('start_date'):
                where_conditions.append("payment_date >= %s")
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append("payment_date <= %s")
                params.append(filters['end_date'])
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
                quarterly_conditions.append("province_id = %s")
                quarterly_params.append(filters['province_id'])
            if filters.get('commune_id'):
                where_conditions.append("commune_id = %s")
                params.append(filters['commune_id'])
                quarterly_conditions.append("commune_id = %s")
                quarterly_params.append(filters['commune_id'])
            if filters.get('colline_id'):
                where_conditions.append("colline_id = %s")
                params.append(filters['colline_id'])
                quarterly_conditions.append("colline_id = %s")
                quarterly_params.append(filters['colline_id'])
            if filters.get('benefit_plan_id'):
                where_conditions.append("programme_id = %s") 
                params.append(filters['benefit_plan_id'])
                quarterly_conditions.append("programme_id = %s")
                quarterly_params.append(filters['benefit_plan_id'])
            if filters.get('year'):
                quarterly_conditions.append("year = %s")
                quarterly_params.append(filters['year'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        quarterly_where = "WHERE " + " AND ".join(quarterly_conditions) if quarterly_conditions else ""
        
        # Use unified payment reporting summary view
        query = f"""
        SELECT 
            -- Aggregate metrics from unified payment view
            COUNT(DISTINCT payment_date) AS total_payment_cycles,
            SUM(total_beneficiaries) AS total_benefit_consumptions,
            SUM(total_amount_paid) AS total_amount_paid,
            AVG(total_amount_paid / NULLIF(total_beneficiaries, 0)) AS avg_amount_per_beneficiary,
            100.0 AS payment_completion_rate,  -- All records in unified view are completed payments
            MAX(payment_date) AS latest_date,
            MAX(quarter) AS quarter,
            MAX(year) AS year
        FROM payment_reporting_unified_summary
        {where_clause}
        """
        
        # Query for quarterly breakdown data - now using unified view
        # Choose the appropriate query based on aggregation level
        if aggregation_level == 'colline':
            # Use unified location view filtered to colline level
            quarterly_query = f"""
            SELECT 
                colline_name || ' (' || commune_name || ')' AS transfer_type,
                colline_id,
                commune_name,
                province_name,
                programme_name,
                programme_id,
                year,
                payment_source,
                payment_status,
                q1_amount, q2_amount, q3_amount, q4_amount,
                q1_beneficiaries, q2_beneficiaries, q3_beneficiaries, q4_beneficiaries,
                SUM(total_beneficiaries) AS total_beneficiaries,
                SUM(total_amount) AS total_amount,
                AVG(avg_female_percentage) AS avg_female_percentage,
                AVG(avg_twa_percentage) AS avg_twa_percentage
            FROM payment_reporting_unified_by_location
            WHERE colline_id IS NOT NULL
            {quarterly_where.replace('WHERE', 'AND') if quarterly_where else ''}
            GROUP BY 
                colline_id, colline_name, commune_id, commune_name,
                province_id, province_name, programme_name, programme_id,
                year, payment_source, payment_status,
                q1_amount, q2_amount, q3_amount, q4_amount,
                q1_beneficiaries, q2_beneficiaries, q3_beneficiaries, q4_beneficiaries
            ORDER BY province_name, commune_name, colline_name, year, payment_source
            """
        else:
            # Use unified quarterly view for programme level
            # Build a WHERE clause without location filters (quarterly view doesn't have them)
            programme_conditions = []
            programme_params = []
            
            if filters:
                if filters.get('benefit_plan_id'):
                    programme_conditions.append("programme_id = %s")
                    programme_params.append(filters['benefit_plan_id'])
                if filters.get('year'):
                    programme_conditions.append("year = %s")
                    programme_params.append(filters['year'])
            
            programme_where = "WHERE " + " AND ".join(programme_conditions) if programme_conditions else ""
            
            quarterly_query = f"""
            SELECT 
                transfer_type,
                programme_id,
                year,
                payment_source,
                payment_status,
                q1_amount, q2_amount, q3_amount, q4_amount,
                q1_beneficiaries, q2_beneficiaries, q3_beneficiaries, q4_beneficiaries,
                total_beneficiaries,
                total_amount,
                avg_female_percentage,
                avg_twa_percentage
            FROM payment_reporting_unified_quarterly
            {programme_where}
            ORDER BY transfer_type, year, payment_source
            """
            
            # Update params for this query
            quarterly_params = programme_params
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            
            data = {
                'overall_metrics': {},
                'by_transfer_type': [],
                'by_location': [],
                'by_community': [],
                'quarterly_data': [],
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
            
            # Get quarterly breakdown data
            cursor.execute(quarterly_query, quarterly_params)
            quarterly_columns = [col[0] for col in cursor.description]
            quarterly_rows = cursor.fetchall()
            
            for q_row in quarterly_rows:
                q_dict = dict(zip(quarterly_columns, q_row))
                
                # Create a combined transfer type that includes the source
                transfer_type_display = q_dict['transfer_type']
                if q_dict['payment_source'] == 'BENEFIT_CONSUMPTION':
                    transfer_type_display += ' (Système)'
                elif q_dict['payment_source'] == 'MONETARY_TRANSFER':
                    transfer_type_display += ' (Externe)'
                
                # Build the data entry
                entry = {
                    'transfer_type': transfer_type_display,
                    'beneficiaries': q_dict['total_beneficiaries'] if q_dict['total_beneficiaries'] else 0,
                    'amount': float(q_dict['total_amount']) if q_dict['total_amount'] else 0.0,
                    'completion_rate': 100.0,  # Unified view doesn't have completion rate, assume 100% for paid
                    'payment_source': q_dict['payment_source'],
                    # Include quarterly breakdown
                    'q1_amount': float(q_dict['q1_amount']) if q_dict['q1_amount'] else 0.0,
                    'q2_amount': float(q_dict['q2_amount']) if q_dict['q2_amount'] else 0.0,
                    'q3_amount': float(q_dict['q3_amount']) if q_dict['q3_amount'] else 0.0,
                    'q4_amount': float(q_dict['q4_amount']) if q_dict['q4_amount'] else 0.0,
                    'q1_beneficiaries': q_dict['q1_beneficiaries'] if q_dict['q1_beneficiaries'] else 0,
                    'q2_beneficiaries': q_dict['q2_beneficiaries'] if q_dict['q2_beneficiaries'] else 0,
                    'q3_beneficiaries': q_dict['q3_beneficiaries'] if q_dict['q3_beneficiaries'] else 0,
                    'q4_beneficiaries': q_dict['q4_beneficiaries'] if q_dict['q4_beneficiaries'] else 0,
                    'female_percentage': float(q_dict['avg_female_percentage']) if q_dict['avg_female_percentage'] else 0.0,
                    'twa_percentage': float(q_dict['avg_twa_percentage']) if q_dict['avg_twa_percentage'] else 0.0,
                }
                
                # Add location info if colline level
                if aggregation_level == 'colline':
                    entry['colline_id'] = q_dict.get('colline_id')
                    entry['commune_name'] = q_dict.get('commune_name')
                    entry['province_name'] = q_dict.get('province_name')
                
                # Add to by_transfer_type for compatibility
                data['by_transfer_type'].append(entry)
                
                # Also add to quarterly_data for explicit quarterly access
                data['quarterly_data'].append({
                    'transfer_type': transfer_type_display,
                    'year': q_dict['year'],
                    'payment_source': q_dict['payment_source'],
                    'q1_amount': float(q_dict['q1_amount']) if q_dict['q1_amount'] else 0.0,
                    'q2_amount': float(q_dict['q2_amount']) if q_dict['q2_amount'] else 0.0,
                    'q3_amount': float(q_dict['q3_amount']) if q_dict['q3_amount'] else 0.0,
                    'q4_amount': float(q_dict['q4_amount']) if q_dict['q4_amount'] else 0.0,
                    'q1_beneficiaries': q_dict['q1_beneficiaries'] if q_dict['q1_beneficiaries'] else 0,
                    'q2_beneficiaries': q_dict['q2_beneficiaries'] if q_dict['q2_beneficiaries'] else 0,
                    'q3_beneficiaries': q_dict['q3_beneficiaries'] if q_dict['q3_beneficiaries'] else 0,
                    'q4_beneficiaries': q_dict['q4_beneficiaries'] if q_dict['q4_beneficiaries'] else 0,
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
            
        # Build WHERE clause for filters
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
            if filters.get('commune_id'):
                where_conditions.append("commune_id = %s")
                params.append(filters['commune_id'])
            if filters.get('colline_id'):
                where_conditions.append("colline_id = %s")
                params.append(filters['colline_id'])
            if filters.get('benefit_plan_id'):
                where_conditions.append("benefit_plan_id = %s")
                params.append(filters['benefit_plan_id'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query multiple views for comprehensive trends
        queries = {
            'beneficiaries': f"""
                SELECT EXTRACT(quarter FROM quarter) as quarter, year, 
                       SUM(beneficiary_count) as value,
                       'Beneficiaries' as metric
                FROM dashboard_beneficiary_summary 
                {where_clause}
                GROUP BY EXTRACT(quarter FROM quarter), year
            """,
            'transfers': f"""
                SELECT quarter, year,
                       SUM(total_beneficiaries) as value,
                       'Transfers' as metric
                FROM payment_reporting_unified_summary
                {where_clause.replace('benefit_plan_id', 'programme_id')}
                GROUP BY quarter, year
            """,
            'activities': f"""
                SELECT EXTRACT(quarter FROM date_trunc('quarter', 
                       make_date(year::int, month::int, 1))) as quarter, 
                       year,
                       SUM(total_participants) as value,
                       'Activity Participants' as metric
                FROM dashboard_activities_summary
                {where_clause.replace('benefit_plan_id = %s', '1=1')}
                GROUP BY EXTRACT(quarter FROM date_trunc('quarter', 
                         make_date(year::int, month::int, 1))), year
            """,
            'grievances': """
                SELECT EXTRACT(quarter FROM quarter) as quarter, year,
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
                try:
                    # Determine parameter count needed for this query
                    query_params = []
                    if metric_name == 'beneficiaries' and where_clause:
                        query_params = params
                    elif metric_name == 'transfers' and where_clause:
                        # Adjust params for programme_id replacement
                        query_params = params
                    elif metric_name == 'activities' and where_clause:
                        # Remove benefit_plan_id param if present
                        query_params = [p for i, p in enumerate(params) if i < len(where_conditions) and 'benefit_plan_id' not in where_conditions[i]]
                    # grievances has no params
                    
                    cursor.execute(query, query_params)
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        
                        # Extract quarter and year properly from datetime objects
                        quarter_val = row_dict.get('quarter')
                        year_val = row_dict.get('year')
                        
                        # Handle datetime conversion
                        if quarter_val is None:
                            quarter_num = 1  # Default to Q1
                            quarter_str = 'Q1'
                        elif isinstance(quarter_val, (int, float)):
                            quarter_num = int(quarter_val)
                            quarter_str = f"Q{quarter_num}"
                        else:
                            quarter_num = 1
                            quarter_str = 'Q1'
                        
                        if year_val is None:
                            year_num = 2025  # Default year
                        elif isinstance(year_val, (int, float)):
                            year_num = int(year_val)
                        else:
                            year_num = 2025
                        
                        data['trends'].append({
                            'quarter': quarter_num,
                            'year': year_num,
                            'metric': row_dict['metric'],
                            'value': float(row_dict['value']) if row_dict['value'] else 0,
                            'period': f"{quarter_str} {year_num}"
                        })
                except Exception as e:
                    print(f"Error processing {metric_name} trends: {e}")
                    continue
        
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
    def get_activities_dashboard(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get activities-specific dashboard data using materialized views
        """
        cache_key = f"dashboard_activities_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
            if filters.get('commune_id'):
                where_conditions.append("commune_id = %s")
                params.append(filters['commune_id'])
            if filters.get('colline_id'):
                where_conditions.append("colline_id = %s")
                params.append(filters['colline_id'])
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('month'):
                where_conditions.append("month = %s")
                params.append(filters['month'])
            if filters.get('activity_type'):
                where_conditions.append("activity_type = %s")
                params.append(filters['activity_type'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query for summary statistics
        summary_query = f"""
        SELECT 
            -- Total activities and participants
            COUNT(DISTINCT activity_id) as total_activities,
            SUM(total_participants) as total_participants,
            SUM(male_participants) as total_male_participants,
            SUM(female_participants) as total_female_participants,
            SUM(twa_participants) as total_twa_participants,
            
            -- Calculate percentages
            CASE WHEN SUM(total_participants) > 0 
                THEN (SUM(female_participants)::numeric / SUM(total_participants)::numeric * 100) 
                ELSE 0 END as female_percentage,
            CASE WHEN SUM(total_participants) > 0 
                THEN (SUM(twa_participants)::numeric / SUM(total_participants)::numeric * 100) 
                ELSE 0 END as twa_percentage,
            
            -- Count unique activity types
            COUNT(DISTINCT activity_type) as activity_type_count,
            
            -- Average participants per activity
            CASE WHEN COUNT(DISTINCT activity_id) > 0
                THEN SUM(total_participants)::numeric / COUNT(DISTINCT activity_id)::numeric
                ELSE 0 END as avg_participants_per_activity
                
        FROM dashboard_activities_summary
        {where_clause}
        """
        
        # Query for activities by type breakdown
        by_type_query = f"""
        SELECT 
            activity_type,
            activity_count,
            total_participants,
            male_participants,
            female_participants,
            twa_participants,
            -- Pre-computed percentages from view
            CASE WHEN total_participants > 0 
                THEN (female_participants::numeric / total_participants::numeric * 100) 
                ELSE 0 END as female_percentage,
            CASE WHEN total_participants > 0 
                THEN (twa_participants::numeric / total_participants::numeric * 100) 
                ELSE 0 END as twa_percentage
        FROM dashboard_activities_by_type
        {where_clause}
        ORDER BY total_participants DESC
        """
        
        # Query for monthly trends
        monthly_trends_query = f"""
        SELECT 
            year,
            month,
            activity_type,
            SUM(total_participants) as participants,
            COUNT(DISTINCT activity_id) as activity_count,
            SUM(male_participants) as male_participants,
            SUM(female_participants) as female_participants,
            SUM(twa_participants) as twa_participants
        FROM dashboard_activities_summary
        {where_clause}
        GROUP BY year, month, activity_type
        ORDER BY year DESC, month DESC
        LIMIT 12
        """
        
        # Query for latest activities
        latest_activities_query = f"""
        SELECT 
            activity_id,
            activity_type,
            activity_date,
            province_name,
            commune_name,
            colline_name,
            total_participants,
            male_participants,
            female_participants,
            twa_participants,
            activity_details,
            created_date
        FROM dashboard_activities_summary
        {where_clause}
        ORDER BY activity_date DESC, created_date DESC
        LIMIT 20
        """
        
        with connection.cursor() as cursor:
            data = {
                'summary': {},
                'by_type': [],
                'monthly_trends': [],
                'latest_activities': [],
                'last_updated': datetime.now().isoformat()
            }
            
            # Get summary statistics
            cursor.execute(summary_query, params)
            summary_row = cursor.fetchone()
            
            if summary_row:
                columns = [col[0] for col in cursor.description]
                summary_dict = dict(zip(columns, summary_row))
                
                data['summary'] = {
                    'total_activities': summary_dict.get('total_activities', 0) or 0,
                    'total_participants': summary_dict.get('total_participants', 0) or 0,
                    'male_participants': summary_dict.get('total_male_participants', 0) or 0,
                    'female_participants': summary_dict.get('total_female_participants', 0) or 0,
                    'twa_participants': summary_dict.get('total_twa_participants', 0) or 0,
                    'female_percentage': float(summary_dict.get('female_percentage', 0) or 0),
                    'twa_percentage': float(summary_dict.get('twa_percentage', 0) or 0),
                    'activity_type_count': summary_dict.get('activity_type_count', 0) or 0,
                    'avg_participants_per_activity': float(summary_dict.get('avg_participants_per_activity', 0) or 0)
                }
            
            # Get activities by type breakdown
            cursor.execute(by_type_query, params)
            by_type_columns = [col[0] for col in cursor.description]
            
            for row in cursor.fetchall():
                row_dict = dict(zip(by_type_columns, row))
                data['by_type'].append({
                    'activity_type': row_dict.get('activity_type', 'Unknown'),
                    'activity_count': row_dict.get('activity_count', 0) or 0,
                    'total_participants': row_dict.get('total_participants', 0) or 0,
                    'male_participants': row_dict.get('male_participants', 0) or 0,
                    'female_participants': row_dict.get('female_participants', 0) or 0,
                    'twa_participants': row_dict.get('twa_participants', 0) or 0,
                    'female_percentage': float(row_dict.get('female_percentage', 0) or 0),
                    'twa_percentage': float(row_dict.get('twa_percentage', 0) or 0)
                })
            
            # Get monthly trends
            cursor.execute(monthly_trends_query, params)
            trends_columns = [col[0] for col in cursor.description]
            
            monthly_data = {}
            for row in cursor.fetchall():
                row_dict = dict(zip(trends_columns, row))
                
                # Create month key
                month_key = f"{row_dict['year']}-{str(row_dict['month']).zfill(2)}"
                
                if month_key not in monthly_data:
                    monthly_data[month_key] = {
                        'month': month_key,
                        'year': row_dict['year'],
                        'month_num': row_dict['month'],
                        'total_participants': 0,
                        'total_activities': 0,
                        'male_participants': 0,
                        'female_participants': 0,
                        'twa_participants': 0,
                        'by_type': {}
                    }
                
                # Aggregate totals
                monthly_data[month_key]['total_participants'] += row_dict.get('participants', 0) or 0
                monthly_data[month_key]['total_activities'] += row_dict.get('activity_count', 0) or 0
                monthly_data[month_key]['male_participants'] += row_dict.get('male_participants', 0) or 0
                monthly_data[month_key]['female_participants'] += row_dict.get('female_participants', 0) or 0
                monthly_data[month_key]['twa_participants'] += row_dict.get('twa_participants', 0) or 0
                
                # Store by type breakdown
                activity_type = row_dict.get('activity_type', 'Unknown')
                monthly_data[month_key]['by_type'][activity_type] = {
                    'participants': row_dict.get('participants', 0) or 0,
                    'activities': row_dict.get('activity_count', 0) or 0
                }
            
            # Convert to list and sort by date (most recent first)
            data['monthly_trends'] = sorted(
                monthly_data.values(), 
                key=lambda x: (x['year'], x['month_num']), 
                reverse=True
            )
            
            # Get latest activities
            cursor.execute(latest_activities_query, params)
            latest_columns = [col[0] for col in cursor.description]
            
            for row in cursor.fetchall():
                row_dict = dict(zip(latest_columns, row))
                
                activity = {
                    'activity_id': str(row_dict.get('activity_id')),
                    'activity_type': row_dict.get('activity_type', 'Unknown'),
                    'activity_date': row_dict.get('activity_date').isoformat() if row_dict.get('activity_date') else None,
                    'location': {
                        'province': row_dict.get('province_name'),
                        'commune': row_dict.get('commune_name'),
                        'colline': row_dict.get('colline_name')
                    },
                    'participants': {
                        'total': row_dict.get('total_participants', 0) or 0,
                        'male': row_dict.get('male_participants', 0) or 0,
                        'female': row_dict.get('female_participants', 0) or 0,
                        'twa': row_dict.get('twa_participants', 0) or 0
                    },
                    'details': row_dict.get('activity_details'),
                    'created_date': row_dict.get('created_date').isoformat() if row_dict.get('created_date') else None
                }
                
                # Calculate percentages
                if activity['participants']['total'] > 0:
                    activity['participants']['female_percentage'] = float(
                        activity['participants']['female'] / activity['participants']['total'] * 100
                    )
                    activity['participants']['twa_percentage'] = float(
                        activity['participants']['twa'] / activity['participants']['total'] * 100
                    )
                else:
                    activity['participants']['female_percentage'] = 0.0
                    activity['participants']['twa_percentage'] = 0.0
                
                data['latest_activities'].append(activity)
        
        cache.set(cache_key, data, cls.CACHE_TTL['summary'])
        return data
    
    @classmethod
    def get_activities_dashboard(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get activities dashboard data from materialized views
        """
        cache_key = f"dashboard_activities_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
            
        where_conditions = []
        params = []
        
        if filters:
            # Date range filters (convert to year/month logic since the view aggregates by year/month)
            if filters.get('start_date'):
                start_date = filters['start_date']
                if isinstance(start_date, str):
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    start_year = start_dt.year
                    start_month = start_dt.month
                    where_conditions.append("(year > %s OR (year = %s AND month >= %s))")
                    params.extend([start_year, start_year, start_month])
            if filters.get('end_date'):
                end_date = filters['end_date']
                if isinstance(end_date, str):
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    end_year = end_dt.year
                    end_month = end_dt.month
                    where_conditions.append("(year < %s OR (year = %s AND month <= %s))")
                    params.extend([end_year, end_year, end_month])
            # Location filters
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
            if filters.get('commune_id'):
                where_conditions.append("commune_id = %s")
                params.append(filters['commune_id'])
            if filters.get('colline_id'):
                where_conditions.append("location_id = %s")
                params.append(filters['colline_id'])
            # Time period filters
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('month'):
                where_conditions.append("month = %s")
                params.append(filters['month'])
            # Activity type filter
            if filters.get('activity_type'):
                where_conditions.append("activity_type = %s")
                params.append(filters['activity_type'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Get summary statistics by activity type and validation status
        summary_query = f"""
        SELECT 
            activity_type,
            validation_status,
            SUM(activity_count) as total_activities,
            SUM(total_participants) as total_participants,
            SUM(male_participants) as male_participants,
            SUM(female_participants) as female_participants,
            SUM(twa_participants) as twa_participants,
            SUM(agriculture_beneficiaries) as agriculture_beneficiaries,
            SUM(livestock_beneficiaries) as livestock_beneficiaries,
            SUM(commerce_services_beneficiaries) as commerce_services_beneficiaries
        FROM dashboard_activities_summary
        {where_clause}
        GROUP BY activity_type, validation_status
        """
        
        # Get monthly trends
        monthly_query = f"""
        SELECT 
            year,
            month,
            activity_type,
            SUM(activity_count) as activity_count,
            SUM(total_participants) as total_participants
        FROM dashboard_activities_summary
        {where_clause}
        GROUP BY year, month, activity_type
        ORDER BY year DESC, month DESC
        LIMIT 12
        """
        
        with connection.cursor() as cursor:
            # Get summary data
            cursor.execute(summary_query, params)
            columns = [col[0] for col in cursor.description]
            summary_rows = cursor.fetchall()
            
            # Process summary data by activity type
            summary_by_type = {
                'SensitizationTraining': {
                    'total': 0,
                    'participants': 0,
                    'male': 0,
                    'female': 0,
                    'twa': 0,
                    'validated': 0,
                    'pending': 0,
                    'rejected': 0
                },
                'BehaviorChangePromotion': {
                    'total': 0,
                    'participants': 0,
                    'male': 0,
                    'female': 0,
                    'twa': 0,
                    'validated': 0,
                    'pending': 0,
                    'rejected': 0
                },
                'MicroProject': {
                    'total': 0,
                    'participants': 0,
                    'male': 0,
                    'female': 0,
                    'twa': 0,
                    'validated': 0,
                    'pending': 0,
                    'rejected': 0,
                    'agriculture': 0,
                    'livestock': 0,
                    'commerce': 0
                }
            }
            
            for row in summary_rows:
                row_dict = dict(zip(columns, row))
                activity_type = row_dict['activity_type']
                validation_status = row_dict['validation_status']
                
                if activity_type in summary_by_type:
                    type_data = summary_by_type[activity_type]
                    type_data['total'] += row_dict['total_activities'] or 0
                    type_data['participants'] += row_dict['total_participants'] or 0
                    type_data['male'] += row_dict['male_participants'] or 0
                    type_data['female'] += row_dict['female_participants'] or 0
                    type_data['twa'] += row_dict['twa_participants'] or 0
                    
                    # Count by validation status
                    if validation_status == 'VALIDATED':
                        type_data['validated'] += row_dict['total_activities'] or 0
                    elif validation_status == 'PENDING':
                        type_data['pending'] += row_dict['total_activities'] or 0
                    elif validation_status == 'REJECTED':
                        type_data['rejected'] += row_dict['total_activities'] or 0
                    
                    # MicroProject specific fields
                    if activity_type == 'MicroProject':
                        type_data['agriculture'] += row_dict['agriculture_beneficiaries'] or 0
                        type_data['livestock'] += row_dict['livestock_beneficiaries'] or 0
                        type_data['commerce'] += row_dict['commerce_services_beneficiaries'] or 0
            
            # Get monthly trends
            cursor.execute(monthly_query, params)
            columns = [col[0] for col in cursor.description]
            monthly_rows = cursor.fetchall()
            
            monthly_trends = []
            for row in monthly_rows:
                row_dict = dict(zip(columns, row))
                monthly_trends.append({
                    'year': row_dict['year'],
                    'month': row_dict['month'],
                    'activity_type': row_dict['activity_type'],
                    'activity_count': row_dict['activity_count'] or 0,
                    'total_participants': row_dict['total_participants'] or 0
                })
            
            # Calculate overall statistics
            overall_stats = {
                'total_activities': sum(t['total'] for t in summary_by_type.values()),
                'total_participants': sum(t['participants'] for t in summary_by_type.values()),
                'total_male': sum(t['male'] for t in summary_by_type.values()),
                'total_female': sum(t['female'] for t in summary_by_type.values()),
                'total_twa': sum(t['twa'] for t in summary_by_type.values()),
                'total_validated': sum(t['validated'] for t in summary_by_type.values()),
                'total_pending': sum(t['pending'] for t in summary_by_type.values()),
                'total_rejected': sum(t['rejected'] for t in summary_by_type.values())
            }
            
            data = {
                'overall': overall_stats,
                'by_type': summary_by_type,
                'monthly_trends': monthly_trends,
                'last_updated': datetime.now().isoformat()
            }
        
        cache.set(cache_key, data, cls.CACHE_TTL['summary'])
        return data
    
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