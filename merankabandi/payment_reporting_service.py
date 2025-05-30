"""
Payment Reporting Service
High-performance service for payment data analysis across MonetaryTransfer and BenefitConsumption
"""

from django.db import connection
from django.core.cache import cache
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal
import json


class PaymentReportingService:
    """
    Service for comprehensive payment reporting combining external and internal payments
    """
    
    CACHE_TTL = {
        'summary': 300,      # 5 minutes
        'details': 600,      # 10 minutes
        'trends': 1800,      # 30 minutes
    }
    
    @classmethod
    def get_payment_summary(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get comprehensive payment summary with all dimensions
        """
        cache_key = f"payment_summary_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Build WHERE clause from filters
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
            if filters.get('benefit_plan_id'):
                where_conditions.append("benefit_plan_id = %s")
                params.append(filters['benefit_plan_id'])
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('month'):
                where_conditions.append("month = %s")
                params.append(filters['month'])
            if filters.get('gender'):
                where_conditions.append("gender = %s")
                params.append(filters['gender'])
            if filters.get('is_twa') is not None:
                where_conditions.append("is_twa = %s")
                params.append(filters['is_twa'])
            if filters.get('community_type'):
                where_conditions.append("community_type = %s")
                params.append(filters['community_type'])
            if filters.get('start_date'):
                where_conditions.append("payment_date >= %s")
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append("payment_date <= %s")
                params.append(filters['end_date'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query unified payment summary
        query = f"""
        SELECT 
            -- Overall metrics
            SUM(total_payment_count) as total_payments,
            SUM(total_payment_amount) as total_amount,
            SUM(unique_beneficiaries) as total_beneficiaries,
            AVG(avg_payment_per_beneficiary) as avg_payment_amount,
            
            -- Payment source breakdown
            SUM(CASE WHEN payment_source = 'EXTERNAL' THEN total_payment_count ELSE 0 END) as external_payments,
            SUM(CASE WHEN payment_source = 'EXTERNAL' THEN total_payment_amount ELSE 0 END) as external_amount,
            SUM(CASE WHEN payment_source = 'INTERNAL' THEN total_payment_count ELSE 0 END) as internal_payments,
            SUM(CASE WHEN payment_source = 'INTERNAL' THEN total_payment_amount ELSE 0 END) as internal_amount,
            
            -- Demographics
            AVG(female_percentage) as avg_female_percentage,
            AVG(twa_percentage) as avg_twa_percentage,
            
            -- Coverage
            COUNT(DISTINCT province_id) as provinces_covered,
            COUNT(DISTINCT commune_id) as communes_covered,
            COUNT(DISTINCT colline_id) as collines_covered,
            COUNT(DISTINCT benefit_plan_id) as programs_active
            
        FROM payment_reporting_unified_summary
        {where_clause}
        """
        
        data = {
            'summary': {},
            'breakdown_by_source': [],
            'breakdown_by_gender': [],
            'breakdown_by_location': [],
            'breakdown_by_program': [],
            'breakdown_by_community': [],
            'last_updated': datetime.now().isoformat()
        }
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            
            if row:
                data['summary'] = {
                    'total_payments': int(row[0] or 0),
                    'total_amount': float(row[1] or 0),
                    'total_beneficiaries': int(row[2] or 0),
                    'avg_payment_amount': float(row[3] or 0),
                    'external_payments': int(row[4] or 0),
                    'external_amount': float(row[5] or 0),
                    'internal_payments': int(row[6] or 0),
                    'internal_amount': float(row[7] or 0),
                    'female_percentage': float(row[8] or 0),
                    'twa_percentage': float(row[9] or 0),
                    'provinces_covered': int(row[10] or 0),
                    'communes_covered': int(row[11] or 0),
                    'collines_covered': int(row[12] or 0),
                    'programs_active': int(row[13] or 0),
                }
            
            # Get breakdown by payment source
            breakdown_query = f"""
            SELECT 
                payment_source,
                SUM(total_payment_count) as payment_count,
                SUM(total_payment_amount) as payment_amount,
                SUM(unique_beneficiaries) as beneficiary_count,
                AVG(female_percentage) as female_percentage,
                AVG(twa_percentage) as twa_percentage
            FROM payment_reporting_unified_summary
            {where_clause}
            GROUP BY payment_source
            """
            
            cursor.execute(breakdown_query, params)
            for row in cursor.fetchall():
                data['breakdown_by_source'].append({
                    'source': row[0],
                    'payment_count': int(row[1] or 0),
                    'payment_amount': float(row[2] or 0),
                    'beneficiary_count': int(row[3] or 0),
                    'female_percentage': float(row[4] or 0),
                    'twa_percentage': float(row[5] or 0),
                })
            
            # Get gender breakdown
            gender_query = f"""
            SELECT 
                gender,
                SUM(total_payment_count) as payment_count,
                SUM(total_payment_amount) as payment_amount,
                SUM(unique_beneficiaries) as beneficiary_count
            FROM payment_reporting_unified_summary
            {where_clause}
            GROUP BY gender
            """
            
            cursor.execute(gender_query, params)
            for row in cursor.fetchall():
                if row[0]:  # Skip null genders
                    data['breakdown_by_gender'].append({
                        'gender': row[0],
                        'payment_count': int(row[1] or 0),
                        'payment_amount': float(row[2] or 0),
                        'beneficiary_count': int(row[3] or 0),
                    })
            
            # Get community type breakdown
            community_query = f"""
            SELECT 
                community_type,
                SUM(total_payment_count) as payment_count,
                SUM(total_payment_amount) as payment_amount,
                SUM(unique_beneficiaries) as beneficiary_count,
                AVG(female_percentage) as female_percentage,
                AVG(twa_percentage) as twa_percentage
            FROM payment_reporting_unified_summary
            {where_clause}
            GROUP BY community_type
            """
            
            cursor.execute(community_query, params)
            for row in cursor.fetchall():
                if row[0]:  # Skip null community types
                    data['breakdown_by_community'].append({
                        'community_type': row[0],
                        'payment_count': int(row[1] or 0),
                        'payment_amount': float(row[2] or 0),
                        'beneficiary_count': int(row[3] or 0),
                        'female_percentage': float(row[4] or 0),
                        'twa_percentage': float(row[5] or 0),
                    })
        
        cache.set(cache_key, data, cls.CACHE_TTL['summary'])
        return data
    
    @classmethod
    def get_payment_by_location(cls, filters: Dict[str, Any] = None, level: str = 'province') -> Dict[str, Any]:
        """
        Get payment data aggregated by location at specified level
        """
        cache_key = f"payment_by_location_{level}_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Validate level
        if level not in ['province', 'commune', 'colline']:
            level = 'province'
        
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('benefit_plan_id'):
                where_conditions.append("benefit_plan_id = %s")
                params.append(filters['benefit_plan_id'])
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('month'):
                where_conditions.append("month = %s")
                params.append(filters['month'])
            if filters.get('payment_source'):
                where_conditions.append("payment_source = %s")
                params.append(filters['payment_source'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Query by location level
        query = f"""
        SELECT 
            {level}_id,
            {level}_name,
            SUM(total_payment_count) as payment_count,
            SUM(total_payment_amount) as payment_amount,
            SUM(unique_beneficiaries) as beneficiary_count,
            AVG(avg_payment_per_beneficiary) as avg_payment,
            AVG(female_percentage) as female_percentage,
            AVG(twa_percentage) as twa_percentage
        FROM payment_reporting_by_location
        {where_clause}
        {"" if level == 'province' else f"AND {level}_id IS NOT NULL"}
        GROUP BY {level}_id, {level}_name
        ORDER BY payment_amount DESC
        """
        
        data = {
            'locations': [],
            'total': {
                'payment_count': 0,
                'payment_amount': 0,
                'beneficiary_count': 0,
            },
            'level': level,
            'last_updated': datetime.now().isoformat()
        }
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            
            for row in cursor.fetchall():
                if row[0]:  # Skip null locations
                    location_data = {
                        f'{level}_id': row[0],
                        f'{level}_name': row[1],
                        'payment_count': int(row[2] or 0),
                        'payment_amount': float(row[3] or 0),
                        'beneficiary_count': int(row[4] or 0),
                        'avg_payment': float(row[5] or 0),
                        'female_percentage': float(row[6] or 0),
                        'twa_percentage': float(row[7] or 0),
                    }
                    data['locations'].append(location_data)
                    
                    # Update totals
                    data['total']['payment_count'] += location_data['payment_count']
                    data['total']['payment_amount'] += location_data['payment_amount']
                    data['total']['beneficiary_count'] += location_data['beneficiary_count']
        
        cache.set(cache_key, data, cls.CACHE_TTL['details'])
        return data
    
    @classmethod
    def get_payment_by_program(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get payment data by benefit plan/program
        """
        cache_key = f"payment_by_program_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
        
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('month'):
                where_conditions.append("month = %s")
                params.append(filters['month'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
        SELECT 
            benefit_plan_id,
            benefit_plan_name,
            SUM(total_payment_count) as payment_count,
            SUM(total_payment_amount) as payment_amount,
            SUM(unique_beneficiaries) as beneficiary_count,
            AVG(avg_payment_per_beneficiary) as avg_payment,
            AVG(female_percentage) as female_percentage,
            AVG(twa_percentage) as twa_percentage,
            COUNT(DISTINCT province_id) as provinces_covered
        FROM payment_reporting_by_benefit_plan
        {where_clause}
        GROUP BY benefit_plan_id, benefit_plan_name
        ORDER BY payment_amount DESC
        """
        
        data = {
            'programs': [],
            'total': {
                'payment_count': 0,
                'payment_amount': 0,
                'beneficiary_count': 0,
            },
            'last_updated': datetime.now().isoformat()
        }
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            
            for row in cursor.fetchall():
                if row[0]:  # Skip null programs
                    program_data = {
                        'benefit_plan_id': str(row[0]),
                        'benefit_plan_name': row[1],
                        'payment_count': int(row[2] or 0),
                        'payment_amount': float(row[3] or 0),
                        'beneficiary_count': int(row[4] or 0),
                        'avg_payment': float(row[5] or 0),
                        'female_percentage': float(row[6] or 0),
                        'twa_percentage': float(row[7] or 0),
                        'provinces_covered': int(row[8] or 0),
                    }
                    data['programs'].append(program_data)
                    
                    # Update totals
                    data['total']['payment_count'] += program_data['payment_count']
                    data['total']['payment_amount'] += program_data['payment_amount']
                    data['total']['beneficiary_count'] += program_data['beneficiary_count']
        
        cache.set(cache_key, data, cls.CACHE_TTL['details'])
        return data
    
    @classmethod
    def get_payment_trends(cls, filters: Dict[str, Any] = None, granularity: str = 'month') -> Dict[str, Any]:
        """
        Get payment trends over time
        """
        cache_key = f"payment_trends_{granularity}_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
        
        # Validate granularity
        if granularity not in ['day', 'week', 'month', 'quarter', 'year']:
            granularity = 'month'
        
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('province_id'):
                where_conditions.append("province_id = %s")
                params.append(filters['province_id'])
            if filters.get('benefit_plan_id'):
                where_conditions.append("benefit_plan_id = %s")
                params.append(filters['benefit_plan_id'])
            if filters.get('start_date'):
                where_conditions.append("payment_date >= %s")
                params.append(filters['start_date'])
            if filters.get('end_date'):
                where_conditions.append("payment_date <= %s")
                params.append(filters['end_date'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Aggregate by time period
        group_by = {
            'day': 'payment_date',
            'week': 'week_start',
            'month': "date_trunc('month', payment_date)",
            'quarter': "date_trunc('quarter', payment_date)",
            'year': 'year'
        }
        
        date_field = group_by[granularity]
        
        query = f"""
        SELECT 
            {date_field} as period,
            SUM(total_payment_count) as payment_count,
            SUM(total_payment_amount) as payment_amount,
            SUM(unique_beneficiaries) as beneficiary_count,
            AVG(female_percentage) as female_percentage,
            AVG(twa_percentage) as twa_percentage,
            -- Running totals
            SUM(SUM(total_payment_amount)) OVER (ORDER BY {date_field}) as cumulative_amount,
            SUM(SUM(total_payment_count)) OVER (ORDER BY {date_field}) as cumulative_payments
        FROM payment_reporting_time_series
        {where_clause}
        GROUP BY {date_field}
        ORDER BY {date_field}
        """
        
        data = {
            'trends': [],
            'granularity': granularity,
            'last_updated': datetime.now().isoformat()
        }
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            
            for row in cursor.fetchall():
                period = row[0]
                if hasattr(period, 'isoformat'):
                    period_str = period.isoformat()
                else:
                    period_str = str(period)
                
                data['trends'].append({
                    'period': period_str,
                    'payment_count': int(row[1] or 0),
                    'payment_amount': float(row[2] or 0),
                    'beneficiary_count': int(row[3] or 0),
                    'female_percentage': float(row[4] or 0),
                    'twa_percentage': float(row[5] or 0),
                    'cumulative_amount': float(row[6] or 0),
                    'cumulative_payments': int(row[7] or 0),
                })
        
        cache.set(cache_key, data, cls.CACHE_TTL['trends'])
        return data
    
    @classmethod
    def get_payment_kpis(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Get key performance indicators for payment reporting
        """
        cache_key = f"payment_kpis_{hash(str(filters))}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
        
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('year'):
                where_conditions.append("year = %s")
                params.append(filters['year'])
            if filters.get('month'):
                where_conditions.append("month = %s")
                params.append(filters['month'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
        SELECT 
            SUM(total_payment_amount) as total_disbursed,
            SUM(unique_beneficiaries) as beneficiaries_reached,
            AVG(avg_payment_per_beneficiary) as avg_payment,
            AVG(female_inclusion_rate) as female_inclusion,
            AVG(twa_inclusion_rate) as twa_inclusion,
            COUNT(DISTINCT province_id) as geographic_coverage,
            COUNT(DISTINCT benefit_plan_id) as active_programs,
            SUM(external_amount) / NULLIF(SUM(total_payment_amount), 0) * 100 as external_percentage,
            SUM(internal_amount) / NULLIF(SUM(total_payment_amount), 0) * 100 as internal_percentage,
            AVG(payment_efficiency_score) as efficiency_score
        FROM payment_reporting_kpi_summary
        {where_clause}
        """
        
        data = {
            'kpis': {},
            'targets': {},
            'last_updated': datetime.now().isoformat()
        }
        
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
            
            if row:
                data['kpis'] = {
                    'total_disbursed': float(row[0] or 0),
                    'beneficiaries_reached': int(row[1] or 0),
                    'avg_payment': float(row[2] or 0),
                    'female_inclusion': float(row[3] or 0),
                    'twa_inclusion': float(row[4] or 0),
                    'geographic_coverage': int(row[5] or 0),
                    'active_programs': int(row[6] or 0),
                    'external_percentage': float(row[7] or 0),
                    'internal_percentage': float(row[8] or 0),
                    'efficiency_score': float(row[9] or 0),
                }
                
                # Define targets (these could come from a config table)
                data['targets'] = {
                    'female_inclusion': 50.0,  # 50% target
                    'twa_inclusion': 10.0,     # 10% target
                    'efficiency_score': 85.0,   # 85% efficiency target
                }
        
        cache.set(cache_key, data, cls.CACHE_TTL['summary'])
        return data
    
    @classmethod
    def clear_cache(cls, pattern: str = None):
        """Clear payment reporting cache"""
        if pattern:
            # In production, use cache.delete_pattern(pattern)
            cache.clear()
        else:
            cache.clear()