"""
Consolidated Dashboard Service
Replaces: optimized_dashboard_service.py, payment_reporting_service.py, reporting_services.py

Single service layer for all dashboard queries against materialized views.
"""

from django.db import connection
from django.core.cache import cache
from datetime import datetime
from typing import Dict, Any, Optional
from decimal import Decimal


class DashboardService:
    """
    Unified dashboard service querying materialized views.
    """

    CACHE_TTL = {
        'summary': 300,       # 5 minutes
        'breakdown': 600,     # 10 minutes
        'trends': 1800,       # 30 minutes
        'grievance': 300,     # 5 minutes
        'payment': 600,       # 10 minutes
    }

    # ─── Helpers ───────────────────────────────────────────────────

    @staticmethod
    def _build_where(filters: Dict[str, Any], column_map: Dict[str, str]) -> tuple:
        """Build WHERE clause from filters using a column mapping."""
        conditions, params = [], []
        if not filters:
            return "", params
        for key, col in column_map.items():
            val = filters.get(key)
            if val is not None:
                conditions.append(f"{col} = %s")
                params.append(val)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        return where, params

    @staticmethod
    def _safe_float(val, default=0.0):
        if val is None:
            return default
        return float(val)

    @staticmethod
    def _safe_int(val, default=0):
        if val is None:
            return default
        return int(val)

    @staticmethod
    def _dictfetchall(cursor):
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def _dictfetchone(cursor):
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        return dict(zip(columns, row)) if row else {}

    # ─── Master Summary ───────────────────────────────────────────

    @classmethod
    def get_master_dashboard_summary(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Dashboard overview: beneficiary counts, transfers, grievances."""
        cache_key = f"dashboard_summary_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        has_location_filters = bool(filters and any(
            filters.get(k) for k in ['province_id', 'commune_id', 'colline_id', 'benefit_plan_id', 'year']
        ))

        if has_location_filters:
            data = cls._summary_from_individual_view(filters)
        else:
            data = cls._summary_from_master_view()

        cache.set(cache_key, data, cls.CACHE_TTL['summary'])
        return data

    @classmethod
    def _summary_from_master_view(cls) -> Dict[str, Any]:
        query = """
        SELECT total_beneficiaries, total_transfers, total_amount_paid,
               active_provinces, total_grievances, resolved_grievances
        FROM dashboard_master_summary
        LIMIT 1
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cls._dictfetchone(cursor)

        total_ben = cls._safe_int(row.get('total_beneficiaries'))
        total_amt = cls._safe_float(row.get('total_amount_paid'))
        return {
            'summary': {
                'total_beneficiaries': total_ben,
                'total_transfers': cls._safe_int(row.get('total_transfers')),
                'total_amount_paid': total_amt,
                'avg_amount_per_beneficiary': total_amt / total_ben if total_ben else 0,
                'provinces_covered': cls._safe_int(row.get('active_provinces')),
            },
            'community_breakdown': [],
            'last_updated': datetime.now().isoformat(),
        }

    @classmethod
    def _summary_from_individual_view(cls, filters: Dict[str, Any]) -> Dict[str, Any]:
        conditions, params = [], []

        if filters.get('province_id'):
            conditions.append("province_id = %s")
            params.append(filters['province_id'])
        if filters.get('commune_id'):
            conditions.append("commune_id = %s")
            params.append(filters['commune_id'])
        else:
            conditions.append("commune_id IS NULL")
            conditions.append("colline_id IS NULL")
        if filters.get('colline_id'):
            conditions.append("colline_id = %s")
            params.append(filters['colline_id'])
        else:
            conditions.append("colline_id IS NULL")
        if filters.get('year'):
            conditions.append("year = %s")
            params.append(filters['year'])
        if filters.get('benefit_plan_id'):
            conditions.append("benefit_plan_id = %s")
            params.append(filters['benefit_plan_id'])
        else:
            conditions.append("benefit_plan_code = 'ALL'")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
        SELECT
            SUM(total_beneficiaries) AS total_beneficiaries,
            SUM(total_transfers) AS total_transfers,
            SUM(total_amount_paid) AS total_amount_paid,
            COUNT(DISTINCT province_id) AS provinces_covered
        FROM dashboard_individual_summary
        {where}
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cls._dictfetchone(cursor)

        total_ben = cls._safe_int(row.get('total_beneficiaries'))
        total_amt = cls._safe_float(row.get('total_amount_paid'))
        return {
            'summary': {
                'total_beneficiaries': total_ben,
                'total_transfers': cls._safe_int(row.get('total_transfers')),
                'total_amount_paid': total_amt,
                'avg_amount_per_beneficiary': total_amt / total_ben if total_ben else 0,
                'provinces_covered': cls._safe_int(row.get('provinces_covered')),
            },
            'community_breakdown': [],
            'last_updated': datetime.now().isoformat(),
        }

    # ─── Beneficiary Breakdown ────────────────────────────────────

    @classmethod
    def get_beneficiary_breakdown(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Gender, TWA, household breakdown."""
        cache_key = f"dashboard_beneficiary_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        has_filters = bool(filters and any(
            filters.get(k) for k in ['province_id', 'commune_id', 'colline_id', 'benefit_plan_id', 'year']
        ))

        if has_filters:
            data = cls._breakdown_filtered(filters)
        else:
            data = cls._breakdown_global()

        cache.set(cache_key, data, cls.CACHE_TTL['breakdown'])
        return data

    @classmethod
    def _breakdown_global(cls) -> Dict[str, Any]:
        query = """
        SELECT total_male, total_female, total_twa, total_individuals,
               male_beneficiaries, female_beneficiaries, twa_beneficiaries,
               total_beneficiaries, total_households
        FROM dashboard_master_summary LIMIT 1
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
            r = cls._dictfetchone(cursor)

        total = cls._safe_int(r.get('total_individuals'))
        total_ben = cls._safe_int(r.get('total_beneficiaries'))
        return cls._format_breakdown(r, total, total_ben)

    @classmethod
    def _breakdown_filtered(cls, filters: Dict[str, Any]) -> Dict[str, Any]:
        conditions, params = [], []
        if filters.get('province_id'):
            conditions.append("province_id = %s")
            params.append(filters['province_id'])
        if filters.get('commune_id'):
            conditions.append("commune_id = %s")
            params.append(filters['commune_id'])
        else:
            conditions.append("commune_id IS NULL")
            conditions.append("colline_id IS NULL")
        if filters.get('colline_id'):
            conditions.append("colline_id = %s")
            params.append(filters['colline_id'])
        else:
            conditions.append("colline_id IS NULL")
        if filters.get('year'):
            conditions.append("year = %s")
            params.append(filters['year'])
        if filters.get('benefit_plan_id'):
            conditions.append("benefit_plan_id = %s")
            params.append(filters['benefit_plan_id'])
        else:
            conditions.append("benefit_plan_code = 'ALL'")

        where = "WHERE " + " AND ".join(conditions)
        query = f"""
        SELECT
            SUM(total_male) AS total_male,
            SUM(total_female) AS total_female,
            SUM(total_twa) AS total_twa,
            SUM(total_individuals) AS total_individuals,
            SUM(male_beneficiaries) AS male_beneficiaries,
            SUM(female_beneficiaries) AS female_beneficiaries,
            SUM(twa_beneficiaries) AS twa_beneficiaries,
            SUM(total_beneficiaries) AS total_beneficiaries,
            SUM(total_households) AS total_households
        FROM dashboard_individual_summary {where}
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            r = cls._dictfetchone(cursor)

        total = cls._safe_int(r.get('total_individuals'))
        total_ben = cls._safe_int(r.get('total_beneficiaries'))
        return cls._format_breakdown(r, total, total_ben)

    @classmethod
    def _format_breakdown(cls, r: dict, total: int, total_ben: int) -> Dict[str, Any]:
        def pct(num, denom):
            return round(num / denom * 100, 2) if denom else 0

        male = cls._safe_int(r.get('total_male'))
        female = cls._safe_int(r.get('total_female'))
        twa = cls._safe_int(r.get('total_twa'))
        m_ben = cls._safe_int(r.get('male_beneficiaries'))
        f_ben = cls._safe_int(r.get('female_beneficiaries'))
        t_ben = cls._safe_int(r.get('twa_beneficiaries'))
        households = cls._safe_int(r.get('total_households'))

        return {
            'gender_breakdown': {
                'male': male, 'female': female, 'twa': twa, 'total': total,
                'male_beneficiaries': m_ben, 'female_beneficiaries': f_ben, 'twa_beneficiaries': t_ben,
                'male_percentage': pct(male, total),
                'female_percentage': pct(female, total),
                'twa_percentage': pct(twa, total),
                'male_beneficiaries_percentage': pct(m_ben, total_ben),
                'female_beneficiaries_percentage': pct(f_ben, total_ben),
                'twa_beneficiaries_percentage': pct(t_ben, total_ben),
            },
            'household_breakdown': {
                'total_households': households,
                'total_beneficiaries': total_ben,
            },
            'status_breakdown': [],
            'age_breakdown': [],
            'community_breakdown': [],
            'location_breakdown': [],
            'last_updated': datetime.now().isoformat(),
        }

    # ─── Transfer Performance ─────────────────────────────────────

    @classmethod
    def get_transfer_performance(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Payment performance from unified payment views."""
        cache_key = f"dashboard_transfer_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {
            'province_id': 'province_id',
            'year': 'year',
            'benefit_plan_id': 'programme_id',
        }
        where, params = cls._build_where(filters, col_map)

        # Overall metrics
        query = f"""
        SELECT
            SUM(total_beneficiaries) AS total_paid,
            SUM(total_amount_paid) AS total_amount,
            AVG(female_percentage) AS avg_female_pct,
            AVG(twa_percentage) AS avg_twa_pct,
            COUNT(DISTINCT province_id) AS provinces
        FROM payment_reporting_unified_summary {where}
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            overall = cls._dictfetchone(cursor)

        # By transfer type (from quarterly view)
        q2 = f"""
        SELECT transfer_type, payment_source,
            total_beneficiaries, total_amount,
            q1_amount, q2_amount, q3_amount, q4_amount,
            q1_beneficiaries, q2_beneficiaries, q3_beneficiaries, q4_beneficiaries,
            avg_female_percentage, avg_twa_percentage
        FROM payment_reporting_unified_quarterly
        {"WHERE year = %s" if filters and filters.get('year') else ""}
        ORDER BY total_amount DESC
        """
        p2 = [filters['year']] if filters and filters.get('year') else []
        with connection.cursor() as cursor:
            cursor.execute(q2, p2)
            by_type = cls._dictfetchall(cursor)

        # By location (province level)
        q3 = f"""
        SELECT province_id, province_name,
            SUM(total_beneficiaries) AS beneficiaries,
            SUM(total_amount_paid) AS amount
        FROM payment_reporting_unified_summary {where}
        GROUP BY province_id, province_name
        ORDER BY amount DESC
        """
        with connection.cursor() as cursor:
            cursor.execute(q3, params)
            by_location = cls._dictfetchall(cursor)

        data = {
            'overall_metrics': {
                'total_paid_beneficiaries': cls._safe_int(overall.get('total_paid')),
                'total_amount_paid': cls._safe_float(overall.get('total_amount')),
                'avg_female_percentage': cls._safe_float(overall.get('avg_female_pct')),
                'avg_twa_inclusion_rate': cls._safe_float(overall.get('avg_twa_pct')),
            },
            'by_transfer_type': [
                {
                    'transfer_type': r.get('transfer_type', ''),
                    'payment_source': r.get('payment_source', ''),
                    'beneficiaries': cls._safe_int(r.get('total_beneficiaries')),
                    'amount': cls._safe_float(r.get('total_amount')),
                    'q1_amount': cls._safe_float(r.get('q1_amount')),
                    'q2_amount': cls._safe_float(r.get('q2_amount')),
                    'q3_amount': cls._safe_float(r.get('q3_amount')),
                    'q4_amount': cls._safe_float(r.get('q4_amount')),
                    'q1_beneficiaries': cls._safe_int(r.get('q1_beneficiaries')),
                    'q2_beneficiaries': cls._safe_int(r.get('q2_beneficiaries')),
                    'q3_beneficiaries': cls._safe_int(r.get('q3_beneficiaries')),
                    'q4_beneficiaries': cls._safe_int(r.get('q4_beneficiaries')),
                    'female_percentage': cls._safe_float(r.get('avg_female_percentage')),
                    'twa_percentage': cls._safe_float(r.get('avg_twa_percentage')),
                }
                for r in by_type
            ],
            'by_location': [
                {
                    'province': r.get('province_name', ''),
                    'province_id': cls._safe_int(r.get('province_id')),
                    'beneficiaries': cls._safe_int(r.get('beneficiaries')),
                    'amount': cls._safe_float(r.get('amount')),
                }
                for r in by_location
            ],
            'by_community': [],
            'last_updated': datetime.now().isoformat(),
        }

        cache.set(cache_key, data, cls.CACHE_TTL['payment'])
        return data

    # ─── Quarterly Trends ─────────────────────────────────────────

    @classmethod
    def get_quarterly_trends(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        cache_key = f"dashboard_trends_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {'province_id': 'province_id', 'year': 'year'}
        where, params = cls._build_where(filters, col_map)

        query = f"""
        SELECT year, quarter,
            SUM(total_amount_paid) AS amount,
            SUM(total_beneficiaries) AS beneficiaries
        FROM payment_reporting_unified_summary {where}
        GROUP BY year, quarter
        ORDER BY year, quarter
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cls._dictfetchall(cursor)

        trends = []
        for r in rows:
            y = cls._safe_int(r.get('year'))
            q = cls._safe_int(r.get('quarter'))
            trends.append({
                'quarter': q, 'year': y,
                'period': f"Q{q} {y}",
                'metric': 'amount', 'value': cls._safe_float(r.get('amount')),
            })
            trends.append({
                'quarter': q, 'year': y,
                'period': f"Q{q} {y}",
                'metric': 'beneficiaries', 'value': cls._safe_float(r.get('beneficiaries')),
            })

        data = {'trends': trends, 'last_updated': datetime.now().isoformat()}
        cache.set(cache_key, data, cls.CACHE_TTL['trends'])
        return data

    # ─── Activities Dashboard ─────────────────────────────────────

    @classmethod
    def get_activities_dashboard(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Activity data from dashboard_activities_summary (replaces dashboard_activities_by_type)."""
        cache_key = f"dashboard_activities_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {
            'province_id': 'province_id',
            'year': 'year',
            'month': 'month',
            'activity_type': 'activity_type',
        }
        where, params = cls._build_where(filters, col_map)

        with connection.cursor() as cursor:
            # Overall summary
            cursor.execute(f"""
                SELECT
                    SUM(activity_count) AS total_activities,
                    SUM(total_participants) AS total_participants,
                    SUM(male_participants) AS male_participants,
                    SUM(female_participants) AS female_participants,
                    SUM(twa_participants) AS twa_participants,
                    COUNT(DISTINCT province_id) AS provinces_with_activities
                FROM dashboard_activities_summary {where}
            """, params)
            overall = cls._dictfetchone(cursor)

            # By activity type (replaces dashboard_activities_by_type view)
            cursor.execute(f"""
                SELECT activity_type,
                    SUM(activity_count) AS count,
                    SUM(total_participants) AS participants,
                    SUM(male_participants) AS male_participants,
                    SUM(female_participants) AS female_participants,
                    SUM(twa_participants) AS twa_participants,
                    SUM(agriculture_beneficiaries) AS agriculture,
                    SUM(livestock_beneficiaries) AS livestock,
                    SUM(commerce_services_beneficiaries) AS commerce
                FROM dashboard_activities_summary {where}
                GROUP BY activity_type
            """, params)
            by_type_rows = cls._dictfetchall(cursor)

            # By province
            cursor.execute(f"""
                SELECT province_id, province_name,
                    SUM(activity_count) AS activity_count,
                    SUM(total_participants) AS total_participants
                FROM dashboard_activities_summary {where}
                GROUP BY province_id, province_name
                ORDER BY activity_count DESC
            """, params)
            by_province = cls._dictfetchall(cursor)

            # Monthly trends
            cursor.execute(f"""
                SELECT year, month,
                    SUM(activity_count) AS count,
                    SUM(total_participants) AS participants
                FROM dashboard_activities_summary {where}
                GROUP BY year, month
                ORDER BY year, month
            """, params)
            monthly_trends = cls._dictfetchall(cursor)

        # Build by_type dict keyed by activity_type
        by_type = {}
        for r in by_type_rows:
            by_type[r.get('activity_type', '')] = {
                'count': cls._safe_int(r.get('count')),
                'participants': cls._safe_int(r.get('participants')),
                'male_participants': cls._safe_int(r.get('male_participants')),
                'female_participants': cls._safe_int(r.get('female_participants')),
                'twa_participants': cls._safe_int(r.get('twa_participants')),
                'agriculture_beneficiaries': cls._safe_int(r.get('agriculture')),
                'livestock_beneficiaries': cls._safe_int(r.get('livestock')),
                'commerce_services_beneficiaries': cls._safe_int(r.get('commerce')),
            }

        data = {
            'overall': {
                'total_activities': cls._safe_int(overall.get('total_activities')),
                'total_participants': cls._safe_int(overall.get('total_participants')),
                'male_participants': cls._safe_int(overall.get('male_participants')),
                'female_participants': cls._safe_int(overall.get('female_participants')),
                'twa_participants': cls._safe_int(overall.get('twa_participants')),
                'provinces_with_activities': cls._safe_int(overall.get('provinces_with_activities')),
            },
            'by_type': by_type,
            'by_province': by_province,
            'monthly_trends': [
                {
                    'year': cls._safe_int(r.get('year')),
                    'month': cls._safe_int(r.get('month')),
                    'count': cls._safe_int(r.get('count')),
                    'participants': cls._safe_int(r.get('participants')),
                }
                for r in monthly_trends
            ],
            'last_updated': datetime.now().isoformat(),
        }

        cache.set(cache_key, data, cls.CACHE_TTL['breakdown'])
        return data

    # ─── Grievance Dashboard ──────────────────────────────────────

    @classmethod
    def get_grievance_dashboard(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Grievance summary + distributions from consolidated views."""
        cache_key = f"dashboard_grievance_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        with connection.cursor() as cursor:
            # Summary from dashboard_grievances
            cursor.execute("SELECT * FROM dashboard_grievances LIMIT 1")
            summary_row = cls._dictfetchone(cursor)

            # Status distribution from detail view
            cursor.execute("""
                SELECT status, COUNT(DISTINCT id) AS count
                FROM dashboard_grievance_details
                GROUP BY status ORDER BY count DESC
            """)
            status_dist = cls._dictfetchall(cursor)

            # Category distribution
            cursor.execute("""
                SELECT individual_category AS category, COUNT(DISTINCT id) AS count
                FROM dashboard_grievance_details
                GROUP BY individual_category ORDER BY count DESC
            """)
            category_dist = cls._dictfetchall(cursor)

            # Channel distribution
            cursor.execute("""
                SELECT channel, COUNT(DISTINCT id) AS count
                FROM dashboard_grievance_details
                WHERE channel IS NOT NULL
                GROUP BY channel ORDER BY count DESC
            """)
            channel_dist = cls._dictfetchall(cursor)

            # Priority distribution
            cursor.execute("""
                SELECT priority, COUNT(DISTINCT id) AS count
                FROM dashboard_grievance_details
                WHERE priority IS NOT NULL
                GROUP BY priority ORDER BY count DESC
            """)
            priority_dist = cls._dictfetchall(cursor)

        total = cls._safe_int(summary_row.get('total_tickets'))

        def with_pct(rows, key='count'):
            for r in rows:
                r['percentage'] = round(cls._safe_int(r.get(key)) / total * 100, 2) if total else 0
            return rows

        data = {
            'summary': {
                'total_tickets': total,
                'open_tickets': cls._safe_int(summary_row.get('open_tickets')),
                'in_progress_tickets': cls._safe_int(summary_row.get('in_progress_tickets')),
                'resolved_tickets': cls._safe_int(summary_row.get('resolved_tickets')),
                'closed_tickets': cls._safe_int(summary_row.get('closed_tickets')),
                'sensitive_tickets': cls._safe_int(summary_row.get('sensitive_tickets')),
                'anonymous_tickets': cls._safe_int(summary_row.get('anonymous_tickets')),
                'avg_resolution_days': cls._safe_float(summary_row.get('avg_resolution_days')),
            },
            'status_distribution': with_pct(status_dist),
            'category_distribution': with_pct(category_dist),
            'channel_distribution': with_pct(channel_dist),
            'priority_distribution': with_pct(priority_dist),
            'gender_distribution': [],
            'age_distribution': [],
            'monthly_trend': [],
            'recent_tickets': [],
            'last_updated': datetime.now().isoformat(),
        }

        cache.set(cache_key, data, cls.CACHE_TTL['grievance'])
        return data

    # ─── Payment Reporting (replaces PaymentReportingService) ─────

    @classmethod
    def get_payment_summary(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Comprehensive payment summary from unified view."""
        cache_key = f"payment_summary_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {
            'province_id': 'province_id', 'commune_id': 'commune_id',
            'colline_id': 'colline_id', 'benefit_plan_id': 'programme_id',
            'year': 'year', 'start_date': 'payment_date',
            'payment_source': 'payment_source',
        }
        where, params = cls._build_where(filters, col_map)

        query = f"""
        SELECT
            SUM(payment_count) AS total_payments,
            SUM(total_amount_paid) AS total_amount,
            SUM(total_beneficiaries) AS total_beneficiaries,
            AVG(avg_amount_per_beneficiary) AS avg_payment_amount,
            SUM(CASE WHEN payment_source = 'MONETARY_TRANSFER' THEN payment_count ELSE 0 END) AS external_payments,
            SUM(CASE WHEN payment_source = 'MONETARY_TRANSFER' THEN total_amount_paid ELSE 0 END) AS external_amount,
            SUM(CASE WHEN payment_source = 'BENEFIT_CONSUMPTION' THEN payment_count ELSE 0 END) AS internal_payments,
            SUM(CASE WHEN payment_source = 'BENEFIT_CONSUMPTION' THEN total_amount_paid ELSE 0 END) AS internal_amount,
            AVG(female_percentage) AS female_percentage,
            AVG(twa_percentage) AS twa_percentage,
            COUNT(DISTINCT province_id) AS provinces_covered,
            COUNT(DISTINCT commune_id) AS communes_covered,
            COUNT(DISTINCT colline_id) AS collines_covered,
            COUNT(DISTINCT programme_id) AS programs_active
        FROM payment_reporting_unified_summary {where}
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            s = cls._dictfetchone(cursor)

            # Source breakdown
            cursor.execute(f"""
                SELECT payment_source AS source,
                    SUM(payment_count) AS payment_count,
                    SUM(total_amount_paid) AS payment_amount,
                    SUM(total_beneficiaries) AS beneficiary_count,
                    AVG(female_percentage) AS female_percentage,
                    AVG(twa_percentage) AS twa_percentage
                FROM payment_reporting_unified_summary {where}
                GROUP BY payment_source
            """, params)
            source_rows = cls._dictfetchall(cursor)

        data = {
            'summary': {
                'total_payments': cls._safe_int(s.get('total_payments')),
                'total_amount': cls._safe_float(s.get('total_amount')),
                'total_beneficiaries': cls._safe_int(s.get('total_beneficiaries')),
                'avg_payment_amount': cls._safe_float(s.get('avg_payment_amount')),
                'external_payments': cls._safe_int(s.get('external_payments')),
                'external_amount': cls._safe_float(s.get('external_amount')),
                'internal_payments': cls._safe_int(s.get('internal_payments')),
                'internal_amount': cls._safe_float(s.get('internal_amount')),
                'female_percentage': cls._safe_float(s.get('female_percentage')),
                'twa_percentage': cls._safe_float(s.get('twa_percentage')),
                'provinces_covered': cls._safe_int(s.get('provinces_covered')),
                'communes_covered': cls._safe_int(s.get('communes_covered')),
                'collines_covered': cls._safe_int(s.get('collines_covered')),
                'programs_active': cls._safe_int(s.get('programs_active')),
            },
            'breakdown_by_source': [
                {k: (cls._safe_float(v) if isinstance(v, Decimal) else v)
                 for k, v in r.items()}
                for r in source_rows
            ],
            'breakdown_by_gender': [],
            'breakdown_by_community': [],
            'last_updated': datetime.now().isoformat(),
        }

        cache.set(cache_key, data, cls.CACHE_TTL['payment'])
        return data

    @classmethod
    def get_payment_by_location(cls, filters: Dict[str, Any] = None, level: str = 'province') -> Dict[str, Any]:
        """Payment data by location level."""
        if level not in ('province', 'commune', 'colline'):
            level = 'province'

        cache_key = f"payment_location_{level}_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {
            'benefit_plan_id': 'programme_id',
            'year': 'year',
            'payment_source': 'payment_source',
        }
        where, params = cls._build_where(filters, col_map)
        not_null = f"AND {level}_id IS NOT NULL" if level != 'province' else ""

        query = f"""
        SELECT {level}_id, {level}_name,
            SUM(payment_count) AS payment_count,
            SUM(total_amount_paid) AS payment_amount,
            SUM(total_beneficiaries) AS beneficiary_count,
            AVG(avg_amount_per_beneficiary) AS avg_payment,
            AVG(female_percentage) AS female_percentage,
            AVG(twa_percentage) AS twa_percentage
        FROM payment_reporting_unified_summary
        {where} {not_null}
        GROUP BY {level}_id, {level}_name
        ORDER BY payment_amount DESC
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cls._dictfetchall(cursor)

        locations = []
        total_count, total_amount, total_ben = 0, 0.0, 0
        for r in rows:
            pc = cls._safe_int(r.get('payment_count'))
            pa = cls._safe_float(r.get('payment_amount'))
            bc = cls._safe_int(r.get('beneficiary_count'))
            total_count += pc
            total_amount += pa
            total_ben += bc
            loc = {
                f'{level}_id': r.get(f'{level}_id'),
                f'{level}_name': r.get(f'{level}_name'),
                'payment_count': pc,
                'payment_amount': pa,
                'beneficiary_count': bc,
                'avg_payment': cls._safe_float(r.get('avg_payment')),
                'female_percentage': cls._safe_float(r.get('female_percentage')),
                'twa_percentage': cls._safe_float(r.get('twa_percentage')),
            }
            locations.append(loc)

        data = {
            'locations': locations,
            'total': {'payment_count': total_count, 'payment_amount': total_amount, 'beneficiary_count': total_ben},
            'level': level,
            'last_updated': datetime.now().isoformat(),
        }

        cache.set(cache_key, data, cls.CACHE_TTL['payment'])
        return data

    @classmethod
    def get_payment_by_program(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Payment data by benefit plan."""
        cache_key = f"payment_program_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {'province_id': 'province_id', 'year': 'year'}
        where, params = cls._build_where(filters, col_map)

        query = f"""
        SELECT programme_id AS benefit_plan_id, programme_name AS benefit_plan_name,
            SUM(payment_count) AS payment_count,
            SUM(total_amount_paid) AS payment_amount,
            SUM(total_beneficiaries) AS beneficiary_count,
            AVG(avg_amount_per_beneficiary) AS avg_payment,
            AVG(female_percentage) AS female_percentage,
            AVG(twa_percentage) AS twa_percentage,
            COUNT(DISTINCT province_id) AS provinces_covered
        FROM payment_reporting_unified_summary {where}
        GROUP BY programme_id, programme_name
        ORDER BY payment_amount DESC
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cls._dictfetchall(cursor)

        total_count, total_amount, total_ben = 0, 0.0, 0
        programs = []
        for r in rows:
            pc = cls._safe_int(r.get('payment_count'))
            pa = cls._safe_float(r.get('payment_amount'))
            bc = cls._safe_int(r.get('beneficiary_count'))
            total_count += pc
            total_amount += pa
            total_ben += bc
            programs.append({
                'benefit_plan_id': str(r.get('benefit_plan_id', '')),
                'benefit_plan_name': r.get('benefit_plan_name', ''),
                'payment_count': pc, 'payment_amount': pa, 'beneficiary_count': bc,
                'avg_payment': cls._safe_float(r.get('avg_payment')),
                'female_percentage': cls._safe_float(r.get('female_percentage')),
                'twa_percentage': cls._safe_float(r.get('twa_percentage')),
                'provinces_covered': cls._safe_int(r.get('provinces_covered')),
            })

        data = {
            'programs': programs,
            'total': {'payment_count': total_count, 'payment_amount': total_amount, 'beneficiary_count': total_ben},
            'last_updated': datetime.now().isoformat(),
        }
        cache.set(cache_key, data, cls.CACHE_TTL['payment'])
        return data

    @classmethod
    def get_payment_trends(cls, filters: Dict[str, Any] = None, granularity: str = 'month') -> Dict[str, Any]:
        """Payment trends over time."""
        cache_key = f"payment_trends_{granularity}_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {
            'province_id': 'province_id',
            'benefit_plan_id': 'programme_id',
        }
        where, params = cls._build_where(filters, col_map)

        if granularity == 'quarter':
            group_expr = "year, quarter"
            period_expr = "'Q' || quarter::text || ' ' || year::text"
        elif granularity == 'year':
            group_expr = "year"
            period_expr = "year::text"
        else:
            group_expr = "year, month"
            period_expr = "year::text || '-' || LPAD(month::text, 2, '0')"

        query = f"""
        SELECT {period_expr} AS period,
            SUM(payment_count) AS payment_count,
            SUM(total_amount_paid) AS payment_amount,
            SUM(total_beneficiaries) AS beneficiary_count,
            AVG(female_percentage) AS female_percentage,
            AVG(twa_percentage) AS twa_percentage
        FROM payment_reporting_unified_summary {where}
        GROUP BY {group_expr}
        ORDER BY {group_expr}
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cls._dictfetchall(cursor)

        cumulative_amount, cumulative_payments = 0.0, 0
        trends = []
        for r in rows:
            pa = cls._safe_float(r.get('payment_amount'))
            pc = cls._safe_int(r.get('payment_count'))
            cumulative_amount += pa
            cumulative_payments += pc
            trends.append({
                'period': r.get('period', ''),
                'payment_count': pc, 'payment_amount': pa,
                'beneficiary_count': cls._safe_int(r.get('beneficiary_count')),
                'female_percentage': cls._safe_float(r.get('female_percentage')),
                'twa_percentage': cls._safe_float(r.get('twa_percentage')),
                'cumulative_amount': cumulative_amount,
                'cumulative_payments': cumulative_payments,
            })

        data = {'trends': trends, 'granularity': granularity, 'last_updated': datetime.now().isoformat()}
        cache.set(cache_key, data, cls.CACHE_TTL['trends'])
        return data

    @classmethod
    def get_payment_kpis(cls, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Key performance indicators for payments."""
        cache_key = f"payment_kpis_{hash(str(filters))}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        col_map = {'year': 'year'}
        where, params = cls._build_where(filters, col_map)

        query = f"""
        SELECT
            SUM(total_amount_paid) AS total_disbursed,
            SUM(total_beneficiaries) AS beneficiaries_reached,
            AVG(avg_amount_per_beneficiary) AS avg_payment,
            AVG(female_percentage) AS female_inclusion,
            AVG(twa_percentage) AS twa_inclusion,
            COUNT(DISTINCT province_id) AS geographic_coverage,
            COUNT(DISTINCT programme_id) AS active_programs,
            SUM(CASE WHEN payment_source = 'MONETARY_TRANSFER' THEN total_amount_paid ELSE 0 END)
                / NULLIF(SUM(total_amount_paid), 0) * 100 AS external_pct,
            SUM(CASE WHEN payment_source = 'BENEFIT_CONSUMPTION' THEN total_amount_paid ELSE 0 END)
                / NULLIF(SUM(total_amount_paid), 0) * 100 AS internal_pct
        FROM payment_reporting_unified_summary {where}
        """
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            r = cls._dictfetchone(cursor)

        data = {
            'kpis': {
                'total_disbursed': cls._safe_float(r.get('total_disbursed')),
                'beneficiaries_reached': cls._safe_int(r.get('beneficiaries_reached')),
                'avg_payment': cls._safe_float(r.get('avg_payment')),
                'female_inclusion': cls._safe_float(r.get('female_inclusion')),
                'twa_inclusion': cls._safe_float(r.get('twa_inclusion')),
                'geographic_coverage': cls._safe_int(r.get('geographic_coverage')),
                'active_programs': cls._safe_int(r.get('active_programs')),
                'external_percentage': cls._safe_float(r.get('external_pct')),
                'internal_percentage': cls._safe_float(r.get('internal_pct')),
                'efficiency_score': 0,
            },
            'targets': {
                'female_inclusion': 50.0,
                'twa_inclusion': 10.0,
                'efficiency_score': 85.0,
            },
            'last_updated': datetime.now().isoformat(),
        }
        cache.set(cache_key, data, cls.CACHE_TTL['payment'])
        return data

    # ─── View Management ──────────────────────────────────────────

    @classmethod
    def refresh_views_if_needed(cls):
        """Refresh all materialized views (called by warm_dashboard_cache command)."""
        from .views_manager import MaterializedViewsManager
        return MaterializedViewsManager.refresh_all_views(concurrent=False)
