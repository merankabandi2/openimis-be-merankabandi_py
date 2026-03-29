"""
Geography Service
Aggregates data from existing tables for geography location pages.
Provides province summaries and location detail views with KPIs,
child locations, programs, payment history, and households.
"""

import json as _json
import logging

from django.db import connection
from django.core.cache import cache
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class GeographyService:
    """
    Service layer for geography pages.
    All queries use raw SQL against existing tables and materialized views.
    """

    CACHE_TTL = 300  # 5 minutes

    # ─── Helpers ───────────────────────────────────────────────────

    @staticmethod
    def _dictfetchall(cursor) -> List[Dict[str, Any]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    @staticmethod
    def _dictfetchone(cursor) -> Dict[str, Any]:
        columns = [col[0] for col in cursor.description]
        row = cursor.fetchone()
        return dict(zip(columns, row)) if row else {}

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

    # ─── Location Helpers ──────────────────────────────────────────

    @classmethod
    def _get_location(cls, location_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single location with its parent chain."""
        query = """
            SELECT
                loc."LocationId" AS id,
                loc."LocationUUID" AS uuid,
                loc."LocationCode" AS code,
                loc."LocationName" AS name,
                loc."LocationType" AS type,
                loc."ParentLocationId" AS parent_id,
                p1."LocationId" AS parent_loc_id,
                p1."LocationUUID" AS parent_uuid,
                p1."LocationCode" AS parent_code,
                p1."LocationName" AS parent_name,
                p1."LocationType" AS parent_type,
                p1."ParentLocationId" AS grandparent_id,
                p2."LocationId" AS grandparent_loc_id,
                p2."LocationUUID" AS grandparent_uuid,
                p2."LocationCode" AS grandparent_code,
                p2."LocationName" AS grandparent_name,
                p2."LocationType" AS grandparent_type
            FROM "tblLocations" loc
            LEFT JOIN "tblLocations" p1
                ON p1."LocationId" = loc."ParentLocationId"
                AND p1."ValidityTo" IS NULL
            LEFT JOIN "tblLocations" p2
                ON p2."LocationId" = p1."ParentLocationId"
                AND p2."ValidityTo" IS NULL
            WHERE loc."LocationId" = %s
              AND loc."ValidityTo" IS NULL
        """
        with connection.cursor() as cursor:
            cursor.execute(query, [location_id])
            row = cls._dictfetchone(cursor)
        return row if row else None

    @classmethod
    def _build_location_dict(cls, loc_row: Dict[str, Any]) -> Dict[str, Any]:
        """Build a nested location dict with parent chain from a _get_location row."""
        if not loc_row:
            return {}

        parent = None
        if loc_row.get('parent_loc_id'):
            grandparent = None
            if loc_row.get('grandparent_loc_id'):
                grandparent = {
                    'id': cls._safe_int(loc_row['grandparent_loc_id']),
                    'uuid': str(loc_row['grandparent_uuid']) if loc_row.get('grandparent_uuid') else None,
                    'code': loc_row.get('grandparent_code'),
                    'name': loc_row.get('grandparent_name'),
                    'type': loc_row.get('grandparent_type'),
                    'parent': None,
                }
            parent = {
                'id': cls._safe_int(loc_row['parent_loc_id']),
                'uuid': str(loc_row['parent_uuid']) if loc_row.get('parent_uuid') else None,
                'code': loc_row.get('parent_code'),
                'name': loc_row.get('parent_name'),
                'type': loc_row.get('parent_type'),
                'parent': grandparent,
            }

        return {
            'id': cls._safe_int(loc_row['id']),
            'uuid': str(loc_row['uuid']) if loc_row.get('uuid') else None,
            'code': loc_row.get('code'),
            'name': loc_row.get('name'),
            'type': loc_row.get('type'),
            'parent': parent,
        }

    # ─── KPI Aggregation ──────────────────────────────────────────

    @classmethod
    def _get_kpis_for_location(
        cls, location_id: int, location_type: str,
        benefit_plan_id: Optional[str] = None, year: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Aggregate KPIs for a given location.
        Uses group table for households/individuals and materialized view for payments.
        """
        # Build location filter for groups depending on level
        # Groups sit at colline level (type V). group.location_id = colline LocationId.
        if location_type == 'D':
            # Province: colline -> commune -> province
            group_where = """
                g.location_id IN (
                    SELECT col."LocationId"
                    FROM "tblLocations" col
                    INNER JOIN "tblLocations" com
                        ON com."LocationId" = col."ParentLocationId"
                        AND com."ValidityTo" IS NULL
                    WHERE com."ParentLocationId" = %s
                      AND col."ValidityTo" IS NULL
                      AND col."LocationType" = 'V'
                )
            """
            payment_where = 'p.province_id = %s'
        elif location_type == 'W':
            # Commune: colline -> commune
            group_where = """
                g.location_id IN (
                    SELECT col."LocationId"
                    FROM "tblLocations" col
                    WHERE col."ParentLocationId" = %s
                      AND col."ValidityTo" IS NULL
                      AND col."LocationType" = 'V'
                )
            """
            payment_where = 'p.commune_id = %s'
        else:
            # Colline (V): direct match
            group_where = 'g.location_id = %s'
            payment_where = 'p.colline_id = %s'

        # Household + individual counts
        group_params = [location_id]
        gb_extra_cond = ''
        if benefit_plan_id:
            gb_extra_cond = 'AND gb.benefit_plan_id = %s'
            group_params.append(benefit_plan_id)

        household_query = f"""
            SELECT
                COUNT(DISTINCT g."UUID") AS total_households,
                COUNT(DISTINCT gi.individual_id) AS total_individuals,
                COUNT(DISTINCT gb."UUID") AS total_beneficiaries
            FROM individual_group g
            LEFT JOIN individual_groupindividual gi
                ON gi.group_id = g."UUID" AND gi."isDeleted" = false
            LEFT JOIN social_protection_groupbeneficiary gb
                ON gb.group_id = g."UUID" AND gb."isDeleted" = false
                {gb_extra_cond}
            WHERE {group_where}
              AND g."isDeleted" = false
        """

        with connection.cursor() as cursor:
            cursor.execute(household_query, group_params)
            hh = cls._dictfetchone(cursor)

        total_households = cls._safe_int(hh.get('total_households'))
        total_individuals = cls._safe_int(hh.get('total_individuals'))
        total_beneficiaries = cls._safe_int(hh.get('total_beneficiaries'))

        # Payment aggregation from materialized view
        payment_conditions = [payment_where]
        payment_params = [location_id]
        if benefit_plan_id:
            payment_conditions.append('p.programme_id = %s')
            payment_params.append(benefit_plan_id)
        if year:
            payment_conditions.append('p.year = %s')
            payment_params.append(year)

        payment_where_clause = ' AND '.join(payment_conditions)

        payment_query = f"""
            SELECT
                COALESCE(SUM(p.total_amount_paid), 0) AS total_amount_disbursed,
                COALESCE(COUNT(DISTINCT (p.year, p.quarter)), 0) AS payment_cycle_count
            FROM payment_reporting_unified_summary p
            WHERE {payment_where_clause}
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(payment_query, payment_params)
                pay = cls._dictfetchone(cursor)
        except Exception as e:
            logger.warning("Geography query failed: %s", e)
            pay = {}

        total_amount_disbursed = cls._safe_float(pay.get('total_amount_disbursed'))
        payment_cycle_count = cls._safe_int(pay.get('payment_cycle_count'))
        payment_rate = (total_beneficiaries / total_households * 100) if total_households > 0 else 0.0

        return {
            'total_households': total_households,
            'total_individuals': total_individuals,
            'total_beneficiaries': total_beneficiaries,
            'total_amount_disbursed': total_amount_disbursed,
            'payment_cycle_count': payment_cycle_count,
            'payment_rate': round(payment_rate, 2),
        }

    # ─── Children ──────────────────────────────────────────────────

    @classmethod
    def _get_children(
        cls, location_id: int, location_type: str,
        benefit_plan_id: Optional[str] = None, year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get child locations with aggregated stats.
        Province -> communes, Commune -> collines, Colline -> empty.
        """
        if location_type == 'V':
            return []

        if location_type == 'D':
            # Children are communes; groups are at colline level under each commune
            child_group_join = """
                LEFT JOIN "tblLocations" col
                    ON col."ParentLocationId" = child."LocationId"
                    AND col."ValidityTo" IS NULL
                    AND col."LocationType" = 'V'
                LEFT JOIN individual_group g
                    ON g.location_id = col."LocationId"
                    AND g."isDeleted" = false
            """
            payment_level = 'commune_id'
            child_count_subquery = """
                (SELECT COUNT(*)
                 FROM "tblLocations" sub
                 WHERE sub."ParentLocationId" = child."LocationId"
                   AND sub."ValidityTo" IS NULL) AS child_count
            """
        else:
            # location_type == 'W', children are collines; groups are directly at colline
            child_group_join = """
                LEFT JOIN individual_group g
                    ON g.location_id = child."LocationId"
                    AND g."isDeleted" = false
            """
            payment_level = 'colline_id'
            child_count_subquery = '0 AS child_count'

        bp_join = ''
        bp_cond = ''
        bp_params = []
        if benefit_plan_id:
            bp_join = """
                LEFT JOIN social_protection_groupbeneficiary gb_bp
                    ON gb_bp.group_id = g."UUID" AND gb_bp."isDeleted" = false
            """
            bp_cond = 'AND gb_bp.benefit_plan_id = %s'
            bp_params = [benefit_plan_id]

        query = f"""
            SELECT
                child."LocationId" AS id,
                child."LocationUUID" AS uuid,
                child."LocationCode" AS code,
                child."LocationName" AS name,
                child."LocationType" AS type,
                COUNT(DISTINCT g."UUID") AS total_households,
                COUNT(DISTINCT gb."UUID") AS total_beneficiaries,
                {child_count_subquery}
            FROM "tblLocations" child
            {child_group_join}
            LEFT JOIN social_protection_groupbeneficiary gb
                ON gb.group_id = g."UUID" AND gb."isDeleted" = false
            {bp_join}
            WHERE child."ParentLocationId" = %s
              AND child."ValidityTo" IS NULL
              {bp_cond}
            GROUP BY child."LocationId", child."LocationUUID",
                     child."LocationCode", child."LocationName",
                     child."LocationType"
            ORDER BY child."LocationName"
        """

        params = [location_id] + bp_params

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            children_rows = cls._dictfetchall(cursor)

        # Fetch payment data for all children in one query
        child_ids = [r['id'] for r in children_rows]
        payment_map = {}
        if child_ids:
            assert payment_level in ('province_id', 'commune_id', 'colline_id'), f"Invalid payment_level: {payment_level}"
            placeholders = ','.join(['%s'] * len(child_ids))
            payment_conditions = [f'p.{payment_level} IN ({placeholders})']
            payment_params = list(child_ids)
            if benefit_plan_id:
                payment_conditions.append('p.programme_id = %s')
                payment_params.append(benefit_plan_id)
            if year:
                payment_conditions.append('p.year = %s')
                payment_params.append(year)
            payment_where = ' AND '.join(payment_conditions)

            pay_query = f"""
                SELECT
                    p.{payment_level} AS loc_id,
                    COALESCE(SUM(p.total_amount_paid), 0) AS total_amount_disbursed
                FROM payment_reporting_unified_summary p
                WHERE {payment_where}
                GROUP BY p.{payment_level}
            """
            try:
                with connection.cursor() as cursor:
                    cursor.execute(pay_query, payment_params)
                    for row in cls._dictfetchall(cursor):
                        payment_map[row['loc_id']] = cls._safe_float(row['total_amount_disbursed'])
            except Exception as e:
                logger.warning("Geography query failed: %s", e)

        children = []
        for r in children_rows:
            hh = cls._safe_int(r.get('total_households'))
            ben = cls._safe_int(r.get('total_beneficiaries'))
            amt = payment_map.get(r['id'], 0.0)
            rate = (ben / hh * 100) if hh > 0 else 0.0
            children.append({
                'id': cls._safe_int(r['id']),
                'uuid': str(r['uuid']) if r.get('uuid') else None,
                'code': r.get('code'),
                'name': r.get('name'),
                'type': r.get('type'),
                'total_households': hh,
                'total_beneficiaries': ben,
                'total_amount_disbursed': amt,
                'payment_rate': round(rate, 2),
                'child_count': cls._safe_int(r.get('child_count')),
            })

        return children

    # ─── Active Programs ───────────────────────────────────────────

    @classmethod
    def _get_active_programs(
        cls, location_id: int, location_type: str,
        benefit_plan_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get active programs for a location via group beneficiaries."""
        if location_type == 'D':
            loc_filter = """
                g.location_id IN (
                    SELECT col."LocationId"
                    FROM "tblLocations" col
                    INNER JOIN "tblLocations" com
                        ON com."LocationId" = col."ParentLocationId"
                        AND com."ValidityTo" IS NULL
                    WHERE com."ParentLocationId" = %s
                      AND col."ValidityTo" IS NULL
                )
            """
        elif location_type == 'W':
            loc_filter = """
                g.location_id IN (
                    SELECT col."LocationId"
                    FROM "tblLocations" col
                    WHERE col."ParentLocationId" = %s
                      AND col."ValidityTo" IS NULL
                )
            """
        else:
            loc_filter = 'g.location_id = %s'

        params = [location_id]
        bp_cond = ''
        if benefit_plan_id:
            bp_cond = 'AND gb.benefit_plan_id = %s'
            params.append(benefit_plan_id)

        query = f"""
            SELECT
                bp."UUID" AS id,
                bp.name,
                bp.code,
                COUNT(DISTINCT gb."UUID") AS beneficiary_count,
                COUNT(DISTINCT g."UUID") AS household_count
            FROM social_protection_groupbeneficiary gb
            INNER JOIN individual_group g
                ON g."UUID" = gb.group_id AND g."isDeleted" = false
            INNER JOIN social_protection_benefitplan bp
                ON bp."UUID" = gb.benefit_plan_id AND bp."isDeleted" = false
            WHERE {loc_filter}
              AND gb."isDeleted" = false
              {bp_cond}
            GROUP BY bp."UUID", bp.name, bp.code
            ORDER BY bp.name
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cls._dictfetchall(cursor)

        # Fetch disbursed amounts and cycle counts for all programs in one query
        programme_ids = [str(r['id']) for r in rows if r.get('id')]
        payment_lookup = {}
        if programme_ids:
            if location_type == 'D':
                pay_loc_where = 'p.province_id = %s'
            elif location_type == 'W':
                pay_loc_where = 'p.commune_id = %s'
            else:
                pay_loc_where = 'p.colline_id = %s'

            placeholders = ','.join(['%s'] * len(programme_ids))
            batch_query = f"""
                SELECT
                    p.programme_id,
                    COALESCE(SUM(p.total_amount_paid), 0) AS amount,
                    COUNT(DISTINCT (p.year, p.quarter)) AS cycle_count
                FROM payment_reporting_unified_summary p
                WHERE {pay_loc_where}
                  AND p.programme_id::text IN ({placeholders})
                GROUP BY p.programme_id
            """
            try:
                with connection.cursor() as cursor:
                    cursor.execute(batch_query, [location_id] + programme_ids)
                    for pay_row in cls._dictfetchall(cursor):
                        pid = str(pay_row['programme_id']) if pay_row.get('programme_id') else None
                        if pid:
                            payment_lookup[pid] = {
                                'amount': cls._safe_float(pay_row.get('amount')),
                                'cycle_count': cls._safe_int(pay_row.get('cycle_count')),
                            }
            except Exception as e:
                logger.warning("Geography query failed: %s", e)

        programs = []
        for r in rows:
            program_id = str(r['id']) if r.get('id') else None
            pay_data = payment_lookup.get(program_id, {})
            programs.append({
                'id': program_id,
                'name': r.get('name'),
                'code': r.get('code'),
                'beneficiary_count': cls._safe_int(r.get('beneficiary_count')),
                'household_count': cls._safe_int(r.get('household_count')),
                'amount_disbursed': pay_data.get('amount', 0.0),
                'cycle_count': pay_data.get('cycle_count', 0),
                'status': r.get('status', ''),
            })

        return programs

    # ─── Payment History ───────────────────────────────────────────

    @classmethod
    def _get_payment_history(
        cls, location_id: int, location_type: str,
        benefit_plan_id: Optional[str] = None, year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get payment history from the materialized view."""
        if location_type == 'D':
            pay_where = 'p.province_id = %s'
        elif location_type == 'W':
            pay_where = 'p.commune_id = %s'
        else:
            pay_where = 'p.colline_id = %s'

        conditions = [pay_where]
        params = [location_id]
        if benefit_plan_id:
            conditions.append('p.programme_id = %s')
            params.append(benefit_plan_id)
        if year:
            conditions.append('p.year = %s')
            params.append(year)

        where_clause = ' AND '.join(conditions)

        query = f"""
            SELECT
                'Q' || p.quarter || ' ' || p.year AS cycle_name,
                MAX(p.payment_date) AS date,
                COALESCE(SUM(p.total_amount_paid), 0) AS amount_paid,
                COALESCE(SUM(p.total_beneficiaries), 0) AS beneficiary_count,
                p.payment_source
            FROM payment_reporting_unified_summary p
            WHERE {where_clause}
            GROUP BY p.year, p.quarter, p.payment_source
            ORDER BY p.year DESC, p.quarter DESC
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cls._dictfetchall(cursor)
        except Exception as e:
            logger.warning("Geography query failed: %s", e)
            rows = []

        history = []
        for r in rows:
            amt_paid = cls._safe_float(r.get('amount_paid'))
            ben_count = cls._safe_int(r.get('beneficiary_count'))
            history.append({
                'cycle_name': r.get('cycle_name', ''),
                'date': str(r['date']) if r.get('date') else None,
                'amount_planned': 0.0,
                'amount_paid': amt_paid,
                'beneficiary_count': ben_count,
                'payment_rate': 0.0,
                'payment_source': r.get('payment_source', ''),
            })

        return history

    # ─── Payment Points ────────────────────────────────────────────

    @classmethod
    def _get_payment_points(
        cls, location_id: int, location_type: str
    ) -> List[Dict[str, Any]]:
        """
        Get payment points for a location.
        - Province (D): direct assignment from merankabandi_provincepaymentpoint
        - Commune (W): inherited from parent province
        - Colline (V): inherited from grandparent province
        """
        if location_type == 'D':
            province_id = location_id
            is_inherited = False
        elif location_type == 'W':
            # Get parent province
            query = """
                SELECT "ParentLocationId"
                FROM "tblLocations"
                WHERE "LocationId" = %s AND "ValidityTo" IS NULL
            """
            with connection.cursor() as cursor:
                cursor.execute(query, [location_id])
                row = cursor.fetchone()
            province_id = row[0] if row else None
            is_inherited = True
        else:
            # Colline: grandparent province
            query = """
                SELECT p2."LocationId"
                FROM "tblLocations" loc
                INNER JOIN "tblLocations" p1
                    ON p1."LocationId" = loc."ParentLocationId"
                    AND p1."ValidityTo" IS NULL
                INNER JOIN "tblLocations" p2
                    ON p2."LocationId" = p1."ParentLocationId"
                    AND p2."ValidityTo" IS NULL
                WHERE loc."LocationId" = %s AND loc."ValidityTo" IS NULL
            """
            with connection.cursor() as cursor:
                cursor.execute(query, [location_id])
                row = cursor.fetchone()
            province_id = row[0] if row else None
            is_inherited = True

        if province_id is None:
            return []

        pp_query = """
            SELECT
                mpp.id,
                pp.name AS payment_point_name
            FROM merankabandi_province_payment_point mpp
            INNER JOIN payroll_paymentpoint pp
                ON pp."UUID" = mpp.payment_point_id
            WHERE mpp.province_id = %s
              AND mpp.is_active = true
            ORDER BY pp.name
        """

        with connection.cursor() as cursor:
            cursor.execute(pp_query, [province_id])
            rows = cls._dictfetchall(cursor)

        return [
            {
                'id': cls._safe_int(r.get('id')),
                'payment_point_name': r.get('payment_point_name', ''),
                'benefit_plan_name': r.get('benefit_plan_name', ''),
                'is_inherited': is_inherited,
            }
            for r in rows
        ]

    # ─── Households (Colline only) ─────────────────────────────────

    @classmethod
    def _get_households(
        cls, location_id: int, location_type: str,
        benefit_plan_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get household list for a colline. Empty for province/commune."""
        if location_type != 'V':
            return []

        params = [location_id]
        gb_extra_cond = ''
        if benefit_plan_id:
            gb_extra_cond = 'AND gb.benefit_plan_id = %s'
            params.append(benefit_plan_id)

        query = f"""
            SELECT
                g."UUID" AS group_uuid,
                g."Json_ext" AS json_ext,
                head_ind."LastName" AS head_last_name,
                head_ind."OtherNames" AS head_other_names,
                COUNT(DISTINCT gi_all.individual_id) AS member_count,
                COALESCE(MAX(gb.status), 'UNKNOWN') AS status
            FROM individual_group g
            LEFT JOIN individual_groupindividual gi_head
                ON gi_head.group_id = g."UUID"
                AND gi_head."isDeleted" = false
                AND gi_head.recipient_type = 'HEAD'
            LEFT JOIN individual_individual head_ind
                ON head_ind."UUID" = gi_head.individual_id
                AND head_ind."isDeleted" = false
            LEFT JOIN individual_groupindividual gi_all
                ON gi_all.group_id = g."UUID"
                AND gi_all."isDeleted" = false
            LEFT JOIN social_protection_groupbeneficiary gb
                ON gb.group_id = g."UUID"
                AND gb."isDeleted" = false
                {gb_extra_cond}
            WHERE g.location_id = %s
              AND g."isDeleted" = false
            GROUP BY g."UUID", g."Json_ext",
                     head_ind."LastName", head_ind."OtherNames"
            ORDER BY head_ind."LastName", head_ind."OtherNames"
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cls._dictfetchall(cursor)

        # Get last payment per group from materialized view
        group_uuids = [str(r['group_uuid']) for r in rows if r.get('group_uuid')]
        payment_map = {}
        if group_uuids:
            # We cannot directly look up by group in the materialized view
            # (it aggregates by location), so payment info comes from json_ext or is omitted.
            pass

        households = []
        for r in rows:
            json_ext = r.get('json_ext') or {}
            if isinstance(json_ext, str):
                try:
                    json_ext = _json.loads(json_ext)
                except (ValueError, TypeError):
                    json_ext = {}

            head_name_parts = []
            if r.get('head_last_name'):
                head_name_parts.append(r['head_last_name'])
            if r.get('head_other_names'):
                head_name_parts.append(r['head_other_names'])
            head_name = ' '.join(head_name_parts) if head_name_parts else ''

            social_id = json_ext.get('social_id', '')
            pmt_score = cls._safe_float(json_ext.get('pmt_score'))
            selection_status = json_ext.get('selection_status', '')
            status = r.get('status') or selection_status

            households.append({
                'group_id': None,
                'group_uuid': str(r['group_uuid']) if r.get('group_uuid') else None,
                'head_of_household_name': head_name,
                'social_id': social_id,
                'status': status,
                'pmt_score': pmt_score,
                'member_count': cls._safe_int(r.get('member_count')),
                'last_payment_date': None,
                'last_payment_amount': 0.0,
            })

        return households

    # ─── Public API ────────────────────────────────────────────────

    @classmethod
    def get_location_detail(
        cls, location_id: int,
        benefit_plan_id: Optional[str] = None,
        year: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Full detail for a single location.
        Returns: location with parent chain, KPIs, children, programs,
        payment history, payment points, households (colline only).
        """
        cache_key = f"geo_location_detail_{location_id}_{benefit_plan_id}_{year}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        loc_row = cls._get_location(location_id)
        if not loc_row:
            return None

        location_type = loc_row.get('type', '')
        location_dict = cls._build_location_dict(loc_row)

        kpis = cls._get_kpis_for_location(location_id, location_type, benefit_plan_id, year)
        children = cls._get_children(location_id, location_type, benefit_plan_id, year)
        programs = cls._get_active_programs(location_id, location_type, benefit_plan_id)
        history = cls._get_payment_history(location_id, location_type, benefit_plan_id, year)
        points = cls._get_payment_points(location_id, location_type)
        households = cls._get_households(location_id, location_type, benefit_plan_id)

        result = {
            'location': location_dict,
            'total_households': kpis['total_households'],
            'total_individuals': kpis['total_individuals'],
            'total_beneficiaries': kpis['total_beneficiaries'],
            'total_amount_disbursed': kpis['total_amount_disbursed'],
            'payment_cycle_count': kpis['payment_cycle_count'],
            'payment_rate': kpis['payment_rate'],
            'children': children,
            'active_programs': programs,
            'payment_history': history,
            'payment_points': points,
            'households': households,
        }

        cache.set(cache_key, result, cls.CACHE_TTL)
        return result

    @classmethod
    def get_provinces_summary(
        cls, benefit_plan_id: Optional[str] = None,
        year: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Summary of all provinces with household, beneficiary, and payment stats.
        """
        cache_key = f"geo_provinces_summary_{benefit_plan_id}_{year}"
        cached = cache.get(cache_key)
        if cached:
            return cached

        bp_join = ''
        bp_cond = ''
        bp_params = []
        if benefit_plan_id:
            bp_join = """
                LEFT JOIN social_protection_groupbeneficiary gb_bp
                    ON gb_bp.group_id = g."UUID" AND gb_bp."isDeleted" = false
            """
            bp_cond = 'AND gb_bp.benefit_plan_id = %s'
            bp_params = [benefit_plan_id]

        # Get all provinces with group counts
        query = f"""
            SELECT
                prov."LocationId" AS id,
                prov."LocationUUID" AS uuid,
                prov."LocationCode" AS code,
                prov."LocationName" AS name,
                COUNT(DISTINCT g."UUID") AS total_households,
                COUNT(DISTINCT gi.individual_id) AS total_individuals,
                COUNT(DISTINCT gb."UUID") AS total_beneficiaries
            FROM "tblLocations" prov
            LEFT JOIN "tblLocations" com
                ON com."ParentLocationId" = prov."LocationId"
                AND com."ValidityTo" IS NULL
            LEFT JOIN "tblLocations" col
                ON col."ParentLocationId" = com."LocationId"
                AND col."ValidityTo" IS NULL
                AND col."LocationType" = 'V'
            LEFT JOIN individual_group g
                ON g.location_id = col."LocationId"
                AND g."isDeleted" = false
            LEFT JOIN individual_groupindividual gi
                ON gi.group_id = g."UUID"
                AND gi."isDeleted" = false
            LEFT JOIN social_protection_groupbeneficiary gb
                ON gb.group_id = g."UUID"
                AND gb."isDeleted" = false
            {bp_join}
            WHERE prov."LocationType" = 'D'
              AND prov."ValidityTo" IS NULL
              {bp_cond}
            GROUP BY prov."LocationId", prov."LocationUUID",
                     prov."LocationCode", prov."LocationName"
            ORDER BY prov."LocationName"
        """

        with connection.cursor() as cursor:
            cursor.execute(query, bp_params)
            province_rows = cls._dictfetchall(cursor)

        # Get payment data per province
        pay_conditions = []
        pay_params = []
        if benefit_plan_id:
            pay_conditions.append('p.programme_id = %s')
            pay_params.append(benefit_plan_id)
        if year:
            pay_conditions.append('p.year = %s')
            pay_params.append(year)

        pay_where = ''
        if pay_conditions:
            pay_where = 'WHERE ' + ' AND '.join(pay_conditions)

        pay_query = f"""
            SELECT
                p.province_id,
                COALESCE(SUM(p.total_amount_paid), 0) AS total_amount_disbursed,
                COALESCE(COUNT(DISTINCT (p.year, p.quarter)), 0) AS payment_cycle_count
            FROM payment_reporting_unified_summary p
            {pay_where}
            GROUP BY p.province_id
        """

        pay_map = {}
        try:
            with connection.cursor() as cursor:
                cursor.execute(pay_query, pay_params)
                for row in cls._dictfetchall(cursor):
                    pay_map[row['province_id']] = {
                        'total_amount_disbursed': cls._safe_float(row['total_amount_disbursed']),
                        'payment_cycle_count': cls._safe_int(row['payment_cycle_count']),
                    }
        except Exception as e:
            logger.warning("Geography query failed: %s", e)

        # Get agency count per province
        agency_query = """
            SELECT
                mpp.province_id,
                COUNT(DISTINCT mpp.payment_point_id) AS agency_count
            FROM merankabandi_province_payment_point mpp
            WHERE mpp.is_active = true
            GROUP BY mpp.province_id
        """

        agency_map = {}
        try:
            with connection.cursor() as cursor:
                cursor.execute(agency_query)
                for row in cls._dictfetchall(cursor):
                    agency_map[row['province_id']] = cls._safe_int(row['agency_count'])
        except Exception as e:
            logger.warning("Geography query failed: %s", e)

        provinces = []
        for r in province_rows:
            prov_id = r['id']
            hh = cls._safe_int(r.get('total_households'))
            ben = cls._safe_int(r.get('total_beneficiaries'))
            pay_data = pay_map.get(prov_id, {})
            amt = pay_data.get('total_amount_disbursed', 0.0)
            cyc = pay_data.get('payment_cycle_count', 0)
            rate = (ben / hh * 100) if hh > 0 else 0.0

            provinces.append({
                'id': cls._safe_int(prov_id),
                'uuid': str(r['uuid']) if r.get('uuid') else None,
                'code': r.get('code'),
                'name': r.get('name'),
                'total_households': hh,
                'total_individuals': cls._safe_int(r.get('total_individuals')),
                'total_beneficiaries': ben,
                'total_amount_disbursed': amt,
                'payment_cycle_count': cyc,
                'payment_rate': round(rate, 2),
                'agency_count': agency_map.get(prov_id, 0),
            })

        cache.set(cache_key, provinces, cls.CACHE_TTL)
        return provinces
