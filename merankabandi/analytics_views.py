"""
Backend API endpoints for self-service analytics and data exploration
"""

import re
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.http import HttpResponse
from django.db import connection
import pandas as pd
from io import BytesIO
import datetime
from typing import Dict, List, Any

from .views_manager import MaterializedViewsManager

# Strict identifier regex: only letters, digits, underscores
_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


class DataExplorationViewSet(viewsets.ViewSet):
    """
    API ViewSet for self-service data exploration of materialized views
    """
    permission_classes = [IsAuthenticated]

    # ── Validation helpers ────────────────────────────────────────

    @staticmethod
    def _validate_view_name(view_name: str) -> None:
        """Check view_name against the MaterializedViewsManager allowlist."""
        allowed = MaterializedViewsManager.get_all_view_names()
        if view_name not in allowed:
            raise ValueError(
                f"Invalid view name '{view_name}'. "
                f"Allowed views: {', '.join(sorted(allowed))}"
            )

    @staticmethod
    def _validate_columns(columns: List[str], view_name: str) -> None:
        """Validate that every requested column exists in the given view."""
        if columns == ['*']:
            return
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s
                  AND n.nspname = 'public'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            """, [view_name])
            valid_columns = {row[0] for row in cursor.fetchall()}

        for col in columns:
            if col not in valid_columns:
                raise ValueError(
                    f"Invalid column '{col}' for view '{view_name}'. "
                    f"Valid columns: {', '.join(sorted(valid_columns))}"
                )

    @staticmethod
    def _validate_order_by(order_by: str, view_name: str) -> str:
        """
        Validate an ORDER BY clause.
        Accepts 'column_name' or 'column_name ASC|DESC'.
        Returns a safe ORDER BY fragment.
        """
        if not order_by:
            return ""
        parts = order_by.strip().split()
        if len(parts) not in (1, 2):
            raise ValueError(
                f"Invalid order_by format '{order_by}'. "
                "Expected 'column_name' or 'column_name ASC|DESC'."
            )
        col_name = parts[0]
        direction = parts[1].upper() if len(parts) == 2 else "ASC"
        if direction not in ("ASC", "DESC"):
            raise ValueError(
                f"Invalid sort direction '{parts[1]}'. Must be ASC or DESC."
            )
        if not _IDENTIFIER_RE.match(col_name):
            raise ValueError(f"Invalid column name in order_by: '{col_name}'")
        # Verify column exists in the view
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 1
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s
                  AND n.nspname = 'public'
                  AND a.attname = %s
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            """, [view_name, col_name])
            if not cursor.fetchone():
                raise ValueError(
                    f"Column '{col_name}' does not exist in view '{view_name}'."
                )
        return f"ORDER BY {col_name} {direction}"

    # ── Endpoints ─────────────────────────────────────────────────

    def list(self, request):
        """Get list of available materialized views for exploration"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        mv.matviewname as view_name,
                        obj_description(c.oid) as description,
                        pg_size_pretty(pg_total_relation_size(c.oid)) as size,
                        mv.ispopulated,
                        COUNT(a.attname) as column_count
                    FROM pg_matviews mv
                    JOIN pg_class c ON c.relname = mv.matviewname
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                        AND n.nspname = mv.schemaname
                    LEFT JOIN pg_attribute a ON a.attrelid = c.oid
                        AND a.attnum > 0 AND NOT a.attisdropped
                    WHERE mv.schemaname = 'public'
                      AND (mv.matviewname LIKE 'dashboard_%%'
                           OR mv.matviewname LIKE 'payment_reporting_%%')
                    GROUP BY mv.matviewname, c.oid, mv.ispopulated
                    ORDER BY mv.matviewname
                """)

                views = []
                for row in cursor.fetchall():
                    views.append({
                        'view_name': row[0],
                        'description': row[1] or 'Materialized view: %s' % row[0],
                        'size': row[2],
                        'is_populated': row[3],
                        'column_count': row[4],
                        'category': self._get_view_category(row[0])
                    })

                return Response({
                    'views': views,
                    'total_count': len(views)
                })

        except Exception as e:
            return Response(
                {'error': 'Failed to get views list: %s' % str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=['get'])
    def view_schema(self, request):
        """Get schema information for a specific materialized view"""
        view_name = request.query_params.get('view_name')

        if not view_name:
            return Response(
                {'error': 'view_name parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            self._validate_view_name(view_name)

            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        a.attname as column_name,
                        t.typname as data_type,
                        CASE
                            WHEN a.attnotnull THEN 'NOT NULL'
                            ELSE 'NULL'
                        END as nullable,
                        col_description(a.attrelid, a.attnum) as description
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_type t ON t.oid = a.atttypid
                    WHERE c.relname = %s
                      AND n.nspname = 'public'
                      AND a.attnum > 0
                      AND NOT a.attisdropped
                    ORDER BY a.attnum
                """, [view_name])

                columns = []
                for row in cursor.fetchall():
                    columns.append({
                        'column_name': row[0],
                        'data_type': row[1],
                        'nullable': row[2],
                        'description': row[3] or '',
                        'filter_type': self._get_filter_type(row[1])
                    })

                if not columns:
                    return Response(
                        {'error': 'View %s not found' % view_name},
                        status=status.HTTP_404_NOT_FOUND
                    )

                return Response({
                    'view_name': view_name,
                    'columns': columns,
                    'total_columns': len(columns)
                })

        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'error': 'Failed to get view schema: %s' % str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=['post'])
    def query_data(self, request):
        """Execute a filtered query on a materialized view"""
        data = request.data
        view_name = data.get('view_name')
        filters = data.get('filters', {})
        columns = data.get('columns', ['*'])
        limit = min(int(data.get('limit', 1000)), 10000)
        offset = int(data.get('offset', 0))
        order_by = data.get('order_by', '')

        if not view_name:
            return Response(
                {'error': 'view_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            self._validate_view_name(view_name)
            self._validate_columns(columns, view_name)
            order_clause = self._validate_order_by(order_by, view_name)

            select_clause = ', '.join(columns) if columns != ['*'] else '*'
            where_clause, params = self._build_where_clause(filters, view_name)

            count_query = (
                "SELECT COUNT(*) FROM %s %s" % (view_name, where_clause)
            )
            query = (
                "SELECT %s FROM %s %s %s LIMIT %%s OFFSET %%s"
                % (select_clause, view_name, where_clause, order_clause)
            )

            with connection.cursor() as cursor:
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()[0]

                cursor.execute(query, params + [limit, offset])
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                data_list = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        if isinstance(value, (datetime.datetime, datetime.date)):
                            row_dict[column_names[i]] = value.isoformat()
                        else:
                            row_dict[column_names[i]] = value
                    data_list.append(row_dict)

                return Response({
                    'data': data_list,
                    'total_count': total_count,
                    'returned_count': len(data_list),
                    'offset': offset,
                    'limit': limit,
                    'columns': column_names
                })

        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'error': 'Query failed: %s' % str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=['post'])
    def export_data(self, request):
        """Export filtered data in various formats (CSV, Excel, JSON)"""
        data = request.data
        view_name = data.get('view_name')
        filters = data.get('filters', {})
        columns = data.get('columns', ['*'])
        export_format = data.get('format', 'csv').lower()
        filename = data.get('filename', '%s_export' % view_name)

        if not view_name:
            return Response(
                {'error': 'view_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        if export_format not in ['csv', 'excel', 'json']:
            return Response(
                {'error': 'format must be one of: csv, excel, json'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            self._validate_view_name(view_name)
            self._validate_columns(columns, view_name)

            select_clause = ', '.join(columns) if columns != ['*'] else '*'
            where_clause, params = self._build_where_clause(filters, view_name)

            query = (
                "SELECT %s FROM %s %s LIMIT 100000"
                % (select_clause, view_name, where_clause)
            )

            with connection.cursor() as cursor:
                cursor.execute(query, params)
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

                df = pd.DataFrame(rows, columns=column_names)

                if export_format == 'csv':
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = (
                        'attachment; filename="%s.csv"' % filename
                    )
                    df.to_csv(response, index=False)
                    return response

                elif export_format == 'excel':
                    response = HttpResponse(
                        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                    response['Content-Disposition'] = (
                        'attachment; filename="%s.xlsx"' % filename
                    )
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='Data', index=False)
                    excel_buffer.seek(0)
                    response.write(excel_buffer.getvalue())
                    return response

                elif export_format == 'json':
                    response = HttpResponse(content_type='application/json')
                    response['Content-Disposition'] = (
                        'attachment; filename="%s.json"' % filename
                    )
                    json_data = df.to_json(orient='records', date_format='iso')
                    response.write(json_data)
                    return response

        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'error': 'Export failed: %s' % str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=False, methods=['get'])
    def aggregate_data(self, request):
        """Get aggregated statistics for a view"""
        view_name = request.query_params.get('view_name')
        group_by = request.query_params.get('group_by', '')
        aggregate_columns = request.query_params.getlist('aggregate_columns')

        if not view_name:
            return Response(
                {'error': 'view_name parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            self._validate_view_name(view_name)

            if group_by:
                self._validate_columns([group_by], view_name)
            if aggregate_columns:
                self._validate_columns(aggregate_columns, view_name)

            with connection.cursor() as cursor:
                if group_by and aggregate_columns:
                    agg_clauses = []
                    for col in aggregate_columns:
                        agg_clauses.extend([
                            "COUNT(%s) as %s_count" % (col, col),
                            "AVG(%s) as %s_avg" % (col, col),
                            "SUM(%s) as %s_sum" % (col, col),
                            "MIN(%s) as %s_min" % (col, col),
                            "MAX(%s) as %s_max" % (col, col),
                        ])

                    query = (
                        "SELECT %s, %s FROM %s GROUP BY %s ORDER BY %s"
                        % (group_by, ', '.join(agg_clauses),
                           view_name, group_by, group_by)
                    )

                    cursor.execute(query)
                    column_names = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    data_list = []
                    for row in rows:
                        row_dict = dict(zip(column_names, row))
                        data_list.append(row_dict)

                    return Response({
                        'aggregated_data': data_list,
                        'group_by': group_by,
                        'aggregate_columns': aggregate_columns
                    })
                else:
                    cursor.execute(
                        "SELECT COUNT(*) FROM %s" % view_name
                    )
                    total_rows = cursor.fetchone()[0]

                    return Response({
                        'total_rows': total_rows,
                        'view_name': view_name
                    })

        except ValueError as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            return Response(
                {'error': 'Aggregation failed: %s' % str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    # ── Internal helpers ──────────────────────────────────────────

    def _get_view_category(self, view_name: str) -> str:
        """Categorize views for UI organization"""
        if view_name.startswith('dashboard_beneficiary'):
            return 'Beneficiaires'
        elif view_name.startswith('dashboard_monetary') or view_name.startswith('payment_reporting'):
            return 'Transferts Monetaires'
        elif view_name.startswith('dashboard_grievance'):
            return 'Reclamations'
        elif view_name.startswith('dashboard_activities') or view_name.startswith('dashboard_microprojects'):
            return 'Activites'
        elif view_name.startswith('dashboard_indicators') or view_name.startswith('dashboard_results'):
            return 'Indicateurs'
        elif view_name.startswith('dashboard_master'):
            return "Vue d'Ensemble"
        else:
            return 'Autres'

    def _get_filter_type(self, data_type: str) -> str:
        """Determine appropriate filter widget type based on data type"""
        if data_type in ['int4', 'int8', 'numeric', 'float4', 'float8']:
            return 'number'
        elif data_type in ['date', 'timestamp', 'timestamptz']:
            return 'date'
        elif data_type == 'bool':
            return 'boolean'
        else:
            return 'text'

    def _build_where_clause(self, filters: Dict[str, Any], view_name: str) -> tuple:
        """Build WHERE clause and parameters from filters.

        Field names in the filter keys are validated against pg_attribute
        for the given view to prevent SQL injection via column names.
        """
        if not filters:
            return "", []

        # Fetch valid columns for this view
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT a.attname
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s
                  AND n.nspname = 'public'
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            """, [view_name])
            valid_columns = {row[0] for row in cursor.fetchall()}

        conditions: List[str] = []
        params: List[Any] = []

        for field, filter_config in filters.items():
            if not isinstance(filter_config, dict):
                continue
            if field not in valid_columns:
                raise ValueError(
                    "Invalid filter column '%s' for view '%s'."
                    % (field, view_name)
                )

            operator = filter_config.get('operator', 'eq')
            value = filter_config.get('value')

            if value is None or value == '':
                continue

            if operator == 'eq':
                conditions.append("%s = %%s" % field)
                params.append(value)
            elif operator == 'ne':
                conditions.append("%s != %%s" % field)
                params.append(value)
            elif operator == 'gt':
                conditions.append("%s > %%s" % field)
                params.append(value)
            elif operator == 'gte':
                conditions.append("%s >= %%s" % field)
                params.append(value)
            elif operator == 'lt':
                conditions.append("%s < %%s" % field)
                params.append(value)
            elif operator == 'lte':
                conditions.append("%s <= %%s" % field)
                params.append(value)
            elif operator == 'contains':
                conditions.append("%s ILIKE %%s" % field)
                params.append("%%%s%%" % value)
            elif operator == 'startswith':
                conditions.append("%s ILIKE %%s" % field)
                params.append("%s%%" % value)
            elif operator == 'in':
                if isinstance(value, list) and value:
                    placeholders = ','.join(['%s'] * len(value))
                    conditions.append("%s IN (%s)" % (field, placeholders))
                    params.extend(value)
            elif operator == 'date_range':
                start_date = filter_config.get('start_date')
                end_date = filter_config.get('end_date')
                if start_date:
                    conditions.append("%s >= %%s" % field)
                    params.append(start_date)
                if end_date:
                    conditions.append("%s <= %%s" % field)
                    params.append(end_date)

        if conditions:
            return "WHERE %s" % ' AND '.join(conditions), params
        else:
            return "", []
