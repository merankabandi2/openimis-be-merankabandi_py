"""
Backend API endpoints for self-service analytics and data exploration
"""

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.http import HttpResponse, JsonResponse
from django.db import connection
from django.conf import settings
import csv
import json
import pandas as pd
from io import StringIO, BytesIO
import datetime
from typing import Dict, List, Any


class DataExplorationViewSet(viewsets.ViewSet):
    """
    API ViewSet for self-service data exploration of materialized views
    """
    permission_classes = [IsAuthenticated]
    
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
                    JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = mv.schemaname
                    LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                    WHERE mv.schemaname = 'public'
                      AND (mv.matviewname LIKE 'dashboard_%' OR mv.matviewname LIKE 'payment_reporting_%')
                    GROUP BY mv.matviewname, c.oid, mv.ispopulated
                    ORDER BY mv.matviewname
                """)
                
                views = []
                for row in cursor.fetchall():
                    views.append({
                        'view_name': row[0],
                        'description': row[1] or f'Materialized view: {row[0]}',
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
                {'error': f'Failed to get views list: {str(e)}'},
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
                        {'error': f'View {view_name} not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )
                
                return Response({
                    'view_name': view_name,
                    'columns': columns,
                    'total_columns': len(columns)
                })
                
        except Exception as e:
            return Response(
                {'error': f'Failed to get view schema: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def query_data(self, request):
        """Execute a filtered query on a materialized view"""
        data = request.data
        view_name = data.get('view_name')
        filters = data.get('filters', {})
        columns = data.get('columns', ['*'])
        limit = min(int(data.get('limit', 1000)), 10000)  # Max 10k records
        offset = int(data.get('offset', 0))
        order_by = data.get('order_by', '')
        
        if not view_name:
            return Response(
                {'error': 'view_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Build query
            select_clause = ', '.join(columns) if columns != ['*'] else '*'
            where_clause, params = self._build_where_clause(filters)
            order_clause = f"ORDER BY {order_by}" if order_by else ""
            
            query = f"""
                SELECT {select_clause}
                FROM {view_name}
                {where_clause}
                {order_clause}
                LIMIT %s OFFSET %s
            """
            
            # Get total count
            count_query = f"""
                SELECT COUNT(*)
                FROM {view_name}
                {where_clause}
            """
            
            with connection.cursor() as cursor:
                # Execute count query
                cursor.execute(count_query, params)
                total_count = cursor.fetchone()[0]
                
                # Execute data query
                cursor.execute(query, params + [limit, offset])
                
                # Get column names
                column_names = [desc[0] for desc in cursor.description]
                
                # Fetch data
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                data_list = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        # Handle datetime and date serialization
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
                
        except Exception as e:
            return Response(
                {'error': f'Query failed: {str(e)}'},
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
        filename = data.get('filename', f'{view_name}_export')
        
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
            # Build query (no limit for export)
            select_clause = ', '.join(columns) if columns != ['*'] else '*'
            where_clause, params = self._build_where_clause(filters)
            
            query = f"""
                SELECT {select_clause}
                FROM {view_name}
                {where_clause}
            """
            
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                # Create pandas DataFrame for easier export
                df = pd.DataFrame(rows, columns=column_names)
                
                # Generate export based on format
                if export_format == 'csv':
                    response = HttpResponse(content_type='text/csv')
                    response['Content-Disposition'] = f'attachment; filename="{filename}.csv"'
                    df.to_csv(response, index=False)
                    return response
                    
                elif export_format == 'excel':
                    response = HttpResponse(
                        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )
                    response['Content-Disposition'] = f'attachment; filename="{filename}.xlsx"'
                    
                    # Use BytesIO for Excel
                    excel_buffer = BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='Data', index=False)
                    
                    excel_buffer.seek(0)
                    response.write(excel_buffer.getvalue())
                    return response
                    
                elif export_format == 'json':
                    response = HttpResponse(content_type='application/json')
                    response['Content-Disposition'] = f'attachment; filename="{filename}.json"'
                    
                    # Convert DataFrame to JSON with proper datetime handling
                    json_data = df.to_json(orient='records', date_format='iso')
                    response.write(json_data)
                    return response
                
        except Exception as e:
            return Response(
                {'error': f'Export failed: {str(e)}'},
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
            with connection.cursor() as cursor:
                if group_by and aggregate_columns:
                    # Build aggregation query
                    agg_clauses = []
                    for col in aggregate_columns:
                        agg_clauses.extend([
                            f"COUNT({col}) as {col}_count",
                            f"AVG({col}) as {col}_avg",
                            f"SUM({col}) as {col}_sum",
                            f"MIN({col}) as {col}_min",
                            f"MAX({col}) as {col}_max"
                        ])
                    
                    query = f"""
                        SELECT 
                            {group_by},
                            {', '.join(agg_clauses)}
                        FROM {view_name}
                        GROUP BY {group_by}
                        ORDER BY {group_by}
                    """
                    
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
                    # Basic statistics
                    cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
                    total_rows = cursor.fetchone()[0]
                    
                    return Response({
                        'total_rows': total_rows,
                        'view_name': view_name
                    })
                    
        except Exception as e:
            return Response(
                {'error': f'Aggregation failed: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _get_view_category(self, view_name: str) -> str:
        """Categorize views for UI organization"""
        if view_name.startswith('dashboard_beneficiary'):
            return 'Bénéficiaires'
        elif view_name.startswith('dashboard_monetary') or view_name.startswith('payment_reporting'):
            return 'Transferts Monétaires'
        elif view_name.startswith('dashboard_grievance'):
            return 'Réclamations'
        elif view_name.startswith('dashboard_activities') or view_name.startswith('dashboard_microprojects'):
            return 'Activités'
        elif view_name.startswith('dashboard_indicators') or view_name.startswith('dashboard_results'):
            return 'Indicateurs'
        elif view_name.startswith('dashboard_master'):
            return 'Vue d\'Ensemble'
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
    
    def _build_where_clause(self, filters: Dict[str, Any]) -> tuple:
        """Build WHERE clause and parameters from filters"""
        if not filters:
            return "", []
        
        conditions = []
        params = []
        
        for field, filter_config in filters.items():
            if not isinstance(filter_config, dict):
                continue
                
            operator = filter_config.get('operator', 'eq')
            value = filter_config.get('value')
            
            if value is None or value == '':
                continue
            
            if operator == 'eq':
                conditions.append(f"{field} = %s")
                params.append(value)
            elif operator == 'ne':
                conditions.append(f"{field} != %s")
                params.append(value)
            elif operator == 'gt':
                conditions.append(f"{field} > %s")
                params.append(value)
            elif operator == 'gte':
                conditions.append(f"{field} >= %s")
                params.append(value)
            elif operator == 'lt':
                conditions.append(f"{field} < %s")
                params.append(value)
            elif operator == 'lte':
                conditions.append(f"{field} <= %s")
                params.append(value)
            elif operator == 'contains':
                conditions.append(f"{field} ILIKE %s")
                params.append(f"%{value}%")
            elif operator == 'startswith':
                conditions.append(f"{field} ILIKE %s")
                params.append(f"{value}%")
            elif operator == 'in':
                if isinstance(value, list) and value:
                    placeholders = ','.join(['%s'] * len(value))
                    conditions.append(f"{field} IN ({placeholders})")
                    params.extend(value)
            elif operator == 'date_range':
                start_date = filter_config.get('start_date')
                end_date = filter_config.get('end_date')
                if start_date:
                    conditions.append(f"{field} >= %s")
                    params.append(start_date)
                if end_date:
                    conditions.append(f"{field} <= %s")
                    params.append(end_date)
        
        if conditions:
            return f"WHERE {' AND '.join(conditions)}", params
        else:
            return "", []