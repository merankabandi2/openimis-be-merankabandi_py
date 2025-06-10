"""
Enhanced Backend API endpoints for self-service analytics and data exploration
Includes comprehensive JSON_ext field dimensions
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
from .enhanced_documents import DATA_EXPLORATION_FIELDS


class EnhancedDataExplorationViewSet(viewsets.ViewSet):
    """
    Enhanced API ViewSet for self-service data exploration with comprehensive dimensions
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
                    view_info = {
                        'view_name': row[0],
                        'description': row[1] or f'Materialized view: {row[0]}',
                        'size': row[2],
                        'is_populated': row[3],
                        'column_count': row[4],
                        'category': self._get_view_category(row[0]),
                        'dimensions': self._get_view_dimensions(row[0])
                    }
                    views.append(view_info)
                
                return Response({
                    'views': views,
                    'total_count': len(views),
                    'available_dimensions': DATA_EXPLORATION_FIELDS
                })
                
        except Exception as e:
            return Response(
                {'error': f'Failed to get views list: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'])
    def view_schema_enhanced(self, request):
        """Get enhanced schema information with dimension metadata"""
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
                    column_info = {
                        'column_name': row[0],
                        'data_type': row[1],
                        'nullable': row[2],
                        'description': row[3] or '',
                        'filter_type': self._get_filter_type(row[1]),
                        'dimension_metadata': self._get_dimension_metadata(row[0])
                    }
                    columns.append(column_info)
                
                if not columns:
                    return Response(
                        {'error': f'View {view_name} not found'},
                        status=status.HTTP_404_NOT_FOUND
                    )
                
                # Get sample values for key dimensions
                dimension_values = self._get_dimension_values(view_name, columns)
                
                return Response({
                    'view_name': view_name,
                    'columns': columns,
                    'total_columns': len(columns),
                    'dimension_values': dimension_values,
                    'suggested_filters': self._get_suggested_filters(view_name)
                })
                
        except Exception as e:
            return Response(
                {'error': f'Failed to get view schema: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'])
    def dimension_values(self, request):
        """Get distinct values for a specific dimension"""
        view_name = request.query_params.get('view_name')
        dimension = request.query_params.get('dimension')
        
        if not view_name or not dimension:
            return Response(
                {'error': 'view_name and dimension parameters are required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            with connection.cursor() as cursor:
                query = f"""
                    SELECT DISTINCT {dimension}, COUNT(*) as count
                    FROM {view_name}
                    WHERE {dimension} IS NOT NULL
                    GROUP BY {dimension}
                    ORDER BY count DESC
                    LIMIT 100
                """
                cursor.execute(query)
                
                values = []
                for row in cursor.fetchall():
                    values.append({
                        'value': row[0],
                        'count': row[1],
                        'label': self._get_dimension_label(dimension, row[0])
                    })
                
                return Response({
                    'dimension': dimension,
                    'values': values,
                    'total_distinct': len(values)
                })
                
        except Exception as e:
            return Response(
                {'error': f'Failed to get dimension values: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def enhanced_query_data(self, request):
        """Execute a filtered query with enhanced dimension support"""
        data = request.data
        view_name = data.get('view_name')
        filters = data.get('filters', {})
        columns = data.get('columns', ['*'])
        limit = min(int(data.get('limit', 1000)), 10000)
        offset = int(data.get('offset', 0))
        order_by = data.get('order_by', '')
        include_stats = data.get('include_stats', False)
        
        if not view_name:
            return Response(
                {'error': 'view_name is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            # Build query with enhanced filters
            select_clause = ', '.join(columns) if columns != ['*'] else '*'
            where_clause, params = self._build_enhanced_where_clause(filters)
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
                
                # Convert to list of dictionaries with enhanced metadata
                data_list = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        column_name = column_names[i]
                        # Handle datetime and date serialization
                        if isinstance(value, (datetime.datetime, datetime.date)):
                            row_dict[column_name] = value.isoformat()
                        else:
                            row_dict[column_name] = value
                        
                        # Add dimension labels
                        if self._is_dimension_field(column_name):
                            row_dict[f"{column_name}_label"] = self._get_dimension_label(column_name, value)
                    
                    data_list.append(row_dict)
                
                response_data = {
                    'data': data_list,
                    'total_count': total_count,
                    'returned_count': len(data_list),
                    'offset': offset,
                    'limit': limit,
                    'columns': column_names
                }
                
                # Add statistics if requested
                if include_stats:
                    response_data['statistics'] = self._get_query_statistics(view_name, where_clause, params)
                
                return Response(response_data)
                
        except Exception as e:
            return Response(
                {'error': f'Query failed: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def cross_dimensional_analysis(self, request):
        """Perform cross-dimensional analysis"""
        data = request.data
        view_name = data.get('view_name')
        dimension1 = data.get('dimension1')
        dimension2 = data.get('dimension2')
        metric = data.get('metric', 'count')
        filters = data.get('filters', {})
        
        if not all([view_name, dimension1, dimension2]):
            return Response(
                {'error': 'view_name, dimension1, and dimension2 are required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        try:
            where_clause, params = self._build_enhanced_where_clause(filters)
            
            # Build aggregation based on metric
            if metric == 'count':
                agg_clause = 'COUNT(*)'
            elif metric.startswith('sum_'):
                field = metric.replace('sum_', '')
                agg_clause = f'SUM({field})'
            elif metric.startswith('avg_'):
                field = metric.replace('avg_', '')
                agg_clause = f'AVG({field})'
            else:
                agg_clause = 'COUNT(*)'
            
            query = f"""
                SELECT 
                    {dimension1},
                    {dimension2},
                    {agg_clause} as metric_value
                FROM {view_name}
                {where_clause}
                GROUP BY {dimension1}, {dimension2}
                ORDER BY metric_value DESC
                LIMIT 500
            """
            
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                
                # Create pivot table structure
                pivot_data = {}
                dimension1_values = set()
                dimension2_values = set()
                
                for row in cursor.fetchall():
                    d1_val = row[0] or 'Non spécifié'
                    d2_val = row[1] or 'Non spécifié'
                    metric_val = row[2] or 0
                    
                    dimension1_values.add(d1_val)
                    dimension2_values.add(d2_val)
                    
                    if d1_val not in pivot_data:
                        pivot_data[d1_val] = {}
                    pivot_data[d1_val][d2_val] = metric_val
                
                # Convert to matrix format for visualization
                matrix_data = {
                    'rows': sorted(list(dimension1_values)),
                    'columns': sorted(list(dimension2_values)),
                    'data': []
                }
                
                for row in matrix_data['rows']:
                    row_data = []
                    for col in matrix_data['columns']:
                        value = pivot_data.get(row, {}).get(col, 0)
                        row_data.append(value)
                    matrix_data['data'].append(row_data)
                
                return Response({
                    'dimension1': dimension1,
                    'dimension2': dimension2,
                    'metric': metric,
                    'matrix': matrix_data,
                    'summary': {
                        'total_cells': len(dimension1_values) * len(dimension2_values),
                        'non_zero_cells': sum(1 for row in pivot_data.values() for val in row.values() if val > 0)
                    }
                })
                
        except Exception as e:
            return Response(
                {'error': f'Cross-dimensional analysis failed: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    def _get_view_dimensions(self, view_name: str) -> List[str]:
        """Get available dimensions for a view"""
        dimension_mapping = {
            'dashboard_beneficiary': ['gender', 'is_batwa', 'has_disability', 'education_level', 'household_type'],
            'dashboard_household': ['household_type', 'is_twa_household', 'residence_type', 'housing_quality_score'],
            'dashboard_vulnerable_groups': ['group_type', 'province', 'coverage_rate'],
            'dashboard_monetary_transfers': ['transfer_type', 'payment_agency', 'province'],
            'dashboard_grievances': ['category', 'status', 'priority', 'channel'],
            'dashboard_activities': ['activity_type', 'province', 'status'],
            'dashboard_indicators': ['section_name', 'status', 'achievement_rate']
        }
        
        for key, dimensions in dimension_mapping.items():
            if view_name.startswith(key):
                return dimensions
        return []
    
    def _get_dimension_metadata(self, column_name: str) -> Dict:
        """Get metadata for a dimension field"""
        # Check all dimension categories
        for category, dimensions in DATA_EXPLORATION_FIELDS.items():
            for dim_group, fields in dimensions.items():
                if column_name in fields:
                    return {
                        'category': category,
                        'group': dim_group,
                        'config': fields[column_name]
                    }
        return {}
    
    def _get_dimension_label(self, dimension: str, value: Any) -> str:
        """Get human-readable label for dimension value"""
        # Map common dimension values to labels
        label_mappings = {
            'gender': {'M': 'Homme', 'F': 'Femme'},
            'is_batwa': {True: 'Batwa', False: 'Non-Batwa'},
            'has_disability': {True: 'Avec Handicap', False: 'Sans Handicap'},
            'status': {
                'ACTIVE': 'Actif',
                'SUSPENDED': 'Suspendu',
                'POTENTIAL': 'Potentiel'
            },
            'household_type': {
                'TYPE_MENAGE_DEUX_PARENTS': 'Deux Parents',
                'TYPE_MENAGE_UN_PARENT': 'Monoparental',
                'TYPE_MENAGE_ORPHELIN': 'Orphelin',
                'TYPE_MENAGE_SANS_LIEN': 'Sans Lien'
            },
            'residence_type': {
                'MILIEU_RESIDENCE_RURAL': 'Rural',
                'MILIEU_RESIDENCE_URBAIN': 'Urbain'
            }
        }
        
        if dimension in label_mappings and value in label_mappings[dimension]:
            return label_mappings[dimension][value]
        
        # For boolean fields
        if isinstance(value, bool):
            return 'Oui' if value else 'Non'
        
        # Default to original value
        return str(value) if value is not None else 'Non spécifié'
    
    def _is_dimension_field(self, column_name: str) -> bool:
        """Check if a column is a dimension field"""
        dimension_fields = [
            'gender', 'is_batwa', 'is_refugee', 'is_returnee', 'is_displaced',
            'has_disability', 'disability_type', 'education_level', 'household_type',
            'residence_type', 'province', 'commune', 'status', 'category',
            'channel', 'priority', 'activity_type', 'transfer_type'
        ]
        return column_name in dimension_fields
    
    def _get_dimension_values(self, view_name: str, columns: List[Dict]) -> Dict:
        """Get sample values for dimension columns"""
        dimension_values = {}
        
        try:
            with connection.cursor() as cursor:
                for column in columns:
                    if self._is_dimension_field(column['column_name']):
                        query = f"""
                            SELECT DISTINCT {column['column_name']}, COUNT(*) as count
                            FROM {view_name}
                            WHERE {column['column_name']} IS NOT NULL
                            GROUP BY {column['column_name']}
                            ORDER BY count DESC
                            LIMIT 20
                        """
                        cursor.execute(query)
                        
                        values = []
                        for row in cursor.fetchall():
                            values.append({
                                'value': row[0],
                                'count': row[1],
                                'label': self._get_dimension_label(column['column_name'], row[0])
                            })
                        
                        dimension_values[column['column_name']] = values
        except:
            pass
        
        return dimension_values
    
    def _get_suggested_filters(self, view_name: str) -> List[Dict]:
        """Get suggested filters based on view type"""
        base_filters = [
            {
                'field': 'province',
                'label': 'Province',
                'type': 'select',
                'priority': 1
            },
            {
                'field': 'date_range',
                'label': 'Période',
                'type': 'daterange',
                'priority': 2
            }
        ]
        
        if 'beneficiary' in view_name or 'household' in view_name:
            base_filters.extend([
                {
                    'field': 'is_batwa',
                    'label': 'Ménage Batwa',
                    'type': 'boolean',
                    'priority': 3
                },
                {
                    'field': 'has_disability',
                    'label': 'Avec Handicap',
                    'type': 'boolean',
                    'priority': 4
                },
                {
                    'field': 'household_type',
                    'label': 'Type de Ménage',
                    'type': 'select',
                    'priority': 5
                }
            ])
        
        return sorted(base_filters, key=lambda x: x['priority'])
    
    def _build_enhanced_where_clause(self, filters: Dict[str, Any]) -> tuple:
        """Build WHERE clause with support for complex dimension filters"""
        if not filters:
            return "", []
        
        conditions = []
        params = []
        
        for field, filter_config in filters.items():
            if not isinstance(filter_config, dict):
                continue
            
            # Handle special dimension filters
            if field == 'vulnerable_groups':
                # Filter for any vulnerable group
                group_conditions = []
                if filter_config.get('include_batwa'):
                    group_conditions.append("is_batwa = true")
                if filter_config.get('include_refugee'):
                    group_conditions.append("is_refugee = true")
                if filter_config.get('include_returnee'):
                    group_conditions.append("is_returnee = true")
                if filter_config.get('include_displaced'):
                    group_conditions.append("is_displaced = true")
                
                if group_conditions:
                    conditions.append(f"({' OR '.join(group_conditions)})")
                continue
            
            # Handle PMT score ranges
            if field == 'pmt_score_range':
                range_value = filter_config.get('value')
                if range_value == '<30':
                    conditions.append("pmt_score < 30")
                elif range_value == '30-40':
                    conditions.append("pmt_score >= 30 AND pmt_score < 40")
                elif range_value == '40-50':
                    conditions.append("pmt_score >= 40 AND pmt_score < 50")
                elif range_value == '>50':
                    conditions.append("pmt_score >= 50")
                continue
            
            # Standard filter handling
            operator = filter_config.get('operator', 'eq')
            value = filter_config.get('value')
            
            if value is None or value == '':
                continue
            
            # Apply standard operators
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
    
    def _get_query_statistics(self, view_name: str, where_clause: str, params: List) -> Dict:
        """Get statistics for the filtered query"""
        try:
            with connection.cursor() as cursor:
                # Get key statistics based on view type
                stats = {}
                
                if 'beneficiary' in view_name:
                    cursor.execute(f"""
                        SELECT 
                            COUNT(DISTINCT individual_id) as total_beneficiaries,
                            COUNT(CASE WHEN gender = 'M' THEN 1 END) as male_count,
                            COUNT(CASE WHEN gender = 'F' THEN 1 END) as female_count,
                            COUNT(CASE WHEN is_batwa = true THEN 1 END) as batwa_count,
                            COUNT(CASE WHEN has_disability = true THEN 1 END) as disabled_count
                        FROM {view_name}
                        {where_clause}
                    """, params)
                    
                    result = cursor.fetchone()
                    stats = {
                        'total_beneficiaries': result[0],
                        'gender_distribution': {
                            'male': result[1],
                            'female': result[2]
                        },
                        'vulnerable_groups': {
                            'batwa': result[3],
                            'disabled': result[4]
                        }
                    }
                
                elif 'transfer' in view_name:
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total_transfers,
                            SUM(paid_total) as total_paid,
                            AVG(payment_rate) as avg_payment_rate
                        FROM {view_name}
                        {where_clause}
                    """, params)
                    
                    result = cursor.fetchone()
                    stats = {
                        'total_transfers': result[0],
                        'total_amount_paid': float(result[1]) if result[1] else 0,
                        'average_payment_rate': float(result[2]) if result[2] else 0
                    }
                
                return stats
        except:
            return {}
    
    def _get_view_category(self, view_name: str) -> str:
        """Categorize views for UI organization"""
        if view_name.startswith('dashboard_beneficiary'):
            return 'Bénéficiaires'
        elif view_name.startswith('dashboard_household'):
            return 'Ménages'
        elif view_name.startswith('dashboard_vulnerable'):
            return 'Groupes Vulnérables'
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