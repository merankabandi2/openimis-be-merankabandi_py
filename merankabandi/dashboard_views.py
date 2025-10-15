"""
M&E Dashboard API Views for Merankabandi Project
Provides REST API endpoints for dashboard data and Excel exports
"""

from datetime import datetime, date
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
import json

from .reporting_services import MEDashboardService, ExcelExportService, IndicatorAggregationService


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def beneficiary_breakdown_api(request):
    """API endpoint for beneficiary breakdown charts"""
    try:
        # Get query parameters
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        location_id = request.GET.get('location_id')
        
        # Convert dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        if location_id:
            location_id = int(location_id)
        
        # Get breakdown data
        data = MEDashboardService.get_beneficiary_breakdown_data(
            start_date=start_date,
            end_date=end_date,
            location_id=location_id
        )
        
        return Response({
            'success': True,
            'data': data
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def refugee_host_breakdown_api(request):
    """API endpoint for refugee vs host community breakdown"""
    try:
        # Get query parameters
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        
        # Convert dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Get breakdown data
        data = MEDashboardService.get_refugee_host_breakdown(
            start_date=start_date,
            end_date=end_date
        )
        
        return Response({
            'success': True,
            'data': data
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def quarterly_rollup_api(request):
    """API endpoint for quarterly/annual rollup data"""
    try:
        # Get query parameters
        year = request.GET.get('year', date.today().year)
        quarter = request.GET.get('quarter')
        
        # Convert to int
        year = int(year)
        if quarter:
            quarter = int(quarter)
        
        # Get rollup data
        data = MEDashboardService.get_quarterly_rollup_data(
            year=year,
            quarter=quarter
        )
        
        return Response({
            'success': True,
            'data': data
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def twa_minority_metrics_api(request):
    """API endpoint for Twa minority specific metrics"""
    try:
        # Get query parameters
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        
        # Convert dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        # Get Twa metrics
        data = MEDashboardService.get_twa_minority_metrics(
            start_date=start_date,
            end_date=end_date
        )
        
        return Response({
            'success': True,
            'data': data
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def excel_export_api(request, report_type):
    """API endpoint for Excel exports"""
    try:
        # Get query parameters
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        location_id = request.GET.get('location_id')
        
        # Convert dates if provided
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        if location_id:
            location_id = int(location_id)
        
        # Route to appropriate export function
        if report_type == 'monetary_transfers':
            return ExcelExportService.export_monetary_transfers_excel(
                start_date=start_date,
                end_date=end_date,
                location_id=location_id
            )
        elif report_type == 'accompanying_measures':
            return ExcelExportService.export_accompanying_measures_excel(
                start_date=start_date,
                end_date=end_date,
                location_id=location_id
            )
        elif report_type == 'microprojects':
            return ExcelExportService.export_microprojects_excel(
                start_date=start_date,
                end_date=end_date,
                location_id=location_id
            )
        else:
            return Response({
                'success': False,
                'error': f'Unknown report type: {report_type}'
            }, status=status.HTTP_400_BAD_REQUEST)
            
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def auto_aggregate_indicators_api(request):
    """API endpoint for triggering automated indicator aggregation"""
    try:
        # Get request parameters
        data = request.data
        aggregation_type = data.get('type', 'household_registration')
        year = data.get('year')
        quarter = data.get('quarter')
        
        if aggregation_type == 'household_registration':
            success, message = IndicatorAggregationService.auto_aggregate_household_registration()
        elif aggregation_type == 'transfer_beneficiaries':
            success, message = IndicatorAggregationService.auto_aggregate_transfer_beneficiaries(
                year=year, quarter=quarter
            )
        else:
            return Response({
                'success': False,
                'error': f'Unknown aggregation type: {aggregation_type}'
            }, status=status.HTTP_400_BAD_REQUEST)
        
        return Response({
            'success': success,
            'message': message
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def dashboard_summary_api(request):
    """API endpoint for overall dashboard summary"""
    try:
        # Get current year data by default
        current_year = date.today().year
        
        # Get various metrics
        beneficiary_data = MEDashboardService.get_beneficiary_breakdown_data()
        refugee_host_data = MEDashboardService.get_refugee_host_breakdown()
        quarterly_data = MEDashboardService.get_quarterly_rollup_data(current_year)
        twa_data = MEDashboardService.get_twa_minority_metrics()
        
        # Compile summary
        summary = {
            'overview': {
                'total_planned_beneficiaries': beneficiary_data['gender_summary']['planned']['men'] + 
                                             beneficiary_data['gender_summary']['planned']['women'] + 
                                             beneficiary_data['gender_summary']['planned']['twa'],
                'total_paid_beneficiaries': beneficiary_data['gender_summary']['paid']['men'] + 
                                          beneficiary_data['gender_summary']['paid']['women'] + 
                                          beneficiary_data['gender_summary']['paid']['twa'],
                'female_percentage': round(
                    (beneficiary_data['gender_summary']['planned']['women'] / 
                     (beneficiary_data['gender_summary']['planned']['men'] + 
                      beneficiary_data['gender_summary']['planned']['women'] + 
                      beneficiary_data['gender_summary']['planned']['twa']) * 100)
                    if (beneficiary_data['gender_summary']['planned']['men'] + 
                        beneficiary_data['gender_summary']['planned']['women'] + 
                        beneficiary_data['gender_summary']['planned']['twa']) > 0 else 0, 2
                ),
                'twa_inclusion_rate': twa_data['inclusion_rate'],
                'host_community_percentage': refugee_host_data['comparison']['host_percentage'],
            },
            'quarterly_trends': quarterly_data,
            'gender_breakdown': beneficiary_data['gender_summary'],
            'community_breakdown': refugee_host_data,
            'twa_metrics': twa_data['summary']
        }
        
        return Response({
            'success': True,
            'data': summary
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def location_performance_api(request):
    """API endpoint for location-based performance metrics"""
    try:
        # Get query parameters
        location_type = request.GET.get('type', 'commune')  # commune, province
        
        from location.models import Location
        from django.db.models import Sum, Count, F
        from .models import MonetaryTransfer
        
        # Get locations based on type
        if location_type == 'province':
            locations = Location.objects.filter(type='D', validity_to__isnull=True)
        else:  # commune
            locations = Location.objects.filter(type='W', validity_to__isnull=True)
        
        performance_data = []
        
        for location in locations:
            # Get transfers for this location
            if location_type == 'province':
                transfers = MonetaryTransfer.objects.filter(location__parent__parent=location)
            else:  # commune
                transfers = MonetaryTransfer.objects.filter(location__parent=location)
            
            # Calculate metrics
            metrics = transfers.aggregate(
                total_planned=Sum(F('planned_men') + F('planned_women')),
                total_paid=Sum(F('paid_men') + F('paid_women')),
                transfer_count=Count('id')
            )
            
            # Calculate payment rate
            payment_rate = 0
            if metrics['total_planned'] and metrics['total_planned'] > 0:
                payment_rate = round((metrics['total_paid'] or 0) / metrics['total_planned'] * 100, 2)
            
            # Determine community type for communes
            community_type = 'HOST' if location.name in MEDashboardService.HOST_COMMUNES else 'REFUGEE'
            
            performance_data.append({
                'location_id': location.id,
                'location_name': location.name,
                'location_type': location_type,
                'community_type': community_type if location_type == 'commune' else 'MIXED',
                'total_planned': metrics['total_planned'] or 0,
                'total_paid': metrics['total_paid'] or 0,
                'payment_rate': payment_rate,
                'transfer_count': metrics['transfer_count'] or 0
            })
        
        # Sort by payment rate descending
        performance_data.sort(key=lambda x: x['payment_rate'], reverse=True)
        
        return Response({
            'success': True,
            'data': performance_data
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def activity_summary_api(request):
    """API endpoint for activity summary across all program components"""
    try:
        # Get query parameters
        start_date = request.GET.get('start_date')
        end_date = request.GET.get('end_date')
        
        # Convert dates if provided
        filters = {}
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        from django.db.models import Sum, Count, F
        from .models import SensitizationTraining, BehaviorChangePromotion, MicroProject, MonetaryTransfer
        
        # Monetary transfers summary
        transfer_filters = {}
        if start_date:
            transfer_filters['transfer_date__gte'] = start_date
        if end_date:
            transfer_filters['transfer_date__lte'] = end_date
            
        transfers_summary = MonetaryTransfer.objects.filter(**transfer_filters).aggregate(
            total_transfers=Count('id'),
            total_planned=Sum(F('planned_men') + F('planned_women')),
            total_paid=Sum(F('paid_men') + F('paid_women')),
            women_planned=Sum('planned_women'),
            women_paid=Sum('paid_women'),
            twa_planned=Sum('planned_twa'),
            twa_paid=Sum('paid_twa')
        )
        
        # Training summary
        training_filters = {}
        if start_date:
            training_filters['sensitization_date__gte'] = start_date
        if end_date:
            training_filters['sensitization_date__lte'] = end_date
            
        training_summary = SensitizationTraining.objects.filter(**training_filters).aggregate(
            total_sessions=Count('id'),
            total_participants=Sum(F('male_participants') + F('female_participants') + F('twa_participants')),
            male_participants=Sum('male_participants'),
            female_participants=Sum('female_participants'),
            twa_participants=Sum('twa_participants')
        )
        
        # Behavior change summary
        behavior_filters = {}
        if start_date:
            behavior_filters['report_date__gte'] = start_date
        if end_date:
            behavior_filters['report_date__lte'] = end_date
            
        behavior_summary = BehaviorChangePromotion.objects.filter(**behavior_filters).aggregate(
            total_activities=Count('id'),
            total_participants=Sum(F('male_participants') + F('female_participants') + F('twa_participants')),
            male_participants=Sum('male_participants'),
            female_participants=Sum('female_participants'),
            twa_participants=Sum('twa_participants')
        )
        
        # Micro-projects summary
        project_filters = {}
        if start_date:
            project_filters['report_date__gte'] = start_date
        if end_date:
            project_filters['report_date__lte'] = end_date
            
        project_summary = MicroProject.objects.filter(**project_filters).aggregate(
            total_projects=Count('id'),
            total_beneficiaries=Sum(
                F('agriculture_beneficiaries') + F('livestock_beneficiaries') + 
                F('commerce_services_beneficiaries')
            ),
            total_participants=Sum(F('male_participants') + F('female_participants') + F('twa_participants')),
            male_participants=Sum('male_participants'),
            female_participants=Sum('female_participants'),
            twa_participants=Sum('twa_participants')
        )
        
        summary = {
            'monetary_transfers': transfers_summary,
            'training': training_summary,
            'behavior_change': behavior_summary,
            'micro_projects': project_summary,
            'overall_totals': {
                'total_direct_beneficiaries': (
                    (transfers_summary['total_paid'] or 0) + 
                    (training_summary['total_participants'] or 0) + 
                    (behavior_summary['total_participants'] or 0) + 
                    (project_summary['total_beneficiaries'] or 0)
                ),
                'total_women': (
                    (transfers_summary['women_paid'] or 0) + 
                    (training_summary['female_participants'] or 0) + 
                    (behavior_summary['female_participants'] or 0) + 
                    (project_summary['female_participants'] or 0)
                ),
                'total_twa': (
                    (transfers_summary['twa_paid'] or 0) + 
                    (training_summary['twa_participants'] or 0) + 
                    (behavior_summary['twa_participants'] or 0) + 
                    (project_summary['twa_participants'] or 0)
                )
            }
        }
        
        return Response({
            'success': True,
            'data': summary
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)