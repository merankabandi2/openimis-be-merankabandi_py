"""
M&E Reporting Services for Merankabandi Project
Provides dashboard data, Excel exports, and automated aggregation capabilities
"""

import pandas as pd
from datetime import datetime, date
from django.db.models import Sum, Count, Q, F, Case, When, Value, CharField
from django.db.models.functions import Extract, Coalesce
from django.http import HttpResponse
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

from .models import (
    SensitizationTraining, BehaviorChangePromotion, MicroProject, 
    MonetaryTransfer, Section, Indicator, IndicatorAchievement
)
from social_protection.models import GroupBeneficiary, BeneficiaryStatus
from location.models import Location


class MEDashboardService:
    """Service for M&E Dashboard data aggregation"""
    
    # Host community communes as specified
    HOST_COMMUNES = ['Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo']
    
    @classmethod
    def get_beneficiary_breakdown_data(cls, start_date=None, end_date=None, location_id=None):
        """Get beneficiary data broken down by gender, community type, location"""
        
        # Base queryset for monetary transfers
        transfers = MonetaryTransfer.objects.all()
        
        if start_date:
            transfers = transfers.filter(transfer_date__gte=start_date)
        if end_date:
            transfers = transfers.filter(transfer_date__lte=end_date)
        if location_id:
            # Filter by province, commune, or colline
            location = Location.objects.get(id=location_id)
            if location.type == 'D':  # Province
                transfers = transfers.filter(location__parent__parent=location)
            elif location.type == 'W':  # Commune
                transfers = transfers.filter(location__parent=location)
            elif location.type == 'V':  # Colline
                transfers = transfers.filter(location=location)
        
        # Aggregate data with community type annotation
        breakdown = transfers.annotate(
            community_type=Case(
                When(location__parent__name__in=cls.HOST_COMMUNES, then=Value('HOST')),
                default=Value('REFUGEE'),
                output_field=CharField()
            )
        ).aggregate(
            total_planned_men=Sum('planned_men'),
            total_planned_women=Sum('planned_women'),
            total_planned_twa=Sum('planned_twa'),
            total_paid_men=Sum('paid_men'),
            total_paid_women=Sum('paid_women'),
            total_paid_twa=Sum('paid_twa'),
        )
        
        # Get breakdown by community type
        community_breakdown = transfers.values('location__parent__name').annotate(
            community_type=Case(
                When(location__parent__name__in=cls.HOST_COMMUNES, then=Value('HOST')),
                default=Value('REFUGEE'),
                output_field=CharField()
            ),
            planned_men=Sum('planned_men'),
            planned_women=Sum('planned_women'),
            planned_twa=Sum('planned_twa'),
            paid_men=Sum('paid_men'),
            paid_women=Sum('paid_women'),
            paid_twa=Sum('paid_twa'),
        ).order_by('community_type', 'location__parent__name')
        
        return {
            'overall_breakdown': breakdown,
            'community_breakdown': list(community_breakdown),
            'gender_summary': {
                'planned': {
                    'men': breakdown['total_planned_men'] or 0,
                    'women': breakdown['total_planned_women'] or 0,
                    'twa': breakdown['total_planned_twa'] or 0,
                },
                'paid': {
                    'men': breakdown['total_paid_men'] or 0,
                    'women': breakdown['total_paid_women'] or 0,
                    'twa': breakdown['total_paid_twa'] or 0,
                }
            }
        }
    
    @classmethod
    def get_refugee_host_breakdown(cls, start_date=None, end_date=None):
        """Get separate breakdown for refugee vs host community data"""
        
        # Base queryset
        transfers = MonetaryTransfer.objects.all()
        
        if start_date:
            transfers = transfers.filter(transfer_date__gte=start_date)
        if end_date:
            transfers = transfers.filter(transfer_date__lte=end_date)
        
        # Host community data
        host_data = transfers.filter(
            location__parent__name__in=cls.HOST_COMMUNES
        ).aggregate(
            planned_beneficiaries=Sum(F('planned_men') + F('planned_women')),
            paid_beneficiaries=Sum(F('paid_men') + F('paid_women')),
            planned_men=Sum('planned_men'),
            planned_women=Sum('planned_women'),
            planned_twa=Sum('planned_twa'),
            paid_men=Sum('paid_men'),
            paid_women=Sum('paid_women'),
            paid_twa=Sum('paid_twa'),
        )
        
        # Refugee community data
        refugee_data = transfers.exclude(
            location__parent__name__in=cls.HOST_COMMUNES
        ).aggregate(
            planned_beneficiaries=Sum(F('planned_men') + F('planned_women')),
            paid_beneficiaries=Sum(F('paid_men') + F('paid_women')),
            planned_men=Sum('planned_men'),
            planned_women=Sum('planned_women'),
            planned_twa=Sum('planned_twa'),
            paid_men=Sum('paid_men'),
            paid_women=Sum('paid_women'),
            paid_twa=Sum('paid_twa'),
        )
        
        return {
            'host_community': host_data,
            'refugee_community': refugee_data,
            'comparison': {
                'total_planned': (host_data['planned_beneficiaries'] or 0) + (refugee_data['planned_beneficiaries'] or 0),
                'total_paid': (host_data['paid_beneficiaries'] or 0) + (refugee_data['paid_beneficiaries'] or 0),
                'host_percentage': round(
                    ((host_data['planned_beneficiaries'] or 0) / 
                     ((host_data['planned_beneficiaries'] or 0) + (refugee_data['planned_beneficiaries'] or 0)) * 100)
                    if (host_data['planned_beneficiaries'] or 0) + (refugee_data['planned_beneficiaries'] or 0) > 0 else 0, 
                    2
                )
            }
        }
    
    @classmethod
    def get_quarterly_rollup_data(cls, year, quarter=None):
        """Aggregate data by quarters/annually"""
        
        # Define quarter date ranges
        quarter_ranges = {
            1: (f'{year}-01-01', f'{year}-03-31'),
            2: (f'{year}-04-01', f'{year}-06-30'), 
            3: (f'{year}-07-01', f'{year}-09-30'),
            4: (f'{year}-10-01', f'{year}-12-31')
        }
        
        if quarter:
            quarters = [quarter]
        else:
            quarters = [1, 2, 3, 4]
        
        quarterly_data = {}
        
        for q in quarters:
            start_date, end_date = quarter_ranges[q]
            
            # Monetary transfers for quarter
            transfers = MonetaryTransfer.objects.filter(
                transfer_date__range=[start_date, end_date]
            ).aggregate(
                total_planned=Sum(F('planned_men') + F('planned_women')),
                total_paid=Sum(F('paid_men') + F('paid_women')),
                transfer_count=Count('id')
            )
            
            # Training activities for quarter
            training = SensitizationTraining.objects.filter(
                sensitization_date__range=[start_date, end_date]
            ).aggregate(
                total_participants=Sum(F('male_participants') + F('female_participants')),
                session_count=Count('id')
            )
            
            # Behavior change activities for quarter
            behavior_change = BehaviorChangePromotion.objects.filter(
                report_date__range=[start_date, end_date]
            ).aggregate(
                total_participants=Sum(F('male_participants') + F('female_participants')),
                activity_count=Count('id')
            )
            
            # Micro projects for quarter
            micro_projects = MicroProject.objects.filter(
                report_date__range=[start_date, end_date]
            ).aggregate(
                total_beneficiaries=Sum(
                    F('agriculture_beneficiaries') + F('livestock_beneficiaries') + 
                    F('commerce_services_beneficiaries')
                ),
                project_count=Count('id')
            )
            
            quarterly_data[f'Q{q}'] = {
                'transfers': transfers,
                'training': training,
                'behavior_change': behavior_change,
                'micro_projects': micro_projects
            }
        
        return quarterly_data
    
    @classmethod
    def get_twa_minority_metrics(cls, start_date=None, end_date=None):
        """Specific metrics for Twa minority tracking"""
        
        # Base filters
        filters = {}
        if start_date:
            filters['transfer_date__gte'] = start_date
        if end_date:
            filters['transfer_date__lte'] = end_date
        
        # Twa participation across all activities
        twa_transfers = MonetaryTransfer.objects.filter(**filters).aggregate(
            planned_twa=Sum('planned_twa'),
            paid_twa=Sum('paid_twa')
        )
        
        training_filters = {}
        if start_date:
            training_filters['sensitization_date__gte'] = start_date
        if end_date:
            training_filters['sensitization_date__lte'] = end_date
            
        twa_training = SensitizationTraining.objects.filter(**training_filters).aggregate(
            twa_participants=Sum('twa_participants')
        )
        # Count sessions with Twa participants separately
        twa_training['sessions_with_twa'] = SensitizationTraining.objects.filter(
            **training_filters, twa_participants__gt=0
        ).count()
        
        behavior_filters = {}
        if start_date:
            behavior_filters['report_date__gte'] = start_date
        if end_date:
            behavior_filters['report_date__lte'] = end_date
            
        twa_behavior = BehaviorChangePromotion.objects.filter(**behavior_filters).aggregate(
            twa_participants=Sum('twa_participants')
        )
        # Count activities with Twa participants separately
        twa_behavior['activities_with_twa'] = BehaviorChangePromotion.objects.filter(
            **behavior_filters, twa_participants__gt=0
        ).count()
        
        project_filters = {}
        if start_date:
            project_filters['report_date__gte'] = start_date
        if end_date:
            project_filters['report_date__lte'] = end_date
            
        twa_projects = MicroProject.objects.filter(**project_filters).aggregate(
            twa_participants=Sum('twa_participants')
        )
        # Count projects with Twa participants separately
        twa_projects['projects_with_twa'] = MicroProject.objects.filter(
            **project_filters, twa_participants__gt=0
        ).count()
        
        # Calculate inclusion rates
        total_transfers = MonetaryTransfer.objects.filter(**filters).aggregate(
            total_planned=Sum(F('planned_men') + F('planned_women'))
        )['total_planned'] or 0
        
        twa_inclusion_rate = 0
        if total_transfers > 0:
            twa_inclusion_rate = round((twa_transfers['planned_twa'] or 0) / total_transfers * 100, 2)
        
        return {
            'transfers': twa_transfers,
            'training': twa_training,
            'behavior_change': twa_behavior,
            'micro_projects': twa_projects,
            'inclusion_rate': twa_inclusion_rate,
            'summary': {
                'total_twa_in_transfers': twa_transfers['planned_twa'] or 0,
                'total_twa_in_training': twa_training['twa_participants'] or 0,
                'total_twa_in_behavior_change': twa_behavior['twa_participants'] or 0,
                'total_twa_in_projects': twa_projects['twa_participants'] or 0,
            }
        }


class ExcelExportService:
    """Service for Excel report generation matching current templates"""
    
    @classmethod
    def export_monetary_transfers_excel(cls, start_date=None, end_date=None, location_id=None):
        """Export monetary transfer data in Excel format matching TRANSFERTS MONETAIRES.xlsx"""
        
        # Get data
        transfers = MonetaryTransfer.objects.select_related('location', 'programme', 'payment_agency')
        
        if start_date:
            transfers = transfers.filter(transfer_date__gte=start_date)
        if end_date:
            transfers = transfers.filter(transfer_date__lte=end_date)
        if location_id:
            location = Location.objects.get(id=location_id)
            if location.type == 'D':  # Province
                transfers = transfers.filter(location__parent__parent=location)
            elif location.type == 'W':  # Commune
                transfers = transfers.filter(location__parent=location)
        
        # Prepare data for Excel
        data = []
        for transfer in transfers:
            quarter = f"T{((transfer.transfer_date.month-1)//3)+1}"
            
            # Safely get location name
            location_name = ''
            if transfer.location and transfer.location.parent:
                location_name = transfer.location.parent.name
            elif transfer.location:
                location_name = transfer.location.name
            
            # Determine transfer type based on programme and location
            transfer_type = "Transferts monétaires ordinaires"
            if location_name and location_name not in MEDashboardService.HOST_COMMUNES:
                transfer_type = "Transferts monétaires aux ménages Réfugiés"
            
            data.append({
                'Anne': transfer.transfer_date.year,
                'Periode': quarter,
                'Localisation': location_name,  # Commune
                'Types des tranferts': transfer_type,
                'Programme': transfer.programme.name if transfer.programme else '',
                'Agence de paiement': transfer.payment_agency.name if transfer.payment_agency else '',
                'Bénéficiaires prévus': transfer.total_planned,
                'Homme prévu': transfer.planned_men,
                'Femme prévue': transfer.planned_women,
                'Twa prévu': transfer.planned_twa,
                'Bénéficiaires payés': transfer.total_paid,
                'Homme payé': transfer.paid_men,
                'Femme payée': transfer.paid_women,
                'Twa payé': transfer.paid_twa,
                'Taux de paiement (%)': round(
                    (transfer.total_paid / transfer.total_planned * 100) 
                    if transfer.total_planned > 0 else 0, 2
                ),
            })
        
        # Create Excel file
        df = pd.DataFrame(data)
        return cls._create_excel_file(df, 'Transferts Monétaires', 'transferts_monetaires')
    
    @classmethod
    def export_accompanying_measures_excel(cls, start_date=None, end_date=None, location_id=None):
        """Export training/sensitization data in Excel format matching MESURES D ACCOMPAGNEMENTS.xlsx"""
        
        # Get training data
        training_data = []
        
        # Sensitization Training
        training = SensitizationTraining.objects.select_related('location')
        if start_date:
            training = training.filter(sensitization_date__gte=start_date)
        if end_date:
            training = training.filter(sensitization_date__lte=end_date)
        if location_id:
            location = Location.objects.get(id=location_id)
            if location.type == 'D':  # Province
                training = training.filter(location__parent__parent=location)
            elif location.type == 'W':  # Commune
                training = training.filter(location__parent=location)
        
        for item in training:
            quarter = f"T{((item.sensitization_date.month-1)//3)+1}"
            
            # Safely get location name
            location_name = ''
            if item.location and item.location.parent:
                location_name = item.location.parent.name
            elif item.location:
                location_name = item.location.name
            
            community_type = "Communautés d'accueil"
            if location_name and location_name in MEDashboardService.HOST_COMMUNES:
                community_type = "Communautés d'accueil"
            else:
                community_type = "Réfugiés"
            
            training_data.append({
                'Anne': item.sensitization_date.year,
                'Periode': quarter,
                'Localisation': location_name,
                'Types des transferts': f"Formation/Sensibilisation - {community_type}",
                'Activité': item.get_category_display() if item.category else 'Formation générale',
                'Animateur': item.facilitator or '',
                'Homme': item.male_participants,
                'Femme': item.female_participants,
                'Twa': item.twa_participants,
                'Total': item.total_participants,
                'Observations': item.observations or '',
            })
        
        # Behavior Change Promotion
        behavior_change = BehaviorChangePromotion.objects.select_related('location')
        if start_date:
            behavior_change = behavior_change.filter(report_date__gte=start_date)
        if end_date:
            behavior_change = behavior_change.filter(report_date__lte=end_date)
        if location_id:
            location = Location.objects.get(id=location_id)
            if location.type == 'D':  # Province
                behavior_change = behavior_change.filter(location__parent__parent=location)
            elif location.type == 'W':  # Commune
                behavior_change = behavior_change.filter(location__parent=location)
        
        for item in behavior_change:
            quarter = f"T{((item.report_date.month-1)//3)+1}"
            
            # Safely get location name
            location_name = ''
            if item.location and item.location.parent:
                location_name = item.location.parent.name
            elif item.location:
                location_name = item.location.name
            
            community_type = "Communautés d'accueil"
            if location_name and location_name in MEDashboardService.HOST_COMMUNES:
                community_type = "Communautés d'accueil"
            else:
                community_type = "Réfugiés"
            
            training_data.append({
                'Anne': item.report_date.year,
                'Periode': quarter,
                'Localisation': location_name,
                'Types des transferts': f"Promotion changement de comportement - {community_type}",
                'Activité': 'Promotion du changement de comportement',
                'Animateur': '',
                'Homme': item.male_participants,
                'Femme': item.female_participants,
                'Twa': item.twa_participants,
                'Total': item.total_beneficiaries,
                'Observations': item.comments or '',
            })
        
        # Create Excel file
        df = pd.DataFrame(training_data)
        return cls._create_excel_file(df, 'Mesures d\'Accompagnement', 'mesures_accompagnement')
    
    @classmethod
    def export_microprojects_excel(cls, start_date=None, end_date=None, location_id=None):
        """Export micro-project data in Excel format matching MICROPROJET.xlsx"""
        
        # Get micro-project data
        projects = MicroProject.objects.select_related('location')
        
        if start_date:
            projects = projects.filter(report_date__gte=start_date)
        if end_date:
            projects = projects.filter(report_date__lte=end_date)
        if location_id:
            location = Location.objects.get(id=location_id)
            if location.type == 'D':  # Province
                projects = projects.filter(location__parent__parent=location)
            elif location.type == 'W':  # Commune
                projects = projects.filter(location__parent=location)
        
        data = []
        for project in projects:
            quarter = f"T{((project.report_date.month-1)//3)+1}"
            
            # Add row for each project type with beneficiaries
            project_types = [
                ('Agriculture', project.agriculture_beneficiaries),
                ('Élevage général', project.livestock_beneficiaries),
                ('Chèvres', project.livestock_goat_beneficiaries),
                ('Porcins', project.livestock_pig_beneficiaries),
                ('Lapins', project.livestock_rabbit_beneficiaries),
                ('Volailles', project.livestock_poultry_beneficiaries),
                ('Bovins', project.livestock_cattle_beneficiaries),
                ('Commerce et services', project.commerce_services_beneficiaries),
            ]
            
            for project_type, beneficiary_count in project_types:
                if beneficiary_count > 0:
                    # Safely get location name
                    location_name = ''
                    if project.location and project.location.parent:
                        location_name = project.location.parent.name
                    elif project.location:
                        location_name = project.location.name
                    
                    data.append({
                        'Anne': project.report_date.year,
                        'Periode': quarter,
                        'Localisation': location_name,
                        'Type de micro-projets appuyés': project_type,
                        'Nombre de bénéficiaires': beneficiary_count,
                        'Participants Hommes': project.male_participants,
                        'Participants Femmes': project.female_participants,
                        'Participants Twa': project.twa_participants,
                        'Total participants': (project.male_participants + 
                                             project.female_participants + 
                                             project.twa_participants),
                    })
            
            # Add other project types
            for other_type in project.other_project_types.all():
                # Safely get location name
                location_name = ''
                if project.location and project.location.parent:
                    location_name = project.location.parent.name
                elif project.location:
                    location_name = project.location.name
                
                data.append({
                    'Anne': project.report_date.year,
                    'Periode': quarter,
                    'Localisation': location_name,
                    'Type de micro-projets appuyés': other_type.name,
                    'Nombre de bénéficiaires': other_type.beneficiary_count,
                    'Participants Hommes': project.male_participants,
                    'Participants Femmes': project.female_participants,
                    'Participants Twa': project.twa_participants,
                    'Total participants': (project.male_participants + 
                                         project.female_participants + 
                                         project.twa_participants),
                })
        
        # Create Excel file
        df = pd.DataFrame(data)
        return cls._create_excel_file(df, 'Micro-Projets', 'microprojects')
    
    @classmethod
    def _create_excel_file(cls, df, sheet_name, filename_prefix):
        """Create formatted Excel file from DataFrame"""
        
        # Create workbook and worksheet
        wb = Workbook()
        ws = wb.active
        ws.title = sheet_name
        
        # Add header styling
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center")
        
        # Add data to worksheet
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)
        
        # Style header row
        for cell in ws[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
        
        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
        
        # Save to BytesIO
        excel_file = BytesIO()
        wb.save(excel_file)
        excel_file.seek(0)
        
        # Create response
        response = HttpResponse(
            excel_file.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{filename_prefix}_{timestamp}.xlsx'
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        return response


class IndicatorAggregationService:
    """Service for automated indicator achievement aggregation"""
    
    @classmethod
    def auto_aggregate_household_registration(cls):
        """Auto-aggregate household registration numbers from GroupBeneficiary"""
        
        # Get total households registered in social registry
        total_households = GroupBeneficiary.objects.filter(
            status=BeneficiaryStatus.ACTIVE
        ).count()
        
        # Separate by community type
        host_households = GroupBeneficiary.objects.filter(
            status=BeneficiaryStatus.ACTIVE,
            group__location__parent__name__in=MEDashboardService.HOST_COMMUNES
        ).count()
        
        refugee_households = total_households - host_households
        
        # Update indicators
        try:
            # Total households indicator - try to find exact match first
            try:
                total_indicator = Indicator.objects.get(
                    name="Ménages des zones ciblées inscrits au Registre social national"
                )
            except Indicator.DoesNotExist:
                # If exact match not found, try with icontains but get the first one
                total_indicators = Indicator.objects.filter(
                    name__icontains="Ménages des zones ciblées inscrits au Registre social national"
                )
                if not total_indicators.exists():
                    raise Indicator.DoesNotExist("Total households indicator not found")
                total_indicator = total_indicators.first()
            
            IndicatorAchievement.objects.create(
                indicator=total_indicator,
                achieved=total_households,
                date=date.today(),
                comment="Auto-aggregated from GroupBeneficiary records"
            )
            
            # Host community households indicator
            try:
                host_indicator = Indicator.objects.get(
                    name="Ménages des zones ciblées inclus dans le registre social national - communautés d'accueil"
                )
            except Indicator.DoesNotExist:
                host_indicators = Indicator.objects.filter(
                    name__icontains="communautés d'accueil"
                ).filter(
                    name__icontains="Ménages"
                ).filter(
                    name__icontains="registre social"
                )
                if not host_indicators.exists():
                    raise Indicator.DoesNotExist("Host community households indicator not found")
                host_indicator = host_indicators.first()
            
            IndicatorAchievement.objects.create(
                indicator=host_indicator,
                achieved=host_households,
                date=date.today(),
                comment="Auto-aggregated from GroupBeneficiary records (host communities)"
            )
            
            # Refugee households indicator
            try:
                refugee_indicator = Indicator.objects.get(
                    name="Ménages des zones ciblées inclus dans le registre social national - réfugiés"
                )
            except Indicator.DoesNotExist:
                refugee_indicators = Indicator.objects.filter(
                    name__icontains="réfugiés"
                ).filter(
                    name__icontains="Ménages"
                ).filter(
                    name__icontains="registre social"
                )
                if not refugee_indicators.exists():
                    raise Indicator.DoesNotExist("Refugee households indicator not found")
                refugee_indicator = refugee_indicators.first()
            
            IndicatorAchievement.objects.create(
                indicator=refugee_indicator,
                achieved=refugee_households,
                date=date.today(),
                comment="Auto-aggregated from GroupBeneficiary records (refugees)"
            )
            
            return True, "Household registration indicators updated successfully"
            
        except Indicator.DoesNotExist as e:
            return False, f"Required indicator not found: {e}"
        except Exception as e:
            return False, f"Error updating indicators: {e}"
    
    @classmethod
    def auto_aggregate_transfer_beneficiaries(cls, year=None, quarter=None):
        """Auto-aggregate transfer beneficiary numbers"""
        
        if not year:
            year = date.today().year
        
        # Define date range
        if quarter:
            quarter_ranges = {
                1: (f'{year}-01-01', f'{year}-03-31'),
                2: (f'{year}-04-01', f'{year}-06-30'), 
                3: (f'{year}-07-01', f'{year}-09-30'),
                4: (f'{year}-10-01', f'{year}-12-31')
            }
            start_date, end_date = quarter_ranges[quarter]
            filters = {'transfer_date__range': [start_date, end_date]}
        else:
            filters = {'transfer_date__year': year}
        
        # Get transfer data
        transfers = MonetaryTransfer.objects.filter(**filters)
        
        # Calculate aggregates
        total_beneficiaries = transfers.aggregate(
            total=Sum(F('paid_men') + F('paid_women'))
        )['total'] or 0
        
        female_beneficiaries = transfers.aggregate(
            total=Sum('paid_women')
        )['total'] or 0
        
        refugee_beneficiaries = transfers.exclude(
            location__parent__name__in=MEDashboardService.HOST_COMMUNES
        ).aggregate(
            total=Sum(F('paid_men') + F('paid_women'))
        )['total'] or 0
        
        # Update indicators
        try:
            # Total beneficiaries
            try:
                total_indicator = Indicator.objects.get(
                    name="Bénéficiaires des programmes de protection sociale"
                )
            except Indicator.DoesNotExist:
                total_indicators = Indicator.objects.filter(
                    name__icontains="Bénéficiaires des programmes de protection sociale"
                ).exclude(
                    name__icontains="Femmes"
                ).exclude(
                    name__icontains="réfugiés"
                )
                if not total_indicators.exists():
                    raise Indicator.DoesNotExist("Total beneficiaries indicator not found")
                total_indicator = total_indicators.first()
            
            period_desc = f"Q{quarter} {year}" if quarter else str(year)
            IndicatorAchievement.objects.create(
                indicator=total_indicator,
                achieved=total_beneficiaries,
                date=date.today(),
                comment=f"Auto-aggregated from MonetaryTransfer records for {period_desc}"
            )
            
            # Female beneficiaries
            try:
                female_indicator = Indicator.objects.get(
                    name="Bénéficiaires des programmes de protection sociale - Femmes"
                )
            except Indicator.DoesNotExist:
                female_indicators = Indicator.objects.filter(
                    name__icontains="Bénéficiaires"
                ).filter(
                    name__icontains="Femmes"
                ).filter(
                    name__icontains="protection sociale"
                )
                if not female_indicators.exists():
                    raise Indicator.DoesNotExist("Female beneficiaries indicator not found")
                female_indicator = female_indicators.first()
            
            IndicatorAchievement.objects.create(
                indicator=female_indicator,
                achieved=female_beneficiaries,
                date=date.today(),
                comment=f"Auto-aggregated from MonetaryTransfer records for {period_desc}"
            )
            
            # Refugee beneficiaries
            try:
                refugee_indicator = Indicator.objects.get(
                    name="Bénéficiaires des programmes de filets de sécurité - réfugiés"
                )
            except Indicator.DoesNotExist:
                refugee_indicators = Indicator.objects.filter(
                    name__icontains="Bénéficiaires"
                ).filter(
                    name__icontains="réfugiés"
                ).filter(
                    Q(name__icontains="filets de sécurité") | Q(name__icontains="protection sociale")
                )
                if not refugee_indicators.exists():
                    raise Indicator.DoesNotExist("Refugee beneficiaries indicator not found")
                refugee_indicator = refugee_indicators.first()
            
            IndicatorAchievement.objects.create(
                indicator=refugee_indicator,
                achieved=refugee_beneficiaries,
                date=date.today(),
                comment=f"Auto-aggregated from MonetaryTransfer records for {period_desc}"
            )
            
            return True, f"Transfer beneficiary indicators updated for {period_desc}"
            
        except Indicator.DoesNotExist as e:
            return False, f"Required indicator not found: {e}"
        except Exception as e:
            return False, f"Error updating indicators: {e}"
    
    @classmethod
    def calculate_achievement_by_dimensions(cls, indicator_id, **dimensions):
        """Calculate achievements by location, gender, community type"""
        
        indicator = Indicator.objects.get(id=indicator_id)
        
        # Base queryset depending on indicator type
        if "transfert" in indicator.name.lower() or "bénéficiaire" in indicator.name.lower():
            queryset = MonetaryTransfer.objects.all()
            value_field = F('paid_men') + F('paid_women')
        elif "formation" in indicator.name.lower() or "sensibilisation" in indicator.name.lower():
            queryset = SensitizationTraining.objects.all()
            value_field = F('male_participants') + F('female_participants') + F('twa_participants')
        else:
            return None
        
        # Apply dimension filters
        if 'location_id' in dimensions:
            location = Location.objects.get(id=dimensions['location_id'])
            if location.type == 'D':  # Province
                queryset = queryset.filter(location__parent__parent=location)
            elif location.type == 'W':  # Commune
                queryset = queryset.filter(location__parent=location)
            elif location.type == 'V':  # Colline
                queryset = queryset.filter(location=location)
        
        if 'community_type' in dimensions:
            if dimensions['community_type'] == 'HOST':
                queryset = queryset.filter(location__parent__name__in=MEDashboardService.HOST_COMMUNES)
            elif dimensions['community_type'] == 'REFUGEE':
                queryset = queryset.exclude(location__parent__name__in=MEDashboardService.HOST_COMMUNES)
        
        if 'start_date' in dimensions:
            date_field = 'transfer_date' if hasattr(queryset.model, 'transfer_date') else 'sensitization_date'
            if hasattr(queryset.model, 'report_date'):
                date_field = 'report_date'
            queryset = queryset.filter(**{f'{date_field}__gte': dimensions['start_date']})
        
        if 'end_date' in dimensions:
            date_field = 'transfer_date' if hasattr(queryset.model, 'transfer_date') else 'sensitization_date'
            if hasattr(queryset.model, 'report_date'):
                date_field = 'report_date'
            queryset = queryset.filter(**{f'{date_field}__lte': dimensions['end_date']})
        
        # Calculate total
        result = queryset.aggregate(total=Sum(value_field))
        
        return result['total'] or 0