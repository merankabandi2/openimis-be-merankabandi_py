"""
OpenSearch Documents for Materialized Views Self-Service Analytics
"""

from django_opensearch_dsl import Document, Index, fields
from django.db import connection
from opensearch_reports.models import OpenSearchDashboard
from opensearch_reports.services import BaseSyncDocument
import uuid
from datetime import datetime


# Index for dashboard analytics data
dashboard_analytics_index = Index('dashboard_analytics')
dashboard_analytics_index.settings(
    number_of_shards=1,
    number_of_replicas=0
)


class DashboardBeneficiaryDocument(BaseSyncDocument):
    """
    OpenSearch document for dashboard_beneficiary_summary materialized view
    """
    # Beneficiary identification
    individual_id = fields.KeywordField()
    first_name = fields.TextField(analyzer='standard')
    last_name = fields.TextField(analyzer='standard')
    other_names = fields.TextField(analyzer='standard')
    birth_date = fields.DateField()
    
    # Demographics
    gender = fields.KeywordField()
    is_batwa = fields.BooleanField()
    province = fields.KeywordField()
    commune = fields.KeywordField()
    zone = fields.KeywordField()
    colline = fields.KeywordField()
    
    # Program participation
    benefit_plan_name = fields.TextField(analyzer='standard')
    benefit_plan_code = fields.KeywordField()
    beneficiary_status = fields.KeywordField()
    date_created = fields.DateField()
    date_updated = fields.DateField()
    
    # Payment tracking
    total_payments = fields.IntegerField()
    last_payment_date = fields.DateField()
    
    # Aggregation fields
    report_date = fields.DateField()
    
    class Django:
        model = None  # Will be populated dynamically from materialized view
        
    class Index:
        name = 'dashboard_beneficiaries'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class DashboardMonetaryTransferDocument(BaseSyncDocument):
    """
    OpenSearch document for dashboard_monetary_transfers materialized view
    """
    # Transfer identification
    transfer_id = fields.KeywordField()
    transfer_type = fields.KeywordField()
    
    # Location
    province = fields.KeywordField()
    commune = fields.KeywordField()
    zone = fields.KeywordField()
    
    # Program details
    programme = fields.KeywordField()
    benefit_plan = fields.KeywordField()
    payment_agency = fields.KeywordField()
    
    # Transfer metrics
    planned_women = fields.IntegerField()
    planned_men = fields.IntegerField()
    planned_twa = fields.IntegerField()
    planned_total = fields.IntegerField()
    
    paid_women = fields.IntegerField()
    paid_men = fields.IntegerField()
    paid_twa = fields.IntegerField()
    paid_total = fields.IntegerField()
    
    # Performance metrics
    payment_rate = fields.FloatField()
    women_payment_rate = fields.FloatField()
    men_payment_rate = fields.FloatField()
    twa_payment_rate = fields.FloatField()
    
    # Dates
    transfer_date = fields.DateField()
    month = fields.IntegerField()
    year = fields.IntegerField()
    quarter = fields.KeywordField()
    
    # Aggregation fields
    report_date = fields.DateField()
    
    class Index:
        name = 'dashboard_monetary_transfers'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class DashboardGrievanceDocument(BaseSyncDocument):
    """
    OpenSearch document for grievance analytics
    """
    # Grievance identification
    ticket_id = fields.KeywordField()
    ticket_code = fields.KeywordField()
    title = fields.TextField(analyzer='standard')
    
    # Categories (supporting multi-value)
    category = fields.KeywordField()
    individual_category = fields.KeywordField()
    category_group = fields.KeywordField()  # cas_sensibles, cas_speciaux, cas_non_sensibles
    
    # Channels (supporting multi-value)
    channel = fields.KeywordField()
    individual_channel = fields.KeywordField()
    normalized_channel = fields.KeywordField()
    
    # Status and priority
    status = fields.KeywordField()
    priority = fields.KeywordField()
    flags = fields.KeywordField()
    
    # Reporter information
    reporter_name = fields.TextField(analyzer='standard')
    reporter_phone = fields.KeywordField()
    gender = fields.KeywordField()
    is_batwa = fields.BooleanField()
    is_beneficiary = fields.BooleanField()
    
    # Location
    province = fields.KeywordField()
    commune = fields.KeywordField()
    zone = fields.KeywordField()
    colline = fields.KeywordField()
    gps_location = fields.GeoPointField()
    
    # Dates
    date_of_incident = fields.DateField()
    date_created = fields.DateField()
    due_date = fields.DateField()
    
    # Resolution
    is_resolved = fields.BooleanField()
    resolution = fields.KeywordField()
    resolver_name = fields.TextField(analyzer='standard')
    
    # Aggregation fields
    report_date = fields.DateField()
    
    class Index:
        name = 'dashboard_grievances'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class DashboardActivitiesDocument(BaseSyncDocument):
    """
    OpenSearch document for activities and microprojects analytics
    """
    # Activity identification
    activity_id = fields.KeywordField()
    activity_type = fields.KeywordField()
    activity_name = fields.TextField(analyzer='standard')
    
    # Location
    province = fields.KeywordField()
    commune = fields.KeywordField()
    zone = fields.KeywordField()
    
    # Targets and achievements
    target_women = fields.IntegerField()
    target_men = fields.IntegerField()
    target_twa = fields.IntegerField()
    target_total = fields.IntegerField()
    
    achieved_women = fields.IntegerField()
    achieved_men = fields.IntegerField()
    achieved_twa = fields.IntegerField()
    achieved_total = fields.IntegerField()
    
    # Performance metrics
    achievement_rate = fields.FloatField()
    women_achievement_rate = fields.FloatField()
    men_achievement_rate = fields.FloatField()
    twa_achievement_rate = fields.FloatField()
    
    # Dates
    start_date = fields.DateField()
    end_date = fields.DateField()
    month = fields.IntegerField()
    year = fields.IntegerField()
    quarter = fields.KeywordField()
    
    # Project details
    project_type = fields.KeywordField()
    budget = fields.FloatField()
    status = fields.KeywordField()
    
    # Aggregation fields
    report_date = fields.DateField()
    
    class Index:
        name = 'dashboard_activities'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class DashboardIndicatorsDocument(BaseSyncDocument):
    """
    OpenSearch document for results framework indicators
    """
    # Indicator identification
    indicator_id = fields.KeywordField()
    indicator_code = fields.KeywordField()
    indicator_name = fields.TextField(analyzer='standard')
    
    # Section information
    section_id = fields.KeywordField()
    section_name = fields.TextField(analyzer='standard')
    
    # Targets and achievements
    target = fields.FloatField()
    achieved = fields.FloatField()
    achievement_rate = fields.FloatField()
    
    # Status
    status = fields.KeywordField()  # on_track, behind, ahead, at_risk
    
    # Dates
    target_date = fields.DateField()
    achievement_date = fields.DateField()
    reporting_period = fields.KeywordField()
    
    # Location
    province = fields.KeywordField()
    commune = fields.KeywordField()
    
    # Aggregation fields
    report_date = fields.DateField()
    year = fields.IntegerField()
    quarter = fields.KeywordField()
    
    class Index:
        name = 'dashboard_indicators'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class DashboardMasterSummaryDocument(BaseSyncDocument):
    """
    OpenSearch document for master dashboard summary with KPIs
    """
    # Summary identification
    summary_type = fields.KeywordField()
    location_level = fields.KeywordField()  # national, province, commune
    location_code = fields.KeywordField()
    location_name = fields.TextField(analyzer='standard')
    
    # Beneficiary KPIs
    total_beneficiaries = fields.IntegerField()
    active_beneficiaries = fields.IntegerField()
    women_beneficiaries = fields.IntegerField()
    men_beneficiaries = fields.IntegerField()
    twa_beneficiaries = fields.IntegerField()
    
    # Payment KPIs
    total_transfers = fields.IntegerField()
    successful_transfers = fields.IntegerField()
    transfer_success_rate = fields.FloatField()
    total_amount_transferred = fields.FloatField()
    
    # Grievance KPIs
    total_grievances = fields.IntegerField()
    resolved_grievances = fields.IntegerField()
    grievance_resolution_rate = fields.FloatField()
    avg_resolution_time = fields.FloatField()
    
    # Activity KPIs
    total_activities = fields.IntegerField()
    completed_activities = fields.IntegerField()
    activity_completion_rate = fields.FloatField()
    
    # Results Framework KPIs
    total_indicators = fields.IntegerField()
    on_track_indicators = fields.IntegerField()
    indicator_performance_rate = fields.FloatField()
    
    # Time dimensions
    report_date = fields.DateField()
    year = fields.IntegerField()
    quarter = fields.KeywordField()
    month = fields.IntegerField()
    
    class Index:
        name = 'dashboard_master_summary'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class MaterializedViewSyncService:
    """
    Service to sync materialized view data to OpenSearch
    """
    
    @staticmethod
    def sync_materialized_view_to_opensearch(view_name, document_class):
        """
        Sync data from a materialized view to OpenSearch
        """
        try:
            # Clear existing data for this view
            document_class._index.delete()
            document_class.init()
            
            # Get data from materialized view
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {view_name}")
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                documents = []
                for row in rows:
                    data = dict(zip(columns, row))
                    
                    # Add unique ID and metadata
                    data['id'] = str(uuid.uuid4())
                    data['sync_timestamp'] = datetime.now()
                    data['source_view'] = view_name
                    
                    # Create document instance
                    doc = document_class(**data)
                    documents.append(doc)
                
                # Bulk index to OpenSearch
                if documents:
                    document_class._index.bulk_create(documents)
                    
            return len(documents)
            
        except Exception as e:
            print(f"Error syncing {view_name} to OpenSearch: {e}")
            return 0
    
    @staticmethod
    def sync_all_dashboard_views():
        """
        Sync all dashboard materialized views to OpenSearch
        """
        view_mappings = {
            'dashboard_beneficiary_summary': DashboardBeneficiaryDocument,
            'dashboard_monetary_transfers': DashboardMonetaryTransferDocument,
            'dashboard_grievances': DashboardGrievanceDocument,
            'dashboard_grievance_category_summary': DashboardGrievanceDocument,
            'dashboard_grievance_channel_summary': DashboardGrievanceDocument,
            'dashboard_activities_summary': DashboardActivitiesDocument,
            'dashboard_microprojects': DashboardActivitiesDocument,
            'dashboard_results_framework': DashboardIndicatorsDocument,
            'dashboard_indicators_by_section': DashboardIndicatorsDocument,
            'dashboard_master_summary': DashboardMasterSummaryDocument,
        }
        
        results = {}
        for view_name, document_class in view_mappings.items():
            try:
                count = MaterializedViewSyncService.sync_materialized_view_to_opensearch(
                    view_name, document_class
                )
                results[view_name] = count
                print(f"✓ Synced {count} records from {view_name}")
            except Exception as e:
                results[view_name] = f"Error: {e}"
                print(f"✗ Failed to sync {view_name}: {e}")
        
        return results