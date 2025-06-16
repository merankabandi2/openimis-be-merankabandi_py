"""
Django management command to sync materialized views data to OpenSearch for self-service analytics
"""

from django.core.management.base import BaseCommand
from django.conf import settings
from merankabandi.documents import MaterializedViewSyncService
import time


class Command(BaseCommand):
    help = 'Sync materialized views data to OpenSearch for self-service analytics'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--view',
            type=str,
            help='Specific materialized view to sync (if not provided, syncs all views)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force sync even if OpenSearch is not configured'
        )
    
    def handle(self, *args, **options):
        if not self.check_opensearch_availability(options.get('force', False)):
            return
        
        self.stdout.write("Starting OpenSearch sync for dashboard analytics...")
        start_time = time.time()
        
        try:
            if options.get('view'):
                # Sync specific view
                view_name = options['view']
                self.sync_specific_view(view_name)
            else:
                # Sync all views
                self.sync_all_views()
            
            elapsed = time.time() - start_time
            self.stdout.write(
                self.style.SUCCESS(
                    f"✓ Successfully completed OpenSearch sync in {elapsed:.2f} seconds"
                )
            )
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error during OpenSearch sync: {e}")
            )
            raise
    
    def check_opensearch_availability(self, force=False):
        """Check if OpenSearch is properly configured"""
        try:
            # Check if opensearch_reports module is available
            import opensearch_reports
            
            # Check if OpenSearch settings are configured
            opensearch_hosts = getattr(settings, 'OPENSEARCH_HOSTS', None)
            if not opensearch_hosts and not force:
                self.stdout.write(
                    self.style.WARNING(
                        "⚠️ OpenSearch is not configured. Set OPENSEARCH_HOSTS in settings or use --force flag."
                    )
                )
                return False
            
            return True
            
        except ImportError:
            self.stdout.write(
                self.style.ERROR(
                    "✗ opensearch_reports module not available. Please add it to openimis.json."
                )
            )
            return False
    
    def sync_specific_view(self, view_name):
        """Sync a specific materialized view to OpenSearch"""
        self.stdout.write(f"Syncing materialized view: {view_name}")
        
        view_mappings = {
            'dashboard_beneficiary_summary': 'DashboardBeneficiaryDocument',
            'payment_reporting_unified_summary': 'DashboardUnifiedPaymentDocument',
            'payment_reporting_unified_quarterly': 'DashboardUnifiedPaymentDocument',
            'payment_reporting_unified_by_location': 'DashboardUnifiedPaymentDocument',
            'dashboard_grievances': 'DashboardGrievanceDocument',
            'dashboard_grievance_category_summary': 'DashboardGrievanceDocument',
            'dashboard_grievance_channel_summary': 'DashboardGrievanceDocument',
            'dashboard_activities_summary': 'DashboardActivitiesDocument',
            'dashboard_microprojects': 'DashboardActivitiesDocument',
            'dashboard_results_framework': 'DashboardIndicatorsDocument',
            'dashboard_indicators_by_section': 'DashboardIndicatorsDocument',
            'dashboard_master_summary': 'DashboardMasterSummaryDocument',
        }
        
        if view_name not in view_mappings:
            available_views = ', '.join(view_mappings.keys())
            self.stdout.write(
                self.style.ERROR(
                    f"✗ Unknown view: {view_name}. Available views: {available_views}"
                )
            )
            return
        
        # Import and get the document class
        from merankabandi.documents import (
            DashboardBeneficiaryDocument,
            DashboardUnifiedPaymentDocument,
            DashboardGrievanceDocument,
            DashboardActivitiesDocument,
            DashboardIndicatorsDocument,
            DashboardMasterSummaryDocument
        )
        
        document_classes = {
            'DashboardBeneficiaryDocument': DashboardBeneficiaryDocument,
            'DashboardUnifiedPaymentDocument': DashboardUnifiedPaymentDocument,
            'DashboardGrievanceDocument': DashboardGrievanceDocument,
            'DashboardActivitiesDocument': DashboardActivitiesDocument,
            'DashboardIndicatorsDocument': DashboardIndicatorsDocument,
            'DashboardMasterSummaryDocument': DashboardMasterSummaryDocument,
        }
        
        document_class = document_classes[view_mappings[view_name]]
        
        try:
            count = MaterializedViewSyncService.sync_materialized_view_to_opensearch(
                view_name, document_class
            )
            self.stdout.write(f"✓ Synced {count} records from {view_name}")
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Failed to sync {view_name}: {e}")
            )
            raise
    
    def sync_all_views(self):
        """Sync all dashboard materialized views to OpenSearch"""
        self.stdout.write("Syncing all dashboard materialized views...")
        
        try:
            results = MaterializedViewSyncService.sync_all_dashboard_views()
            
            # Display results
            self.stdout.write("\nSync Results:")
            self.stdout.write("=" * 80)
            
            total_records = 0
            success_count = 0
            
            for view_name, result in results.items():
                if isinstance(result, int):
                    total_records += result
                    success_count += 1
                    self.stdout.write(f"✓ {view_name:<35} : {result:>8} records")
                else:
                    self.stdout.write(
                        self.style.ERROR(f"✗ {view_name:<35} : {result}")
                    )
            
            self.stdout.write("=" * 80)
            self.stdout.write(f"Successfully synced: {success_count} views")
            self.stdout.write(f"Total records indexed: {total_records}")
            
            # Update OpenSearch dashboard configurations
            self.create_opensearch_dashboards()
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"✗ Error during bulk sync: {e}")
            )
            raise
    
    def create_opensearch_dashboards(self):
        """Create or update OpenSearch dashboard configurations for analytics"""
        try:
            from opensearch_reports.models import OpenSearchDashboard
            
            # Analytics dashboard configurations
            dashboard_configs = [
                {
                    'name': 'Dashboard Analytics - Beneficiaires',
                    'url_slug': 'analytics-beneficiaires',
                    'description': 'Exploration des données des bénéficiaires avec filtrage interactif',
                    'index_pattern': 'dashboard_beneficiaries',
                    'synch_disabled': False,
                },
                {
                    'name': 'Dashboard Analytics - Transferts Monétaires',
                    'url_slug': 'analytics-transferts',
                    'description': 'Analyse des performances des transferts monétaires par région et programme',
                    'index_pattern': 'dashboard_monetary_transfers',
                    'synch_disabled': False,
                },
                {
                    'name': 'Dashboard Analytics - Réclamations',
                    'url_slug': 'analytics-reclamations',
                    'description': 'Analyse des réclamations par catégorie, canal et statut de résolution',
                    'index_pattern': 'dashboard_grievances',
                    'synch_disabled': False,
                },
                {
                    'name': 'Dashboard Analytics - Activités',
                    'url_slug': 'analytics-activites',
                    'description': 'Suivi des activités et microprojets avec taux de réalisation',
                    'index_pattern': 'dashboard_activities',
                    'synch_disabled': False,
                },
                {
                    'name': 'Dashboard Analytics - Indicateurs',
                    'url_slug': 'analytics-indicateurs',
                    'description': 'Performance du cadre de résultats et indicateurs clés',
                    'index_pattern': 'dashboard_indicators',
                    'synch_disabled': False,
                },
                {
                    'name': 'Dashboard Analytics - Vue d\'Ensemble',
                    'url_slug': 'analytics-vue-ensemble',
                    'description': 'Tableau de bord principal avec KPIs consolidés',
                    'index_pattern': 'dashboard_master_summary',
                    'synch_disabled': False,
                },
                {
                    'name': 'Exploration des Données - Interface Libre',
                    'url_slug': 'exploration-donnees',
                    'description': 'Interface d\'exploration libre pour analyse personnalisée',
                    'index_pattern': 'dashboard_*',
                    'synch_disabled': False,
                },
            ]
            
            created_count = 0
            updated_count = 0
            
            for config in dashboard_configs:
                dashboard, created = OpenSearchDashboard.objects.update_or_create(
                    url_slug=config['url_slug'],
                    defaults={
                        'name': config['name'],
                        'description': config['description'],
                        'synch_disabled': config['synch_disabled'],
                        # Note: dashboard_url will be set by the OpenSearch service
                    }
                )
                
                if created:
                    created_count += 1
                else:
                    updated_count += 1
            
            self.stdout.write(f"✓ Dashboard configurations: {created_count} created, {updated_count} updated")
            
        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f"⚠️ Could not create dashboard configurations: {e}")
            )