import os
import json
from datetime import datetime, date
from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.db import transaction

from merankabandi.models import (
    Section, Indicator, IndicatorAchievement,
    ResultFrameworkSnapshot, IndicatorCalculationRule
)
from merankabandi.result_framework_service import ResultFrameworkService

User = get_user_model()


class Command(BaseCommand):
    help = 'Test result framework automation - calculate indicators, create snapshot, generate document'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--action',
            type=str,
            choices=['calculate', 'snapshot', 'document', 'setup', 'all'],
            default='all',
            help='Action to perform'
        )
        parser.add_argument(
            '--indicator',
            type=int,
            help='Specific indicator ID to calculate'
        )
        parser.add_argument(
            '--date-from',
            type=str,
            help='Start date (YYYY-MM-DD)'
        )
        parser.add_argument(
            '--date-to',
            type=str,
            help='End date (YYYY-MM-DD)'
        )
        parser.add_argument(
            '--snapshot-id',
            type=str,
            help='Snapshot ID for document generation'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='./result_framework_test.docx',
            help='Output file path for document'
        )
    
    def handle(self, *args, **options):
        action = options['action']
        
        if action in ['setup', 'all']:
            self.setup_test_data()
        
        if action in ['calculate', 'all']:
            self.test_calculations(options)
        
        if action in ['snapshot', 'all']:
            snapshot_id = self.create_snapshot(options)
            if action == 'all':
                options['snapshot_id'] = str(snapshot_id)
        
        if action in ['document', 'all']:
            self.generate_document(options)
    
    def setup_test_data(self):
        """Load fixtures and setup calculation rules"""
        self.stdout.write("Setting up test data...")
        
        # Load indicators fixture if not already loaded
        if not Indicator.objects.exists():
            fixture_path = 'merankabandi/fixtures/development_intermediate_indicators_simple.json'
            if os.path.exists(fixture_path):
                os.system(f'python manage.py loaddata {fixture_path}')
                self.stdout.write(self.style.SUCCESS("✓ Loaded indicators fixture"))
        
        # Load calculation rules
        rules_fixture = 'merankabandi/fixtures/indicator_calculation_rules.json'
        if os.path.exists(rules_fixture):
            os.system(f'python manage.py loaddata {rules_fixture}')
            self.stdout.write(self.style.SUCCESS("✓ Loaded calculation rules"))
        
        # Add some manual achievements for testing
        self.add_test_achievements()
    
    def add_test_achievements(self):
        """Add some test achievement data"""
        # Add manual achievement for indicator 4 (percentage indicator)
        indicator_4 = Indicator.objects.filter(id=4).first()
        if indicator_4:
            IndicatorAchievement.objects.get_or_create(
                indicator=indicator_4,
                achieved=75.5,
                date=date.today(),
                defaults={'comment': 'Test manual entry'}
            )
        
        # Add manual achievement for indicator 17 (payment timeliness)
        indicator_17 = Indicator.objects.filter(id=17).first()
        if indicator_17:
            IndicatorAchievement.objects.get_or_create(
                indicator=indicator_17,
                achieved=85.0,
                date=date.today(),
                defaults={'comment': 'Test payment timeliness'}
            )
        
        self.stdout.write(self.style.SUCCESS("✓ Added test achievements"))
    
    def test_calculations(self, options):
        """Test indicator calculations"""
        self.stdout.write("\nTesting indicator calculations...")
        
        service = ResultFrameworkService()
        
        date_from = None
        date_to = None
        if options['date_from']:
            date_from = datetime.strptime(options['date_from'], '%Y-%m-%d').date()
        if options['date_to']:
            date_to = datetime.strptime(options['date_to'], '%Y-%m-%d').date()
        
        if options['indicator']:
            # Calculate specific indicator
            indicators = [Indicator.objects.get(id=options['indicator'])]
        else:
            # Calculate a sample of indicators
            indicators = Indicator.objects.filter(id__in=[1, 2, 3, 5, 6, 7, 11, 16, 17])
        
        for indicator in indicators:
            self.stdout.write(f"\n{indicator.name}:")
            
            result = service.calculate_indicator_value(
                indicator.id,
                date_from=date_from,
                date_to=date_to
            )
            
            if 'error' in result:
                self.stdout.write(self.style.ERROR(f"  Error: {result['error']}"))
            else:
                self.stdout.write(f"  Target: {indicator.target}")
                self.stdout.write(f"  Achieved: {result['value']}")
                self.stdout.write(f"  Type: {result.get('calculation_type', 'UNKNOWN')}")
                
                if indicator.target > 0:
                    percentage = (result['value'] / float(indicator.target)) * 100
                    self.stdout.write(f"  Progress: {percentage:.1f}%")
                
                if 'gender_breakdown' in result:
                    self.stdout.write(f"  Gender breakdown: {result['gender_breakdown']}")
    
    def create_snapshot(self, options):
        """Create a result framework snapshot"""
        self.stdout.write("\nCreating snapshot...")
        
        service = ResultFrameworkService()
        
        # Get or create a test user
        user = User.objects.filter(username='Admin').first()
        if not user:
            user = User.objects.first()
        
        date_from = None
        date_to = None
        if options['date_from']:
            date_from = datetime.strptime(options['date_from'], '%Y-%m-%d').date()
        if options['date_to']:
            date_to = datetime.strptime(options['date_to'], '%Y-%m-%d').date()
        
        snapshot = service.create_snapshot(
            name=f"Test Snapshot - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            description="Test snapshot created by management command",
            user=user,
            date_from=date_from,
            date_to=date_to
        )
        
        self.stdout.write(self.style.SUCCESS(f"✓ Created snapshot: {snapshot.id}"))
        
        # Display summary
        total_indicators = sum(len(s['indicators']) for s in snapshot.data['sections'])
        self.stdout.write(f"\nSnapshot summary:")
        self.stdout.write(f"  Total sections: {len(snapshot.data['sections'])}")
        self.stdout.write(f"  Total indicators: {total_indicators}")
        
        return snapshot.id
    
    def generate_document(self, options):
        """Generate result framework document"""
        self.stdout.write("\nGenerating document...")
        
        service = ResultFrameworkService()
        
        snapshot_id = options.get('snapshot_id')
        
        try:
            document = service.generate_document(snapshot_id=snapshot_id, format='docx')
            
            # Save document
            output_path = options['output']
            document.save(output_path)
            
            self.stdout.write(self.style.SUCCESS(f"✓ Document saved to: {output_path}"))
            
            # Display file info
            file_size = os.path.getsize(output_path)
            self.stdout.write(f"  File size: {file_size:,} bytes")
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error generating document: {str(e)}"))
    
    def display_snapshot_data(self, snapshot_data):
        """Display snapshot data in a readable format"""
        for section in snapshot_data['sections']:
            self.stdout.write(f"\n{section['name']}:")
            
            for indicator in section['indicators']:
                achieved = indicator['achieved']
                target = indicator['target']
                percentage = indicator['percentage']
                
                status = "✓" if percentage >= 80 else "○" if percentage >= 50 else "✗"
                
                self.stdout.write(
                    f"  {status} {indicator['name']}: "
                    f"{achieved:,.0f}/{target:,.0f} ({percentage:.1f}%)"
                )