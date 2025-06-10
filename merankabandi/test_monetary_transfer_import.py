from django.test import TestCase
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth import get_user_model
from datetime import date
import uuid
import pandas as pd
from io import BytesIO
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

from location.models import Location, LocationType
from payroll.models import PaymentPoint
from social_protection.models import BenefitPlan
from merankabandi.monetary_transfer_import_service import MonetaryTransferImportService
from merankabandi.models import MonetaryTransfer

User = get_user_model()


class MonetaryTransferImportServiceTest(TestCase):
    """Test MonetaryTransferImportService functionality"""
    
    def setUp(self):
        """Set up test data"""
        # Create test user
        self.user = User.objects.create_user(
            username='test_importer',
            password='test123'
        )
        
        # Create location types
        self.province_type = LocationType.objects.create(code='D', name='Province')
        self.commune_type = LocationType.objects.create(code='W', name='Commune')  
        self.colline_type = LocationType.objects.create(code='V', name='Colline')
        
        # Create locations
        self.province = Location.objects.create(
            code='P001',
            name='Test Province',
            type='D'
        )
        
        self.commune = Location.objects.create(
            code='C001',
            name='Test Commune',
            type='W',
            parent=self.province
        )
        
        self.colline = Location.objects.create(
            code='V001',
            name='Test Colline',
            type='V',
            parent=self.commune
        )
        
        # Create benefit plan (programme)
        self.benefit_plan = BenefitPlan.objects.create(
            code='BP001',
            name='Test Programme',
            max_beneficiaries=1000
        )
        
        # Create payment point (agency)
        self.payment_point = PaymentPoint.objects.create(
            name='Test Agency',
            location=self.commune
        )
    
    def create_test_csv_content(self):
        """Create test CSV content with proper column names"""
        csv_content = """Date des transferts,Commune,Colline,Programme,Agence de paiement,Femmes prévues,Hommes prévus,Twa prévus,Femmes payées,Hommes payés,Twa payés,Montant prévu,Montant transféré
2024-01-01,Test Commune,Test Colline,Test Programme,Test Agency,100,80,20,95,78,19,10000000,9600000
2024-01-02,Test Commune,Test Colline,Test Programme,Test Agency,120,90,25,118,88,24,12000000,11800000"""
        return csv_content.encode('utf-8')
    
    def create_test_excel_content(self):
        """Create test Excel content"""
        df = pd.DataFrame({
            'Date des transferts': ['2024-01-01', '2024-01-02'],
            'Commune': ['Test Commune', 'Test Commune'],
            'Colline': ['Test Colline', 'Test Colline'],
            'Programme': ['Test Programme', 'Test Programme'],
            'Agence de paiement': ['Test Agency', 'Test Agency'],
            'Femmes prévues': [100, 120],
            'Hommes prévus': [80, 90],
            'Twa prévus': [20, 25],
            'Femmes payées': [95, 118],
            'Hommes payés': [78, 88],
            'Twa payés': [19, 24],
            'Montant prévu': [10000000, 12000000],
            'Montant transféré': [9600000, 11800000]
        })
        
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        return excel_buffer.getvalue()
    
    def test_validate_file_success(self):
        """Test successful CSV file validation"""
        # Create CSV file
        csv_file = SimpleUploadedFile(
            "transfers.csv",
            self.create_test_csv_content(),
            content_type="text/csv"
        )
        
        is_valid, error = MonetaryTransferImportService.validate_file(csv_file)
        
        self.assertTrue(is_valid)
        self.assertIsNone(error)
    
    def test_validate_file_invalid_format(self):
        """Test validation with invalid file format"""
        txt_file = SimpleUploadedFile(
            "invalid.txt",
            b"This is not a CSV or Excel file",
            content_type="text/plain"
        )
        
        is_valid, error = MonetaryTransferImportService.validate_file(txt_file)
        
        self.assertFalse(is_valid)
        self.assertIn('File type not supported', error)
    
    def test_validate_file_invalid_extension(self):
        """Test validation with invalid file extension"""
        txt_file = SimpleUploadedFile(
            "invalid.pdf",
            b"This is not a CSV or Excel file",
            content_type="text/csv"  # MIME type is OK but extension is wrong
        )
        
        is_valid, error = MonetaryTransferImportService.validate_file(txt_file)
        
        self.assertFalse(is_valid)
        self.assertIn('File extension not supported', error)
    
    def test_import_from_excel_success(self):
        """Test successful import from CSV"""
        # Create CSV file
        csv_file = SimpleUploadedFile(
            "transfers.csv",
            self.create_test_csv_content(),
            content_type="text/csv"
        )
        
        result = MonetaryTransferImportService.import_from_excel(csv_file, self.user)
        
        self.assertTrue(result['success'])
        self.assertEqual(result['imported'], 2)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(len(result['invalid_items']), 0)
        
        # Check that transfers were created
        transfers = MonetaryTransfer.objects.all()
        self.assertEqual(transfers.count(), 2)
        
        # Check first transfer details
        transfer1 = transfers.first()
        self.assertEqual(transfer1.location, self.colline)
        self.assertEqual(transfer1.programme, self.benefit_plan)
        self.assertEqual(transfer1.payment_agency, self.payment_point)
        self.assertEqual(transfer1.planned_women, 100)
        self.assertEqual(transfer1.paid_women, 95)
    
    def test_import_from_excel_missing_columns(self):
        """Test import with missing required columns"""
        # CSV content missing required columns
        csv_content = """Date,Commune,Programme
01/01/2024,Test Commune,Test Programme"""
        
        csv_file = SimpleUploadedFile(
            "invalid.csv",
            csv_content.encode('utf-8'),
            content_type="text/csv"
        )
        
        result = MonetaryTransferImportService.import_from_excel(csv_file, self.user)
        
        self.assertFalse(result['success'])
        self.assertIn('Missing required columns', result['error'])
        self.assertEqual(result['imported'], 0)
    
    def test_import_from_excel_location_not_found(self):
        """Test import when location is not found"""
        # CSV with non-existent location
        csv_content = """Date des transferts,Commune,Colline,Programme,Agence de paiement,Femmes prévues,Hommes prévus,Twa prévus,Femmes payées,Hommes payés,Twa payés,Montant prévu,Montant transféré
2024-01-01,Non Existent Commune,Non Existent Colline,Test Programme,Test Agency,100,80,20,95,78,19,10000000,9600000"""
        
        csv_file = SimpleUploadedFile(
            "transfers.csv",
            csv_content.encode('utf-8'),
            content_type="text/csv"
        )
        
        result = MonetaryTransferImportService.import_from_excel(csv_file, self.user)
        
        self.assertTrue(result['success'])  # Partial success
        self.assertEqual(result['imported'], 0)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(len(result['invalid_items']), 1)
        self.assertIn('Location not found', result['invalid_items'][0]['error'])
    
    def test_import_from_excel_validation_error(self):
        """Test import with validation error (paid > planned)"""
        # CSV where paid women exceed planned women
        csv_content = """Date des transferts,Commune,Colline,Programme,Agence de paiement,Femmes prévues,Hommes prévus,Twa prévus,Femmes payées,Hommes payés,Twa payés,Montant prévu,Montant transféré
2024-01-01,Test Commune,Test Colline,Test Programme,Test Agency,100,80,20,105,78,19,10000000,9600000"""
        
        csv_file = SimpleUploadedFile(
            "transfers.csv",
            csv_content.encode('utf-8'),
            content_type="text/csv"
        )
        
        result = MonetaryTransferImportService.import_from_excel(csv_file, self.user)
        
        self.assertTrue(result['success'])  # Partial success
        self.assertEqual(result['imported'], 0)
        self.assertEqual(result['failed'], 1)
        self.assertEqual(len(result['invalid_items']), 1)
        self.assertIn('Paid women cannot exceed planned women', result['invalid_items'][0]['error'])
    
    def test_export_to_excel(self):
        """Test export to Excel functionality"""
        # Create some test transfers
        MonetaryTransfer.objects.create(
            id=uuid.uuid4(),
            transfer_date=date(2024, 1, 1),
            location=self.colline,
            programme=self.benefit_plan,
            payment_agency=self.payment_point,
            planned_women=100,
            planned_men=80,
            planned_twa=20,
            paid_women=95,
            paid_men=78,
            paid_twa=19,
            planned_amount=10000000,
            transferred_amount=9600000
        )
        
        # Test export
        response = MonetaryTransferImportService.export_to_excel()
        
        self.assertEqual(
            response['Content-Type'],
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        self.assertIn('attachment; filename=', response['Content-Disposition'])
        self.assertIn('transferts_monetaires_', response['Content-Disposition'])
    
    def test_get_import_template(self):
        """Test template generation"""
        response = MonetaryTransferImportService.get_import_template()
        
        self.assertEqual(
            response['Content-Type'],
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        self.assertEqual(
            response['Content-Disposition'],
            'attachment; filename="template_transferts_monetaires.xlsx"'
        )