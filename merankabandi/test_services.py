from django.test import TestCase
from unittest.mock import Mock, patch
from datetime import date
import uuid

from merankabandi.models import (
    SensitizationTraining, BehaviorChangePromotion, MicroProject,
    Section, Indicator, IndicatorAchievement
)
from merankabandi.services_validation import KoboDataValidationService


class KoboDataValidationServiceTest(TestCase):
    """Test KoboToolbox data validation service"""
    
    def setUp(self):
        """Set up test data"""
        self.user = Mock()
        self.user.id = 1
        self.user.username = 'test_validator'
    
    def test_validate_sensitization_training_success(self):
        """Test successful validation of sensitization training"""
        # Create a training record
        training = SensitizationTraining.objects.create(
            id=uuid.uuid4(),
            sensitization_date=date.today(),
            male_participants=10,
            female_participants=15,
            validation_status='PENDING'
        )
        
        # Validate the training
        with patch('django.utils.timezone.now') as mock_now:
            mock_now.return_value = date.today()
            success, validated_training, error = KoboDataValidationService.validate_sensitization_training(
                user=self.user,
                training_id=training.id,
                status='VALIDATED',
                comment='Data verified'
            )
        
        # Assert validation was successful
        self.assertTrue(success)
        self.assertIsNone(error)
        self.assertEqual(validated_training.validation_status, 'VALIDATED')
        self.assertEqual(validated_training.validated_by, self.user)
        self.assertEqual(validated_training.validation_comment, 'Data verified')
    
    def test_validate_sensitization_training_already_validated(self):
        """Test validation of already validated training"""
        # Create an already validated training
        training = SensitizationTraining.objects.create(
            id=uuid.uuid4(),
            sensitization_date=date.today(),
            male_participants=10,
            female_participants=15,
            validation_status='VALIDATED'
        )
        
        # Try to validate again
        success, validated_training, error = KoboDataValidationService.validate_sensitization_training(
            user=self.user,
            training_id=training.id,
            status='VALIDATED'
        )
        
        # Assert validation failed
        self.assertFalse(success)
        self.assertIn('already validated', error.lower())
    
    def test_validate_behavior_change_rejection(self):
        """Test rejection of behavior change promotion"""
        # Create a behavior change record
        behavior_change = BehaviorChangePromotion.objects.create(
            id=uuid.uuid4(),
            report_date=date.today(),
            male_participants=20,
            female_participants=25,
            validation_status='PENDING'
        )
        
        # Reject the record
        with patch('django.utils.timezone.now') as mock_now:
            mock_now.return_value = date.today()
            success, rejected_record, error = KoboDataValidationService.validate_behavior_change(
                user=self.user,
                behavior_change_id=behavior_change.id,
                status='REJECTED',
                comment='Participant numbers seem incorrect'
            )
        
        # Assert rejection was successful
        self.assertTrue(success)
        self.assertIsNone(error)
        self.assertEqual(rejected_record.validation_status, 'REJECTED')
        self.assertEqual(rejected_record.validation_comment, 'Participant numbers seem incorrect')
    
    def test_validate_microproject_not_found(self):
        """Test validation of non-existent microproject"""
        non_existent_id = uuid.uuid4()
        
        # Try to validate non-existent microproject
        success, microproject, error = KoboDataValidationService.validate_microproject(
            user=self.user,
            microproject_id=non_existent_id,
            status='VALIDATED'
        )
        
        # Assert validation failed
        self.assertFalse(success)
        self.assertIsNone(microproject)
        self.assertEqual(error, 'Microproject record not found')
    
    def test_get_pending_validations_sensitization(self):
        """Test retrieving pending sensitization training records"""
        # Create some training records
        for i in range(3):
            SensitizationTraining.objects.create(
                id=uuid.uuid4(),
                sensitization_date=date.today(),
                male_participants=10,
                female_participants=10,
                validation_status='PENDING' if i < 2 else 'VALIDATED'
            )
        
        # Get pending records
        pending = KoboDataValidationService.get_pending_validations('sensitization')
        
        # Assert correct number of pending records
        self.assertEqual(pending.count(), 2)
        
        # Test with invalid model type
        invalid_pending = KoboDataValidationService.get_pending_validations('invalid_type')
        self.assertIsNone(invalid_pending)
    
    def test_get_pending_validations_with_filters(self):
        """Test retrieving pending records with date filters"""
        # Create training on different dates
        training1 = SensitizationTraining.objects.create(
            id=uuid.uuid4(),
            sensitization_date=date(2024, 1, 1),
            male_participants=10,
            female_participants=10,
            validation_status='PENDING'
        )
        
        training2 = SensitizationTraining.objects.create(
            id=uuid.uuid4(),
            sensitization_date=date(2024, 6, 1),
            male_participants=10,
            female_participants=10,
            validation_status='PENDING'
        )
        
        # Get pending records with date filter
        pending = KoboDataValidationService.get_pending_validations(
            'sensitization',
            date_from=date(2024, 3, 1)
        )
        
        # Should only get the June training
        self.assertEqual(pending.count(), 1)
        self.assertEqual(pending.first().id, training2.id)