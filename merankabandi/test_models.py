from django.test import TestCase
from django.utils import timezone
from datetime import date
import uuid

from merankabandi.models import (
    SensitizationTraining, BehaviorChangePromotion, MicroProject,
    MonetaryTransfer, Section, Indicator, IndicatorAchievement
)


class MerankabandiModelsTest(TestCase):
    """Test Merankabandi models creation and properties"""
    
    def test_section_creation(self):
        """Test creation of Section model"""
        section = Section.objects.create(
            name='Objectifs de développement'
        )
        self.assertEqual(str(section), 'Objectifs de développement')
    
    def test_indicator_creation(self):
        """Test creation of Indicator model"""
        section = Section.objects.create(
            name='Test Section'
        )
        
        indicator = Indicator.objects.create(
            section=section,
            name='Taux de réduction de la pauvreté',
            pbc='PBC001',
            baseline=45.5,
            target=30.0,
            observation='Indicateur clé du programme'
        )
        
        self.assertEqual(indicator.section, section)
        self.assertEqual(indicator.baseline, 45.5)
        self.assertEqual(indicator.target, 30.0)
        self.assertIn('Taux de réduction', str(indicator))
    
    def test_indicator_achievement_creation(self):
        """Test creation of IndicatorAchievement model"""
        indicator = Indicator.objects.create(
            name='Test Indicator',
            baseline=0,
            target=100
        )
        
        achievement = IndicatorAchievement.objects.create(
            indicator=indicator,
            achieved=45.5,
            date=date.today(),
            comment='Mid-year progress'
        )
        
        self.assertEqual(achievement.indicator, indicator)
        self.assertEqual(achievement.achieved, 45.5)
        self.assertIn('45.5', str(achievement))
    
    def test_sensitization_training_total_participants(self):
        """Test SensitizationTraining total_participants property"""
        training = SensitizationTraining(
            id=uuid.uuid4(),
            sensitization_date=date.today(),
            male_participants=25,
            female_participants=30,
            twa_participants=5
        )
        
        # Note: twa_participants are not included in total_participants
        # based on the model property implementation
        self.assertEqual(training.total_participants, 55)
    
    def test_behavior_change_total_beneficiaries(self):
        """Test BehaviorChangePromotion total_beneficiaries property"""
        behavior_change = BehaviorChangePromotion(
            id=uuid.uuid4(),
            report_date=date.today(),
            male_participants=20,
            female_participants=25,
            twa_participants=3
        )
        
        # All participants are included in total_beneficiaries
        self.assertEqual(behavior_change.total_beneficiaries, 48)
    
    def test_monetary_transfer_totals(self):
        """Test MonetaryTransfer total properties"""
        transfer = MonetaryTransfer(
            transfer_date=date.today(),
            planned_women=100,
            planned_men=80,
            planned_twa=20,
            paid_women=95,
            paid_men=78,
            paid_twa=19
        )
        
        self.assertEqual(transfer.total_planned, 200)
        self.assertEqual(transfer.total_paid, 192)
    
    def test_microproject_validation_status_default(self):
        """Test MicroProject default validation status"""
        microproject = MicroProject(
            id=uuid.uuid4(),
            report_date=date.today(),
            male_participants=10,
            female_participants=15
        )
        
        self.assertEqual(microproject.validation_status, 'PENDING')
    
    def test_sensitization_training_validation_fields(self):
        """Test SensitizationTraining validation fields"""
        training = SensitizationTraining(
            id=uuid.uuid4(),
            sensitization_date=date.today(),
            male_participants=10,
            female_participants=15
        )
        
        # Check default values
        self.assertEqual(training.validation_status, 'PENDING')
        self.assertIsNone(training.validated_by)
        self.assertIsNone(training.validation_date)
        self.assertIsNone(training.validation_comment)
        self.assertIsNone(training.kobo_submission_id)