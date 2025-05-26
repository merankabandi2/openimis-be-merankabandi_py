import logging
from django.db import transaction
from django.utils import timezone
from merankabandi.models import SensitizationTraining, BehaviorChangePromotion, MicroProject

logger = logging.getLogger(__name__)


class KoboDataValidationService:
    """
    Service for validating KoboToolbox data (MicroProject, BehaviorChangePromotion, SensitizationTraining)
    """
    
    @classmethod
    @transaction.atomic
    def validate_sensitization_training(cls, user, training_id, status, comment=None):
        """
        Validate or reject a sensitization training record
        
        Args:
            user: User performing the validation
            training_id: UUID of the training record
            status: 'VALIDATED' or 'REJECTED'
            comment: Optional validation comment
            
        Returns:
            tuple: (success, training, error_message)
        """
        try:
            training = SensitizationTraining.objects.get(id=training_id)
            
            if training.validation_status != 'PENDING':
                return False, training, f"Training already {training.validation_status.lower()}"
            
            training.validation_status = status
            training.validated_by = user
            training.validation_date = timezone.now()
            training.validation_comment = comment
            training.save()
            
            return True, training, None
            
        except SensitizationTraining.DoesNotExist:
            return False, None, "Training record not found"
        except Exception as e:
            logger.error(f"Error validating training: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)
    
    @classmethod
    @transaction.atomic
    def validate_behavior_change(cls, user, behavior_change_id, status, comment=None):
        """
        Validate or reject a behavior change promotion record
        
        Args:
            user: User performing the validation
            behavior_change_id: UUID of the behavior change record
            status: 'VALIDATED' or 'REJECTED'
            comment: Optional validation comment
            
        Returns:
            tuple: (success, behavior_change, error_message)
        """
        try:
            behavior_change = BehaviorChangePromotion.objects.get(id=behavior_change_id)
            
            if behavior_change.validation_status != 'PENDING':
                return False, behavior_change, f"Record already {behavior_change.validation_status.lower()}"
            
            behavior_change.validation_status = status
            behavior_change.validated_by = user
            behavior_change.validation_date = timezone.now()
            behavior_change.validation_comment = comment
            behavior_change.save()
            
            return True, behavior_change, None
            
        except BehaviorChangePromotion.DoesNotExist:
            return False, None, "Behavior change record not found"
        except Exception as e:
            logger.error(f"Error validating behavior change: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)
    
    @classmethod
    @transaction.atomic
    def validate_microproject(cls, user, microproject_id, status, comment=None):
        """
        Validate or reject a microproject record
        
        Args:
            user: User performing the validation
            microproject_id: UUID of the microproject record
            status: 'VALIDATED' or 'REJECTED'
            comment: Optional validation comment
            
        Returns:
            tuple: (success, microproject, error_message)
        """
        try:
            microproject = MicroProject.objects.get(id=microproject_id)
            
            if microproject.validation_status != 'PENDING':
                return False, microproject, f"Microproject already {microproject.validation_status.lower()}"
            
            microproject.validation_status = status
            microproject.validated_by = user
            microproject.validation_date = timezone.now()
            microproject.validation_comment = comment
            microproject.save()
            
            return True, microproject, None
            
        except MicroProject.DoesNotExist:
            return False, None, "Microproject record not found"
        except Exception as e:
            logger.error(f"Error validating microproject: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)
    
    @classmethod
    def get_pending_validations(cls, model_type, location=None, date_from=None, date_to=None):
        """
        Get pending validation records for a specific model type
        
        Args:
            model_type: 'sensitization', 'behavior_change', or 'microproject'
            location: Optional location filter
            date_from: Optional start date filter
            date_to: Optional end date filter
            
        Returns:
            QuerySet of pending records
        """
        model_map = {
            'sensitization': SensitizationTraining,
            'behavior_change': BehaviorChangePromotion,
            'microproject': MicroProject
        }
        
        model = model_map.get(model_type)
        if not model:
            return None
        
        queryset = model.objects.filter(validation_status='PENDING')
        
        if location:
            queryset = queryset.filter(location=location)
            
        if date_from:
            date_field = 'sensitization_date' if model_type == 'sensitization' else 'report_date'
            queryset = queryset.filter(**{f"{date_field}__gte": date_from})
            
        if date_to:
            date_field = 'sensitization_date' if model_type == 'sensitization' else 'report_date'
            queryset = queryset.filter(**{f"{date_field}__lte": date_to})
        
        return queryset.order_by('-id')