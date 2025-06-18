import graphene
import uuid
from datetime import datetime
from django.core.exceptions import PermissionDenied
from django.db import transaction
from django.http import HttpResponse
from core.schema import OpenIMISMutation
from .models import ResultFrameworkSnapshot, IndicatorAchievement, Indicator
from .result_framework_service import ResultFrameworkService


class CreateResultFrameworkSnapshotInput(graphene.InputObjectType):
    """Input for creating a result framework snapshot"""
    name = graphene.String(required=True)
    description = graphene.String()
    date_from = graphene.Date()
    date_to = graphene.Date()


class UpdateIndicatorAchievementInput(graphene.InputObjectType):
    """Input for updating indicator achievement"""
    indicator_id = graphene.Int(required=True)
    achieved = graphene.Float(required=True)
    date = graphene.Date()
    comment = graphene.String()


class GenerateResultFrameworkDocumentInput(graphene.InputObjectType):
    """Input for generating result framework document"""
    snapshot_id = graphene.ID()
    format = graphene.String(default_value='docx')
    date_from = graphene.Date()
    date_to = graphene.Date()


class CreateResultFrameworkSnapshotMutation(OpenIMISMutation):
    """Create a new result framework snapshot"""
    _mutation_module = "merankabandi"
    _mutation_class = "CreateResultFrameworkSnapshotMutation"
    
    class Input:
        name = graphene.String(required=True)
        description = graphene.String()
        date_from = graphene.Date()
        date_to = graphene.Date()
    
    @classmethod
    def async_mutate(cls, user, **data):
        try:
            if not user or user.is_anonymous:
                raise PermissionDenied("User must be authenticated")
                
            service = ResultFrameworkService()
            
            snapshot = service.create_snapshot(
                name=data.get('name'),
                description=data.get('description', ''),
                user=user,
                date_from=data.get('date_from'),
                date_to=data.get('date_to')
            )
            
            return {
                'success': True,
                'message': f'Snapshot created successfully with ID: {snapshot.id}',
                'detail': str(snapshot.id)
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'detail': None
            }


class UpdateIndicatorAchievementMutation(OpenIMISMutation):
    """Update or create indicator achievement"""
    _mutation_module = "merankabandi"
    _mutation_class = "UpdateIndicatorAchievementMutation"
    
    class Input:
        indicator_id = graphene.Int(required=True)
        achieved = graphene.Float(required=True)
        date = graphene.Date()
        comment = graphene.String()
    
    @classmethod
    def async_mutate(cls, user, **data):
        try:
            if not user or user.is_anonymous:
                raise PermissionDenied("User must be authenticated")
                
            indicator = Indicator.objects.get(id=data['indicator_id'])
            
            # Create new achievement record
            achievement = IndicatorAchievement.objects.create(
                indicator=indicator,
                achieved=data['achieved'],
                date=data.get('date') or datetime.now().date(),
                comment=data.get('comment', '')
            )
            
            return {
                'success': True,
                'message': f'Achievement updated for indicator: {indicator.name}',
                'detail': str(achievement.id)
            }
        except Indicator.DoesNotExist:
            return {
                'success': False,
                'message': 'Indicator not found',
                'detail': None
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'detail': None
            }


class GenerateResultFrameworkDocumentMutation(graphene.Mutation):
    """Generate result framework document"""
    class Arguments:
        snapshot_id = graphene.ID()
        format = graphene.String(default_value='docx')
        date_from = graphene.Date()
        date_to = graphene.Date()
    
    success = graphene.Boolean()
    message = graphene.String()
    document_url = graphene.String()
    
    @classmethod
    def mutate(cls, root, info, snapshot_id=None, format='docx', date_from=None, date_to=None):
        try:
            user = info.context.user
            if not user or user.is_anonymous:
                raise PermissionDenied("User must be authenticated")
                
            service = ResultFrameworkService()
            
            # Generate document
            document = service.generate_document(snapshot_id=snapshot_id, format=format)
            
            # Save document to file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"result_framework_{timestamp}.{format}"
            filepath = f"result_framework_docs/{filename}"
            
            # Create response
            if format == 'docx':
                # For now, return the document object
                # In production, you would save this to a file storage system
                return cls(
                    success=True,
                    message='Document generated successfully',
                    document_url=filepath
                )
            else:
                return cls(
                    success=False,
                    message=f'Unsupported format: {format}',
                    document_url=None
                )
                
        except Exception as e:
            return cls(
                success=False,
                message=str(e),
                document_url=None
            )


class FinalizeSnapshotMutation(OpenIMISMutation):
    """Finalize a snapshot to prevent further changes"""
    _mutation_module = "merankabandi"
    _mutation_class = "FinalizeSnapshotMutation"
    
    class Input:
        snapshot_id = graphene.ID(required=True)
    
    @classmethod
    def async_mutate(cls, user, **data):
        try:
            if not user or user.is_anonymous:
                raise PermissionDenied("User must be authenticated")
                
            snapshot = ResultFrameworkSnapshot.objects.get(id=data['snapshot_id'])
            
            if snapshot.status != 'DRAFT':
                return {
                    'success': False,
                    'message': 'Snapshot is not in DRAFT status',
                    'detail': None
                }
            
            snapshot.status = 'FINALIZED'
            snapshot.save()
            
            return {
                'success': True,
                'message': 'Snapshot finalized successfully',
                'detail': str(snapshot.id)
            }
        except ResultFrameworkSnapshot.DoesNotExist:
            return {
                'success': False,
                'message': 'Snapshot not found',
                'detail': None
            }
        except Exception as e:
            return {
                'success': False,
                'message': str(e),
                'detail': None
            }