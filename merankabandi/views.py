from datetime import date
from django.http import HttpResponse
from django.template.loader import render_to_string
from payroll.models import BenefitConsumption, BenefitConsumptionStatus
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from social_protection.models import GroupBeneficiary as Beneficiary, Group
from individual.models import GroupIndividual, Individual
from django.conf import settings
from django.http import FileResponse, HttpResponseForbidden
from pathlib import Path
import base64
import os
from rest_framework.response import Response
from rest_framework import status

from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.pagination import PageNumberPagination
from rest_framework.decorators import action
from oauth2_provider.contrib.rest_framework import TokenHasScope
import logging
import subprocess
import threading
from django.http import JsonResponse
from datetime import date

from .monetary_transfer_import_service import MonetaryTransferImportService

from .serializers import (
    IndividualPaymentRequestSerializer,
    PaymentAccountAcknowledgmentSerializer,
    PaymentAccountAttributionListSerializer,
    PaymentAccountAttributionSerializer,
    PaymentAcknowledgmentSerializer,
    PaymentConsolidationSerializer,
    PaymentBatchAcknowledgmentSerializer,
    PaymentBatchConsolidationSerializer,
    PhoneNumberAttributionRequestSerializer,
    BeneficiaryPhoneDataSerializer,
    ResponseSerializer
)
from .services import PaymentAccountAttributionService, PaymentApiService, PhoneNumberAttributionService

logger = logging.getLogger(__name__)



class BeneficiaryCardGenerator:
    def __init__(self):
        self.font_config = FontConfiguration()
        self.css = self._get_card_css()
    
    def _get_card_css(self):
        """Define CSS for card styling"""
        return CSS(string='''
            @page {
                size: A4;
                margin: 1cm;
            }
            
            .card {
                height: 13cm;
                width: 18.5cm;
                margin: 0 0 0 0;
                page-break-inside: avoid;
                position: relative;
                font-family: Arial, sans-serif;
            }
            
            .card-header {
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                font-size: 12pt;
                font-weight: bold;
                margin-bottom: 0;
            }
            .header-text {
                flex-grow: 1;
                text-align: center;
            }
            .social-id-container {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 0.5cm;
            }
            .social-id {
                flex-grow: 1;
                font-weight: bold;
                text-align: center;
                font-size: 16pt;
                margin-top: 0.5cm
            }
            
            .logo {
                height: 2cm;
                object-fit: contain;
            }
            .photo {
                height: 3cm;
                object-fit: contain;
            }

            .field {
                margin-top: 0.3cm;
                margin-bottom: 0.3cm;
                display: flex;
                align-items: center;
            }
            
            .field-label {
                display: inline-block;
                font-size: 10pt;
                width: 6cm;
            }
            
            .field-value {
                font-weight: bold;
                font-size: 12pt;
            }
            
            .declaration {
                font-size: 10pt;
                margin-top: 0.6cm;
            }
    
            .card2 {
                margin-top: 1.3cm;
            }
        ''')

    def _get_image_data_url(self, image_path):
        """Convert image to data URL for embedding in HTML"""
        if not image_path or not Path(image_path).exists():
            return ""
        
        # Read the image file and convert to base64
        with open(image_path, 'rb') as img_file:
            image_data = base64.b64encode(img_file.read()).decode('utf-8')
            
        # Get the file extension for MIME type
        file_ext = os.path.splitext(image_path)[1].lower()
        mime_type = {
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml'
        }.get(file_ext, 'application/octet-stream')

        return f'data:{mime_type};base64,{image_data}'

    def _get_image_url(self, request, image_path):
        """Get image URL - in this case returning the direct URL"""
        current_site = request.build_absolute_uri('/').rstrip('/')
        return f"{current_site}/{image_path}"

    def generate_card_html(self, request, beneficiary):
        """Generate HTML for beneficiary card"""
        individual = beneficiary.group.groupindividuals.get(recipient_type=GroupIndividual.RecipientType.PRIMARY).individual
        household = individual.groupindividuals.get().group
        base_dir = os.path.join(settings.PHOTOS_BASE_PATH, str(household.json_ext.get('deviceid', '')), str(household.json_ext.get('date_collecte', '')).replace('-', ''))
        clean_path = f"photo_repondant_{str(individual.json_ext.get('social_id', ''))}.jpg"
        photo_path = os.path.join(base_dir, clean_path)
        moyen_telecom = beneficiary.json_ext.get('moyen_telecom', '')

        colline = beneficiary.group.location
        
        logo_path = os.path.join(settings.STATIC_ROOT, 'merankabandi/logo.png')
        # Get current date for fallback
        current_date = date.today().strftime('%Y-%m-%d')
        
        context = {
            'logo_url': self._get_image_data_url(logo_path),
            'photo_url': self._get_image_data_url(photo_path),
            'social_id': beneficiary.group.code,
            'individual': individual,
            'telephone': moyen_telecom.get('msisdn', '') if moyen_telecom else '',
            'date_enregistrement': moyen_telecom.get('responseDate', current_date) if moyen_telecom else current_date,
            'province': colline.parent.parent.name,
            'commune': colline.parent.name,
            'colline': colline.name,
        }
        
        return render_to_string('beneficiary_card.html', context)

    def generate_beneficiary_cards(self, request, beneficiary):
        """Generate PDF with front and back cards for a single beneficiary"""
        html = self.generate_card_html(request, beneficiary)
        
        html_doc = HTML(string=html)
        return html_doc.write_pdf(
            stylesheets=[self.css],
            font_config=self.font_config
        )

def generate_beneficiary_card_view(request, social_id):
    """View for generating a single beneficiary's card"""
    try:
        beneficiary = Beneficiary.objects.get(group__code=social_id)
        
        generator = BeneficiaryCardGenerator()
        pdf = generator.generate_beneficiary_cards(request, beneficiary)
        
        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="card_{social_id}.pdf"'
        response.write(pdf)
        
        return response
        
    except Beneficiary.DoesNotExist:
        return HttpResponse("Beneficiary not found", status=404)

def generate_colline_cards_view(request, commune_name):
    """View for generating cards for all beneficiaries in a colline"""
    try:
        beneficiaries = Beneficiary.objects.filter(
            group__location__parent__name=commune_name,
            json_ext__moyen_telecom__phoneNumber__isnull=False
        )
        
        generator = BeneficiaryCardGenerator()
        all_cards_html = []
        
        for beneficiary in beneficiaries:
            all_cards_html.append(generator.generate_card_html(request, beneficiary))
        
        combined_html = '\n'.join(all_cards_html)
        
        html_doc = HTML(string=combined_html)
        pdf = html_doc.write_pdf(
            stylesheets=[generator.css],
            font_config=generator.font_config
        )
        
        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="colline_{commune_name}_cards.pdf"'
        response.write(pdf)
        
        return response
        
    except Exception as e:
        return HttpResponse(f"Error generating cards: {str(e)}", status=500)  

def generate_location_cards_view(request, location_id):
    """View for generating cards for all beneficiaries in a specific location"""
    try:
        from location.models import Location
        
        location = Location.objects.get(id=location_id)
        
        # Get beneficiaries for this location based on its type
        location_type = location.type
        
        if location_type == 'D':  # District/Province
            beneficiaries = Beneficiary.objects.filter(
                group__location__parent__parent_id=location_id,
                json_ext__moyen_telecom__msisdn__isnull=False
            )
        elif location_type == 'W':  # Commune
            beneficiaries = Beneficiary.objects.filter(
                group__location__parent_id=location_id,
                json_ext__moyen_telecom__msisdn__isnull=False
            )
        elif location_type == 'V':  # Colline
            beneficiaries = Beneficiary.objects.filter(
                group__location_id=location_id,
                json_ext__moyen_telecom__msisdn__isnull=False
            )
        else:
            return HttpResponse(f"Unsupported location type: {location_type}", status=400)
        
        if not beneficiaries.exists():
            return HttpResponse(f"No beneficiaries with registered phone numbers found for this location", status=404)
            
        generator = BeneficiaryCardGenerator()
        all_cards_html = []
        
        for beneficiary in beneficiaries:
            all_cards_html.append(generator.generate_card_html(request, beneficiary))
        
        combined_html = '\n'.join(all_cards_html)
        
        html_doc = HTML(string=combined_html)
        pdf = html_doc.write_pdf(
            stylesheets=[generator.css],
            font_config=generator.font_config
        )
        
        response = HttpResponse(content_type='application/pdf')
        response['Content-Disposition'] = f'attachment; filename="location_{location_id}_cards.pdf"'
        response.write(pdf)
        
        return response
        
    except Exception as e:
        return HttpResponse(f"Error generating cards: {str(e)}", status=500)

def beneficiary_photo_view(request, type, id):
    individual = Individual.objects.get(id=id)
    household = individual.groupindividuals.get().group
    base_dir = os.path.join(settings.PHOTOS_BASE_PATH, str(household.json_ext.get('deviceid', '')), str(household.json_ext.get('date_collecte', '')).replace('-', ''))
    clean_path = f"{type}_repondant_{str(individual.json_ext.get('social_id', ''))}.jpg"
    
    # Define your permission logic here
    if not has_image_access_permission(request.user, clean_path):
        return HttpResponseForbidden("Access denied")
    
    # Construct the full file path
    file_path = os.path.join(base_dir, clean_path)
    
    if not os.path.exists(file_path):
        return HttpResponseForbidden("File not found")

    # Serve the file
    return FileResponse(open(file_path, 'rb'))

def has_image_access_permission(user, image_path):
    return True
    """
    Define your custom permission logic here.
    This is just an example - modify according to your needs.
    """
    # Example: Check if user has specific permission
    if user.has_perm('myapp.view_protected_images'):
        return True
        
    # Example: Check if image belongs to user's group
    if image_path.startswith(f'group_{user.groups.first().id}/'):
        return True
        
    # Example: Check if image is in user's allowed categories
    allowed_categories = user.profile.allowed_image_categories.all()
    image_category = get_image_category(image_path)
    if image_category in allowed_categories:
        return True
    
    return False

class StandardResultsSetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 100


class PhoneNumberAttributionViewSet(viewsets.ViewSet):
    """
    API endpoint for phone number attribution.
    
    GET: Retrieve beneficiaries requiring phone number verification
    POST: Verify and attribute phone numbers to beneficiaries
    """
    pagination_class = StandardResultsSetPagination
    permission_classes = [TokenHasScope]
    required_scopes = ['group_beneficiary:read']

    def get_required_scopes(self, request):
        """Return appropriate scopes based on request method"""
        method_scopes = {
            'GET': ['group_beneficiary:read'],
            'POST': ['group_beneficiary:write']
        }
        return method_scopes.get(request.method, [])
    

    def list(self, request):
        """
        GET: List beneficiaries requiring phone number verification.
        Optional filters: commune, programme
        """

        application_name = request.auth.application.name
        commune = request.query_params.get('commune')
        programme = request.query_params.get('programme')
        
        # Get beneficiaries awaiting phone verification
        queryset = PhoneNumberAttributionService.get_pending_phone_verifications(
            commune=commune, 
            programme=programme
        )
        
        # Paginate results
        paginator = self.pagination_class()
        page = paginator.paginate_queryset(queryset, request)
        
        # Serialize data
        serializer = BeneficiaryPhoneDataSerializer(
            page, 
            many=True,
            context={'request': request}
        )
        
        return paginator.get_paginated_response(serializer.data)

    def create(self, request):
        """
        POST: Verify and attribute phone number to beneficiary
        Supports both single and batch operations (50-100 items)
        """

        application_name = request.auth.application.name
        
        # Check if this is a batch request
        if isinstance(request.data, list):
            return self._handle_batch_creation(request)
        
        # Single request handling
        request_serializer = PhoneNumberAttributionRequestSerializer(data=request.data)
        if not request_serializer.is_valid():
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'invalid_data',
                    'message': request_serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )
            
        # Process phone number attribution
        success, beneficiary, message = PhoneNumberAttributionService.process_phone_attribution(
            request_serializer.validated_data,
            request.user
        )
        
        if not success:
            response_data = {
                'status': 'FAILURE',
                'error_code': 'processing_error',
                'message': message
            }
            
            # Determine appropriate status code
            if not beneficiary:
                response_status = status.HTTP_404_NOT_FOUND
            elif message and 'state' in message:
                response_status = status.HTTP_400_BAD_REQUEST
            else:
                response_status = status.HTTP_500_INTERNAL_SERVER_ERROR
                
            return Response(response_data, status=response_status)
        
        # Return success response
        response_serializer = ResponseSerializer(data={
            'status': 'SUCCESS',
            'error_code': None,
            'message': 'Phone number successfully processed'
        })
        response_serializer.is_valid()
        return Response(response_serializer.validated_data)
    
    def _handle_batch_creation(self, request):
        """
        Handle batch phone number attribution
        """
        batch_data = request.data
        
        # Validate batch size
        if len(batch_data) > 100:
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'batch_size_exceeded',
                    'message': 'Batch size cannot exceed 100 items'
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        results = []
        errors = []
        success_count = 0
        
        for idx, item in enumerate(batch_data):
            # Validate individual item
            request_serializer = PhoneNumberAttributionRequestSerializer(data=item)
            if not request_serializer.is_valid():
                errors.append({
                    'index': idx,
                    'socialid': item.get('socialid', 'unknown'),
                    'error': request_serializer.errors
                })
                continue
            
            # Process phone number attribution
            success, beneficiary, message = PhoneNumberAttributionService.process_phone_attribution(
                request_serializer.validated_data,
                request.user
            )
            
            if success:
                success_count += 1
                results.append({
                    'index': idx,
                    'socialid': request_serializer.validated_data['socialid'],
                    'status': 'SUCCESS'
                })
            else:
                errors.append({
                    'index': idx,
                    'socialid': request_serializer.validated_data['socialid'],
                    'error': message
                })
        
        # Return batch response
        return Response({
            'status': 'BATCH_COMPLETED',
            'total_items': len(batch_data),
            'success_count': success_count,
            'error_count': len(errors),
            'results': results,
            'errors': errors
        })
    
    @action(detail=False, methods=['get'], url_path='stats')
    def stats(self, request):
        """
        GET: Retrieve statistics about phone number verification
        """
        try:
            # Count beneficiaries by status
            pending_count = Beneficiary.objects.filter(
                json_ext__moyen_telecom__msisdn__isnull=True
            ).count()
            
            rejected_count = Beneficiary.objects.filter(
                json_ext__moyen_telecom__status='REJECTED'
            ).count()
            
            attributed_count = Beneficiary.objects.filter(
                json_ext__moyen_telecom__status='SUCCESS',
                json_ext__moyen_telecom__msisdn__isnull=False
            ).count()

            failed_count = Beneficiary.objects.filter(
                json_ext__moyen_telecom__status='FAILED'
            ).count()
            
            # Return statistics
            return Response({
                'pending_count': pending_count,
                'rejected_count': rejected_count,
                'failed_count': failed_count,
                'attributed_count': attributed_count,
                'total': pending_count + rejected_count + failed_count + attributed_count
            })
            
        except Exception as e:
            logger.error(f"Error getting phone verification stats: {str(e)}")
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'stats_error',
                    'message': 'Error retrieving statistics'
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
   

class PaymentAccountAttributionViewSet(viewsets.ViewSet):
    """
    API endpoint for payment account attribution workflow.
    
    GET: Retrieve beneficiary data for account attribution
    POST: Acknowledge receipt of data or attribute payment account
    """
    pagination_class = StandardResultsSetPagination
    permission_classes = [TokenHasScope]
    required_scopes = ['group_beneficiary:read']

    def get_required_scopes(self, request):
        """Return appropriate scopes based on request method"""
        method_scopes = {
            'GET': ['group_beneficiary:read'],
            'POST': ['group_beneficiary:write']
        }
        return method_scopes.get(request.method, [])
    
    def list(self, request):
        """
        GET: List beneficiaries requiring payment account attribution.
        Optional filters: commune, programme
        """
        application_name = request.auth.application.name
        commune = request.query_params.get('commune')
        programme = request.query_params.get('programme')
        
        # Get beneficiaries awaiting account attribution
        queryset = PaymentAccountAttributionService.get_pending_account_attributions(
            commune=commune, 
            programme=programme
        )
        
        # Paginate results
        paginator = self.pagination_class()
        page = paginator.paginate_queryset(queryset, request)
        
        # Serialize data
        serializer = PaymentAccountAttributionListSerializer(
            page, 
            many=True,
            context={'request': request}
        )
        
        return paginator.get_paginated_response(serializer.data)

    def create(self, request):
        """
        POST: Handle acknowledgment or attribution based on payload
        Supports both single and batch operations (50-100 items)
        """
        # Check if this is a batch request
        if isinstance(request.data, list):
            return self._handle_batch_creation(request)
        
        # Single request handling
        payload = request.data
        
        if 'tp_account_number' in payload:
            return self.handle_attribution(request)
        elif 'status' in payload and payload.get('status') in ['ACCEPTED', 'REJECTED']:
            return self.handle_acknowledgment(request)
        else:
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'invalid_operation',
                    'message': 'Could not determine operation type from payload'
                },
                status=status.HTTP_400_BAD_REQUEST
            )
    
    def _handle_batch_creation(self, request):
        """
        Handle batch payment account operations
        """
        batch_data = request.data
        
        # Validate batch size
        if len(batch_data) > 100:
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'batch_size_exceeded',
                    'message': 'Batch size cannot exceed 100 items'
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        results = []
        errors = []
        success_count = 0
        
        for idx, item in enumerate(batch_data):
            # Determine operation type
            if 'tp_account_number' in item:
                result = self._process_batch_attribution(idx, item, request)
            elif 'status' in item and item.get('status') in ['ACCEPTED', 'REJECTED']:
                result = self._process_batch_acknowledgment(idx, item, request)
            else:
                errors.append({
                    'index': idx,
                    'socialid': item.get('socialid', 'unknown'),
                    'error': 'Could not determine operation type from payload'
                })
                continue
            
            if result['success']:
                success_count += 1
                results.append(result)
            else:
                errors.append(result)
        
        # Return batch response
        return Response({
            'status': 'BATCH_COMPLETED',
            'total_items': len(batch_data),
            'success_count': success_count,
            'error_count': len(errors),
            'results': results,
            'errors': errors
        })
    
    def _process_batch_acknowledgment(self, idx, item, request):
        """Process individual acknowledgment in batch"""
        application_name = request.auth.application.name
        serializer = PaymentAccountAcknowledgmentSerializer(data={**item, "agence": application_name})
        if not serializer.is_valid():
            return {
                'success': False,
                'index': idx,
                'socialid': item.get('socialid', 'unknown'),
                'error': serializer.errors
            }
        
        success, beneficiary, message = PaymentAccountAttributionService.process_acknowledgment(
            serializer.validated_data
        )
        
        if success:
            return {
                'success': True,
                'index': idx,
                'socialid': serializer.validated_data['socialid'],
                'status': 'ACKNOWLEDGED'
            }
        else:
            return {
                'success': False,
                'index': idx,
                'socialid': serializer.validated_data['socialid'],
                'error': message
            }
    
    def _process_batch_attribution(self, idx, item, request):
        """Process individual attribution in batch"""
        application_name = request.auth.application.name
        serializer = PaymentAccountAttributionSerializer(data={**item, "agence": application_name})
        if not serializer.is_valid():
            return {
                'success': False,
                'index': idx,
                'socialid': item.get('socialid', 'unknown'),
                'error': serializer.errors
            }
        
        success, beneficiary, message = PaymentAccountAttributionService.process_account_attribution(
            serializer.validated_data
        )
        
        if success:
            return {
                'success': True,
                'index': idx,
                'socialid': serializer.validated_data['socialid'],
                'status': 'ATTRIBUTED'
            }
        else:
            return {
                'success': False,
                'index': idx,
                'socialid': serializer.validated_data['socialid'],
                'error': message
            }
            
    def handle_acknowledgment(self, request):
        """
        Handle acknowledgment of beneficiary data
        """
        # Validate request data
        application_name = request.auth.application.name
        serializer = PaymentAccountAcknowledgmentSerializer(data={**request.data, "agence": application_name})
        if not serializer.is_valid():
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'invalid_data',
                    'message': serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )
            
        # Process acknowledgment
        success, beneficiary, message = PaymentAccountAttributionService.process_acknowledgment(
            serializer.validated_data
        )
        
        if not success:
            response_data = {
                'status': 'FAILURE',
                'error_code': 'processing_error',
                'message': message
            }
            
            # Determine appropriate status code
            if not beneficiary:
                response_status = status.HTTP_404_NOT_FOUND
            elif message and 'state' in message:
                response_status = status.HTTP_400_BAD_REQUEST
            else:
                response_status = status.HTTP_500_INTERNAL_SERVER_ERROR
                
            return Response(response_data, status=response_status)
        
        # Return success response
        response_serializer = ResponseSerializer(data={
            'status': 'SUCCESS',
            'error_code': None,
            'message': 'Payment account acknowledgment successful'
        })
        response_serializer.is_valid()
        return Response(response_serializer.validated_data)
            
    def handle_attribution(self, request):
        """
        Handle payment account attribution
        """
        # Validate request data
        application_name = request.auth.application.name
        serializer = PaymentAccountAttributionSerializer(data={**request.data, "agence": application_name})
        if not serializer.is_valid():
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'invalid_data',
                    'message': serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )
            
        # Process account attribution
        success, beneficiary, message = PaymentAccountAttributionService.process_account_attribution(
            serializer.validated_data
        )
        
        if not success:
            response_data = {
                'status': 'FAILURE',
                'error_code': 'processing_error',
                'message': message
            }
            
            # Determine appropriate status code
            if not beneficiary:
                response_status = status.HTTP_404_NOT_FOUND
            elif message and 'state' in message:
                response_status = status.HTTP_400_BAD_REQUEST
            else:
                response_status = status.HTTP_500_INTERNAL_SERVER_ERROR
                
            return Response(response_data, status=response_status)
        
        # Return success response
        response_serializer = ResponseSerializer(data={
            'status': 'SUCCESS',
            'error_code': None,
            'message': 'Payment account attribution successful'
        })
        response_serializer.is_valid()
        return Response(response_serializer.validated_data)
    
    @action(detail=False, methods=['get'], url_path='stats')
    def stats(self, request):
        """
        GET: Retrieve statistics about payment account attribution
        """
        try:
            # Count beneficiaries by status
            pending_attribution = Beneficiary.objects.filter(
                json_ext__moyen_paiement__status='ACCEPTED'
            ).count()
            
            rejected = Beneficiary.objects.filter(
                json_ext__moyen_paiement__status='REJECTED'
            ).count()
            
            created = Beneficiary.objects.filter(
                json_ext__moyen_paiement__status='SUCCESS'
            ).count()
            
            failed = Beneficiary.objects.filter(
                json_ext__moyen_paiement__status='FAILED'
            ).count()
            
            # Return statistics
            return Response({
                'pending_attribution': pending_attribution,
                'rejected': rejected,
                'created': created,
                'failed': failed,
                'total': pending_attribution + rejected + created + failed
            })
            
        except Exception as e:
            logger.error(f"Error getting account attribution stats: {str(e)}")
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'stats_error',
                    'message': 'Error retrieving statistics'
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class PaymentRequestViewSet(viewsets.ViewSet):
    """
    API endpoint for payment requests.
    
    GET: Retrieve individual payment requests for payment agency
    POST: Acknowledge receipt or update payment status
    """
    pagination_class = StandardResultsSetPagination
    permission_classes = [TokenHasScope]
    required_scopes = ['benefit_consumption:read']
    
    def get_required_scopes(self, request):
        """Return appropriate scopes based on request method"""
        method_scopes = {
            'GET': ['benefit_consumption:read'],
            'POST': ['benefit_consumption:write']
        }
        return method_scopes.get(request.method, [])


    def list(self, request):
        """
        GET: List individual payment requests for payment agency
        Query parameters: commune, payment_cycle, programme, start_date, end_date
        """

        application_name = request.auth.application.name
        # Get filter parameters from query params
        commune = request.query_params.get('commune')
        payment_cycle = request.query_params.get('payment_cycle')
        programme = request.query_params.get('programme')
        start_date = request.query_params.get('start_date')
        end_date = request.query_params.get('end_date')
        
        # Get payment requests with new filters
        queryset = PaymentApiService.get_individual_payment_requests(
            payment_provider_id=application_name,
            payment_cycle_id=payment_cycle,
            commune=commune,
            programme=programme,
            start_date=start_date,
            end_date=end_date
        )
        
        # Paginate results
        paginator = self.pagination_class()
        page = paginator.paginate_queryset(queryset, request)
        
        # Serialize data with new field names
        serializer = IndividualPaymentRequestSerializer(page, many=True)
        
        return paginator.get_paginated_response(serializer.data)
    
    def retrieve(self, request, pk=None):
        """
        GET: Retrieve a specific payment request by code
        """
        payment_request = PaymentApiService.get_payment_request_by_code(pk)
        
        if not payment_request:
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'not_found',
                    'message': f'Payment request with code {pk} not found or not available for payment'
                },
                status=status.HTTP_404_NOT_FOUND
            )
        
        serializer = IndividualPaymentRequestSerializer(payment_request)
        return Response(serializer.data)
    
    def create(self, request):
        """
        POST: Handle acknowledgment and consolidation operations
        Supports both single and batch operations (up to 100 items)
        """
        # Check if this is a batch request
        if isinstance(request.data, list):
            return self._handle_batch_creation(request)
            
        # Single request handling
        # Determine operation type based on fields present
        operation = None
        if 'numero_interne_paiement' in request.data and 'retour_transactionid' in request.data:
            if 'status' in request.data and request.data['status'] in ['ACCEPTED', 'REJECTED']:
                operation = 'acknowledge'
        elif 'retour_transactionid' in request.data and 'payment_date' in request.data:
            if 'status' in request.data and request.data['status'] in ['SUCCESS', 'FAILURE']:
                operation = 'consolidate'

        user = request.user
        
        # Handle acknowledgment
        if operation == 'acknowledge':
            serializer = PaymentAcknowledgmentSerializer(data=request.data)
            if not serializer.is_valid():
                return Response(
                    {
                        'status': 'FAILURE',
                        'error_code': 'INVALID_DATA',
                        'message': str(serializer.errors)
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Process acknowledgment
            data = serializer.validated_data
            application_name = request.auth.application.name if request.auth else None
            success, benefit, message = PaymentApiService.acknowledge_payment_request(
                user,
                code=data['numero_interne_paiement'],
                status=data['status'],
                transaction_reference=data['retour_transactionid'],
                payment_agency_id=application_name,
                error_code=data.get('error_code'),
                message=data.get('message')
            )
            
            if not success:
                return Response(
                    {
                        'status': 'FAILURE',
                        'error_code': 'INVALID_TRANSACTION_ID' if benefit is None else 'ACKNOWLEDGMENT_FAILED'
                    },
                    status=status.HTTP_404_NOT_FOUND if benefit is None else status.HTTP_400_BAD_REQUEST
                )
            
            # Return success response
            return Response({
                'status': 'SUCCESS',
                'error_code': ''
            })
        
        # Handle consolidation
        elif operation == 'consolidate':
            serializer = PaymentConsolidationSerializer(data=request.data)
            if not serializer.is_valid():
                return Response(
                    {
                        'status': 'FAILURE',
                        'error_code': 'INVALID_DATA',
                        'message': str(serializer.errors)
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Process consolidation
            data = serializer.validated_data
            application_name = request.auth.application.name if request.auth else None
            success, benefit, message = PaymentApiService.consolidate_payment(
                user,
                transaction_reference=data['retour_transactionid'],
                payment_date=data['payment_date'],
                receipt_number=data.get('receipt_number'),
                status=data['status'],
                error_code=data.get('error_code'),
                message=data.get('message'),
                payment_agency_id=application_name
            )
            
            if not success:
                return Response(
                    {
                        'status': 'FAILURE',
                        'error_code': 'INVALID_TRANSACTION_ID' if benefit is None else 'CONSOLIDATION_FAILED'
                    },
                    status=status.HTTP_404_NOT_FOUND if benefit is None else status.HTTP_400_BAD_REQUEST
                )
            
            # Return success response
            return Response({
                'status': 'SUCCESS',
                'error_code': ''
            })
        
        # Invalid operation
        else:
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'INVALID_OPERATION',
                    'message': 'Could not determine operation type. Check required fields for acknowledgment or consolidation.'
                },
                status=status.HTTP_400_BAD_REQUEST
            )
    
    def _handle_batch_creation(self, request):
        """
        Handle batch payment request operations
        """
        batch_data = request.data
        user = request.user
        
        # Validate batch size
        if len(batch_data) > 100:
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'BATCH_SIZE_EXCEEDED',
                    'message': 'Batch size cannot exceed 100 items'
                },
                status=status.HTTP_400_BAD_REQUEST
            )
        
        results = []
        errors = []
        success_count = 0
        
        for idx, item in enumerate(batch_data):
            # Determine operation type based on status in batch items
            operation = None
            if 'status' in item:
                if item['status'] in ['ACCEPTED', 'REJECTED']:
                    operation = 'acknowledge'
                elif item['status'] in ['PAID', 'FAILED', 'REJECTED']:
                    operation = 'consolidate'
            
            if not operation:
                errors.append({
                    'index': idx,
                    'code': item.get('code', 'unknown'),
                    'error': 'Could not determine operation type from payload'
                })
                continue
            
            # Process based on operation type
            if operation == 'acknowledge':
                result = self._process_batch_acknowledgment(idx, item, user, request)
            else:
                result = self._process_batch_consolidation(idx, item, user, request)
            
            if result['success']:
                success_count += 1
                results.append(result)
            else:
                errors.append(result)
        
        # Return batch response
        return Response({
            'status': 'BATCH_COMPLETED',
            'total_items': len(batch_data),
            'success_count': success_count,
            'error_count': len(errors),
            'results': results,
            'errors': errors
        })
    
    def _process_batch_acknowledgment(self, idx, item, user, request):
        """Process individual acknowledgment in batch"""
        serializer = PaymentBatchAcknowledgmentSerializer(data=item)
        if not serializer.is_valid():
            return {
                'success': False,
                'index': idx,
                'code': item.get('code', 'unknown'),
                'error': str(serializer.errors)
            }
        
        data = serializer.validated_data
        # Get application name from the request
        application_name = request.auth.application.name if request.auth else None
        
        success, benefit, message = PaymentApiService.acknowledge_payment_request(
            user,
            code=data['code'],
            status=data['status'],
            payment_agency_id=application_name,
            error_code=data.get('error_code'),
            message=data.get('message')
        )
        
        if success:
            return {
                'success': True,
                'index': idx,
                'code': data['code'],
                'status': 'ACKNOWLEDGED'
            }
        else:
            return {
                'success': False,
                'index': idx,
                'code': data['code'],
                'error': message or 'Error processing payment acknowledgment'
            }
    
    def _process_batch_consolidation(self, idx, item, user, request):
        """Process individual consolidation in batch"""
        serializer = PaymentBatchConsolidationSerializer(data=item)
        if not serializer.is_valid():
            return {
                'success': False,
                'index': idx,
                'code': item.get('code', 'unknown'),
                'error': str(serializer.errors)
            }
        
        data = serializer.validated_data
        # Get application name from the request
        application_name = request.auth.application.name if request.auth else None
        
        success, benefit, message = PaymentApiService.update_payment_status(
            user,
            code=data['code'],
            status=data['status'],
            payment_agency_id=application_name,
            transaction_reference=data.get('transaction_reference'),
            transaction_date=data.get('transaction_date'),
            error_code=data.get('error_code'),
            message=data.get('message')
        )
        
        if success:
            return {
                'success': True,
                'index': idx,
                'code': data['code'],
                'status': data['status']
            }
        else:
            return {
                'success': False,
                'index': idx,
                'code': data['code'],
                'error': message or 'Error processing payment status update'
            }
    
    @action(detail=False, methods=['get'], url_path='stats')
    def stats(self, request):
        """
        GET: Get statistics about payment requests
        """
        try:
            
            application_name = request.auth.application.name
            
            # Get payment requests
            payment_requests = PaymentApiService.get_individual_payment_requests(
                payment_provider_id=application_name
            )
            
            # Calculate statistics
            total_count = payment_requests.count()
            total_amount = sum(req.amount or 0 for req in payment_requests)
            
            # Count by status from acknowledgments
            acknowledged = 0
            rejected = 0
            
            for req in payment_requests:
                if not req.json_ext or 'payment_provider' not in req.json_ext:
                    continue
                    
                if req.json_ext['payment_provider'].get('acknowledgment_status') == 'ACCEPTED':
                    acknowledged += 1
                elif req.json_ext['payment_provider'].get('acknowledgment_status') == 'REJECTED':
                    rejected += 1
            
            # Count by payment status
            paid = BenefitConsumption.objects.filter(
                status=BenefitConsumptionStatus.RECONCILED
            ).count()
            
            return Response({
                'total_requests': total_count,
                'total_amount': float(total_amount),
                'acknowledged': acknowledged,
                'rejected': rejected,
                'pending': total_count - acknowledged - rejected,
                'paid': paid,
                'failed': rejected
            })
            
        except Exception as e:
            logger.error(f"Error retrieving payment stats: {str(e)}")
            return Response(
                {
                    'status': 'FAILURE',
                    'error_code': 'stats_error',
                    'message': 'Error retrieving statistics'
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

def _run_generate_cards_command(location_type, location_id, location_name):
    """Run the generate_cards command in a background thread"""
    output_file = f"{location_type}_{location_id}_{date.today().strftime('%Y%m%d')}.pdf"
    
    if location_type == 'province':
        cmd = ['python', 'manage.py', 'generate_cards', '--province', location_name, '--output', output_file]
    elif location_type == 'commune':
        cmd = ['python', 'manage.py', 'generate_cards', '--commune', location_name, '--output', output_file]
    elif location_type == 'colline':
        cmd = ['python', 'manage.py', 'generate_cards', '--colline', location_name, '--output', output_file]
    else:
        return
    
    # Run the command in a background process
    subprocess.Popen(cmd, 
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=settings.BASE_DIR)

def trigger_background_card_generation(request, location_id, location_type=None):
    """View for triggering card generation as a background task"""
    try:
        from location.models import Location
        
        location = Location.objects.get(id=location_id)
        location_name = location.name
        
        # If location type is not specified, determine it from the database
        if not location_type:
            location_type = location.type
            if location_type == 'D':
                location_type = 'province'
            elif location_type == 'W':
                location_type = 'commune'
            elif location_type == 'V':
                location_type = 'colline'
            else:
                return JsonResponse({
                    "success": False,
                    "message": f"Unsupported location type: {location_type}"
                }, status=400)
                
        # Start background task
        thread = threading.Thread(
            target=_run_generate_cards_command,
            args=(location_type, location_id, location_name)
        )
        thread.daemon = True
        thread.start()
        
        return JsonResponse({
            "success": True,
            "message": f"Card generation for {location_type} {location_name} started in the background. The PDF will be available shortly.",
            "location_id": location_id,
            "location_name": location_name,
            "location_type": location_type
        })
        
    except Exception as e:
        return JsonResponse({
            "success": False,
            "message": f"Error starting background card generation: {str(e)}"
        }, status=500)


class MonetaryTransferViewSet(viewsets.ViewSet):
    """
    ViewSet for MonetaryTransfer Excel import/export operations
    """
    permission_classes = [IsAuthenticated]
    
    @action(detail=False, methods=['post'], url_path='import')
    def import_excel(self, request):
        """
        POST: Import MonetaryTransfer data from Excel file
        """
        try:
            # Check if file is included in request
            if 'file' not in request.FILES:
                return Response(
                    {
                        'success': False,
                        'error': 'No file uploaded'
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            file = request.FILES['file']
            
            # Process import
            result = MonetaryTransferImportService.import_from_excel(
                file=file,
                user=request.user
            )
            
            # Return result
            return Response(result, status=status.HTTP_200_OK if result['success'] else status.HTTP_400_BAD_REQUEST)
            
        except Exception as e:
            logger.error(f"Error in MonetaryTransfer import: {str(e)}")
            return Response(
                {
                    'success': False,
                    'error': str(e)
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'], url_path='export')
    def export_excel(self, request):
        """
        GET: Export MonetaryTransfer data to Excel
        Query params: start_date, end_date, location_id, programme_id, payment_agency_id
        """
        try:
            # Get filters from query params
            filters = {
                'start_date': request.query_params.get('start_date'),
                'end_date': request.query_params.get('end_date'),
                'location_id': request.query_params.get('location_id'),
                'programme_id': request.query_params.get('programme_id'),
                'payment_agency_id': request.query_params.get('payment_agency_id'),
            }
            
            # Remove None values
            filters = {k: v for k, v in filters.items() if v is not None}
            
            # Generate Excel file
            response = MonetaryTransferImportService.export_to_excel(filters=filters)
            
            return response
            
        except Exception as e:
            logger.error(f"Error in MonetaryTransfer export: {str(e)}")
            return Response(
                {
                    'success': False,
                    'error': str(e)
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'], url_path='template')
    def download_template(self, request):
        """
        GET: Download Excel template for import
        """
        try:
            response = MonetaryTransferImportService.get_import_template()
            return response
            
        except Exception as e:
            logger.error(f"Error generating template: {str(e)}")
            return Response(
                {
                    'success': False,
                    'error': str(e)
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class GroupBeneficiaryCheckViewSet(viewsets.ViewSet):
    """
    API endpoint to check if a household code exists and is registered as a beneficiary household
    """
    permission_classes = [TokenHasScope]
    required_scopes = ['beneficiary:status_check']
    
    def get_required_scopes(self, request):
        """Return appropriate scopes based on request method"""
        # Only require the beneficiary:status_check scope for all operations
        return ['beneficiary:status_check']
    
    @action(detail=False, methods=['post'], url_path='check')
    def check_group_beneficiary(self, request):
        """
        POST: Check if a household with given socialID exists and is registered as a beneficiary household
        Returns true if the household exists and is registered as a beneficiary, false otherwise
        
        Request body:
        - socialID: The household's social ID (required)
        - colline: Filter by colline (household location name) (optional)
        - commune: Filter by commune (colline's parent name) (optional)
        - firstname: Filter by primary recipient's first name (optional)
        - lastname: Filter by primary recipient's last name (optional)
        
        Example: POST /api/group-beneficiary/check/
        Body: {"socialID": "BDI001234", "colline": "Rohero", "commune": "Mukaza"}
        Response: {"exists": true, "socialID": "BDI001234"}
        """
        try:
            # Get socialID from request body
            social_id = request.data.get('socialID')
            if not social_id:
                return Response(
                    {
                        "error": "socialID is required",
                        "exists": False
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            # Start with base query
            query = Beneficiary.objects.filter(group__code=social_id)
            
            # Apply optional filters from request body
            colline = request.data.get('colline')
            if colline:
                query = query.filter(group__location__name__iexact=colline)
            
            commune = request.data.get('commune')
            if commune:
                query = query.filter(group__location__parent__name__iexact=commune)
            
            firstname = request.data.get('firstname')
            if firstname:
                query = query.filter(
                    group__groupindividuals__recipient_type='PRIMARY',
                    group__groupindividuals__individual__first_name__iexact=firstname
                )
            
            lastname = request.data.get('lastname')
            if lastname:
                query = query.filter(
                    group__groupindividuals__recipient_type='PRIMARY',
                    group__groupindividuals__individual__last_name__iexact=lastname
                )
            
            # Check if any record exists with all the filters
            exists = query.exists()
            
            return Response({
                "exists": exists,
                "socialID": social_id
            })
            
        except Exception as e:
            logger.error(f"Error checking beneficiary household for socialID {request.data.get('socialID')}: {str(e)}")
            return Response(
                {
                    "error": "Error checking beneficiary household",
                    "socialID": request.data.get('socialID'),
                    "exists": False
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )