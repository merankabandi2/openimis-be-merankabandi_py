from django.http import HttpResponse
from django.template.loader import render_to_string
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from social_protection.models import GroupBeneficiary as Beneficiary 
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
import logging

from .serializers import (
    PaymentAccountAcknowledgmentSerializer,
    PaymentAccountAttributionListSerializer,
    PaymentAccountAttributionSerializer,
    PhoneNumberAttributionRequestSerializer,
    BeneficiaryPhoneDataSerializer,
    ResponseSerializer
)
from .services import PaymentAccountAttributionService, PhoneNumberAttributionService

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
                margin: 0.5cm;
            }
            
            .card {
                height: 14cm;
                width: 19.5cm;
                margin: 0 auto 0 auto;
                page-break-inside: avoid;
                position: relative;
                font-family: Arial, sans-serif;
            }
            
            .card-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 12pt;
                font-weight: bold;
                margin-bottom: 0.5cm;
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
            }
            
            .logo, .photo {
                width: 1.5cm;
                height: 2cm;
                object-fit: contain;
                margin-left: 1cm;
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
        moyen_paiement = beneficiary.json_ext.get('moyen_telecom', '')

        colline = beneficiary.group.location
        
        logo_path = os.path.join(settings.STATIC_ROOT, 'merankabandi/logo.png')
        context = {
            'logo_url': self._get_image_data_url(logo_path),
            'photo_url': self._get_image_data_url(photo_path),
            'social_id': beneficiary.group.code,
            'individual': individual,
            'telephone': moyen_paiement.get('msisdn', '') if moyen_paiement else '',
            'date_enregistrement': moyen_paiement.get('responseDate', '2025-02-19') if moyen_paiement else '2025-02-19',
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
            json_ext__moyen_paiement__phoneNumber__isnull=False
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
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination

    def list(self, request):
        """
        GET: List beneficiaries requiring phone number verification.
        Optional filters: commune, programme
        """
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
        """
        # Validate request data
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
        success, beneficiary, error_message = PhoneNumberAttributionService.process_phone_attribution(
            request_serializer.validated_data,
            request.user
        )
        
        if not success:
            response_data = {
                'status': 'FAILURE',
                'error_code': 'processing_error',
                'message': error_message
            }
            
            # Determine appropriate status code
            if not beneficiary:
                response_status = status.HTTP_404_NOT_FOUND
            elif error_message and 'state' in error_message:
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
    permission_classes = [IsAuthenticated]
    pagination_class = StandardResultsSetPagination

    def list(self, request):
        """
        GET: List beneficiaries requiring payment account attribution.
        Optional filters: commune, programme
        """
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
        """
        # Determine operation type based on payload
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
            
    def handle_acknowledgment(self, request):
        """
        Handle acknowledgment of beneficiary data
        """
        # Validate request data
        serializer = PaymentAccountAcknowledgmentSerializer(data=request.data)
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
        success, beneficiary, error_message = PaymentAccountAttributionService.process_acknowledgment(
            serializer.validated_data
        )
        
        if not success:
            response_data = {
                'status': 'FAILURE',
                'error_code': 'processing_error',
                'message': error_message
            }
            
            # Determine appropriate status code
            if not beneficiary:
                response_status = status.HTTP_404_NOT_FOUND
            elif error_message and 'state' in error_message:
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
        serializer = PaymentAccountAttributionSerializer(data=request.data)
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
        success, beneficiary, error_message = PaymentAccountAttributionService.process_account_attribution(
            serializer.validated_data
        )
        
        if not success:
            response_data = {
                'status': 'FAILURE',
                'error_code': 'processing_error',
                'message': error_message
            }
            
            # Determine appropriate status code
            if not beneficiary:
                response_status = status.HTTP_404_NOT_FOUND
            elif error_message and 'state' in error_message:
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
