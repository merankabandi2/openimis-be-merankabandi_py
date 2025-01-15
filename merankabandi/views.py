from django.http import HttpResponse
from django.template.loader import render_to_string
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
from social_protection.models import GroupBeneficiary as Beneficiary 
from individual.models import GroupIndividual, Individual
from django.http import FileResponse, HttpResponseForbidden
import os
from django.contrib.auth.decorators import login_required


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
                width: 19.5cm;  /* A4 width - 2cm margins */
                margin: 0 auto 0 auto;
                page-break-inside: avoid;
                position: relative;
                font-family: Arial, sans-serif;
            }
            
            .card-header {
                text-align: center;
                font-size: 16pt;
                font-weight: bold;
                margin-bottom: 0.5cm;
            }

            .card-content {
                display: flex;
                justify-content: space-between;
                margin-bottom: 0.5cm;
            }

            .social-id {
                font-weight: bold;
                text-align: center;
                font-size: 24pt;
                margin-bottom: 0.5cm;
            }
            
            .logo, .photo {
                width: 1.5cm;
                height: 2cm;
                object-fit: contain;
                floar: right;
            }
            
            .fields {
                margin-top: 1cm;
            }

            .field {
                margin-bottom: 0.4cm;
                display: flex;
                align-items: center;
            }
            
            .field-label {
                display: inline-block;
                width: 6cm;
            }
            
            .field-value {
                font-weight: bold;
            }
            
            .declaration {
                font-size: 10pt;
            }
            
            /* Back side styling */
            .card-back {
                text-align: center;
                page-break-before: always;
            }
            .bullet-points {
                text-align: left;
                margin-top: 1cm;
                padding-left: 1cm;
            }
        ''')

    def _get_image_url(self, request, image_path):
        """Get image URL - in this case returning the direct URL"""
        current_site = request.build_absolute_uri('/').rstrip('/')
        return f"{current_site}/{image_path}"

    def generate_card_html(self, request, beneficiary):
        current_site = request.build_absolute_uri('/').rstrip('/')
        photo_url = f"{current_site}/api/merankabandi/beneficiary-photo/photo/{individual.id}/"

        """Generate HTML for beneficiary card"""
        individual = beneficiary.group.groupindividuals.get(role=GroupIndividual.Role.HEAD).individual
        colinne = beneficiary.group.location
        
        context = {
            'logo_url': self._get_image_url(request, 'front/static/media/openIMIS.18731b76.png'),
            'photo_url': photo_url,
            'social_id': beneficiary.group.code,
            'individual': individual,
            'province': colinne.parent.parent.name,
            'commune': colinne.parent.name,
            'colline': colinne.name,
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

def generate_colline_cards_view(request, benefit_plan_id, colline_id):
    """View for generating cards for all beneficiaries in a colline"""
    try:
        beneficiaries = Beneficiary.objects.filter(
            benefit_plan=benefit_plan_id, 
            location=colline_id
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
        response['Content-Disposition'] = f'attachment; filename="colline_{colline_id}_cards.pdf"'
        response.write(pdf)
        
        return response
        
    except Exception as e:
        return HttpResponse(f"Error generating cards: {str(e)}", status=500)  

def beneficiary_photo_view(request, type, id):
    individual = Individual.objects.get(id=id)
    household = individual.groupindividuals.get().group
    base_dir = os.path.join('../../photos', str(household.json_ext.get('deviceid', '')), str(household.json_ext.get('date_collecte', '')).replace('-', ''))
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