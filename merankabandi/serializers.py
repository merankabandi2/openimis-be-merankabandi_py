from payroll.models import BenefitConsumption
from rest_framework import serializers
from django.urls import reverse
import logging
from datetime import datetime

from social_protection.models import GroupBeneficiary

logger = logging.getLogger(__name__)

class BeneficiaryPhoneDataSerializer(serializers.ModelSerializer):
    """
    Serializer for retrieving beneficiary data for phone number attribution.
    """
    photo = serializers.SerializerMethodField()
    ci_recto = serializers.SerializerMethodField()
    ci_verso = serializers.SerializerMethodField()
    niveau1_label = serializers.SerializerMethodField()
    niveau2_label = serializers.SerializerMethodField()
    niveau3_label = serializers.SerializerMethodField()
    nom = serializers.SerializerMethodField()
    prenom = serializers.SerializerMethodField()
    pere = serializers.SerializerMethodField()
    mere = serializers.SerializerMethodField()
    date_naissance = serializers.SerializerMethodField()
    genre = serializers.SerializerMethodField()
    cni = serializers.SerializerMethodField()
    socialid = serializers.SerializerMethodField()

    class Meta:
        model = GroupBeneficiary
        fields = [
            'photo', 'ci_recto', 'ci_verso', 'niveau1_label', 'niveau2_label', 'niveau3_label',
            'nom', 'prenom', 'pere', 'mere', 'date_naissance', 'genre', 'cni', 'socialid'
        ]

    def get_recipient(self, obj):
        """Helper method to get primary recipient individual"""
        try:
            recipient = (obj.group.groupindividuals
                        .filter(recipient_type='PRIMARY')
                        .select_related('individual')
                        .first())
            return recipient.individual if recipient else None
        except Exception as e:
            logger.error(f"Error getting recipient: {str(e)}")
            return None

    def get_photo(self, obj):
        recipient = self.get_recipient(obj)
        if recipient:
            url = reverse('beneficiary_photo', kwargs={
                'type': 'photo',
                'id': str(recipient.id)
            })
            return self.context['request'].build_absolute_uri(url)
        return None

    def get_ci_recto(self, obj):
        recipient = self.get_recipient(obj)
        if recipient:
            url = reverse('beneficiary_photo', kwargs={
                'type': 'photo_ci1',
                'id': str(recipient.id)
            })
            return self.context['request'].build_absolute_uri(url)
        return None

    def get_ci_verso(self, obj):
        recipient = self.get_recipient(obj)
        if recipient:
            url = reverse('beneficiary_photo', kwargs={
                'type': 'photo_ci2',
                'id': str(recipient.id)
            })
            return self.context['request'].build_absolute_uri(url)
        return None

    def get_niveau3_label(self, obj):
        """Get colline name"""
        return obj.group.location.name if obj.group.location else None

    def get_niveau2_label(self, obj):
        """Get commune name"""
        try:
            return obj.group.location.parent.name if obj.group.location and obj.group.location.parent else None
        except Exception as e:
            logger.error(f"Error getting commune: {str(e)}")
            return None

    def get_niveau1_label(self, obj):
        """Get province name"""
        try:
            location = obj.group.location
            if location and location.parent and location.parent.parent:
                return location.parent.parent.name
            return None
        except Exception as e:
            logger.error(f"Error getting province: {str(e)}")
            return None

    def get_nom(self, obj):
        recipient = self.get_recipient(obj)
        return recipient.last_name if recipient else None

    def get_prenom(self, obj):
        recipient = self.get_recipient(obj)
        return recipient.first_name if recipient else None

    def get_date_naissance(self, obj):
        recipient = self.get_recipient(obj)
        return recipient.dob if recipient else None

    def get_pere(self, obj):
        recipient = self.get_recipient(obj)
        if recipient and recipient.json_ext and 'pere' in recipient.json_ext:
            return recipient.json_ext.get('pere')
        return None

    def get_mere(self, obj):
        recipient = self.get_recipient(obj)
        if recipient and recipient.json_ext and 'mere' in recipient.json_ext:
            return recipient.json_ext.get('mere')
        return None

    def get_genre(self, obj):
        recipient = self.get_recipient(obj)
        if recipient and recipient.json_ext and 'sexe' in recipient.json_ext:
            return recipient.json_ext.get('sexe')
        return None

    def get_cni(self, obj):
        recipient = self.get_recipient(obj)
        if recipient and recipient.json_ext and 'ci' in recipient.json_ext:
            return recipient.json_ext.get('ci')
        return None

    def get_socialid(self, obj):
        return obj.group.code



class PhoneNumberAttributionSerializer(serializers.Serializer):
    """
    Serializer for phone number attribution/verification.
    """
    cni = serializers.CharField(required=True)
    socialid = serializers.CharField(required=True)
    msisdn = serializers.CharField(required=True)
    status = serializers.ChoiceField(choices=['ACCEPTED', 'REJECTED'], required=True)
    error_code = serializers.CharField(required=False, allow_blank=True)
    error_message = serializers.CharField(required=False, allow_blank=True)

    def validate(self, data):
        # Validate that error_code and error_message are provided if status is REJECTED
        if data.get('status') == 'REJECTED':
            if not data.get('error_code'):
                raise serializers.ValidationError("error_code is required when status is REJECTED")
            if not data.get('error_message'):
                raise serializers.ValidationError("error_message is required when status is REJECTED")
        return data
        
    def save(self, beneficiary):
        """Save phone number attribution data to beneficiary"""
        try:
            data = self.validated_data
            json_ext = beneficiary.json_ext or {}
            
            # Ensure nested structure exists
            if 'moyen_telecom' not in json_ext:
                json_ext['moyen_telecom'] = {}
                
            # Update phone number info
            json_ext['moyen_telecom']['phoneNumber'] = data['msisdn']
            json_ext['moyen_telecom']['status'] = data['status']
            
            if data['status'] == 'REJECTED':
                json_ext['moyen_telecom']['error_code'] = data['error_code']
                json_ext['moyen_telecom']['error_message'] = data['error_message']
            
            beneficiary.json_ext = json_ext
            beneficiary.save()
            return True
        except Exception as e:
            logger.error(f"Error saving phone attribution data: {str(e)}")
            return False


class PhoneNumberAttributionRequestSerializer(serializers.Serializer):
    """
    Serializer for handling phone number attribution requests.
    """
    cni = serializers.CharField(required=True)
    socialid = serializers.CharField(required=True)
    msisdn = serializers.CharField(required=False)
    status = serializers.ChoiceField(choices=['ACCEPTED', 'REJECTED', 'SUCCESS', 'FAILURE'], required=True)
    error_code = serializers.CharField(required=False, allow_blank=True)
    error_message = serializers.CharField(required=False, allow_blank=True)

    def validate(self, data):
        """
        Validate the phone number attribution data.
        Ensure error details are provided when status is REJECTED.
        """
        if data.get('status') == 'REJECTED':
            if not data.get('error_code'):
                raise serializers.ValidationError("error_code is required when status is REJECTED")
            if not data.get('error_message'):
                raise serializers.ValidationError("error_message is required when status is REJECTED")
        if data.get('status') == 'SUCCESS':
            if not data.get('msisdn'):
                raise serializers.ValidationError("msisdn is required when status is SUCCESS")
        return data


class PaymentAccountAttributionListSerializer(BeneficiaryPhoneDataSerializer):
    """
    Serializer for listing beneficiary data for payment account attribution.
    """
    msisdn = serializers.SerializerMethodField()

    class Meta:
        model = GroupBeneficiary
        fields = [
            'photo', 'ci_recto', 'ci_verso', 'niveau1_label', 'niveau2_label', 'niveau3_label',
            'nom', 'prenom', 'pere', 'mere', 'date_naissance', 'genre', 'cni', 'socialid', 'msisdn'
        ]

    def get_msisdn(self, obj):
        if obj.json_ext and 'moyen_telecom' in obj.json_ext:
            return obj.json_ext.get('moyen_telecom').get('msisdn')
        return None


class PaymentAccountAcknowledgmentSerializer(serializers.Serializer):
    """
    Serializer for acknowledging receipt of beneficiary data for account attribution.
    """
    cni = serializers.CharField(required=True)
    socialid = serializers.CharField(required=True)
    msisdn = serializers.CharField(required=True)
    status = serializers.ChoiceField(choices=['ACCEPTED', 'REJECTED'], required=True)
    error_code = serializers.CharField(required=False, allow_blank=True)
    error_message = serializers.CharField(required=False, allow_blank=True)

    def validate(self, data):
        # Validate that error_code and error_message are provided if status is REJECTED
        if data.get('status') == 'REJECTED':
            if not data.get('error_code'):
                raise serializers.ValidationError("error_code is required when status is REJECTED")
            if not data.get('error_message'):
                raise serializers.ValidationError("error_message is required when status is REJECTED")
        return data
        
    def save(self, beneficiary):
        """Save acknowledgment data to beneficiary"""
        try:
            data = self.validated_data
            json_ext = beneficiary.json_ext or {}
            
            # Ensure phone number is also saved to moyen_paiement
            if 'moyen_paiement' not in json_ext:
                json_ext['moyen_paiement'] = {}
                
            #json_ext['moyen_paiement']['phoneNumber'] = data['msisdn']
            json_ext['moyen_paiement']['status'] = data['status']
            
            if data['status'] == 'REJECTED':
                json_ext['moyen_paiement']['error_code'] = data['error_code']
                json_ext['moyen_paiement']['error_message'] = data['error_message']
            
            beneficiary.json_ext = json_ext
            beneficiary.save()
            return True
        except Exception as e:
            logger.error(f"Error saving acknowledgment data: {str(e)}")
            return False


class PaymentAccountAttributionSerializer(serializers.Serializer):
    """
    Serializer for attributing payment account to beneficiary.
    """
    cni = serializers.CharField(required=True)
    socialid = serializers.CharField(required=True)
    msisdn = serializers.CharField(required=True)
    tp_account_number = serializers.CharField(required=True)
    status = serializers.ChoiceField(choices=['SUCCESS', 'FAILURE'], required=True)
    error_code = serializers.CharField(required=False, allow_blank=True)
    error_message = serializers.CharField(required=False, allow_blank=True)

    def validate(self, data):
        # Validate that error_code and error_message are provided if status is FAILURE
        if data.get('status') == 'FAILURE':
            if not data.get('error_code'):
                raise serializers.ValidationError("error_code is required when status is FAILURE")
            if not data.get('error_message'):
                raise serializers.ValidationError("error_message is required when status is FAILURE")
        return data
        
    def save(self, beneficiary):
        """Save payment account attribution data to beneficiary"""
        try:
            data = self.validated_data
            json_ext = beneficiary.json_ext or {}
            
            # Ensure nested structure exists
            if 'moyen_paiement' not in json_ext:
                json_ext['moyen_paiement'] = {}
                
            # Update payment account info
            #json_ext['moyen_paiement']['phoneNumber'] = data['msisdn']
            json_ext['moyen_paiement']['tp_account_number'] = data['tp_account_number']
            json_ext['moyen_paiement']['status'] = data['status']
            
            if data['status'] == 'FAILURE':
                json_ext['moyen_paiement']['error_code'] = data['error_code']
                json_ext['moyen_paiement']['error_message'] = data['error_message']
            
            beneficiary.json_ext = json_ext
            beneficiary.save()
            return True
        except Exception as e:
            logger.error(f"Error saving payment account data: {str(e)}")
            return False


class ResponseSerializer(serializers.Serializer):
    """
    Common response serializer for acknowledgment and attribution operations.
    """
    status = serializers.ChoiceField(choices=['SUCCESS', 'FAILURE'])
    error_code = serializers.CharField(required=False, allow_null=True)
    error_message = serializers.CharField(required=False, allow_null=True)

class IndividualPaymentRequestSerializer(serializers.ModelSerializer):
    """
    Serializer for individual payment requests to be provided to payment agencies
    """
    prenom = serializers.SerializerMethodField()
    nom = serializers.SerializerMethodField()
    genre = serializers.SerializerMethodField()
    naissance_date = serializers.SerializerMethodField()
    num_cni = serializers.SerializerMethodField()
    code = serializers.SerializerMethodField()
    montant = serializers.SerializerMethodField()
    
    class Meta:
        model = BenefitConsumption
        fields = [
            'prenom', 'nom', 'num_cni', 'naissance_date', 
            'genre', 'code', 'montant'
        ]
    
    def get_prenom(self, obj):
        """Get first name"""
        return obj.individual.first_name if obj.individual else ""
    
    def get_nom(self, obj):
        """Get last name"""
        return obj.individual.last_name if obj.individual else ""
    
    def get_genre(self, obj):
        """Get gender from individual json_ext"""
        if obj.individual and obj.individual.json_ext and 'sexe' in obj.individual.json_ext:
            return obj.individual.json_ext.get('sexe')
        return ""
    
    def get_naissance_date(self, obj):
        """Get date of birth"""
        return obj.individual.dob if obj.individual and obj.individual.dob else None
    
    def get_num_cni(self, obj):
        """Get CNI from individual json_ext"""
        if obj.individual and obj.individual.json_ext and 'ci' in obj.individual.json_ext:
            return obj.individual.json_ext.get('ci')
        return ""
    
    def get_code(self, obj):
        """Get payment code"""
        return obj.code
    
    def get_montant(self, obj):
        """Get payment amount"""
        return str(obj.amount) if obj.amount else "0"


class PaymentAcknowledgmentSerializer(serializers.Serializer):
    """
    Serializer for payment request acknowledgment
    """
    code = serializers.CharField(required=True)
    payment_agency_id = serializers.CharField(required=True)
    status = serializers.ChoiceField(choices=['ACCEPTED', 'REJECTED'], required=True)
    status_code = serializers.CharField(required=False, allow_blank=True)
    error_message = serializers.CharField(required=False, allow_blank=True)
    
    def validate(self, data):
        """Validate the acknowledgment data"""
        # Ensure status_code and error_message are provided if status is REJECTED
        if data.get('status') == 'REJECTED':
            if not data.get('status_code'):
                raise serializers.ValidationError({
                    'status_code': 'Status code is required when status is REJECTED'
                })
        
        return data


class PaymentStatusUpdateSerializer(serializers.Serializer):
    """
    Serializer for payment status update
    """
    code = serializers.CharField(required=True)
    payment_agency_id = serializers.CharField(required=True)
    status = serializers.ChoiceField(choices=['PAID', 'FAILED'], required=True)
    transaction_reference = serializers.CharField(required=False, allow_blank=True)
    transaction_date = serializers.CharField(required=False, allow_blank=True)
    error_code = serializers.CharField(required=False, allow_blank=True)
    error_message = serializers.CharField(required=False, allow_blank=True)
    
    def validate(self, data):
        """Validate the payment status update data"""
        # Validate based on status
        if data.get('status') == 'PAID':
            if not data.get('transaction_reference'):
                raise serializers.ValidationError({
                    'transaction_reference': 'Transaction reference is required when status is PAID'
                })
                
            # Validate transaction date format if provided
            if data.get('transaction_date'):
                try:
                    # Try parsing the date
                    datetime.fromisoformat(data.get('transaction_date').replace('Z', '+00:00'))
                except ValueError:
                    raise serializers.ValidationError({
                        'transaction_date': 'Invalid date format. Use ISO 8601 format (e.g., 2023-01-01T12:00:00Z)'
                    })
        
        elif data.get('status') == 'FAILED':
            if not data.get('error_code'):
                raise serializers.ValidationError({
                    'error_code': 'Error code is required when status is FAILED'
                })
        
        return data

