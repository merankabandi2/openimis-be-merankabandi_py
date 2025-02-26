import logging
import base64
import hashlib
import requests
from datetime import datetime
from typing import Optional, Dict, Any

from django.db import transaction
from django.db.models import Q, Sum, Count
from merankabandi.apps import MerankabandiConfig
from payroll.models import BenefitConsumption, BenefitConsumptionStatus, Payroll, PayrollStatus
from social_protection.models import GroupBeneficiary
from individual.models import GroupIndividual, Individual


logger = logging.getLogger(__name__)


class LumicashPaymentService:
    """Service for interacting with Lumicash Pay on Behalf API"""
    
    def __init__(self):
        self.base_url = MerankabandiConfig.lumicash_payment_service_base_url
        self.endpoint = MerankabandiConfig.lumicash_payment_service_endpoint
        self.basic_auth_user = MerankabandiConfig.lumicash_payment_service_basic_auth_user
        self.basic_auth_pass = MerankabandiConfig.lumicash_payment_service_basic_auth_pass
        self.partner_code = MerankabandiConfig.lumicash_payment_service_partner_code
        self.api_key = MerankabandiConfig.lumicash_payment_service_api_key

    def _generate_request_id(self) -> str:
        """Generate request ID in format PPPPyyMMddHHmmssfff"""
        timestamp = datetime.now().strftime("%y%m%d%H%M%S%f")[:17]  # Taking first 3 digits of microseconds
        return f"{self.partner_code}{timestamp}"

    def _format_amount(self, amount: float) -> str:
        """Format amount to ####.## format"""
        return "{:.2f}".format(amount)

    def _generate_signature(self, request_date: str, trans_amount: float, 
                          des_mobile: str, request_id: str) -> str:
        """Generate MD5 signature for the request"""
        formatted_amount = self._format_amount(trans_amount)
        raw_text = (
            self.api_key +
            request_date +
            formatted_amount +
            self.partner_code +
            des_mobile +
            request_id
        )
        return hashlib.md5(raw_text.encode()).hexdigest()

    def _get_auth_header(self) -> Dict[str, str]:
        """Generate Basic Auth header"""
        credentials = f"{self.basic_auth_user}:{self.basic_auth_pass}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/json'
        }

    def pay_on_behalf(self, 
                     des_mobile: str, 
                     trans_amount: float,
                     content: Optional[str] = None,
                     description: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a payment on behalf of partner to customer wallet
        
        Args:
            des_mobile: Destination phone number
            trans_amount: Amount to transfer
            content: Optional payment content
            description: Optional payment description
            
        Returns:
            Dict containing API response
            
        Raises:
            requests.exceptions.RequestException: If API call fails
            ValueError: If validation fails
        """
        try:
            # Input validation
            if not des_mobile or not trans_amount:
                raise ValueError("Destination mobile and amount are required")
            if trans_amount <= 0:
                raise ValueError("Amount must be greater than 0")

            # Generate request parameters
            request_date = datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]
            request_id = self._generate_request_id()
            formatted_amount = self._format_amount(trans_amount)

            # Generate signature
            signature = self._generate_signature(
                request_date=request_date,
                trans_amount=trans_amount,
                des_mobile=des_mobile,
                request_id=request_id
            )

            # Prepare request payload
            payload = {
                "RequestId": request_id,
                "RequestDate": request_date,
                "PartnerCode": self.partner_code,
                "DesMobile": des_mobile,
                "TransAmount": formatted_amount,
                "Content": content,
                "Description": description,
                "Signature": signature
            }

            # Make API call
            response = requests.post(
                f"{self.base_url}{self.endpoint}",
                headers=self._get_auth_header(),
                json=payload
            )
            
            response.raise_for_status()
            response_data = response.json()

            # Log transaction details
            logger.info(
                "Lumicash payment completed - RequestId: %s, TransCode: %s, Amount: %s",
                request_id,
                response_data.get('TransCode'),
                formatted_amount
            )

            return response_data

        except requests.exceptions.RequestException as e:
            logger.error("Lumicash API error - %s", str(e))
            raise

        except Exception as e:
            logger


class PaymentDataService:

    @classmethod
    def get_benefits_for_agence(cls, agence, commune, status):
        filters = Q(
            payrollbenefitconsumption__payroll__payment_point__name=agence,
            payroll__location__name=commune,
            is_deleted=False,
            status=status,
            payrollbenefitconsumption__is_deleted=False,
            payrollbenefitconsumption__payroll__is_deleted=False,
        )
        benefits = BenefitConsumption.objects.filter(filters).order_by('id')
        return benefits
    
    @classmethod
    def get_benefits_attached_to_payroll(cls, payroll, status):
        filters = Q(
            payrollbenefitconsumption__payroll_id=payroll.id,
            is_deleted=False,
            status=status,
            payrollbenefitconsumption__is_deleted=False,
            payrollbenefitconsumption__payroll__is_deleted=False,
        )
        benefits = BenefitConsumption.objects.filter(filters).order_by('id')
        return benefits
    
    def get_payment_data(cls, payroll, user):
        benefits = cls.get_benefits_attached_to_payroll(payroll, BenefitConsumptionStatus.ACCEPTED)
        payment_gateway_connector = cls.PAYMENT_GATEWAY
        benefits_to_approve = []
        for benefit in benefits:
            if payment_gateway_connector.send_payment(benefit.code, benefit.amount):
                benefits_to_approve.append(benefit)
            else:
                # Handle the case where a benefit payment is rejected
                logger.info(f"Payment for benefit ({benefit.code}) was rejected.")
        if benefits_to_approve:
            cls.approve_for_payment_benefit_consumption(benefits_to_approve, user)



class PhoneNumberAttributionService:
    """
    Service class for phone number attribution operations.
    """
    
    @staticmethod
    def find_beneficiary_by_identifiers(cni, socialid):
        """
        Find a beneficiary using CNI and Social ID
        
        Args:
            cni (str): National ID card number
            socialid (str): Social ID
            
        Returns:
            GroupBeneficiary or None: Found beneficiary or None
        """
        try:
            # Find individuals by CNI
            individuals = Individual.objects.filter(json_ext__ci=cni)
            
            # Find group beneficiaries by socialid
            group_beneficiaries = GroupBeneficiary.objects.filter(
                json_ext__social_id=socialid
            )
            
            # Get group beneficiaries where individual is a primary recipient
            for individual in individuals:
                group_individuals = GroupIndividual.objects.filter(
                    individual=individual,
                    recipient_type='PRIMARY'
                )
                
                for group_individual in group_individuals:
                    matching_beneficiaries = group_beneficiaries.filter(
                        group=group_individual.group
                    )
                    
                    if matching_beneficiaries.exists():
                        return matching_beneficiaries.first()
            
            return None
        except Exception as e:
            logger.error(f"Error finding beneficiary: {str(e)}")
            return None
    
    @classmethod
    @transaction.atomic
    def process_phone_attribution(cls, data, user):
        """
        Process phone number attribution request
        
        Args:
            data (dict): Validated phone attribution data
            
        Returns:
            tuple: (success (bool), beneficiary or None, error_message or None)
        """
        try:
            # Find the beneficiary
            beneficiary = cls.find_beneficiary_by_identifiers(
                data['cni'], 
                data['socialid']
            )
            
            if not beneficiary:
                return False, None, "Beneficiary not found"
            
            # Verify beneficiary is in the correct state
            if beneficiary.status != 'ACTIVE':
                return False, beneficiary, f"Invalid beneficiary state: {beneficiary.status}"
            
            # Update phone number data in json_ext
            json_ext = beneficiary.json_ext or {}
            
            # Ensure nested structure exists
            if 'moyen_telecom' not in json_ext or not json_ext['moyen_telecom']:
                json_ext['moyen_telecom'] = {}

            # Update phone number info
            json_ext['moyen_telecom']['status'] = data['status']
            if data['status'] == 'SUCCESS':
                json_ext['moyen_telecom']['msisdn'] = data['msisdn']
            
            if data['status'] in ['REJECTED', 'FAILURE']:
                json_ext['moyen_telecom']['error_code'] = data['error_code']
                json_ext['moyen_telecom']['error_message'] = data['error_message']
            
            beneficiary.json_ext = json_ext
            
            # Save changes
            beneficiary.save(user=user)
            
            return True, beneficiary, None
            
        except Exception as e:
            logger.error(f"Error in process_phone_attribution: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)
    
    @classmethod
    def get_pending_phone_verifications(cls, commune=None, programme=None):
        """
        Get all beneficiaries awaiting phone verification
        
        Args:
            commune (str, optional): Filter by commune name
            programme (str, optional): Filter by programme name
            
        Returns:
            QuerySet: Filtered queryset of beneficiaries
        """
        queryset = GroupBeneficiary.objects.filter(
            json_ext__moyen_telecom__msisdn__isnull=True
        ).select_related(
            'group', 
            'benefit_plan'
        ).prefetch_related(
            'group__groupindividuals', 
            'group__groupindividuals__individual',
            'group__location',
            'group__location__parent',
            'group__location__parent__parent'
        )
        
        # Apply filters if provided
        if commune:
            queryset = queryset.filter(
                group__location__parent__name__iexact=commune
            )
            
        if programme:
            queryset = queryset.filter(
                benefit_plan__name__iexact=programme
            )
            
        return queryset


class PaymentAccountAttributionService:
    """
    Service class for payment account attribution operations.
    """
    
    @staticmethod
    def find_beneficiary_by_identifiers(cni, socialid):
        """
        Find a beneficiary using CNI and Social ID
        
        Args:
            cni (str): National ID card number
            socialid (str): Social ID
            
        Returns:
            GroupBeneficiary or None: Found beneficiary or None
        """
        try:
            # Find individuals by CNI
            individuals = Individual.objects.filter(json_ext__ci=cni)
            
            # Find group beneficiaries by socialid
            group_beneficiaries = GroupBeneficiary.objects.filter(
                json_ext__socialid=socialid
            )
            
            # Get group beneficiaries where individual is a primary recipient
            for individual in individuals:
                group_individuals = GroupIndividual.objects.filter(
                    individual=individual,
                    recipient_type='PRIMARY'
                )
                
                for group_individual in group_individuals:
                    matching_beneficiaries = group_beneficiaries.filter(
                        group=group_individual.group
                    )
                    
                    if matching_beneficiaries.exists():
                        return matching_beneficiaries.first()
            
            return None
        except Exception as e:
            logger.error(f"Error finding beneficiary: {str(e)}")
            return None
    
    @classmethod
    def get_pending_account_attributions(cls, commune=None, programme=None):
        """
        Get all beneficiaries awaiting payment account attribution
        
        Args:
            commune (str, optional): Filter by commune name
            programme (str, optional): Filter by programme name
            
        Returns:
            QuerySet: Filtered queryset of beneficiaries
        """
        queryset = GroupBeneficiary.objects.filter(
            json_ext__moyen_telecom__msisdn__isnull=False,
            json_ext__moyen_telecom__status='SUCCESS'
        ).select_related(
            'group', 
            'benefit_plan'
        ).prefetch_related(
            'group__groupindividuals', 
            'group__groupindividuals__individual',
            'group__location',
            'group__location__parent',
            'group__location__parent__parent'
        )
        
        # Apply filters if provided
        if commune:
            queryset = queryset.filter(
                group__location__parent__name__iexact=commune
            )
            
        if programme:
            queryset = queryset.filter(
                benefit_plan__name__iexact=programme
            )
            
        return queryset
    
    @classmethod
    @transaction.atomic
    def process_acknowledgment(cls, data, user):
        """
        Process payment account acknowledgment request
        
        Args:
            data (dict): Validated acknowledgment data
            
        Returns:
            tuple: (success (bool), beneficiary or None, error_message or None)
        """
        try:
            # Find the beneficiary
            beneficiary = cls.find_beneficiary_by_identifiers(
                data['cni'], 
                data['socialid']
            )
            
            if not beneficiary:
                return False, None, "Beneficiary not found"
            
            # Verify beneficiary is in the correct state
            valid_states = ['PENDING_ACCOUNT_ATTRIBUTION', 'PHONE_VERIFIED']
            if beneficiary.status not in valid_states:
                return False, beneficiary, f"Invalid beneficiary state: {beneficiary.status}"
            
            # Update account acknowledgment data in json_ext
            json_ext = beneficiary.json_ext or {}
            
            # Ensure nested structure exists
            if 'moyen_paiement' not in json_ext or not json_ext['moyen_paiement']:
                json_ext['moyen_paiement'] = {}
                
            # Update payment account info
            json_ext['moyen_paiement']['phoneNumber'] = data['msisdn']
            json_ext['moyen_paiement']['status'] = data['status']
            
            if data['status'] == 'REJECTED':
                json_ext['moyen_paiement']['error_code'] = data['error_code']
                json_ext['moyen_paiement']['error_message'] = data['error_message']
            
            beneficiary.json_ext = json_ext
            
            # Update beneficiary status
            if data['status'] == 'ACCEPTED':
                beneficiary.status = 'PENDING_ACCOUNT_CREATION'
            else:
                beneficiary.status = 'ACCOUNT_ATTRIBUTION_REJECTED'
            
            # Save changes
            beneficiary.save(user=user)
            
            return True, beneficiary, None
            
        except Exception as e:
            logger.error(f"Error in process_acknowledgment: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)
    
    @classmethod
    @transaction.atomic
    def process_account_attribution(cls, data, user):
        """
        Process payment account attribution request
        
        Args:
            data (dict): Validated account attribution data
            
        Returns:
            tuple: (success (bool), beneficiary or None, error_message or None)
        """
        try:
            # Find the beneficiary
            beneficiary = cls.find_beneficiary_by_identifiers(
                data['cni'], 
                data['socialid']
            )
            
            if not beneficiary:
                return False, None, "Beneficiary not found"
            
            # Verify beneficiary is in the correct state
            if beneficiary.status != 'PENDING_ACCOUNT_CREATION':
                return False, beneficiary, f"Invalid beneficiary state: {beneficiary.status}"
            
            # Update account attribution data in json_ext
            json_ext = beneficiary.json_ext or {}
            
            # Ensure nested structure exists
            if 'moyen_paiement' not in json_ext or not json_ext['moyen_paiement']:
                json_ext['moyen_paiement'] = {}
                
            # Update payment account info
            json_ext['moyen_paiement']['phoneNumber'] = data['msisdn']
            json_ext['moyen_paiement']['tp_account_number'] = data['tp_account_number']
            json_ext['moyen_paiement']['status'] = data['status']
            
            if data['status'] == 'FAILURE':
                json_ext['moyen_paiement']['error_code'] = data['error_code']
                json_ext['moyen_paiement']['error_message'] = data['error_message']
            
            beneficiary.json_ext = json_ext
            
            # Save changes
            beneficiary.save(user=user)
            
            return True, beneficiary, None
            
        except Exception as e:
            logger.error(f"Error in process_account_attribution: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)


class PaymentApiService:
    """
    Service for payment API operations.
    Handles fetching payment requests, acknowledgment, and status updates.
    """
    
    @classmethod
    def get_individual_payment_requests(cls, payment_provider_id=None, payment_cycle_id=None, commune=None):
        """
        Get individual payment requests for payment provider.
        Returns payment requests awaiting payment for the current payment cycle.
        
        Args:
            payment_provider_id (int, optional): Filter by payment provider ID
            payment_cycle_id (int, optional): Filter by payment cycle ID
            commune (str, optional): Filter by commune name
            
        Returns:
            QuerySet: BenefitConsumption objects ready for payment
        """
        try:
            # Get payrolls that are approved for payment
            payroll_query = Payroll.objects.filter(status=PayrollStatus.APPROVE_FOR_PAYMENT)
            
            # Filter by payment provider if specified
            if payment_provider_id:
                payroll_query = payroll_query.filter(payment_point_id=payment_provider_id)
                
            # Filter by payment cycle if specified
            if payment_cycle_id:
                payroll_query = payroll_query.filter(payment_cycle_id=payment_cycle_id)
                
            # Filter by commune if specified
            if commune:
                payroll_query = payroll_query.filter(
                    Q(location__name__iexact=commune) | 
                    Q(location__code__iexact=commune)
                )
                
            # Get payroll IDs
            payroll_ids = payroll_query.values_list('id', flat=True)
            
            # Get benefit consumptions linked to these payrolls
            benefit_query = BenefitConsumption.objects.filter(
                payrollbenefitconsumption__payroll_id__in=payroll_ids,
                status=BenefitConsumptionStatus.ACCEPTED
            ).select_related('individual')
            
            return benefit_query
            
        except Exception as e:
            logger.error(f"Error getting individual payment requests: {str(e)}")
            return BenefitConsumption.objects.none()
    
    @classmethod
    def get_payment_request_by_code(cls, code):
        """
        Get an individual payment request by its code
        
        Args:
            code (str): Payment request code
            
        Returns:
            BenefitConsumption or None: The payment request or None if not found
        """
        try:
            return BenefitConsumption.objects.filter(
                code=code,
                status__in=[BenefitConsumptionStatus.ACCEPTED, BenefitConsumptionStatus.APPROVE_FOR_PAYMENT]
            ).select_related('individual').first()
        except Exception as e:
            logger.error(f"Error getting payment request by code: {str(e)}")
            return None
    
    @classmethod
    @transaction.atomic
    def acknowledge_payment_request(cls, user, code, status, error_code=None, error_message=None):
        """
        Acknowledge receipt of payment request by payment provider
        
        Args:
            code (str): Payment request code
            status (str): "ACCEPTED" or "REJECTED"
            error_code (str, optional): Error code if rejected
            error_message (str, optional): Error message if rejected
            
        Returns:
            tuple: (success (bool), benefit or None, error_message or None)
        """
        try:
            # Find the payment request
            benefit = BenefitConsumption.objects.filter(
                code=code,
                status=BenefitConsumptionStatus.APPROVE_FOR_PAYMENT
            ).select_related('individual').first()
            
            if not benefit:
                return False, None, f"Payment request with code {code} not found or not in valid state"
            
            benefit.status = status

            # Update payment request with acknowledgment info
            json_ext = benefit.json_ext or {}
            
            # Ensure payment_provider field exists
            if 'payment_provider' not in json_ext:
                json_ext['payment_provider'] = {}
                
            # Update acknowledgment info
            json_ext['payment_provider']['acknowledgment_status'] = status
            json_ext['payment_provider']['acknowledgment_date'] = datetime.now().isoformat()
            
            if status == 'REJECTED':
                if not error_code:
                    return False, benefit, "Status code is required when status is REJECTED"
                    
                json_ext['payment_provider']['error_code'] = error_code
                json_ext['payment_provider']['error_message'] = error_message or ""
                
                # Update benefit status to rejected
                
            # If accepted, keep the status as APPROVE_FOR_PAYMENT
            
            # Save changes
            benefit.json_ext = json_ext
            benefit.save(user=user)
            
            return True, benefit, None
            
        except Exception as e:
            logger.error(f"Error acknowledging payment request: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)
    
    @classmethod
    @transaction.atomic
    def update_payment_status(cls, user, code, status, transaction_reference=None,
                           transaction_date=None, error_code=None, error_message=None):
        """
        Update payment status after payment execution
        
        Args:
            code (str): Payment request code
            status (str): "PAID" or "FAILED"
            transaction_reference (str, optional): Transaction reference if paid
            transaction_date (str, optional): Transaction date if paid
            error_code (str, optional): Error code if failed
            error_message (str, optional): Error message if failed
            
        Returns:
            tuple: (success (bool), benefit or None, error_message or None)
        """
        try:
            # Find the payment request by code
            benefit = BenefitConsumption.objects.filter(
                code=code
            ).select_related('individual').first()
            
            if not benefit:
                return False, None, f"Payment request with code {code} not found"
                
            # Check if already reconciled
            if benefit.status == BenefitConsumptionStatus.RECONCILED:
                return False, benefit, f"Payment request already reconciled"
            
            json_ext = benefit.json_ext or {}

            # Update payment status
            if 'payment_execution' not in json_ext:
                json_ext['payment_execution'] = {}
                
            json_ext['payment_execution']['status'] = status
            json_ext['payment_execution']['date'] = datetime.now().isoformat()
            
            if status == 'PAID':
                if not transaction_reference:
                    return False, benefit, "Transaction reference is required when status is PAID"
                    
                json_ext['payment_execution']['transaction_reference'] = transaction_reference
                json_ext['payment_execution']['transaction_date'] = transaction_date or datetime.now().isoformat()
                
                # Update benefit status to reconciled
                benefit.status = BenefitConsumptionStatus.RECONCILED
                
            elif status == 'FAILED':
                if not error_code:
                    return False, benefit, "Error code is required when status is FAILED"
                    
                json_ext['payment_execution']['error_code'] = error_code
                json_ext['payment_execution']['error_message'] = error_message or ""
                
                # Update benefit status to rejected
                benefit.status = BenefitConsumptionStatus.REJECTED
                
            else:
                return False, benefit, f"Invalid status: {status}"
            
            # Save changes
            benefit.json_ext = json_ext
            benefit.save()
            
            return True, benefit, None
            
        except Exception as e:
            logger.error(f"Error updating payment status: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)