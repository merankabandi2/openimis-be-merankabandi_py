import logging
import base64
import hashlib
from merankabandi.models import (
    MonetaryTransfer, Section, Indicator, IndicatorAchievement, ProvincePaymentPoint,
    SensitizationTraining, BehaviorChangePromotion, MicroProject
)
from merankabandi.validation import (
    MonetaryTransferValidation, SectionValidation, IndicatorValidation, 
    IndicatorAchievementValidation, ProvincePaymentPointValidation
)
import requests
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd
from django.http import HttpResponse
from io import BytesIO
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows
import uuid

from django.db import transaction
from core.services import BaseService
from core.services.utils import model_representation, output_result_success
from core.signals import register_service_signal
from django.db.models import Q
from merankabandi.apps import MerankabandiConfig
from payroll.models import BenefitConsumption, BenefitConsumptionStatus, Payroll, PayrollStatus
from individual.models import GroupIndividual, Individual
from location.models import Location
from payment_cycle.models import PaymentCycle
from payroll.services import PayrollService
from payroll.models import PaymentPoint
from social_protection.models import GroupBeneficiary, BenefitPlan
from contribution_plan.models import PaymentPlan
from dateutil.relativedelta import relativedelta
from django.utils import timezone

logger = logging.getLogger(__name__)

class PayrollGenerationService:
    """
    Service for generating payrolls across multiple communes in a province
    """
    
    def __init__(self, user):
        self.user = user
    
    def generate_province_payroll(self, province_id, payment_plan_id, payment_date):
        """
        Generate payroll for all communes in a province that have active beneficiaries
        
        Args:
            province_id (str): UUID of the province location
            payment_plan_id (str): UUID of the benefit plan
            payment_date (date): Payment date for the payroll
            
        Returns:
            dict: Summary of generated payrolls
        """
        
        try:
            # Validate province
            province = Location.objects.filter(id=province_id, type='D').first()
            if not province:
                return {
                    'success': False,
                    'error': 'Province not found or invalid',
                    'generated_payrolls': []
                }
                
            # Validate payment plan
            payment_plan = PaymentPlan.objects.filter(id=payment_plan_id).first()
            benefit_plan_id = payment_plan.benefit_plan_id
            benefit_plan = payment_plan.benefit_plan
            
            if not payment_plan:
                return {
                    'success': False,
                    'error': 'No payment plan found or invalid',
                    'generated_payrolls': []
                }
            
            # Get the payment point associated with this province and payment plan
            province_payment_point = ProvincePaymentPoint.objects.filter(
                province_id=province_id, 
                payment_plan_id=payment_plan_id
            ).first()
            
            payment_point = province_payment_point.payment_point if province_payment_point else None
            if not payment_point:
                return {
                    'success': False,
                    'error': 'No payment point found or invalid',
                    'generated_payrolls': []
                }
            
            # Find the appropriate payment cycle for the given date
            payment_cycle = PaymentCycle.objects.filter(
                start_date__lte=payment_date,
                end_date__gte=payment_date,
                status='ACTIVE'
            ).first()
            
            if not payment_cycle:
                return {
                    'success': False,
                    'error': 'No active payment cycle found for the specified date',
                    'generated_payrolls': []
                }
            
            # Get all communes in this province with beneficiary counts in a single query
            from django.db.models import Count
            communes_with_counts = Location.objects.filter(
                parent=province, 
                type='W'
            ).annotate(
                beneficiary_count=Count(
                    'children__groups__groupbeneficiary',
                    filter=Q(
                        children__groups__groupbeneficiary__benefit_plan_id=benefit_plan_id,
                        children__groups__groupbeneficiary__status='ACTIVE',
                        children__groups__groupbeneficiary__is_deleted=False
                    )
                )
            ).filter(beneficiary_count__gt=0)
            
            # Convert to the expected format
            communes_with_beneficiaries = [
                {
                    'commune': commune,
                    'beneficiary_count': commune.beneficiary_count
                } 
                for commune in communes_with_counts
            ]
            
            # No eligible communes found
            if not communes_with_beneficiaries:
                return {
                    'success': False,
                    'error': 'No communes found with eligible beneficiaries',
                    'generated_payrolls': []
                }
            
            # Generate payroll for each eligible commune
            payroll_service = PayrollService(self.user)
            generated_payrolls = []
            
            for commune_data in communes_with_beneficiaries:
                commune = commune_data['commune']
                
                # Prepare payroll data
                # Import relativedelta for date calculations
                
                payroll_data = {
                    'name': f"Demande de paiement du {payment_date.strftime('%d/%m/%Y')} pour la commune de {commune.name}",
                    'payment_cycle_id': payment_cycle.id,
                    'payment_plan_id': payment_plan.id,
                    'payment_point_id': payment_point.id,
                    'location_id': commune.id,
                    'date_valid_from': payment_date,
                    'date_valid_to': payment_date + relativedelta(months=1)
                }
                
                # Create the payroll
                try:
                    result = payroll_service.create(payroll_data)
                    if 'data' in result and 'id' in result['data']:
                        generated_payrolls.append({
                            'commune_id': str(commune.id),
                            'commune_name': commune.name,
                            'payroll_id': result['data']['id'],
                            'beneficiary_count': commune_data['beneficiary_count'],
                            'name': f"Demande de paiement du {payment_date.strftime('%d/%m/%Y')} pour la commune de {commune.name}",
                        })
                    else:
                        logger.error(f"Failed to create payroll for commune {commune.name}: {result}")
                except Exception as e:
                    logger.error(f"Error creating payroll for commune {commune.name}: {str(e)}")
            
            # Return summary
            return {
                'success': True,
                'payment_cycle_id': str(payment_cycle.id),
                'payment_cycle_code': payment_cycle.code,
                'payment_date': payment_date.isoformat(),
                'province_id': str(province.id),
                'province_name': province.name,
                'benefit_plan_id': str(benefit_plan.id),
                'benefit_plan_name': benefit_plan.name,
                'generated_payrolls': generated_payrolls,
                'total_payrolls': len(generated_payrolls),
                'total_beneficiaries': sum(p['beneficiary_count'] for p in generated_payrolls)
            }
            
        except Exception as e:
            logger.error(f"Error generating province-wide payroll: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'generated_payrolls': []
            }


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
            # Single optimized query combining all conditions
            beneficiary = GroupBeneficiary.objects.filter(
                group__code=socialid,
                group__groupindividuals__recipient_type='PRIMARY',
                group__groupindividuals__individual__json_ext__ci=cni
            ).select_related(
                'group',
                'benefit_plan'
            ).prefetch_related(
                'group__groupindividuals__individual'
            ).first()
            
            return beneficiary
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
            tuple: (success (bool), beneficiary or None, message or None)
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

            json_ext['moyen_telecom']['agence'] = data['agence']
            # Update phone number info
            json_ext['moyen_telecom']['status'] = data['status']
            if data['status'] == 'SUCCESS':
                json_ext['moyen_telecom']['msisdn'] = data['msisdn']
            
            if data['status'] in ['REJECTED', 'FAILURE']:
                json_ext['moyen_telecom']['error_code'] = data['error_code']
                json_ext['moyen_telecom']['message'] = data['message']
            
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
    def get_system_user():
        """Get or create system user for API operations"""
        from django.contrib.auth import get_user_model
        User = get_user_model()
        user = User.objects.filter(username='user1').first()
        return user
    
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
            # Single optimized query combining all conditions
            beneficiary = GroupBeneficiary.objects.filter(
                group__code=socialid,
                group__groupindividuals__recipient_type='PRIMARY',
                group__groupindividuals__individual__json_ext__ci=cni
            ).select_related(
                'group',
                'benefit_plan'
            ).prefetch_related(
                'group__groupindividuals__individual'
            ).first()
            
            return beneficiary
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
            json_ext__moyen_telecom__status='SUCCESS',
        ).exclude(
            # Exclude only ACCEPTED accounts (allow REJECTED to be reprocessed)
            Q(json_ext__moyen_paiement__status__isnull=False) & Q(json_ext__moyen_paiement__status='ACCEPTED')
        ).exclude(
            # Exclude accounts that already have been created (SUCCESS)
            Q(json_ext__moyen_paiement__status__isnull=False) & Q(json_ext__moyen_paiement__status='SUCCESS')
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
    def process_acknowledgment(cls, data, user=None):
        """
        Process payment account acknowledgment request
        
        Args:
            data (dict): Validated acknowledgment data
            user: User performing the operation (optional, will use system user if not provided)
            
        Returns:
            tuple: (success (bool), beneficiary or None, message or None)
        """
        try:
            # Get system user if no user provided
            if not user:
                user = cls.get_system_user()
            
            # Find the beneficiary
            beneficiary = cls.find_beneficiary_by_identifiers(
                data['cni'], 
                data['socialid']
            )
            
            if not beneficiary:
                return False, None, "Beneficiary not found"
            
            # Verify beneficiary is in the correct state
            valid_states = ['VALIDATED', 'POTENTIAL', 'ACTIVE']
            if beneficiary.status not in valid_states:
                return False, beneficiary, f"Invalid beneficiary state: {beneficiary.status}"
            
            # Update account acknowledgment data in json_ext
            json_ext = beneficiary.json_ext or {}
            
            # Check if already acknowledged
            if 'moyen_paiement' in json_ext and json_ext['moyen_paiement']:
                current_status = json_ext['moyen_paiement'].get('status')
                if current_status == 'ACCEPTED':
                    return False, beneficiary, "This record has already been acknowledged with status ACCEPTED"
                elif current_status == 'REJECTED':
                    # Allow re-processing of rejected accounts
                    logger.info(f"Re-processing previously rejected account for socialid: {data['socialid']}")
                elif current_status == 'SUCCESS':
                    return False, beneficiary, "This record already has a payment account created"
            
            # Ensure nested structure exists
            if 'moyen_paiement' not in json_ext or not json_ext['moyen_paiement']:
                json_ext['moyen_paiement'] = {}
            
            # Update payment account info
            json_ext['moyen_paiement']['agence'] = data['agence']
            json_ext['moyen_paiement']['phoneNumber'] = data['msisdn']
            json_ext['moyen_paiement']['status'] = data['status']
            
            if data['status'] == 'REJECTED':
                json_ext['moyen_paiement']['error_code'] = data['error_code']
                json_ext['moyen_paiement']['message'] = data['message']
            
            beneficiary.json_ext = json_ext
            
            # Update beneficiary status
            # if data['status'] == 'ACCEPTED':
            #     beneficiary.status = 'PENDING_ACCOUNT_CREATION'
            # else:
            #     beneficiary.status = 'ACCOUNT_ATTRIBUTION_REJECTED'
            
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
    def process_account_attribution(cls, data, user=None):
        """
        Process payment account attribution request
        
        Args:
            data (dict): Validated account attribution data
            user: User performing the operation (optional, will use system user if not provided)
            
        Returns:
            tuple: (success (bool), beneficiary or None, message or None)
        """
        try:
            # Get system user if no user provided
            if not user:
                user = cls.get_system_user()
            # Find the beneficiary
            beneficiary = cls.find_beneficiary_by_identifiers(
                data['cni'], 
                data['socialid']
            )
            
            if not beneficiary:
                return False, None, "Beneficiary not found"
            
            # Verify beneficiary is in the correct state
            valid_states = ['VALIDATED', 'POTENTIAL', 'ACTIVE']
            if beneficiary.status not in valid_states:
                return False, beneficiary, f"Invalid beneficiary state: {beneficiary.status}"
            
            # Update account attribution data in json_ext
            json_ext = beneficiary.json_ext or {}
            
            # Check current status
            if 'moyen_paiement' in json_ext and json_ext['moyen_paiement']:
                current_status = json_ext['moyen_paiement'].get('status')
                if current_status == 'SUCCESS':
                    return False, beneficiary, "This account has already been created successfully"
                elif current_status == 'REJECTED':
                    # Allow creation after rejection - this is a retry
                    logger.info(f"Creating account for previously rejected beneficiary: {data['socialid']}")
                elif current_status == 'ACCEPTED':
                    # This is the normal flow - proceeding with account creation
                    logger.info(f"Creating account for acknowledged beneficiary: {data['socialid']}")
            
            # Ensure nested structure exists
            if 'moyen_paiement' not in json_ext or not json_ext['moyen_paiement']:
                json_ext['moyen_paiement'] = {}
                
            # Update payment account info
            json_ext['moyen_paiement']['agence'] = data['agence']
            json_ext['moyen_paiement']['phoneNumber'] = data['msisdn']
            json_ext['moyen_paiement']['tp_account_number'] = data['tp_account_number']
            json_ext['moyen_paiement']['status'] = data['status']
            
            if data['status'] == 'FAILURE':
                json_ext['moyen_paiement']['error_code'] = data['error_code']
                json_ext['moyen_paiement']['message'] = data['message']
            else:
                if beneficiary.status != 'ACTIVE':
                    beneficiary.status = 'ACTIVE'

            
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
    
    @staticmethod
    def get_system_user():
        """Get or create system user for API operations"""
        from django.contrib.auth import get_user_model
        User = get_user_model()
        user = User.objects.filter(username='user1').first()
        return user

    @classmethod
    def get_individual_payment_requests(cls, payment_provider=None, payment_cycle_id=None, 
                                      commune=None, programme=None, start_date=None, end_date=None):
        """
        Get individual payment requests for payment provider.
        Returns payment requests awaiting payment for the current payment cycle.
        
        Args:
            payment_provider (str): Filter by payment provider
            payment_cycle_id (int, optional): Filter by payment cycle ID
            commune (str, optional): Filter by commune name
            programme (str, optional): Filter by programme name
            start_date (str, optional): Filter by start date
            end_date (str, optional): Filter by end date
            
        Returns:
            QuerySet: BenefitConsumption objects ready for payment
        """
        try:
            # Get payrolls that are approved for payment
            payroll_query = Payroll.objects.filter(status=PayrollStatus.APPROVE_FOR_PAYMENT)
            
            # Filter by payment provider if specified
            if payment_provider:
                payroll_query = payroll_query.filter(payment_point__name__icontains=payment_provider)
                
            # Filter by payment cycle if specified
            if payment_cycle_id:
                payroll_query = payroll_query.filter(payment_cycle_id=payment_cycle_id)
                
            # Filter by commune if specified
            if commune:
                payroll_query = payroll_query.filter(
                    Q(location__name__iexact=commune) | 
                    Q(location__code__iexact=commune)
                )
            
            # Filter by programme if specified
            if programme:
                payroll_query = payroll_query.filter(
                    benefit_plan__name__icontains=programme
                )
                
            # Filter by date range if specified
            if start_date:
                payroll_query = payroll_query.filter(date_created__gte=start_date)
            if end_date:
                payroll_query = payroll_query.filter(date_created__lte=end_date)
                
            # Get payroll IDs
            payroll_ids = payroll_query.values_list('id', flat=True)
            
            # Get benefit consumptions linked to these payrolls
            benefit_query = BenefitConsumption.objects.filter(
                payrollbenefitconsumption__payroll_id__in=payroll_ids,
                status=BenefitConsumptionStatus.ACCEPTED
            ).select_related('individual').distinct()
            
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
    def acknowledge_payment_request(cls, code, status, transaction_reference=None, 
                                  payment_agency=None, error_code=None, message=None, user=None):
        """
        Acknowledge receipt of payment request by payment provider
        
        Args:
            user: User performing the acknowledgment
            code (str): Payment request code
            status (str): "ACCEPTED" or "REJECTED"
            transaction_reference (str, optional): Transaction reference from provider
            payment_agency (str, optional): Payment agency ID
            error_code (str, optional): Error code if rejected
            message (str, optional): Error message if rejected
            
        Returns:
            tuple: (success (bool), benefit or None, message or None)
        """
        try:
            # Get system user if no user provided
            if not user:
                user = cls.get_system_user()
            # Find the payment request
            benefit = BenefitConsumption.objects.filter(
                code=code,
                status__in=[BenefitConsumptionStatus.ACCEPTED, BenefitConsumptionStatus.APPROVE_FOR_PAYMENT]
            ).select_related('individual').first()
            
            if not benefit:
                return False, None, f"Payment request with code {code} not found or not in valid state"
            
            # Verify payment provider has access to this benefit consumption
            if payment_agency:
                # Check if benefit consumption is linked to a payroll with this payment provider
                from payroll.models import PayrollBenefitConsumption, Payroll
                has_access = PayrollBenefitConsumption.objects.filter(
                    benefit=benefit,
                    payroll__payment_point__name=payment_agency,
                    payroll__status=PayrollStatus.APPROVE_FOR_PAYMENT
                ).exists()
                
                if not has_access:
                    return False, None, f"Payment request {code} not found or not accessible"

            # Update payment request with acknowledgment info
            json_ext = benefit.json_ext or {}
            
            # Ensure payment_provider field exists
            if 'payment_provider' not in json_ext:
                json_ext['payment_provider'] = {}
                
            # Update acknowledgment info
            json_ext['payment_provider']['acknowledgment_status'] = status
            json_ext['payment_provider']['acknowledgment_date'] = datetime.now().isoformat()
            
            if transaction_reference:
                json_ext['payment_provider']['transaction_reference'] = transaction_reference
                
            if payment_agency:
                json_ext['payment_provider']['agence'] = payment_agency

            if status == 'ACCEPTED':
                # Keep status as APPROVE_FOR_PAYMENT when acknowledged
                pass
                
            elif status == 'REJECTED':
                if not error_code:
                    return False, benefit, "Error code is required when status is REJECTED"
                    
                json_ext['payment_provider']['error_code'] = error_code
                json_ext['payment_provider']['message'] = message or ""
                
                # Update benefit status to rejected
                benefit.status = BenefitConsumptionStatus.REJECTED
                
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
    def consolidate_payment(cls, transaction_reference, payment_date, 
                          receipt_number=None, status='SUCCESS', error_code=None, message=None,
                          payment_agency=None, user=None):
        """
        Consolidate payment after completion
        
        Args:
            user: User performing the consolidation
            transaction_reference (str): Transaction reference from payment provider
            payment_date (date): Effective date of payment
            receipt_number (str, optional): Receipt number
            status (str): "SUCCESS" or "FAILURE"
            error_code (str, optional): Error code if failed
            message (str, optional): Error message if failed
            payment_agency (str, optional): Payment agency ID for access control
            
        Returns:
            tuple: (success (bool), benefit or None, message or None)
        """
        try:
            if not user:
                user = cls.get_system_user()
            # Find the payment request by transaction reference in json_ext
            benefits = BenefitConsumption.objects.filter(
                json_ext__payment_provider__transaction_reference=transaction_reference
            ).select_related('individual')
            
            if not benefits.exists():
                return False, None, f"Payment request with transaction reference {transaction_reference} not found"
            
            benefit = benefits.first()
            
            # Verify payment provider has access to this benefit consumption
            if payment_agency:
                # Check if benefit consumption is linked to a payroll with this payment provider
                from payroll.models import PayrollBenefitConsumption, Payroll
                has_access = PayrollBenefitConsumption.objects.filter(
                    benefit=benefit,
                    payroll__payment_point__name=payment_agency,
                    payroll__status=PayrollStatus.APPROVE_FOR_PAYMENT
                ).exists()
                
                if not has_access:
                    return False, None, f"Payment request with transaction reference {transaction_reference} not found or not accessible"
            
            # Check if already reconciled
            if benefit.status == BenefitConsumptionStatus.RECONCILED:
                return False, benefit, f"Payment request already reconciled"
            
            json_ext = benefit.json_ext or {}
            
            # Update consolidation info
            if 'payment_consolidation' not in json_ext:
                json_ext['payment_consolidation'] = {}
                
            json_ext['payment_consolidation']['status'] = status
            json_ext['payment_consolidation']['payment_date'] = payment_date.isoformat() if hasattr(payment_date, 'isoformat') else str(payment_date)
            json_ext['payment_consolidation']['date'] = datetime.now().isoformat()
            
            if receipt_number:
                json_ext['payment_consolidation']['receipt_number'] = receipt_number
            
            if status == 'SUCCESS':
                benefit.receipt = transaction_reference
                # Update benefit status to reconciled
                benefit.status = BenefitConsumptionStatus.RECONCILED
                
            elif status == 'FAILURE':
                if not error_code:
                    return False, benefit, "Error code is required when status is FAILURE"
                    
                json_ext['payment_consolidation']['error_code'] = error_code
                json_ext['payment_consolidation']['message'] = message or ""
                
                # Update benefit status to rejected
                benefit.status = BenefitConsumptionStatus.REJECTED
                
            else:
                return False, benefit, f"Invalid status: {status}"
            
            # Save changes
            benefit.json_ext = json_ext
            benefit.save(user=user)
            
            return True, benefit, None
            
        except Exception as e:
            logger.error(f"Error consolidating payment: {str(e)}")
            if transaction.get_connection().in_atomic_block:
                transaction.set_rollback(True)
            return False, None, str(e)
        
class MonetaryTransferService(BaseService):
    OBJECT_TYPE = MonetaryTransfer

    def __init__(self, user, validation_class=MonetaryTransferValidation):
        super().__init__(user, validation_class)

    @register_service_signal('monetary_transfer_service.create')
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal('monetary_transfer_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal('monetary_transfer_service.delete')
    def delete(self, obj_data):
        return super().delete(obj_data)
    
    def save_instance(self, obj_):
        obj_.save()
        dict_repr = model_representation(obj_)
        return output_result_success(dict_representation=dict_repr)


class SectionService(BaseService):
    OBJECT_TYPE = Section

    def __init__(self, user, validation_class=SectionValidation):
        super().__init__(user, validation_class)

    @register_service_signal('section_service.create')
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal('section_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal('section_service.delete')
    def delete(self, obj_data):
        return super().delete(obj_data)
    
    def save_instance(self, obj_):
        obj_.save()
        dict_repr = model_representation(obj_)
        return output_result_success(dict_representation=dict_repr)


class IndicatorService(BaseService):
    OBJECT_TYPE = Indicator

    def __init__(self, user, validation_class=IndicatorValidation):
        super().__init__(user, validation_class)

    @register_service_signal('indicator_service.create')
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal('indicator_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal('indicator_service.delete')
    def delete(self, obj_data):
        return super().delete(obj_data)
    
    def save_instance(self, obj_):
        obj_.save()
        dict_repr = model_representation(obj_)
        return output_result_success(dict_representation=dict_repr)


class ProvincePaymentPointService(BaseService):
    """
    Service for managing payment points at province level
    """
    OBJECT_TYPE = ProvincePaymentPoint
    
    def __init__(self, user, validation_class=ProvincePaymentPointValidation):
        super().__init__(user, validation_class)
    
    @register_service_signal('province_payment_point_service.create')
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal('province_payment_point_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal('province_payment_point_service.delete')
    def delete(self, obj_data):
        return super().delete(obj_data)
    
    def save_instance(self, obj_):
        obj_.save()
        dict_repr = model_representation(obj_)
        return output_result_success(dict_representation=dict_repr)
    
    def add_province_payment_point(self, province_id, payment_point_id, payment_plan_id=None):
        """
        Add a payment point to a province
        
        Args:
            province_id (str): UUID of the province location
            payment_point_id (str): UUID of the payment point
            payment_plan_id (str, optional): UUID of the payment plan
            
        Returns:
            dict: Summary of operation results
        """
        
        try:
            # Create the province payment point
            result = self.create({
                'province_id': province_id,
                'payment_point_id': payment_point_id,
                'payment_plan_id': payment_plan_id
            })
            
            if 'success' in result and result['success']:
                # Validate province
                province = Location.objects.filter(id=province_id, type='D').first()
                
                # Validate payment point
                payment_point = PaymentPoint.objects.filter(id=payment_point_id).first()
                
                # If payment_plan_id is provided, verify it exists
                benefit_plan = None
                if payment_plan_id:
                    # Get the payment plan and associated benefit plan
                    payment_plan = PaymentPlan.objects.filter(id=payment_plan_id).first()
                    if payment_plan:
                        benefit_plan = payment_plan.benefit_plan

                # Return summary
                return {
                    'success': True,
                    'province_id': str(province.id),
                    'province_name': province.name,
                    'payment_point_id': str(payment_point.id),
                    'payment_point_name': payment_point.name,
                    'benefit_plan_id': str(benefit_plan.id) if benefit_plan else None,
                    'benefit_plan_name': benefit_plan.name if benefit_plan else None,
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error occurred')
                }
            
        except Exception as e:
            logger.error(f"Error adding province-wide payment point: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }


class IndicatorAchievementService(BaseService):
    OBJECT_TYPE = IndicatorAchievement

    def __init__(self, user, validation_class=IndicatorAchievementValidation):
        super().__init__(user, validation_class)

    @register_service_signal('indicator_achievement_service.create')
    def create(self, obj_data):
        return super().create(obj_data)

    @register_service_signal('indicator_achievement_service.update')
    def update(self, obj_data):
        return super().update(obj_data)

    @register_service_signal('indicator_achievement_service.delete')
    def delete(self, obj_data):
        return super().delete(obj_data)
    
    def save_instance(self, obj_):
        obj_.save()
        dict_repr = model_representation(obj_)
        return output_result_success(dict_representation=dict_repr)
