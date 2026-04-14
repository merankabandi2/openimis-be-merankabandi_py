"""
Merankabandi-specific benefit converters.

Extends upstream calcrule converters to add:
- Phone number from moyen_paiement / moyen_telecom
- Account number from moyen_paiement
- json_ext with payment data
"""
import logging

from calcrule_social_protection.converters.group_beneficiary import GroupToBenefitConverter
from calcrule_social_protection.converters.beneficiary import BeneficiaryToBenefitConverter

logger = logging.getLogger(__name__)


class MeraBenefitConverterMixin:
    """Adds Burundi payment method data to benefit json_ext."""

    def to_benefit_obj(self, entity, amount, payment_plan, payment_cycle):
        benefit = super().to_benefit_obj(entity, amount, payment_plan, payment_cycle)
        self._build_json_ext(benefit, entity)
        return benefit

    def _build_json_ext(self, benefit, entity):
        phone_number = self._extract_phone_number(entity)
        account_number = self._extract_account_number(entity)

        benefit["json_ext"] = {
            "phoneNumber": phone_number,
            "tp_account_number": account_number,
        }

    def _extract_phone_number(self, entity):
        """Extract phone number from entity json_ext (moyen_paiement or moyen_telecom)."""
        if not entity or not hasattr(entity, 'json_ext') or not entity.json_ext:
            return ''
        ext = entity.json_ext if isinstance(entity.json_ext, dict) else {}

        moyen_paiement = ext.get('moyen_paiement', {})
        if moyen_paiement and isinstance(moyen_paiement, dict):
            phone = moyen_paiement.get('phoneNumber', '')
            if phone:
                return phone

        moyen_telecom = ext.get('moyen_telecom', {})
        if moyen_telecom and isinstance(moyen_telecom, dict):
            msisdn = moyen_telecom.get('msisdn', '')
            if msisdn:
                return msisdn

        return ''

    def _extract_account_number(self, entity):
        """Extract account number from entity json_ext."""
        if not entity or not hasattr(entity, 'json_ext') or not entity.json_ext:
            return ''
        ext = entity.json_ext if isinstance(entity.json_ext, dict) else {}
        moyen_paiement = ext.get('moyen_paiement', {})
        return moyen_paiement.get('tp_account_number', '') if isinstance(moyen_paiement, dict) else ''


class MeraGroupToBenefitConverter(MeraBenefitConverterMixin, GroupToBenefitConverter):
    """Group benefit converter with Burundi payment data and code generation."""


class MeraBeneficiaryToBenefitConverter(MeraBenefitConverterMixin, BeneficiaryToBenefitConverter):
    """Individual benefit converter with Burundi payment data and code generation."""
