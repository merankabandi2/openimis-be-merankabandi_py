"""
Merankabandi-specific benefit converters.

Extends upstream calcrule converters to add Burundi payment method data
(phone number from moyen_paiement) to benefit json_ext.
"""
from calcrule_social_protection.converters import (
    GroupToBenefitConverter,
    BeneficiaryToBenefitConverter,
)


class MeraBenefitConverterMixin:
    """Adds Burundi payment method data to benefit json_ext."""

    def to_benefit_obj(self, entity, amount, payment_plan, payment_cycle):
        benefit = super().to_benefit_obj(entity, amount, payment_plan, payment_cycle)
        self._build_payment_data(benefit, entity)
        return benefit

    def _build_payment_data(self, benefit, entity):
        if not entity or not hasattr(entity, 'json_ext') or not entity.json_ext:
            return
        json_ext = entity.json_ext if isinstance(entity.json_ext, dict) else {}
        moyen_paiement = json_ext.get('moyen_paiement')
        if moyen_paiement and isinstance(moyen_paiement, dict):
            benefit.setdefault("json_ext", {})
            benefit["json_ext"]["phoneNumber"] = moyen_paiement.get('phoneNumber', '')


class MeraGroupToBenefitConverter(MeraBenefitConverterMixin, GroupToBenefitConverter):
    """Group benefit converter with Burundi payment data."""
    pass


class MeraBeneficiaryToBenefitConverter(MeraBenefitConverterMixin, BeneficiaryToBenefitConverter):
    """Individual benefit converter with Burundi payment data."""
    pass
