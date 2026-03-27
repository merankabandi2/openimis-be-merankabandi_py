"""
Merankabandi-specific benefit package strategies.

Override CONVERTER_BENEFIT to use Mera converters that add payment method data.
"""
from calcrule_social_protection.strategies.benefit_package_group_strategy import GroupBenefitPackageStrategy
from calcrule_social_protection.strategies.benefit_package_individual_strategy import IndividualBenefitPackageStrategy
from merankabandi.calcrule_converters import MeraGroupToBenefitConverter, MeraBeneficiaryToBenefitConverter


class MeraGroupBenefitPackageStrategy(GroupBenefitPackageStrategy):
    CONVERTER_BENEFIT = MeraGroupToBenefitConverter


class MeraIndividualBenefitPackageStrategy(IndividualBenefitPackageStrategy):
    CONVERTER_BENEFIT = MeraBeneficiaryToBenefitConverter
