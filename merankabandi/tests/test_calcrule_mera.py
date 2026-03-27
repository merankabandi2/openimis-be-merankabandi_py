"""Tests for Merankabandi calculation rule extraction."""
from django.test import TestCase
from core.test_helpers import LogInHelper


class MeraConverterTests(TestCase):
    """Tests for Mera benefit converters with payment data extraction."""

    @classmethod
    def setUpTestData(cls):
        cls.user = LogInHelper().get_or_create_user_api(username='mera_calcrule_test')

    def test_mixin_extracts_phone_number(self):
        """MeraBenefitConverterMixin extracts phoneNumber from moyen_paiement."""
        from merankabandi.calcrule_converters import MeraBenefitConverterMixin

        mixin = MeraBenefitConverterMixin()
        benefit = {}

        class FakeEntity:
            json_ext = {"moyen_paiement": {"phoneNumber": "79123456"}}

        mixin._build_payment_data(benefit, FakeEntity())
        self.assertEqual(benefit["json_ext"]["phoneNumber"], "79123456")

    def test_mixin_no_moyen_paiement(self):
        """No json_ext modification when moyen_paiement is absent."""
        from merankabandi.calcrule_converters import MeraBenefitConverterMixin

        mixin = MeraBenefitConverterMixin()
        benefit = {}

        class FakeEntity:
            json_ext = {"some_other_field": "value"}

        mixin._build_payment_data(benefit, FakeEntity())
        self.assertNotIn("json_ext", benefit)

    def test_mixin_empty_json_ext(self):
        """No crash when entity has no json_ext."""
        from merankabandi.calcrule_converters import MeraBenefitConverterMixin

        mixin = MeraBenefitConverterMixin()
        benefit = {}

        class FakeEntity:
            json_ext = None

        mixin._build_payment_data(benefit, FakeEntity())
        self.assertNotIn("json_ext", benefit)

    def test_mixin_preserves_existing_json_ext(self):
        """Payment data is merged into existing json_ext, not overwritten."""
        from merankabandi.calcrule_converters import MeraBenefitConverterMixin

        mixin = MeraBenefitConverterMixin()
        benefit = {"json_ext": {"existing_key": "existing_value"}}

        class FakeEntity:
            json_ext = {"moyen_paiement": {"phoneNumber": "79999999"}}

        mixin._build_payment_data(benefit, FakeEntity())
        self.assertEqual(benefit["json_ext"]["phoneNumber"], "79999999")
        self.assertEqual(benefit["json_ext"]["existing_key"], "existing_value")


class MeraCalcRuleTests(TestCase):
    """Tests for Mera calculation rule registration and routing."""

    def test_mera_calc_rule_has_unique_uuid(self):
        from merankabandi.calcrule_rules import MeraPaymentCalcRule
        from calcrule_social_protection.calculation_rule import SocialProtectionCalculationRule
        self.assertNotEqual(
            MeraPaymentCalcRule.uuid,
            SocialProtectionCalculationRule.uuid,
            "Mera calc rule must have a different UUID from upstream"
        )

    def test_mera_calc_rule_registered_in_calculation_rules(self):
        from calculation.apps import CALCULATION_RULES
        from merankabandi.calcrule_rules import MeraPaymentCalcRule
        rule_uuids = [r.uuid for r in CALCULATION_RULES]
        self.assertIn(MeraPaymentCalcRule.uuid, rule_uuids)

    def test_mera_calc_rule_check_calculation_matches_own_uuid(self):
        from merankabandi.calcrule_rules import MeraPaymentCalcRule

        class FakePaymentPlan:
            calculation = MeraPaymentCalcRule.uuid

        self.assertTrue(MeraPaymentCalcRule.check_calculation(FakePaymentPlan()))

    def test_mera_calc_rule_check_calculation_rejects_upstream_uuid(self):
        from merankabandi.calcrule_rules import MeraPaymentCalcRule

        class FakePaymentPlan:
            calculation = "32d96b58-898a-460a-b357-5fd4b95cd87c"

        self.assertFalse(MeraPaymentCalcRule.check_calculation(FakePaymentPlan()))


class PMTScoringServiceTests(TestCase):
    """Tests for renamed PMT scoring service."""

    def test_scoring_service_is_not_calc_rule(self):
        from merankabandi.pmt_scoring_service import BurundiPMTScoringService
        self.assertFalse(
            hasattr(BurundiPMTScoringService, 'uuid'),
            "BurundiPMTScoringService should not have a uuid (not a calc rule)"
        )

    def test_scoring_service_not_in_calculation_rules(self):
        from calculation.apps import CALCULATION_RULES
        from merankabandi.pmt_scoring_service import BurundiPMTScoringService
        self.assertNotIn(BurundiPMTScoringService, CALCULATION_RULES)

    def test_scoring_service_has_score_method(self):
        from merankabandi.pmt_scoring_service import BurundiPMTScoringService
        self.assertTrue(hasattr(BurundiPMTScoringService, 'score_beneficiaries'))

    def test_scoring_service_urban_rural_produce_different_scores(self):
        from merankabandi.pmt_scoring_service import BurundiPMTScoringService
        data = {
            'chef_sexe': 'M', 'chef_age': 40, 'ntot': 5,
            'n014': 2, 'n65': 0, 'n1564': 3,
            'chef_instruction': 'INSTRUCTION_NIVEAU_PRIMAIRE_ACHEVE',
            'logement_piece': 3, 'provab': '01',
        }
        urban = BurundiPMTScoringService._score_urban(data)
        rural = BurundiPMTScoringService._score_rural(data)
        self.assertNotEqual(urban, rural, "Urban and rural scores should differ")
        self.assertIsInstance(urban, int)
        self.assertIsInstance(rural, int)
