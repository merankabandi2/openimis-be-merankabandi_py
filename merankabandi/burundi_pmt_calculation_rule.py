from calcrule_social_protection.calculation_rule import SocialProtectionCalculationRule
from social_protection.models import BeneficiaryStatus
import logging

logger = logging.getLogger(__name__)


class BurundiPMTCalculationRule(SocialProtectionCalculationRule):
    version = 2
    uuid = "42d96b58-898a-460a-b357-5fd4b95cd87d"
    calculation_rule_name = "Burundi PMT - Proxy Means Test"
    description = (
        "Dual urban/rural PMT scoring based on Burundi national formula. "
        "Coefficients from legacy bulk_pmt.php5."
    )

    # Base intercepts
    BASE_URBAN = 9.397
    BASE_RURAL = 9.835

    # Household size adjustments (ntot -> penalty)
    HOUSEHOLD_SIZE_URBAN = {2: -0.068, 3: -0.165, 4: -0.268, 5: -0.321, 6: -0.415, 7: -0.465}
    HOUSEHOLD_SIZE_URBAN_MAX = -0.540  # ntot >= 8
    HOUSEHOLD_SIZE_RURAL = {2: -0.346, 3: -0.533, 4: -0.696, 5: -0.807, 6: -0.941, 7: -1.030}
    HOUSEHOLD_SIZE_RURAL_MAX = -1.181  # ntot >= 8

    # Education level adjustments (instruction code -> adjustment)
    EDUCATION_URBAN = {
        'INSTRUCTION_NIVEAU_PRIMAIRE_NON_ACHEVE': 0.058,
        'INSTRUCTION_NIVEAU_PRIMAIRE_ACHEVE': 0.082,
        'INSTRUCTION_NIVEAU_7_10_ECOFO_4': 0.128,
        'INSTRUCTION_CYCLE_SUPERIEUR': 0.252,
        'INSTRUCTION_CYCLE_UNIVERSITAIRE_1': 0.345,
        'INSTRUCTION_CYCLE_UNIVERSITAIRE_2': 0.345,
        'INSTRUCTION_CYCLE_UNIVERSITAIRE_3': 0.345,
    }
    EDUCATION_RURAL = {
        'INSTRUCTION_NIVEAU_PRIMAIRE_NON_ACHEVE': 0.074,
        'INSTRUCTION_NIVEAU_PRIMAIRE_ACHEVE': 0.036,
        'INSTRUCTION_NIVEAU_7_10_ECOFO_4': 0.109,
        'INSTRUCTION_CYCLE_SUPERIEUR': 0.292,
        'INSTRUCTION_CYCLE_UNIVERSITAIRE_1': 0.322,
        'INSTRUCTION_CYCLE_UNIVERSITAIRE_2': 0.322,
        'INSTRUCTION_CYCLE_UNIVERSITAIRE_3': 0.322,
    }

    # Province group adjustments (province code -> adjustment)
    PROVINCE_GROUPS_URBAN = {
        'group_1': ({'04', '06', '15', '16'}, -0.097),
        'group_2': ({'07', '08', '09', '12', '14'}, 0.014),
        'group_3': ({'03', '10', '18'}, -0.187),
        'group_4': ({'01', '02', '05', '11', '13'}, -0.103),
    }
    PROVINCE_GROUPS_RURAL = {
        'group_1': ({'04', '06', '15', '16'}, 0.0),  # no adjustment in rural
        'group_2': ({'07', '08', '09', '12', '14'}, 0.048),
        'group_3': ({'03', '10', '18'}, 0.078),
        'group_4': ({'01', '02', '05', '11', '13'}, 0.012),
    }

    # Non-grid electricity sources (penalty trigger)
    NON_GRID_ELECTRICITY = {
        'LOGEMENT_ELECTRICITE_LAMPE_PETROLE_BOUGIE',
        'LOGEMENT_ELECTRICITE_TORCHE',
        'LOGEMENT_ELECTRICITE_BOIS',
        'LOGEMENT_ELECTRICITE_LANTERNE_SOLAIRE',
    }

    # Improved cooking fuels (bonus trigger)
    IMPROVED_COOKING = {'LOGEMENT_CUISSON_CHARBON', 'LOGEMENT_CUISSON_GAZ'}

    # Poor roof materials (rural penalty trigger)
    POOR_ROOF = {
        'LOGEMENT_TOIT_PAS_TOIT', 'LOGEMENT_TOIT_BAMBOU',
        'LOGEMENT_TOIT_NATTES', 'LOGEMENT_TOIT_PLANCHES',
        'LOGEMENT_TOIT_CARTONS', 'LOGEMENT_TOIT_TENTE',
        'LOGEMENT_TOIT_AUTRE',
    }

    @classmethod
    def calculate(cls, payment_plan, **kwargs):
        benefit_plan = kwargs.get('benefit_plan') or (
            payment_plan.benefit_plan if payment_plan else None
        )
        if not benefit_plan:
            raise ValueError("No benefit plan provided.")

        beneficiaries_qs = kwargs.get(
            'beneficiaries_queryset',
            cls._get_targeting_queryset(benefit_plan),
        )

        updated_count = 0
        for beneficiary in beneficiaries_qs:
            data = cls._collect_household_data(beneficiary)
            score_urban = cls._score_urban(data)
            score_rural = cls._score_rural(data)

            # Select score based on commune milieu type
            milieu = data.get('type_milieu_residence', 'MILIEU_RESIDENCE_RURAL')
            if milieu == 'MILIEU_RESIDENCE_URBAIN':
                score = score_urban
            else:
                score = score_rural

            if not beneficiary.json_ext:
                beneficiary.json_ext = {}
            beneficiary.json_ext['pmt_score'] = score
            beneficiary.json_ext['pmt_score_urban'] = score_urban
            beneficiary.json_ext['pmt_score_rural'] = score_rural
            beneficiary.save()
            updated_count += 1

        logger.info(
            "PMT calculation completed: %d beneficiaries in plan %s",
            updated_count, benefit_plan.code,
        )
        return f"Scored {updated_count} households."

    @classmethod
    def _collect_household_data(cls, beneficiary):
        """Merge individual + group json_ext into a flat dict."""
        data = {}
        if hasattr(beneficiary, 'group') and beneficiary.group:
            data.update(beneficiary.group.json_ext or {})
        data.update(beneficiary.json_ext or {})
        return data

    @classmethod
    def _get_household_size_adj(cls, ntot, size_table, max_adj):
        if ntot >= 8:
            return max_adj
        return size_table.get(ntot, 0.0)

    @classmethod
    def _get_province_adj(cls, province_code, groups):
        for _, (codes, adj) in groups.items():
            if province_code in codes:
                return adj
        return 0.0

    @classmethod
    def _score_urban(cls, data):
        score = cls.BASE_URBAN

        # Head of household sex
        if data.get('chef_sexe') == 'M':
            score += 0.132

        # Head age
        chef_age = data.get('chef_age', 0)
        if isinstance(chef_age, (int, float)):
            score -= 0.005 * chef_age

        # Household size
        ntot = int(data.get('ntot', 1) or 1)
        score += cls._get_household_size_adj(
            ntot, cls.HOUSEHOLD_SIZE_URBAN, cls.HOUSEHOLD_SIZE_URBAN_MAX
        )

        # Dependency ratio: (n014 + n65) / n1564
        n014 = int(data.get('n014', 0) or 0)
        n65 = int(data.get('n65', 0) or 0)
        n1564 = int(data.get('n1564', 1) or 1)
        depend = (n014 + n65) / max(n1564, 1)
        score += 0.048 * depend

        # Education
        instruction = data.get('chef_instruction', '')
        score += cls.EDUCATION_URBAN.get(instruction, 0.0)

        # Rooms per person
        logement_piece = int(data.get('logement_piece', 0) or 0)
        score += 0.261 * (logement_piece / max(ntot, 1))

        # Electricity (non-grid penalty)
        electricite = data.get('logement_electricite', '')
        if electricite in cls.NON_GRID_ELECTRICITY:
            score -= 0.064

        # Cooking fuel (improved bonus)
        cuisson = data.get('logement_cuisson', '')
        if cuisson in cls.IMPROVED_COOKING:
            score += 0.191

        # Toilets
        toilettes = data.get('logement_toilettes', '')
        if toilettes != 'LOGEMENT_TOILETTES_CHASSE_EAU':
            score -= 0.236
        if toilettes == 'LOGEMENT_TOILETTES_PAS_TOILETTES':
            score += 0.236 - 0.178  # net: -0.178

        # Possessions
        if data.get('possessions_radio') not in (None, '0', 0, False):
            score += 0.119
        if data.get('possessions_smartphone') not in (None, '0', 0, False):
            score += 0.187
        if data.get('possessions_matelas') not in (None, '0', 0, False):
            score += 0.239
        if data.get('possessions_velo') not in (None, '0', 0, False):
            score += 0.122

        # Province adjustment
        province = data.get('provab', '')
        score += cls._get_province_adj(province, cls.PROVINCE_GROUPS_URBAN)

        return int(score * 1000)

    @classmethod
    def _score_rural(cls, data):
        score = cls.BASE_RURAL

        # Head of household sex
        if data.get('chef_sexe') == 'M':
            score += 0.117

        # Head age
        chef_age = data.get('chef_age', 0)
        if isinstance(chef_age, (int, float)):
            score -= 0.002 * chef_age

        # Household size
        ntot = int(data.get('ntot', 1) or 1)
        score += cls._get_household_size_adj(
            ntot, cls.HOUSEHOLD_SIZE_RURAL, cls.HOUSEHOLD_SIZE_RURAL_MAX
        )

        # Dependency ratio
        n014 = int(data.get('n014', 0) or 0)
        n65 = int(data.get('n65', 0) or 0)
        n1564 = int(data.get('n1564', 1) or 1)
        depend = (n014 + n65) / max(n1564, 1)
        score += 0.036 * depend

        # Education
        instruction = data.get('chef_instruction', '')
        score += cls.EDUCATION_RURAL.get(instruction, 0.0)

        # Rooms per person
        logement_piece = int(data.get('logement_piece', 0) or 0)
        score += 0.231 * (logement_piece / max(ntot, 1))

        # Electricity (non-grid penalty)
        electricite = data.get('logement_electricite', '')
        if electricite in cls.NON_GRID_ELECTRICITY:
            score -= 0.054

        # Cooking fuel
        cuisson = data.get('logement_cuisson', '')
        if cuisson in cls.IMPROVED_COOKING:
            score += 0.346

        # Roof (poor materials penalty — rural only)
        toit = data.get('logement_toit', '')
        if toit in cls.POOR_ROOF:
            score -= 0.105

        # Floor (rural only)
        sol = data.get('logement_sol', '')
        if sol == 'LOGEMENT_SOL_PIERRE_BRIQUE':
            score -= 0.122
        elif sol in ('LOGEMENT_SOL_CIMENT', 'LOGEMENT_SOL_CARRELAGE'):
            score += 0.059

        # Toilets
        toilettes = data.get('logement_toilettes', '')
        if toilettes != 'LOGEMENT_TOILETTES_CHASSE_EAU':
            score -= 0.248
        if toilettes == 'LOGEMENT_TOILETTES_PAS_TOILETTES':
            score += 0.248 - 0.115  # net: -0.115

        # Possessions
        if data.get('possessions_radio') not in (None, '0', 0, False):
            score += 0.086
        if data.get('possessions_smartphone') not in (None, '0', 0, False):
            score += 0.136
        if data.get('possessions_matelas') not in (None, '0', 0, False):
            score += 0.226
        if data.get('possessions_houe') not in (None, '0', 0, False):
            score += 0.112
        if data.get('possessions_machette') not in (None, '0', 0, False):
            score += 0.067
        if data.get('possessions_velo') not in (None, '0', 0, False):
            score += 0.091

        # Province adjustment
        province = data.get('provab', '')
        score += cls._get_province_adj(province, cls.PROVINCE_GROUPS_RURAL)

        return int(score * 1000)

    @classmethod
    def _get_targeting_queryset(cls, benefit_plan):
        from social_protection.models import GroupBeneficiary, Beneficiary
        model = (
            GroupBeneficiary
            if benefit_plan.type == benefit_plan.BenefitPlanType.GROUP_TYPE
            else Beneficiary
        )
        return model.objects.filter(
            benefit_plan=benefit_plan,
            status=BeneficiaryStatus.POTENTIAL,
        )
