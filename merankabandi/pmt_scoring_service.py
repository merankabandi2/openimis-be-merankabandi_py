"""
Burundi PMT (Proxy Means Test) scoring service.

Used during targeting (household import/collecte), NOT during payroll generation.
Called from TriggerPMTCalculationMutation.

This is a standalone service — not a calculation rule, not registered in CALCULATION_RULES.
"""
from social_protection.models import BeneficiaryStatus
import logging

logger = logging.getLogger(__name__)


class BurundiPMTScoringService:
    """Dual urban/rural PMT scoring based on Burundi national formula."""

    # Base intercepts
    BASE_URBAN = 9.397
    BASE_RURAL = 9.835

    # Household size adjustments (ntot -> penalty)
    HOUSEHOLD_SIZE_URBAN = {2: -0.068, 3: -0.165, 4: -0.268, 5: -0.321, 6: -0.415, 7: -0.465}
    HOUSEHOLD_SIZE_URBAN_MAX = -0.540
    HOUSEHOLD_SIZE_RURAL = {2: -0.346, 3: -0.533, 4: -0.696, 5: -0.807, 6: -0.941, 7: -1.030}
    HOUSEHOLD_SIZE_RURAL_MAX = -1.181

    # Education level adjustments
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

    # Province group adjustments
    PROVINCE_GROUPS_URBAN = {
        'group_1': ({'04', '06', '15', '16'}, -0.097),
        'group_2': ({'07', '08', '09', '12', '14'}, 0.014),
        'group_3': ({'03', '10', '18'}, -0.187),
        'group_4': ({'01', '02', '05', '11', '13'}, -0.103),
    }
    PROVINCE_GROUPS_RURAL = {
        'group_1': ({'04', '06', '15', '16'}, 0.0),
        'group_2': ({'07', '08', '09', '12', '14'}, 0.048),
        'group_3': ({'03', '10', '18'}, 0.078),
        'group_4': ({'01', '02', '05', '11', '13'}, 0.012),
    }

    NON_GRID_ELECTRICITY = {
        'LOGEMENT_ELECTRICITE_LAMPE_PETROLE_BOUGIE',
        'LOGEMENT_ELECTRICITE_TORCHE',
        'LOGEMENT_ELECTRICITE_BOIS',
        'LOGEMENT_ELECTRICITE_LANTERNE_SOLAIRE',
    }
    IMPROVED_COOKING = {'LOGEMENT_CUISSON_CHARBON', 'LOGEMENT_CUISSON_GAZ'}
    POOR_ROOF = {
        'LOGEMENT_TOIT_PAS_TOIT', 'LOGEMENT_TOIT_BAMBOU',
        'LOGEMENT_TOIT_NATTES', 'LOGEMENT_TOIT_PLANCHES',
        'LOGEMENT_TOIT_CARTONS', 'LOGEMENT_TOIT_TENTE',
        'LOGEMENT_TOIT_AUTRE',
    }

    @classmethod
    def _load_formula(cls, benefit_plan):
        targeting = (benefit_plan.json_ext or {}).get('targeting', {})
        formula_id = targeting.get('pmt_formula_id')
        if not formula_id:
            return None
        from merankabandi.models import PmtFormula
        try:
            return PmtFormula.objects.get(id=formula_id, is_active=True)
        except PmtFormula.DoesNotExist:
            logger.warning("PmtFormula %s not found or inactive, using hardcoded defaults", formula_id)
            return None

    @classmethod
    def score_beneficiaries(cls, benefit_plan, **kwargs):
        """Score beneficiaries using Burundi PMT formula.

        Called from TriggerPMTCalculationMutation during targeting.
        Sets pmt_score, pmt_score_urban, pmt_score_rural, selection_status on each beneficiary.
        """
        formula = cls._load_formula(benefit_plan)
        # Username required by HistoryBusinessModel.save(); default to 'system'
        # when not provided (background jobs / shell).
        username = kwargs.get('username', 'system')

        beneficiaries_qs = kwargs.get(
            'beneficiaries_queryset',
            cls._get_targeting_queryset(benefit_plan),
        )

        updated_count = 0
        for beneficiary in beneficiaries_qs:
            data = cls._collect_household_data(beneficiary)
            score_urban = cls._score_urban(data, formula=formula)
            score_rural = cls._score_rural(data, formula=formula)

            milieu = data.get('type_milieu_residence', 'MILIEU_RESIDENCE_RURAL')
            score = score_urban if milieu == 'MILIEU_RESIDENCE_URBAIN' else score_rural

            # Write to GroupBeneficiary.json_ext (consumers: WizardBeneficiaryList FE)
            if not beneficiary.json_ext:
                beneficiary.json_ext = {}
            beneficiary.json_ext['pmt_score'] = score
            beneficiary.json_ext['pmt_score_urban'] = score_urban
            beneficiary.json_ext['pmt_score_rural'] = score_rural
            beneficiary.json_ext['selection_status'] = 'PMT_SCORED'
            beneficiary.save(username=username)

            # Also write to Group.json_ext (consumers: apply_quota_selection,
            # WizardSummaryPanel, WizardValidationPanel). Group is the source of
            # truth for the selection state machine per the UI contract.
            if hasattr(beneficiary, 'group') and beneficiary.group:
                grp = beneficiary.group
                grp_ext = grp.json_ext or {}
                grp_ext['pmt_score'] = score
                grp_ext['pmt_score_urban'] = score_urban
                grp_ext['pmt_score_rural'] = score_rural
                grp_ext['selection_status'] = 'PMT_SCORED'
                grp.json_ext = grp_ext
                grp.save(username=username)

            updated_count += 1

        logger.info("PMT scoring completed: %d beneficiaries in plan %s", updated_count, benefit_plan.code)
        return f"Scored {updated_count} households."

    @classmethod
    def _collect_household_data(cls, beneficiary):
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
    def _score_urban(cls, data, formula=None):
        score = float(formula.base_score_urban) if formula else cls.BASE_URBAN
        if data.get('chef_sexe') == 'M':
            score += 0.132
        chef_age = data.get('chef_age', 0)
        if isinstance(chef_age, (int, float)):
            score -= 0.005 * chef_age
        ntot = int(data.get('ntot', 1) or 1)
        score += cls._get_household_size_adj(ntot, cls.HOUSEHOLD_SIZE_URBAN, cls.HOUSEHOLD_SIZE_URBAN_MAX)
        n014 = int(data.get('n014', 0) or 0)
        n65 = int(data.get('n65', 0) or 0)
        n1564 = int(data.get('n1564', 1) or 1)
        score += 0.048 * ((n014 + n65) / max(n1564, 1))
        score += cls.EDUCATION_URBAN.get(data.get('chef_instruction', ''), 0.0)
        logement_piece = int(data.get('logement_piece', 0) or 0)
        score += 0.261 * (logement_piece / max(ntot, 1))
        if data.get('logement_electricite', '') in cls.NON_GRID_ELECTRICITY:
            score -= 0.064
        if data.get('logement_cuisson', '') in cls.IMPROVED_COOKING:
            score += 0.191
        toilettes = data.get('logement_toilettes', '')
        if toilettes != 'LOGEMENT_TOILETTES_CHASSE_EAU':
            score -= 0.236
        if toilettes == 'LOGEMENT_TOILETTES_PAS_TOILETTES':
            score += 0.236 - 0.178
        if data.get('possessions_radio') not in (None, '0', 0, False):
            score += 0.119
        if data.get('possessions_smartphone') not in (None, '0', 0, False):
            score += 0.187
        if data.get('possessions_matelas') not in (None, '0', 0, False):
            score += 0.239
        if data.get('possessions_velo') not in (None, '0', 0, False):
            score += 0.122
        score += cls._get_province_adj(data.get('provab', ''), cls.PROVINCE_GROUPS_URBAN)
        return int(score * 1000)

    @classmethod
    def _score_rural(cls, data, formula=None):
        score = float(formula.base_score_rural) if formula else cls.BASE_RURAL
        if data.get('chef_sexe') == 'M':
            score += 0.117
        chef_age = data.get('chef_age', 0)
        if isinstance(chef_age, (int, float)):
            score -= 0.002 * chef_age
        ntot = int(data.get('ntot', 1) or 1)
        score += cls._get_household_size_adj(ntot, cls.HOUSEHOLD_SIZE_RURAL, cls.HOUSEHOLD_SIZE_RURAL_MAX)
        n014 = int(data.get('n014', 0) or 0)
        n65 = int(data.get('n65', 0) or 0)
        n1564 = int(data.get('n1564', 1) or 1)
        score += 0.036 * ((n014 + n65) / max(n1564, 1))
        score += cls.EDUCATION_RURAL.get(data.get('chef_instruction', ''), 0.0)
        logement_piece = int(data.get('logement_piece', 0) or 0)
        score += 0.231 * (logement_piece / max(ntot, 1))
        if data.get('logement_electricite', '') in cls.NON_GRID_ELECTRICITY:
            score -= 0.054
        if data.get('logement_cuisson', '') in cls.IMPROVED_COOKING:
            score += 0.346
        if data.get('logement_toit', '') in cls.POOR_ROOF:
            score -= 0.105
        sol = data.get('logement_sol', '')
        if sol == 'LOGEMENT_SOL_PIERRE_BRIQUE':
            score -= 0.122
        elif sol in ('LOGEMENT_SOL_CIMENT', 'LOGEMENT_SOL_CARRELAGE'):
            score += 0.059
        toilettes = data.get('logement_toilettes', '')
        if toilettes != 'LOGEMENT_TOILETTES_CHASSE_EAU':
            score -= 0.248
        if toilettes == 'LOGEMENT_TOILETTES_PAS_TOILETTES':
            score += 0.248 - 0.115
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
        score += cls._get_province_adj(data.get('provab', ''), cls.PROVINCE_GROUPS_RURAL)
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
