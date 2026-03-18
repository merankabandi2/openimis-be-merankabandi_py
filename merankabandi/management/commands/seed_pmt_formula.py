from django.core.management.base import BaseCommand
from merankabandi.models import PmtFormula


class Command(BaseCommand):
    help = 'Create the default Burundi PMT formula from legacy weights'

    def handle(self, *args, **options):
        formula, created = PmtFormula.objects.update_or_create(
            name='Burundi National PMT',
            defaults={
                'description': 'Formule PMT nationale du Burundi basée sur bulk_pmt.php5',
                'base_score_urban': 9.397,
                'base_score_rural': 9.835,
                'is_active': True,
                'variables': [
                    {"field": "chef_sexe", "category": "demographics", "condition": "M",
                     "urban_weight": 0.132, "rural_weight": 0.117},
                    {"field": "chef_age", "category": "demographics", "type": "continuous",
                     "urban_weight": -0.005, "rural_weight": -0.002},
                    {"field": "ntot", "category": "demographics", "type": "household_size",
                     "urban_max": -0.540, "rural_max": -1.181,
                     "urban_bands": {"2": -0.068, "3": -0.165, "4": -0.268, "5": -0.321, "6": -0.415, "7": -0.465},
                     "rural_bands": {"2": -0.346, "3": -0.533, "4": -0.696, "5": -0.807, "6": -0.941, "7": -1.030}},
                    {"field": "dependency_ratio", "category": "demographics", "type": "computed",
                     "urban_weight": 0.048, "rural_weight": 0.036},
                    {"field": "chef_instruction", "category": "education", "type": "lookup",
                     "urban_lookup": {
                         "INSTRUCTION_NIVEAU_PRIMAIRE_NON_ACHEVE": 0.058,
                         "INSTRUCTION_NIVEAU_PRIMAIRE_ACHEVE": 0.082,
                         "INSTRUCTION_NIVEAU_7_10_ECOFO_4": 0.128,
                         "INSTRUCTION_CYCLE_SUPERIEUR": 0.252,
                         "INSTRUCTION_CYCLE_UNIVERSITAIRE_1": 0.345,
                         "INSTRUCTION_CYCLE_UNIVERSITAIRE_2": 0.345,
                         "INSTRUCTION_CYCLE_UNIVERSITAIRE_3": 0.345,
                     },
                     "rural_lookup": {
                         "INSTRUCTION_NIVEAU_PRIMAIRE_NON_ACHEVE": 0.074,
                         "INSTRUCTION_NIVEAU_PRIMAIRE_ACHEVE": 0.036,
                         "INSTRUCTION_NIVEAU_7_10_ECOFO_4": 0.109,
                         "INSTRUCTION_CYCLE_SUPERIEUR": 0.292,
                         "INSTRUCTION_CYCLE_UNIVERSITAIRE_1": 0.322,
                         "INSTRUCTION_CYCLE_UNIVERSITAIRE_2": 0.322,
                         "INSTRUCTION_CYCLE_UNIVERSITAIRE_3": 0.322,
                     }},
                    {"field": "logement_piece", "category": "housing", "type": "per_capita",
                     "urban_weight": 0.261, "rural_weight": 0.231},
                    {"field": "logement_electricite", "category": "housing", "type": "set_penalty",
                     "penalty_values": [
                         "LOGEMENT_ELECTRICITE_LAMPE_PETROLE_BOUGIE", "LOGEMENT_ELECTRICITE_TORCHE",
                         "LOGEMENT_ELECTRICITE_BOIS", "LOGEMENT_ELECTRICITE_LANTERNE_SOLAIRE",
                     ],
                     "urban_weight": -0.064, "rural_weight": -0.054},
                    {"field": "logement_cuisson", "category": "housing", "type": "set_bonus",
                     "bonus_values": ["LOGEMENT_CUISSON_CHARBON", "LOGEMENT_CUISSON_GAZ"],
                     "urban_weight": 0.191, "rural_weight": 0.346},
                    {"field": "logement_toit", "category": "housing", "type": "set_penalty", "rural_only": True,
                     "penalty_values": [
                         "LOGEMENT_TOIT_PAS_TOIT", "LOGEMENT_TOIT_BAMBOU", "LOGEMENT_TOIT_NATTES",
                         "LOGEMENT_TOIT_PLANCHES", "LOGEMENT_TOIT_CARTONS", "LOGEMENT_TOIT_TENTE",
                         "LOGEMENT_TOIT_AUTRE",
                     ],
                     "urban_weight": 0, "rural_weight": -0.105},
                    {"field": "possessions_radio", "category": "possessions", "type": "boolean",
                     "urban_weight": 0.119, "rural_weight": 0.086},
                    {"field": "possessions_smartphone", "category": "possessions", "type": "boolean",
                     "urban_weight": 0.187, "rural_weight": 0.136},
                    {"field": "possessions_matelas", "category": "possessions", "type": "boolean",
                     "urban_weight": 0.239, "rural_weight": 0.226},
                    {"field": "possessions_velo", "category": "possessions", "type": "boolean",
                     "urban_weight": 0.122, "rural_weight": 0.091},
                    {"field": "possessions_houe", "category": "possessions", "type": "boolean", "rural_only": True,
                     "urban_weight": 0, "rural_weight": 0.112},
                    {"field": "possessions_machette", "category": "possessions", "type": "boolean", "rural_only": True,
                     "urban_weight": 0, "rural_weight": 0.067},
                ],
                'geographic_adjustments': {
                    "urban": {
                        "group_1": {"provinces": ["04", "06", "15", "16"], "adjustment": -0.097},
                        "group_2": {"provinces": ["07", "08", "09", "12", "14"], "adjustment": 0.014},
                        "group_3": {"provinces": ["03", "10", "18"], "adjustment": -0.187},
                        "group_4": {"provinces": ["01", "02", "05", "11", "13"], "adjustment": -0.103},
                    },
                    "rural": {
                        "group_1": {"provinces": ["04", "06", "15", "16"], "adjustment": 0.0},
                        "group_2": {"provinces": ["07", "08", "09", "12", "14"], "adjustment": 0.048},
                        "group_3": {"provinces": ["03", "10", "18"], "adjustment": 0.078},
                        "group_4": {"provinces": ["01", "02", "05", "11", "13"], "adjustment": 0.012},
                    },
                },
            },
        )
        action = "Created" if created else "Updated"
        self.stdout.write(f"{action} PMT formula: {formula.name} (id={formula.id})")
