"""
Enhanced OpenSearch Documents for Materialized Views Self-Service Analytics
Includes comprehensive JSON_ext field dimensions
"""

from django_opensearch_dsl import Document, fields
from django_opensearch_dsl.indices import Index
from django.db import connection
from opensearch_reports.models import OpenSearchDashboard
from opensearch_reports.service import BaseSyncDocument
import uuid
from datetime import datetime


# Index for dashboard analytics data
dashboard_analytics_index = Index('dashboard_analytics')
dashboard_analytics_index.settings(
    number_of_shards=1,
    number_of_replicas=0
)


class DashboardBeneficiaryDocument(BaseSyncDocument):
    """
    Enhanced OpenSearch document for dashboard_beneficiary_summary materialized view
    Includes comprehensive household and individual JSON_ext fields
    """
    # Beneficiary identification
    individual_id = fields.KeywordField()
    first_name = fields.TextField(analyzer='standard')
    last_name = fields.TextField(analyzer='standard')
    other_names = fields.TextField(analyzer='standard')
    birth_date = fields.DateField()
    
    # Core Demographics
    gender = fields.KeywordField()  # M, F
    age_group = fields.KeywordField()  # 0-5, 5-18, 18-60, 60+
    
    # Vulnerable Groups (from household)
    is_batwa = fields.BooleanField()  # menage_mutwa = OUI
    is_refugee = fields.BooleanField()  # menage_refugie = OUI
    is_returnee = fields.BooleanField()  # menage_rapatrie = OUI  
    is_displaced = fields.BooleanField()  # menage_deplace = OUI
    community_type = fields.KeywordField()  # REFUGEE, RETURNEE, DISPLACED, HOST
    
    # Disability and Health (from individual)
    has_disability = fields.BooleanField()  # handicap = OUI
    disability_type = fields.KeywordField()  # TYPE_HANDICAP_PHYSIQUE, MENTAL, SENSORIEL, AUTRE, ALBINOS, NANISME
    has_chronic_disease = fields.BooleanField()  # maladie_chro = OUI
    chronic_disease_type = fields.KeywordField()  # MALADIE_CHRO_TYPE_*
    
    # Education (from individual)
    education_level = fields.KeywordField()  # instruction field values
    is_literate = fields.BooleanField()  # lit = OUI
    attending_school = fields.BooleanField()  # va_ecole = OUI
    
    # Household Characteristics
    household_type = fields.KeywordField()  # TYPE_MENAGE_DEUX_PARENTS, TYPE_MENAGE_UN_PARENT, etc
    household_vulnerability = fields.KeywordField()  # vulnerable_ressenti: VULNERABILITE_RESSENTI_OUI/BCQ/NON
    household_status = fields.KeywordField()  # etat: INSCRIT, REBUT, LISTE_PROV_OK, etc
    pmt_score = fields.FloatField()  # score_pmt_initial
    
    # Location
    province = fields.KeywordField()
    commune = fields.KeywordField()
    zone = fields.KeywordField()
    colline = fields.KeywordField()
    residence_type = fields.KeywordField()  # milieu_residence: MILIEU_RESIDENCE_RURAL/URBAIN
    
    # Housing Conditions
    housing_type = fields.KeywordField()  # logement_type
    housing_ownership = fields.KeywordField()  # logement_statut
    housing_walls = fields.KeywordField()  # logement_murs
    housing_roof = fields.KeywordField()  # logement_toit
    housing_floor = fields.KeywordField()  # logement_sol
    water_source = fields.KeywordField()  # logement_eau_boisson
    cooking_fuel = fields.KeywordField()  # logement_cuisson
    electricity_source = fields.KeywordField()  # logement_electricite
    toilet_type = fields.KeywordField()  # logement_toilettes
    
    # Food Security
    food_insecure = fields.BooleanField()  # alimentaire_sans_nourriture = OUI
    food_shortage_frequency = fields.KeywordField()  # alimentaire_frequence
    
    # Economic Indicators
    has_land = fields.BooleanField()  # a_terres = OUI
    has_livestock = fields.BooleanField()  # a_elevage = OUI
    receives_transfers = fields.BooleanField()  # transfert_recoit = OUI
    
    # Individual Role
    is_household_head = fields.BooleanField()  # est_chef = OUI
    relationship_to_head = fields.KeywordField()  # lien
    
    # Activity and Employment
    main_activity = fields.KeywordField()  # activite
    civil_status_documented = fields.BooleanField()  # etat_civil = OUI
    marital_status = fields.KeywordField()  # statut_matrimonial
    
    # Contact
    phone_status = fields.KeywordField()  # telephone_etat: PHONE_VALIDE/INVALIDE
    
    # Program participation
    benefit_plan_name = fields.TextField(analyzer='standard')
    benefit_plan_code = fields.KeywordField()
    beneficiary_status = fields.KeywordField()
    date_created = fields.DateField()
    date_updated = fields.DateField()
    
    # Payment tracking
    total_payments = fields.IntegerField()
    last_payment_date = fields.DateField()
    payment_method = fields.KeywordField()  # moyen_paiement->agence
    payment_status = fields.KeywordField()  # moyen_paiement->etat/status
    
    # Aggregation fields
    report_date = fields.DateField()
    
    class Django:
        model = None  # Will be populated dynamically from materialized view
        
    class Index:
        name = 'dashboard_beneficiaries_enhanced'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class DashboardHouseholdDocument(BaseSyncDocument):
    """
    OpenSearch document for household-level analytics
    """
    # Household identification
    household_id = fields.KeywordField()
    social_id = fields.KeywordField()
    
    # Demographics
    household_size = fields.IntegerField()
    num_women = fields.IntegerField()
    num_men = fields.IntegerField()
    num_children = fields.IntegerField()
    num_elderly = fields.IntegerField()
    
    # Vulnerable Group Status
    is_twa_household = fields.BooleanField()
    is_refugee_household = fields.BooleanField()
    is_returnee_household = fields.BooleanField()
    is_displaced_household = fields.BooleanField()
    
    # Members with vulnerabilities
    num_disabled = fields.IntegerField()
    num_chronic_disease = fields.IntegerField()
    
    # Household Characteristics
    household_type = fields.KeywordField()
    vulnerability_level = fields.KeywordField()
    pmt_score = fields.FloatField()
    pmt_score_range = fields.KeywordField()  # <30, 30-40, 40-50, >50
    
    # Location
    province = fields.KeywordField()
    commune = fields.KeywordField()
    zone = fields.KeywordField()
    colline = fields.KeywordField()
    residence_type = fields.KeywordField()
    
    # Housing Quality Index (composite)
    housing_quality_score = fields.FloatField()
    has_improved_water = fields.BooleanField()
    has_improved_sanitation = fields.BooleanField()
    has_electricity = fields.BooleanField()
    num_rooms = fields.IntegerField()
    
    # Economic Status
    has_land = fields.BooleanField()
    has_livestock = fields.BooleanField()
    num_cattle = fields.IntegerField()
    num_goats = fields.IntegerField()
    num_pigs = fields.IntegerField()
    num_chickens = fields.IntegerField()
    
    # Assets
    has_radio = fields.BooleanField()
    has_smartphone = fields.BooleanField()
    has_bicycle = fields.BooleanField()
    has_television = fields.BooleanField()
    asset_score = fields.FloatField()
    
    # Program Status
    registration_status = fields.KeywordField()
    benefit_plan_enrolled = fields.BooleanField()
    num_payments_received = fields.IntegerField()
    total_amount_received = fields.FloatField()
    
    # Dates
    registration_date = fields.DateField()
    last_update_date = fields.DateField()
    
    class Index:
        name = 'dashboard_households'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


class DashboardVulnerableGroupsDocument(BaseSyncDocument):
    """
    Specialized document for vulnerable groups analytics
    """
    # Group identification
    group_type = fields.KeywordField()  # TWA, REFUGEE, RETURNEE, DISPLACED
    
    # Location
    province = fields.KeywordField()
    commune = fields.KeywordField()
    zone = fields.KeywordField()
    
    # Demographics
    total_households = fields.IntegerField()
    total_individuals = fields.IntegerField()
    women_count = fields.IntegerField()
    men_count = fields.IntegerField()
    children_count = fields.IntegerField()
    elderly_count = fields.IntegerField()
    disabled_count = fields.IntegerField()
    
    # Program Coverage
    enrolled_households = fields.IntegerField()
    enrolled_individuals = fields.IntegerField()
    coverage_rate = fields.FloatField()
    
    # Payment Performance
    total_payments = fields.IntegerField()
    successful_payments = fields.IntegerField()
    payment_success_rate = fields.FloatField()
    total_amount_paid = fields.FloatField()
    avg_amount_per_household = fields.FloatField()
    
    # Socio-economic indicators
    avg_pmt_score = fields.FloatField()
    percent_with_land = fields.FloatField()
    percent_with_livestock = fields.FloatField()
    percent_food_insecure = fields.FloatField()
    percent_improved_housing = fields.FloatField()
    
    # Education
    literacy_rate = fields.FloatField()
    school_attendance_rate = fields.FloatField()
    
    # Time dimensions
    report_date = fields.DateField()
    year = fields.IntegerField()
    quarter = fields.KeywordField()
    month = fields.IntegerField()
    
    class Index:
        name = 'dashboard_vulnerable_groups'
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0,
        }


# Add new mappings to the sync service
ENHANCED_VIEW_MAPPINGS = {
    # Original mappings
    'dashboard_beneficiary_summary': DashboardBeneficiaryDocument,
    'dashboard_monetary_transfers': 'DashboardMonetaryTransferDocument',
    'dashboard_grievances': 'DashboardGrievanceDocument',
    'dashboard_grievance_category_summary': 'DashboardGrievanceDocument',
    'dashboard_grievance_channel_summary': 'DashboardGrievanceDocument',
    'dashboard_activities_summary': 'DashboardActivitiesDocument',
    'dashboard_microprojects': 'DashboardActivitiesDocument',
    'dashboard_results_framework': 'DashboardIndicatorsDocument',
    'dashboard_indicators_by_section': 'DashboardIndicatorsDocument',
    'dashboard_master_summary': 'DashboardMasterSummaryDocument',
    
    # New enhanced mappings
    'dashboard_households': DashboardHouseholdDocument,
    'dashboard_vulnerable_groups': DashboardVulnerableGroupsDocument,
}


# Field configurations for data exploration
DATA_EXPLORATION_FIELDS = {
    'household_dimensions': {
        'demographic': {
            'household_type': {
                'label': 'Type de Ménage',
                'values': ['TYPE_MENAGE_DEUX_PARENTS', 'TYPE_MENAGE_UN_PARENT', 'TYPE_MENAGE_ORPHELIN', 'TYPE_MENAGE_SANS_LIEN']
            },
            'vulnerability_level': {
                'label': 'Vulnérabilité Ressentie', 
                'values': ['VULNERABILITE_RESSENTI_BCQ', 'VULNERABILITE_RESSENTI_OUI', 'VULNERABILITE_RESSENTI_NON']
            },
            'residence_type': {
                'label': 'Milieu de Résidence',
                'values': ['MILIEU_RESIDENCE_RURAL', 'MILIEU_RESIDENCE_URBAIN']
            }
        },
        'vulnerable_groups': {
            'is_twa': {
                'label': 'Ménage Batwa',
                'values': ['OUI', 'NON']
            },
            'is_refugee': {
                'label': 'Ménage Réfugié',
                'values': ['OUI', 'NON']
            },
            'is_returnee': {
                'label': 'Ménage Rapatrié',
                'values': ['OUI', 'NON']
            },
            'is_displaced': {
                'label': 'Ménage Déplacé',
                'values': ['OUI', 'NON']
            }
        },
        'housing': {
            'housing_type': {
                'label': 'Type de Logement',
                'values': ['LOGEMENT_TYPE_MAISON_ISOLEE', 'LOGEMENT_TYPE_APPARTEMENT', 'LOGEMENT_TYPE_MAISON_CONCESSION', 'LOGEMENT_TYPE_VILLA_MODERNE']
            },
            'housing_ownership': {
                'label': 'Statut du Logement',
                'values': ['LOGEMENT_STATUT_PROPRIETE_SANS_TITRE', 'LOGEMENT_STATUT_PROPRIETE_AVEC_TITRE', 'LOGEMENT_STATUT_LOCATAIRE', 'LOGEMENT_STATUT_LOGEMENT_GRATUIT']
            },
            'water_source': {
                'label': 'Source d\'Eau',
                'values': ['LOGEMENT_EAU_BOISSON_BORNE_FONTAINE', 'LOGEMENT_EAU_BOISSON_PUITS', 'LOGEMENT_EAU_BOISSON_RIVIERE', 'LOGEMENT_EAU_BOISSON_FORAGE', 'LOGEMENT_EAU_BOISSON_ROBINET_DOMICILE']
            },
            'electricity_source': {
                'label': 'Source d\'Électricité',
                'values': ['LOGEMENT_ELECTRICITE_TORCHE', 'LOGEMENT_ELECTRICITE_BOIS', 'LOGEMENT_ELECTRICITE_REGIDESO', 'LOGEMENT_ELECTRICITE_PLAQUE_SOLAIRE']
            }
        },
        'economic': {
            'has_land': {
                'label': 'Possède des Terres',
                'values': ['OUI', 'NON']
            },
            'has_livestock': {
                'label': 'Possède du Bétail',
                'values': ['OUI', 'NON']
            },
            'food_insecure': {
                'label': 'Insécurité Alimentaire',
                'values': ['OUI', 'NON']
            }
        }
    },
    'individual_dimensions': {
        'demographics': {
            'gender': {
                'label': 'Sexe',
                'values': ['M', 'F']
            },
            'age_group': {
                'label': 'Groupe d\'Âge',
                'values': ['0-5', '5-18', '18-60', '60+']
            }
        },
        'disability': {
            'has_disability': {
                'label': 'Handicap',
                'values': ['OUI', 'NON']
            },
            'disability_type': {
                'label': 'Type de Handicap',
                'values': ['TYPE_HANDICAP_PHYSIQUE', 'TYPE_HANDICAP_MENTAL', 'TYPE_HANDICAP_SENSORIEL', 'TYPE_HANDICAP_AUTRE', 'TYPE_HANDICAP_ALBINOS', 'TYPE_HANDICAP_NANISME']
            },
            'has_chronic_disease': {
                'label': 'Maladie Chronique',
                'values': ['OUI', 'NON']
            }
        },
        'education': {
            'education_level': {
                'label': 'Niveau d\'Instruction',
                'values': ['INSTRUCTION_SANS_NIVEAU', 'INSTRUCTION_NIVEAU_PRIMAIRE_NON_ACHEVE', 'INSTRUCTION_NIVEAU_PRIMAIRE_ACHEVE', 'INSTRUCTION_NIVEAU_7_10_ECOFO_4', 'INSTRUCTION_CYCLE_SUPERIEUR', 'INSTRUCTION_CYCLE_UNIVERSITAIRE_1']
            },
            'is_literate': {
                'label': 'Sait Lire',
                'values': ['OUI', 'NON']
            },
            'attending_school': {
                'label': 'Va à l\'École',
                'values': ['OUI', 'NON']
            }
        },
        'activity': {
            'main_activity': {
                'label': 'Activité Principale',
                'values': ['ACTIVITE_AGRICULTEUR', 'ACTIVITE_OUVRIER_AGRICOLE', 'ACTIVITE_AUCUNE', 'ACTIVITE_COMMERCE', 'ACTIVITE_POTIER', 'ACTIVITE_MACON_CONSTRUCTION']
            },
            'is_household_head': {
                'label': 'Chef de Ménage',
                'values': ['OUI', 'NON']
            },
            'marital_status': {
                'label': 'Statut Matrimonial',
                'values': ['STATUT_MATRIMONIAL_MARIE', 'STATUT_MATRIMONIAL_CELIBATAIRE', 'STATUT_MATRIMONIAL_UNION', 'STATUT_MATRIMONIAL_VEUF', 'STATUT_MATRIMONIAL_DIVORCE']
            }
        }
    },
    'numeric_ranges': {
        'pmt_score': {
            'label': 'Score PMT',
            'ranges': ['<30', '30-40', '40-50', '>50']
        },
        'household_size': {
            'label': 'Taille du Ménage',
            'ranges': ['1-2', '3-4', '5-6', '7+']
        },
        'num_rooms': {
            'label': 'Nombre de Pièces',
            'ranges': ['1', '2', '3', '4+']
        }
    }
}