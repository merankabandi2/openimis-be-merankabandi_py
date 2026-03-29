"""
KoBo form converter for the new 2025 grievance form (atpoVbHXZCdLD9ETHTv6z4).

Maps the restructured form (3 case types: réclamation, remplacement, suppression)
to Ticket model + json_ext structure.
"""
import logging
from datetime import datetime

from core.models import User
from grievance_social_protection.models import Ticket
from merankabandi.workflow_models import ReplacementRequest
from merankabandi.workflow_service import WorkflowService

logger = logging.getLogger('openIMIS')

# Form ID for the new 2025 form
FORM_ID = 'atpoVbHXZCdLD9ETHTv6z4'

# Default system user for imports (should be configured)
DEFAULT_USER_ID = '17bf084f-9aa9-4eb3-a1f1-b2a6dcc3ec03'


def _get(data, path, default=None):
    """Navigate nested KoBo data with slash-separated paths."""
    parts = path.split('/')
    value = data
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return default
    return value if value is not None else default


def _derive_status(data):
    """Derive Ticket status from form resolution fields."""
    resolved = _get(data, 'group_mb37s20/Est_ce_que_cette_plainte_a_d_j')
    if resolved == 'oui':
        return 'RESOLVED'
    sub_status = _get(data, 'group_mb37s20/Si_non_r_solue_quel_st_le_statut_actuel_')
    if sub_status == 'en_cours_de_traitement_interne':
        return 'IN_PROGRESS'
    return 'OPEN'


def _derive_category(data):
    """Derive upstream category from case type + subcategory."""
    case_type = _get(data, 'group_jl6wb36/Quel_est_le_type_de_cas_que_vo')
    if case_type == 'cas_de_remplacement':
        return 'remplacement'
    if case_type == 'cas_de_suppression__retrait_du_programme':
        return 'suppression'
    # For réclamation, use the subcategory
    reclamation_type = _get(data, 'group_mk9yc92/group_wl7av77/Quel_type_de_r_clamation_souha')
    if reclamation_type == 'cas_sensibles':
        cats = _get(data, 'group_mk9yc92/group_wl7av77/Si_cas_sensibles_pr_cisez_', '')
        return cats.split(' ')[0] if cats else 'cas_sensibles'
    if reclamation_type == 'cas_sp_ciaux':
        cats = _get(data, 'group_mk9yc92/group_wl7av77/Si_cas_sp_ciaux_pr_cisez_', '')
        return cats.split(' ')[0] if cats else 'cas_sp_ciaux'
    if reclamation_type == 'cas_non_sensibles':
        cats = _get(data, 'group_mk9yc92/group_wl7av77/Si_cas_non_sensibles_pr_cisez_', '')
        return cats.split(' ')[0] if cats else 'cas_non_sensibles'
    return None


def _derive_flags(data):
    """Derive flags from reclamation type."""
    reclamation_type = _get(data, 'group_mk9yc92/group_wl7av77/Quel_type_de_r_clamation_souha')
    if reclamation_type == 'cas_sensibles':
        return 'SENSITIVE'
    if reclamation_type == 'cas_sp_ciaux':
        return 'SPECIAL'
    return None


def _build_json_ext(data):
    """Build the full json_ext structure from KoBo data."""
    case_type = _get(data, 'group_jl6wb36/Quel_est_le_type_de_cas_que_vo')

    json_ext = {
        'form_version': '2025_v2',
        'form_id': FORM_ID,
        'case_type': case_type,

        'reporter': {
            'is_beneficiary': _get(data, 'group_jv1rf66/_1_tes_vous_b_n_ficiaire_du_p'),
            'beneficiary_type': _get(data, 'group_jv1rf66/_2_Si_oui_quel_est_ype_de_b_n_ficiaire_'),
            'is_batwa': _get(data, 'group_jv1rf66/_3_Le_plaignant_appa_le_autochtone_Batwa_'),
            'is_anonymous': _get(data, 'group_jv1rf66/_4_Voulez_vous_rester_anonyme_'),
            'name': _get(data, 'group_jv1rf66/_5_Nom_et_Pr_nom_de_personne_plaignante_'),
            'gender': _get(data, 'group_jv1rf66/_6_Sexe_du_plaignant_'),
            'birth_year': _get(data, 'group_jv1rf66/_7_Ann_e_de_naissance_'),
            'phone': _get(data, 'group_jv1rf66/_8_T_l_phone'),
            'cni_number': _get(data, 'group_jv1rf66/_9_Num_ro_CNI_'),
        },

        'location': {
            'province': _get(data, 'group_mg1dn99/Province'),
            'commune': _get(data, 'group_mg1dn99/Commune'),
            'zone': _get(data, 'group_mg1dn99/Zone'),
            'colline': _get(data, 'group_mg1dn99/Colline'),
            'milieu_residence': _get(data, 'group_mg1dn99/Milieu_de_r_sidence'),
            'gps': _get(data, 'group_mg1dn99/Coordonn_es_GPRS_'),
        },

        'categorization': {
            'reclamation_type': _get(data, 'group_mk9yc92/group_wl7av77/Quel_type_de_r_clamation_souha'),
            'sensitive_categories': (_get(data, 'group_mk9yc92/group_wl7av77/Si_cas_sensibles_pr_cisez_') or '').split(' ') if _get(data, 'group_mk9yc92/group_wl7av77/Si_cas_sensibles_pr_cisez_') else [],
            'special_categories': (_get(data, 'group_mk9yc92/group_wl7av77/Si_cas_sp_ciaux_pr_cisez_') or '').split(' ') if _get(data, 'group_mk9yc92/group_wl7av77/Si_cas_sp_ciaux_pr_cisez_') else [],
            'non_sensitive_categories': (_get(data, 'group_mk9yc92/group_wl7av77/Si_cas_non_sensibles_pr_cisez_') or '').split(' ') if _get(data, 'group_mk9yc92/group_wl7av77/Si_cas_non_sensibles_pr_cisez_') else [],
        },

        'submission': {
            'channels': (_get(data, 'group_gm7ai04/Par_quel_s_canal_au_l_soumis_sa_plainte_') or '').split(' '),
            'collector_name': _get(data, 'group_qw2bg35/Nom_et_Pr_nom_de_la_ecueilli_la_plainte_'),
            'collector_function': _get(data, 'group_qw2bg35/Fonction_'),
            'collector_phone': _get(data, 'group_qw2bg35/Num_ro_de_t_l_phone_'),
            'collection_date': _get(data, 'Date_de_collecte'),
            'signature': _get(data, 'Signature_de_l_agent_re_votre_nom_complet'),
        },

        'resolution_initial': {
            'is_resolved': _get(data, 'group_mb37s20/Est_ce_que_cette_plainte_a_d_j'),
            'current_status': _get(data, 'group_mb37s20/Si_non_r_solue_quel_st_le_statut_actuel_'),
        },
    }

    # Add replacement section if applicable
    if case_type == 'cas_de_remplacement':
        json_ext['replacement'] = {
            'motif': _get(data, 'group_mk9yc92/group_ib7ws94/Pr_cisez_le_motif_du_remplacem'),
            'relationship': _get(data, 'group_qj06k38/Quelles_sont_les_diff_rentes_r'),
            'relationship_other': _get(data, 'group_qj06k38/Pr_cisez_l_autre'),
            'replaced_social_id': _get(data, 'group_qj06k38/Social_ID_du_b_n_ficiaire_remplac'),
            'new_recipient': {
                'nom': _get(data, 'group_qj06k38/Nom_du_nouveau_percepteur'),
                'prenom': _get(data, 'group_qj06k38/Pr_nom_du_nouveau_percepteur'),
                'date_naissance': _get(data, 'group_qj06k38/Date_de_Naissance_du_nouveau_percepteur'),
                'sexe': _get(data, 'group_qj06k38/Sexe_'),
                'telephone': _get(data, 'group_qj06k38/Num_ro_de_t_l_phone_u_nouveau_percepteur'),
                'cni': _get(data, 'group_qj06k38/Num_ro_CNI_du_nouveau_percepteur_'),
            },
            'attachments': {
                'cni_recto': _get(data, 'group_qj06k38/Prendre_une_photo_de_au_percepteur_Retro'),
                'cni_verso': _get(data, 'group_qj06k38/Prendre_une_photo_de_percepteur_Verso_'),
                'photo_passeport': _get(data, 'group_qj06k38/Prendre_une_photo_de_nouveau_percepteur_'),
                'certificat_deces': _get(data, 'group_qj06k38/Si_d_c_s_Prendre_un_Certificat_de_d_c_s_'),
                'pv_familial': _get(data, 'group_qj06k38/Prendre_une_photo_du_le_comit_collinaire'),
            },
        }

    # Add suppression section if applicable
    if case_type == 'cas_de_suppression__retrait_du_programme':
        json_ext['suppression'] = {
            'motif': _get(data, 'group_mk9yc92/group_xi58w45/Pr_cisez_le_motif_de_suppression_'),
        }

    # Add migration section if applicable
    if 'migration' in (json_ext['categorization'].get('special_categories') or []):
        json_ext['migration'] = {
            'province': _get(data, 'group_co1to01/Province_002'),
            'commune': _get(data, 'group_co1to01/Commune_10'),
            'zone': _get(data, 'group_co1to01/Zone_10'),
            'colline': _get(data, 'group_co1to01/Colline_Quartier_Migrant'),
            'milieu_residence': _get(data, 'group_co1to01/Milieu_de_r_sidence_Migrant'),
        }

    return json_ext


class GrievanceConverterV2:
    """Converts new KoBo grievance form data to Ticket + json_ext."""

    @classmethod
    def to_ticket(cls, kobo_data):
        """Convert a single KoBo submission to a Ticket."""
        user = User.objects.get(id=DEFAULT_USER_ID)

        colline_code = _get(kobo_data, 'group_mg1dn99/Colline', '')
        collection_date = _get(kobo_data, 'Date_de_collecte', '')
        title = f"{colline_code}-{collection_date}"

        json_ext = _build_json_ext(kobo_data)
        channels = ' '.join(json_ext.get('submission', {}).get('channels', []))

        ticket = Ticket(
            id=kobo_data.get('_uuid'),
            title=title,
            description=_get(kobo_data, 'group_nn77z12/Br_ve_description_de_la_plaint'),
            status=_derive_status(kobo_data),
            category=_derive_category(kobo_data),
            flags=_derive_flags(kobo_data),
            channel=channels,
            date_of_incident=datetime.strptime(collection_date, '%Y-%m-%d').date() if collection_date else None,
            json_ext=json_ext,
            user_created=user,
            user_updated=user,
        )
        return ticket

    @classmethod
    def import_and_create_workflow(cls, kobo_data):
        """Import ticket and auto-create workflow."""
        ticket = cls.to_ticket(kobo_data)
        ticket.save(username='kobo_import')

        # Create ReplacementRequest if applicable
        json_ext = ticket.json_ext or {}
        if json_ext.get('case_type') == 'cas_de_remplacement':
            replacement_data = json_ext.get('replacement', {})
            new_recipient = replacement_data.get('new_recipient', {})
            if replacement_data.get('replaced_social_id'):
                ReplacementRequest.objects.create(
                    ticket=ticket,
                    replaced_social_id=replacement_data.get('replaced_social_id', ''),
                    motif=replacement_data.get('motif', ''),
                    relationship=replacement_data.get('relationship', ''),
                    new_nom=new_recipient.get('nom', ''),
                    new_prenom=new_recipient.get('prenom', ''),
                    new_date_naissance=new_recipient.get('date_naissance'),
                    new_sexe=new_recipient.get('sexe', ''),
                    new_telephone=new_recipient.get('telephone'),
                    new_cni=new_recipient.get('cni', ''),
                    json_ext={'attachments': replacement_data.get('attachments')},
                )

        # Auto-create workflow
        workflows = WorkflowService.auto_create_workflow(ticket)
        logger.info(
            f"Imported ticket {ticket.code} with {len(workflows)} workflow(s)"
        )
        return ticket, workflows

    @classmethod
    def import_batch(cls, kobo_data_list):
        """Import a batch of KoBo submissions."""
        results = []
        for kobo_data in kobo_data_list:
            try:
                ticket, workflows = cls.import_and_create_workflow(kobo_data)
                results.append((ticket, workflows, None))
            except Exception as e:
                logger.error(f"Failed to import grievance {kobo_data.get('_uuid')}: {e}")
                results.append((None, None, str(e)))
        return results
