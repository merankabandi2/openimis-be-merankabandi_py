from django.core.management.base import BaseCommand
from merankabandi.workflow_models import WorkflowTemplate, WorkflowStepTemplate


TEMPLATES = [
    {
        'name': 'replacement_deces',
        'label': 'Remplacement - Décès du bénéficiaire',
        'case_type': 'remplacement:d_c_s_du_b_n_ficiaire',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('validate_death_cert', 'Valider le certificat de décès', 'RVBG', 'validate_death_certificate'),
            ('create_replacement', 'Créer la demande de remplacement', 'RVBG', 'create_replacement_request'),
            ('deactivate', "Désactiver l'ancien bénéficiaire", 'RIUIRCH', 'beneficiary_deactivate'),
            ('replace', 'Créer le nouveau bénéficiaire', 'RIUIRCH', 'beneficiary_replace'),
            ('setup_account', 'Configurer le compte de paiement', 'RTM', 'create_mobile_account'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'replacement_emigration',
        'label': 'Remplacement - Émigration du bénéficiaire',
        'case_type': 'remplacement:d_m_nagement_du_b_n_ficiaire',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('validate_migration', 'Valider les données de migration', 'RIUIRCH', 'validate_migration_data'),
            ('create_replacement', 'Créer la demande de remplacement', 'RVBG', 'create_replacement_request'),
            ('deactivate', "Désactiver l'ancien bénéficiaire", 'RIUIRCH', 'beneficiary_deactivate'),
            ('update_location', 'Mettre à jour la localité du migrant', 'RIUIRCH', 'location_update'),
            ('replace', 'Créer le nouveau bénéficiaire', 'RIUIRCH', 'beneficiary_replace'),
            ('setup_account', 'Configurer le compte de paiement', 'RTM', 'create_mobile_account'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'replacement_remariage',
        'label': 'Remplacement - Remariage du bénéficiaire',
        'case_type': 'remplacement:remariage_du_b_n_ficiaire',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('investigate', 'Investiguer le ménage', 'RVBG', 'investigate_household'),
            ('create_replacement', 'Créer la demande de remplacement', 'RVBG', 'create_replacement_request'),
            ('deactivate', "Désactiver l'ancien bénéficiaire", 'RIUIRCH', 'beneficiary_deactivate'),
            ('replace', 'Créer le nouveau bénéficiaire', 'RIUIRCH', 'beneficiary_replace'),
            ('setup_account', 'Configurer le compte de paiement', 'RTM', 'create_mobile_account'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'replacement_refus',
        'label': 'Remplacement - Refus de statut de bénéficiaire',
        'case_type': 'remplacement:perte_du_statut_de_b_n_ficiaire',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('create_replacement', 'Créer la demande de remplacement', 'RVBG', 'create_replacement_request'),
            ('deactivate', "Désactiver l'ancien bénéficiaire", 'RIUIRCH', 'beneficiary_deactivate'),
            ('replace', 'Créer le nouveau bénéficiaire', 'RIUIRCH', 'beneficiary_replace'),
            ('setup_account', 'Configurer le compte de paiement', 'RTM', 'create_mobile_account'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'suppression_erreur_inclusion',
        'label': "Suppression - Erreur d'inclusion",
        'case_type': 'suppression:erreur_d_inclusion',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('investigate', "Investiguer l'inclusion", 'RIUIRCH', 'investigate_inclusion'),
            ('deactivate', 'Désactiver le bénéficiaire', 'RIUIRCH', 'beneficiary_deactivate'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'suppression_volontaire',
        'label': 'Suppression - Demande volontaire',
        'case_type': 'suppression:demande_volontaire_du_b_n_ficiaire',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('confirm', 'Confirmer la demande volontaire', 'RIUIRCH', 'confirm_voluntary'),
            ('deactivate', 'Désactiver le bénéficiaire', 'RIUIRCH', 'beneficiary_deactivate'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'suppression_double',
        'label': 'Suppression - Double inscription',
        'case_type': 'suppression:double_inscription_d_tect_e',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('identify_dup', 'Identifier le doublon', 'RIUIRCH', 'identify_duplicate'),
            ('merge', 'Fusionner les enregistrements', 'RIUIRCH', 'merge_records'),
            ('deactivate', 'Désactiver le doublon', 'RIUIRCH', 'beneficiary_deactivate'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'suppression_deces_sans_remplacement',
        'label': 'Suppression - Décès sans remplacement',
        'case_type': 'suppression:d_c_s_sans_demande_de_remplacement',
        'steps': [
            ('verify_social_id', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('validate_death_cert', 'Valider le certificat de décès', 'RVBG', 'validate_death_certificate'),
            ('deactivate', 'Désactiver le bénéficiaire', 'RIUIRCH', 'beneficiary_deactivate'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'payment_non_reception',
        'label': 'Paiement - Non réception',
        'case_type': 'reclamation:non_sensible:probl_me_de_paiement__non_r_ception__mon',
        'steps': [
            ('verify_payment', "Vérifier l'historique de paiement", 'RTM', 'verify_payment_history'),
            ('investigate', 'Investiguer le paiement', 'RTM', 'investigate_payment'),
            ('reissue', 'Réémettre le paiement', 'RTM', 'payment_reissue'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'payment_retard',
        'label': 'Paiement - Retard',
        'case_type': 'reclamation:non_sensible:probl_me_de_paiement__retard',
        'steps': [
            ('verify_payment', "Vérifier l'historique de paiement", 'RTM', 'verify_payment_history'),
            ('investigate', 'Investiguer le retard', 'RTM', 'investigate_delay'),
            ('escalate', "Escalader à l'agence de paiement", 'RTM', 'escalate_payment_agency'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'payment_montant_incorrect',
        'label': 'Paiement - Montant incorrect',
        'case_type': 'reclamation:non_sensible:probl_me_de_paiement__montant',
        'steps': [
            ('verify_payment', "Vérifier l'historique de paiement", 'RTM', 'verify_payment_history'),
            ('calculate', 'Calculer le complément', 'RTM', 'calculate_complement'),
            ('reissue', 'Réémettre le paiement', 'RTM', 'payment_reissue'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'sim_lost_stolen_blocked',
        'label': 'Carte SIM - Perdue/volée/bloquée',
        'case_type': 'reclamation:non_sensible:carte_sim__bloqu_e__vol_e__perdue__etc',
        'steps': [
            ('suspend', 'Suspendre le compte', 'RTM', 'account_suspend'),
            ('new_sim', 'Attribuer nouvelle carte SIM', 'RTM', 'sim_attribution'),
            ('reactivate', 'Réactiver le compte', 'RTM', 'account_reactivate'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'phone_lost_stolen',
        'label': 'Téléphone - Perdu/volé/endommagé',
        'case_type': 'reclamation:non_sensible:probl_mes_de_t_l_phone__vol__endommag__n',
        'steps': [
            ('suspend', 'Suspendre le compte', 'RTM', 'account_suspend'),
            ('new_phone', 'Attribuer nouveau téléphone', 'RTM', 'phone_attribution'),
            ('new_sim', 'Attribuer nouvelle carte SIM', 'RTM', 'sim_attribution'),
            ('reactivate', 'Réactiver le compte', 'RTM', 'account_reactivate'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'phone_no_tm',
        'label': 'Téléphone - Ne reçoit pas les TM',
        'case_type': 'reclamation:non_sensible:probl_mes_de_t_l_phone__no_tm',
        'steps': [
            ('diagnostic', 'Diagnostic technique', 'RTM', 'technical_diagnostic'),
            ('sim_check', 'Vérifier la carte SIM', 'RTM', 'sim_check'),
            ('resolve', 'Résolution manuelle', 'RTM', 'manual_resolution'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'phone_password_forgot',
        'label': 'Téléphone - Mot de passe oublié',
        'case_type': 'reclamation:non_sensible:probl_mes_de_t_l_phone__mdp',
        'steps': [
            ('reset', 'Réinitialiser le PIN', 'RTM', 'pin_reset'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'account_not_activated',
        'label': 'Compte - Non activé',
        'case_type': 'reclamation:non_sensible:probl_mes_de_compte_mobile_money__ecocas',
        'steps': [
            ('create_account', 'Créer le compte mobile', 'RTM', 'create_mobile_account'),
            ('reactivate', 'Activer le compte', 'RTM', 'account_reactivate'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'account_blocked',
        'label': 'Compte - Bloqué',
        'case_type': 'reclamation:non_sensible:probl_mes_de_compte_mobile_money__bloque',
        'steps': [
            ('investigate', 'Investiguer le compte', 'RTM', 'investigate_account'),
            ('unblock', 'Débloquer le compte', 'RTM', 'unblock_account'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
    {
        'name': 'data_correction',
        'label': 'Incohérence des données personnelles',
        'case_type': 'reclamation:non_sensible:incoh_rence_des_donn_es_personnelles__nu',
        'steps': [
            ('verify', "Vérifier l'individu", 'RSI', 'verify_individual'),
            ('update', 'Mettre à jour les données', 'RSI', 'individual_update'),
            ('notify', 'Notification finale', 'RSI', 'notify_completion'),
        ],
    },
    {
        'name': 'information_request',
        'label': "Demande d'information et d'assistance",
        'case_type': 'reclamation:non_sensible:demande_d_information',
        'steps': [
            ('provide', "Fournir l'information", 'OT', 'provide_information'),
            ('resolve', 'Résolution', 'OT', 'manual_resolution'),
            ('notify', 'Notification finale', 'OT', 'notify_completion'),
        ],
    },
    {
        'name': 'special_erreur_inclusion',
        'label': "Cas spécial - Erreur d'inclusion potentielle",
        'case_type': 'reclamation:speciale:erreur_d_inclusion_potentielle',
        'steps': [
            ('investigate', "Investiguer l'inclusion", 'RIUIRCH', 'investigate_inclusion'),
            ('resolve', 'Résolution manuelle', 'RIUIRCH', 'manual_resolution'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'special_cible_pas_collecte',
        'label': 'Cas spécial - Ciblé mais pas collecté',
        'case_type': 'reclamation:speciale:cibl__mais_pas_collect',
        'steps': [
            ('verify', 'Vérifier les données de ciblage', 'RIUIRCH', 'verify_targeting'),
            ('add', 'Ajouter à la collecte', 'RIUIRCH', 'add_to_collection'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'special_cible_collecte',
        'label': 'Cas spécial - Ciblé et collecté (non enregistré)',
        'case_type': 'reclamation:speciale:cibl__et_collect',
        'steps': [
            ('verify_targeting', 'Vérifier le ciblage', 'RIUIRCH', 'verify_targeting'),
            ('verify_individual', "Vérifier l'enregistrement", 'RIUIRCH', 'verify_individual'),
            ('re_register', 'Ré-enregistrer', 'RIUIRCH', 're_register'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'migration_changement_localite',
        'label': 'Cas spécial - Migration / Changement de localité',
        'case_type': 'reclamation:speciale:migration',
        'steps': [
            ('verify', 'Vérifier le Social ID', 'RIUIRCH', 'verify_social_id'),
            ('validate', 'Valider les données de migration', 'RIUIRCH', 'validate_migration_data'),
            ('update', 'Mettre à jour la localité', 'RIUIRCH', 'location_update'),
            ('notify', 'Notification finale', 'RIUIRCH', 'notify_completion'),
        ],
    },
    {
        'name': 'sensitive_eas_hs',
        'label': 'Cas sensible - EAS/HS',
        'case_type': 'reclamation:sensible:eas_hs__exploitation__abus_sexuel___harc',
        'steps': [
            ('referral', 'Référer aux services sociaux', 'RVBG', 'external_referral'),
            ('hospital', 'Accompagnement hospitalier', 'RVBG', 'hospital_referral'),
            ('legal', 'Plainte légale', 'RVBG', 'legal_complaint'),
            ('psychosocial', 'Support psychosocial', 'RVBG', 'psychosocial_support'),
            ('resolve', 'Résolution', 'RVBG', 'manual_resolution'),
            ('notify', 'Notification finale', 'RVBG', 'notify_completion'),
        ],
    },
    {
        'name': 'sensitive_prelevements',
        'label': 'Cas sensible - Prélèvements de fonds',
        'case_type': 'reclamation:sensible:pr_l_vements_de_fonds',
        'steps': [
            ('investigate', 'Investiguer les prélèvements', 'RDO', 'investigate_funds'),
            ('resolve', 'Résolution', 'RDO', 'manual_resolution'),
            ('notify', 'Notification finale', 'RDO', 'notify_completion'),
        ],
    },
    {
        'name': 'sensitive_corruption',
        'label': 'Cas sensible - Détournement de fonds / Corruption',
        'case_type': 'reclamation:sensible:d_tournement_de_fonds___corruption',
        'steps': [
            ('escalate', 'Escalader à la direction', 'RDO', 'escalate_management'),
            ('investigate', 'Investigation', 'RDO', 'investigate_corruption'),
            ('discipline', 'Action disciplinaire', 'RDO', 'disciplinary_action'),
            ('notify', 'Notification finale', 'RDO', 'notify_completion'),
        ],
    },
    {
        'name': 'sensitive_conflit_familial',
        'label': 'Cas sensible - Conflit familial',
        'case_type': 'reclamation:sensible:conflit_familial',
        'steps': [
            ('mediation', 'Médiation', 'RVBG', 'mediation'),
            ('reassign', 'Réattribuer le percepteur', 'RVBG', 'reassign_payment_recipient'),
            ('resolve', 'Résolution', 'RVBG', 'manual_resolution'),
            ('notify', 'Notification finale', 'RVBG', 'notify_completion'),
        ],
    },
    {
        'name': 'sensitive_accident',
        'label': 'Cas sensible - Accident grave / Négligence',
        'case_type': 'reclamation:sensible:accident_grave_ou_n_gligence_professionn',
        'steps': [
            ('investigate', "Investiguer l'incident", 'RVBG', 'investigate_incident'),
            ('referral', 'Référer aux services externes', 'RVBG', 'external_referral'),
            ('resolve', 'Résolution', 'RVBG', 'manual_resolution'),
            ('notify', 'Notification finale', 'RVBG', 'notify_completion'),
        ],
    },
    {
        'name': 'phone_number_reassignment',
        'label': 'Réattribution de numéro de téléphone',
        'case_type': 'reclamation:non_sensible:phone_reassignment',
        'steps': [
            ('verify', 'Vérifier les enregistrements téléphoniques', 'RTM', 'verify_phone_records'),
            ('swap', 'Permuter les numéros', 'RTM', 'phone_number_swap'),
            ('update', "Mettre à jour l'individu", 'RSI', 'individual_update'),
            ('notify', 'Notification finale', 'RTM', 'notify_completion'),
        ],
    },
]


class Command(BaseCommand):
    help = 'Seed the 29 grievance workflow templates with their steps'

    def add_arguments(self, parser):
        parser.add_argument('--force', action='store_true', help='Delete and recreate all templates')

    def handle(self, *args, **options):
        force = options.get('force', False)

        for tpl_data in TEMPLATES:
            existing = WorkflowTemplate.objects.filter(name=tpl_data['name']).first()
            if existing and not force:
                self.stdout.write(f"  SKIP {tpl_data['name']} (exists)")
                continue
            if existing and force:
                existing.delete()

            tpl = WorkflowTemplate.objects.create(
                name=tpl_data['name'],
                label=tpl_data['label'],
                case_type=tpl_data['case_type'],
                is_active=True,
                json_ext=tpl_data.get('json_ext'),
            )

            for order, (name, label, role, action_type) in enumerate(tpl_data['steps'], start=1):
                WorkflowStepTemplate.objects.create(
                    workflow_template=tpl,
                    name=name,
                    label=label,
                    order=order,
                    role=role,
                    action_type=action_type,
                    is_required=True,
                    json_ext={'sla_days': 7},
                )

            self.stdout.write(self.style.SUCCESS(f"  CREATED {tpl_data['name']} ({len(tpl_data['steps'])} steps)"))

        total = WorkflowTemplate.objects.filter(is_active=True).count()
        self.stdout.write(self.style.SUCCESS(f"\nTotal active templates: {total}"))
