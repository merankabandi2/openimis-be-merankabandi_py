"""
Management command to normalize json_ext field names on Individual and Group records.
Renames non-normalized KoBoToolbox field names to clean snake_case equivalents,
and drops survey metadata / redundant fields.

Usage:
    python manage.py normalize_json_ext --dry-run       # Preview changes
    python manage.py normalize_json_ext                  # Apply changes
    python manage.py normalize_json_ext --table individual  # Only individuals
    python manage.py normalize_json_ext --table group       # Only groups
"""

import json
import logging
from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# INDIVIDUAL json_ext: rename mapping
# ─────────────────────────────────────────────────────────────────────────────
INDIVIDUAL_RENAME = {
    # Core demographic fields
    "Lien.avec.le.chef.de.ménage.de": "lien",
    "Statut.matrimonial.de": "etat_civil",
    "Age.au.premier.mariage.de": "age_mariage",
    "Nationalité.de": "nationalite",
    "Aptitude.à.lire.et.ou.écrire.de": "lit",
    "Niveau.d.instruction.le.plus.élevé.de": "instruction",
    # Health
    "Quel.type.de.handicap.a...": "type_handicap",
    "Si.Oui..ce.handicap.l.empêche.t.il.de.travailler..": "handicap_empechement",
    "X..a.t.il.une.maladie.chronique": "maladie_chro",
    "Quel.type.de.maladie.chronique...a.t.il..": "maladie_chro_type",
    "X.est.elle.enceinte.ou.allaitante..": "enceinte_allaitante",
    # Birth certificate (yes/no, NOT the CNI number)
    "Possession.d.un.acte.d.Etat.civil..naissance..de": "acte_naissance",
}

# INDIVIDUAL json_ext: keys to drop (redundant or survey metadata)
INDIVIDUAL_DROP = [
    # Redundant with existing normalized fields
    "Nom.et.prénom.du.membre.du.ménage",
    "Sexe.de",
    "Date.de.naissance.de",
    "Agecal",
    "Age.au.dernier.anniversaire..en.année..de",
    "chief1",
    "X..a.t.il.un.handicap..",  # redundant with 'handicap'
    # Disease sub-booleans (detail of maladie_chro_type)
    "Diabète",
    "Hypertension.artérielle",
    "Epilepsie",
    "Cancer",
    "VIH.SIDA",
    "Autres",
    "Autre.type.de.maladie.chronique",
    # Survey internals
    "A2_1",
    "A2_2",
    "SECTD",
    "SECTG",
    "SECTF",
    "SECTMF",
    "SECTMH",
    "SECTMFI",
    "SECTMG",
    "conjoint_single",
    "conj_cel",
    "ID",
    # KoBo submission metadata
    "_index",
    "_parent_table_name",
    "_parent_index",
    "_submission__id",
    "_submission__uuid",
    "_submission__submission_time",
    "_submission__validation_status",
    "_submission__notes",
    "_submission__status",
    "_submission__submitted_by",
    "_submission___version__",
    "_submission__tags",
]

# ─────────────────────────────────────────────────────────────────────────────
# GROUP json_ext: rename mapping
# ─────────────────────────────────────────────────────────────────────────────
GROUP_RENAME = {
    # Respondent identity
    "Nom.et.prénom.du.répondant": "repondant_nom",
    "Nom.et.prénom.du.chef.de.ménage": "chef_menage_nom",
    "Nom.et.prénom.du.père.du.répondant": "repondant_pere",
    "Nom.et.prénom.de.la.mère.du.répondant": "repondant_mere",
    "Numéro.de.téléphone.du.chef.de.ménage": "telephone",
    "Numéro.de.téléphone.du.répondant": "repondant_telephone",
    "Numéro.de.téléphone.de.la.personne.de.contact.s.il.n.a.pas.lui.même.un.téléphone": "telephone_contact",
    "Age.du.répondant": "repondant_age",
    "Sexe.du.répondant": "repondant_sexe",
    "Lien.de.parenté.avec.le.chef.du.ménage": "repondant_lien",
    "Autre.lien.de.parenté.avec.le.chef.du.ménage": "repondant_lien_autre",
    # Respondent CNI
    "Numéro.de.la.carte.nationale.d.identité..CNI.": "cni",
    "Photo.du.répondant": "photo",
    "Photo_du_r_pondant_URL": "photo_URL",
    "Photo.de.la.carte.nationale.d.identité..Recto.": "cni_photo_recto",
    "Photo_Recto_URL": "cni_photo_recto_url",
    "Photo.de.la.carte.nationale.d.indetité..Verso.": "cni_photo_verso",
    "Photo_Verso_URL": "cni_photo_verso_url",
    "Date.da.le.délivrance.de.la.carte.nationale.d.identité": "ci_date_emission",
    "Nom.de.la.Commune.qui.a.délivré.la.carte.nationale.d.identité": "ci_lieu_emission",
    # Household type
    "Milieu.de.résidence": "milieu_residence",
    "Date.de.l.interview": "date_creation",
    "Type.de.ménage": "type_menage",
    "Ménage.avec.deux.parents": "menage_deux_parents",
    "Ménage.mono.parental": "menage_type_monoparental",
    "Si..Ménage.mono.parental..est.ce.le.père.ou.la.mère.": "menage_monoparental_detail",
    "Ménage.d.orphelin": "menage_orphelin",
    "Ménage.sans.lien.de.parenté": "menage_sans_parente",
    "Ménage.d.un.mutwa": "menage_mutwa",
    "Ménage.d.un.déplacé": "menage_deplace",
    "Ménage.d.un.rapatrié.récent..moins.de.2.ans.dans.le.pays.": "menage_rapatrie",
    "Taille.du.ménage": "taille_menage",
    # Housing
    "Quel.est.le.type.de.votre.logement..": "logement_type",
    "Nombre.de.pièce.utilisé.pour.dormir": "logement_pieces",
    "Statut.d.occupation": "logement_statut",
    "Autre.statut.d.occupation": "logement_statut_autre",
    "Autre.statut.d.occupation_1": "logement_statut_autre",
    "Principal.matériau.de.construction.des.murs.externes": "logement_murs",
    "Autre.principal.matériau.de.construction.des.murs.externes": "logement_murs_autre",
    "Principal.matériau.du.toit": "logement_toit",
    "Autre.principal.matériau.du.toit": "logement_toit_autre",
    "Revêtement.du.sol.de.la.maison": "logement_sol",
    "Autre.revêtement.du.sol.de.la.maison": "logement_sol_autre",
    "Quel.est.le.mode.d.approvisionnement.en.eau.de.boisson..": "logement_eau_boisson",
    "Autre.mode.d.approvisionnement.en.eau.de.boisson": "logement_eau_autre",
    "Distance.entre.le.ménage.et.le.point.d.approvisionnement.en.eau.de.boisson..en.m": "logement_distance_eau",
    "Quelle.est.la.principale.source.d.éclairage.du.logement": "logement_electricite",
    "Type.de.combustibles.pour.la.cuisson": "logement_cuisson",
    "Autre.type.de.combustibles.pour.la.cuisson": "logement_cuisson_autre",
    "Quel.type.de.toilette.dispose.le.ménage..": "logement_toilettes",
    # Cooking fuel sub-booleans
    "Bois.ramassé": "cuisson_bois_ramasse",
    "Bois.acheté": "cuisson_bois_achete",
    "Charbon.de.bois": "cuisson_charbon",
    "Gaz": "cuisson_gaz",
    "Electricité": "cuisson_electricite",
    "Pétrole.huile": "cuisson_petrole",
    "Déchets.d.animaux": "cuisson_dechets_animaux",
    "Tourbe": "cuisson_tourbe",
    # Possessions
    "Poste.de.radio.Radio.simple.ou.Radio.cassette": "possessions_radio",
    "Poste.de.télévision": "possessions_tele",
    "Téléphone.portable": "possessions_smartphone",
    "Fer.à.repasser": "possessions_fer",
    "Vêtements.et.chaussures.de.sortie.ou.de.fete": "possessions_vetements",
    "Réfrigérateur": "possessions_frigo",
    "Matelas": "possessions_matelas",
    "Lit": "possessions_lits",
    "Chaises": "possessions_chaises",
    "Houe": "possessions_houe",
    "Machette.Serpette": "possessions_machette",
    "Vélo.ou.Bicyclette": "possessions_velo",
    "Cuisinière": "possessions_cuisinieres",
    "Voiture": "possessions_voitures",
    "Moulin": "possessions_moulins",
    "Filet.de.pêche": "possessions_filets",
    "Pirogue": "possessions_pirogues",
    "Possédez.vous.un.savon.pour.la.toilette.et.pour.la.lessive..": "possessions_savon",
    # Agriculture
    "Possédez.vous.des.terrains.cultivables.propres..": "a_terres",
    "Exploitez.vous.des.terres.qui.ne.vous.appartiennent.pas..terres.louées.ou.prêtée": "exploite_terres",
    "Votre.production.est.elle.satisfaisante.à.vos.besoins.alimentaires..": "production_satisfaction",
    "Si.pas.satisfaisant..qu.est.ce.qui.vous.empêche.de.produire.de.façon.satisfaisan": "production_raisons",
    "Pas.de.propriété.suffisante": "production_raison_propriete",
    "Difficulté.d.avoir.des.engrais": "production_raison_engrais",
    "Difficulté.d.avoir.des.semences.sélectionnées": "production_raison_semences",
    "Sécheresse": "production_raison_secheresse",
    "Forte.pluviométrie..inondation.grêle": "production_raison_pluie",
    "Maladies.des.plantes": "production_raison_maladies",
    # Livestock
    "Pratiquez.vous.l.élevage..": "a_elevage",
    "Combien.de.têtes.de.Bovin.avez.vous..": "elevage_bovins",
    "Combien.de.têtes.d.Ovin.avez.vous..": "elevage_ovins",
    "Combien.de.têtes.de.Caprin.avez.vous..": "elevage_caprins",
    "Combien.de.têtes.de.Porcin.avez.vous..": "elevage_porcins",
    "Combien.de.têtes.de.Volailles.avez.vous..": "elevage_volailles",
    "Combien.de.têtes.de.Cuniculture.lapins..avez.vous..": "elevage_lapins",
    "Combien.de.têtes.de..avez.vous..": "elevage_autres",
    "Où.vivent.ces.animaux": "elevage_ou",
    # Livestock type booleans
    "Bovin": "elevage_type_bovin",
    "Ovin": "elevage_type_ovin",
    "Caprin": "elevage_type_caprin",
    "Porcin": "elevage_type_porcin",
    "Volailles": "elevage_type_volailles",
    "Cuniculture..lapins.": "elevage_type_lapins",
    # Livestock housing booleans
    "A.l.étable": "elevage_lieu_etable",
    "A.la.maison": "elevage_lieu_maison",
    "Dans.la.concession.à.l.aire.libre": "elevage_lieu_aire_libre",
    "Attaché.dans.la.cours": "elevage_lieu_attache",
    # Food security
    "Ces.7.derniers.jours..est.il.arrivé.que.le.ménage.soit.sans.nourriture.du.tout..": "alimentaire_sans_nourriture",
    "Avec.quelle.fréquence.au.cours.de.ces.7.derniers.jour..": "alimentaire_frequence",
    "Combien.de.fois.par.jour.mangent.les.enfants.de.moins.de.5ans..": "alimentaire_num_repas_5_ans",
    "Combien.de.fois.par.jour.mangent.les.adultes": "alimentaire_num_repas_adultes",
    "Avez.vous.été.formé..sensibilisé.sur.l.alimentation.équilibrée..": "alimentaire_formation",
    "Dépendez.vous.entierement.d.une.aide.alimentaire.humanitaire.": "alimentaire_dependance_aide",
    "La.répondante.est.elle.enceinte.ou.allaittante...": "repondante_enceinte_allaitante",
    # Health section
    "Le.ménage.a.t.il.connu.des.cas.de.decès.les.12.derniers.mois..": "sante_deces_12_mois",
    "Quelle.était.la.raison.de.ce.decès..": "sante_deces_raison",
    "Y.a.t.il.des.membres.de.votre.famille.qui.ont.eu.des.problèmes.de.santé.au.cours": "sante_problemes",
    "Etes.vous.couverts.par.une.assurance.maladie..": "assurance",
    "Si.Oui..laquelle..": "assurance_type",
    "Autres.types.d.assurance": "assurance_type_autre",
    # Financial inclusion
    "Est.ce.qu.un.membre.de.votre.ménage.possède.un.compte.bancaire.dans.un.établisse": "transfert_compte",
    "A.t.il.de.l.épargne.sur.ce.compte.au.cours.des.12.derniers.mois..": "transfert_epargne",
    "Est.ce.qu.un.membre.de.votre.ménage.participe.à.un.groupement.d.épargne.et.de.cr": "transfert_groupement",
    "Avez.vous.beneficié.d.un.pret.aux.cours.des.douze.derniers.mois..": "transfert_pret",
    "Quelle.est.la.source.de.ce.prêt..": "transfert_pret_source",
    # Transfers
    "Recevez.vous.des.transferts.monétaires.ou.des.appuis.en.nature..": "transfert_recoit",
    "Si.oui..est.ce.en.espèce.ou.en.nature..": "transfert_nature",
    "Si.c.est.en.nature.qu.est.ce.que.vous.avez.reçu..": "transfert_nature_type",
    # Transfer sources (monetary)
    "Un.membre.de.la.famille": "transfert_source_famille",
    "Programmes.projets.de.l.Etat": "transfert_source_etat",
    "Partenaire.nationaux.internationaux_1": "transfert_source_partenaire",
    "Association.d.appartenances_1": "transfert_source_asso",
    "Autres_4": "transfert_source_autres",
    "Sécurité.sociale..INSS.ONPR.": "transfert_source_securite_sociale",
    # Le ménage est déjà bénéficiaire
    "Le.ménage.fait.il.déjà.partie.des.ménages.bénéficiaires.du.projet.MERANKABANDI.": "deja_beneficiaire",
}

# GROUP json_ext: keys to drop (survey metadata, section markers, redundant)
GROUP_DROP = [
    # Survey flow / section markers
    "start", "end", "username", "phonenumber",
    "Pouvons.nous.commencer..",
    "DebuLoc", "DebuID", "DebuA", "DebuB", "DebuC", "DebuE", "DebuF", "DebuH", "DebuI", "DebuJ",
    "FinID", "FinA", "FinB", "FinC", "FinE", "FinF", "FinH", "FinI", "FinJ",
    "calculate", "sum_chief",
    "Numéro.séquentiel.du.questionnaire.géneré.automatiquement",
    "Enquêteur..veuillez.selectionner.le.site.de.collecte",
    "Veuillez.selectionner.votre.nom",
    "enqueteur_label",
    "Statut.du.questionnaire.à.la.fin.de.l.interview",
    "Vous.avez.indiqué.deux.chefs.de.ménage..veuillez.rentrer.et.corriger",
    "Vous.n.avez.indiqué.aucun.chef.de.ménage..veuillez.rentrer.et.corriger",
    "Autre..à.préciser.",
    "Autre..préciser.",
    "HB",
    "hhid",
    # Section headers (no data value)
    "Est.ce.que.votre.ménage.possède.les.objets.suivants..",
    "Quel.type.d.élevage..",
    "Si.Oui..pour.quelles.raisons..vous.êtes.il.arrivé.d.être.sans.nourriture.au.cour",
    "Manque.de.ressources.financières",
    "Indisponibilité.des.denrées.alimentaires.sur.le.marché",
    "Precisez.si..autre.",
    "Si.oui..de.quelle.source.dépendez.vous.pour.l.aide.alimentaire.",
    "Si.oui..de.quelle.source.recevez.vous.des.transferts.monétaires.",
    "Si.oui..lesquels",
    "Partenaire.nationaux.internationaux",
    "Association.d.appartenances",
    "Vous.avez.parlé.de.programmes.projet.d.Etat..Veuillez.préciser.le.nom.du.program",
    "Programmes.projets.de.l.Etat..donnez.le.nom.du.programme.",
    "Autres.a.preciser",
    "Autre.source",
    "Autre.endroit.où.vivent.ces.animaux",
    "Autre.type.d.élevage",
    # Sub-fields of multi-select that are redundant (already captured)
    "Aucun", "Aucun_1", "Autres", "Autres_1", "Autres_2", "Autres_3",
    "Precisez.si..autre._1",
    # Nature type codes (opaque)
    "J1ba", "J1bb", "J1bc", "J1bd", "J1be", "J1bf",
    # KoBo submission metadata
    "_id", "_uuid", "_submission_time", "_validation_status",
    "_notes", "_status", "_submitted_by", "__version__", "_tags", "_index",
    # Redundant location fields (already on the model or in normalized keys)
    "Province_1", "Commune_1", "Colline_1",
    "province_id", "province_code", "commune_id", "commune_code",
    # Long nutrition detail fields (keep as separate export if needed)
    "Quelle.nourriture.solide.ou.semi.solide.a.mangé.votre.enfant.durant.les.dernière",
    "Parmi.les.groupes.d.aliments.suivants..lesquels.avez.vous.consommées.durant.les",
    "Votre.enfant.âgé.de.0.à.5.ans.a.t.il.reçu.les.services.de.nutrition.au.cours.des",
    "Si.Oui..lesquels.parmi.les.services.de.nutrition.ci...dessous.",
    "Suplementation.en.poudre.de.micronutriements",
    "Suplementation.en.vitamine.A",
    "Deparasitage.a.l.albendazole",
    "Avez.vous.recu.des.services.de.nutrition.au.cours.des.deux.derniers.mois.",
    "Non.Applicable", "Non.Applicable_1",
    # Extra survey codes found on groupbeneficiary
    "C2", "F111_1", "H10", "H11", "H11A", "HE", "Q109", "Q110", "SECTA",
    "ID",
    "veuillez.scanner.le.codebar.sur.le.coupon",
    "Sous.colline.Rue",
    "Poisson..fruits.de.mer.et..viande..Viande.de.boeuf..viande.de.mouton..poulet..po",
    "Si.Oui..laquelle..",
    "Autres.types.d.assurance",
    "Etes.vous.couverts.par.une.assurance.maladie..",
]

# GROUP/BENEFICIARY: additional renames for GPS coordinates
GROUP_RENAME_EXTRA = {
    "_Q109_latitude": "latitude",
    "_Q109_longitude": "longitude",
    "_Q109_altitude": "altitude",
    "_Q109_precision": "gps_precision",
}
# Merge into GROUP_RENAME
GROUP_RENAME.update(GROUP_RENAME_EXTRA)

# Nutrition sub-fields to drop (long food group names, duplicated with _1 suffix)
NUTRITION_DROP_PREFIXES = [
    "CEREALES", "TUBERCULES", "LEGUMINEUSES", "NOIX", "LAIT",
    "VIANDE", "ABATS", "POISSON", "LEGUMES", "FRUITS", "AUTRES.LEGUMES",
    "AUTRES.FRUITS", "Œufs", "Sucre", "Lait.et.produits",
]

BATCH_SIZE = 5000


def normalize_record(ext, rename_map, drop_list, drop_prefixes=None):
    """Apply renames and drops to a json_ext dict. Returns (new_dict, changed)."""
    changed = False
    new_ext = {}

    drop_set = set(drop_list)

    for key, value in ext.items():
        # Check if should be dropped
        if key in drop_set:
            changed = True
            continue

        # Check prefix drops
        if drop_prefixes:
            should_drop = False
            for prefix in drop_prefixes:
                if key.startswith(prefix):
                    should_drop = True
                    break
            if should_drop:
                changed = True
                continue

        # Check if should be renamed
        if key in rename_map:
            new_key = rename_map[key]
            # Only rename if the target key doesn't already exist with a value
            if new_key not in ext or ext.get(new_key) in (None, ''):
                new_ext[new_key] = value
            else:
                # Target key already has a value, keep the existing one
                new_ext[new_key] = ext[new_key]
            changed = True
        else:
            new_ext[key] = value

    return new_ext, changed


class Command(BaseCommand):
    help = "Normalize json_ext field names on Individual and Group records"

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Preview changes without applying them',
        )
        parser.add_argument(
            '--table', choices=['individual', 'group', 'beneficiary'],
            help='Only process one table (default: all three)',
        )
        parser.add_argument(
            '--migrate-respondent', action='store_true',
            help='Copy respondent/head fields from group to matching individuals',
        )
        parser.add_argument(
            '--batch-size', type=int, default=BATCH_SIZE,
            help=f'Batch size for updates (default: {BATCH_SIZE})',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        table = options.get('table')
        migrate_respondent = options.get('migrate_respondent')
        batch_size = options['batch_size']

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN — no changes will be applied"))

        if migrate_respondent:
            self._migrate_respondent_fields(batch_size=batch_size, dry_run=dry_run)
            return

        if table in (None, 'individual'):
            self._process_table(
                table_name='individual_individual',
                label='Individual',
                detect_pattern='Lien.avec',
                rename_map=INDIVIDUAL_RENAME,
                drop_list=INDIVIDUAL_DROP,
                drop_prefixes=None,
                batch_size=batch_size,
                dry_run=dry_run,
            )

        if table in (None, 'group'):
            self._process_table(
                table_name='individual_group',
                label='Group',
                detect_pattern='répondant',
                rename_map=GROUP_RENAME,
                drop_list=GROUP_DROP,
                drop_prefixes=NUTRITION_DROP_PREFIXES,
                batch_size=batch_size,
                dry_run=dry_run,
            )

        if table in (None, 'beneficiary'):
            self._process_table(
                table_name='social_protection_groupbeneficiary',
                label='GroupBeneficiary',
                detect_pattern='Milieu.de.résidence',
                rename_map=GROUP_RENAME,
                drop_list=GROUP_DROP,
                drop_prefixes=NUTRITION_DROP_PREFIXES,
                batch_size=batch_size,
                dry_run=dry_run,
            )

    def _process_table(self, table_name, label, detect_pattern,
                       rename_map, drop_list, drop_prefixes, batch_size, dry_run):
        self.stdout.write(f"\n{'='*60}")
        self.stdout.write(f"Processing {label} records ({table_name})")
        self.stdout.write(f"{'='*60}")

        # Count affected records
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE "isDeleted" = false
                  AND "Json_ext" IS NOT NULL
                  AND "Json_ext"::text LIKE %s
            """, [f'%{detect_pattern}%'])
            total = cursor.fetchone()[0]

        self.stdout.write(f"Found {total:,} records to normalize")
        if total == 0:
            return

        # Process in batches — no OFFSET because updated records drop out of
        # the LIKE filter, so we always fetch from the top of the remaining set.
        updated = 0
        processed = 0

        while True:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT "UUID", "Json_ext" FROM {table_name}
                    WHERE "isDeleted" = false
                      AND "Json_ext" IS NOT NULL
                      AND "Json_ext"::text LIKE %s
                    ORDER BY "UUID"
                    LIMIT %s
                """, [f'%{detect_pattern}%', batch_size])
                rows = cursor.fetchall()

            if not rows:
                break

            batch_updates = []
            for uuid_val, ext_raw in rows:
                ext = ext_raw if isinstance(ext_raw, dict) else json.loads(ext_raw)
                new_ext, changed = normalize_record(ext, rename_map, drop_list, drop_prefixes)
                if changed:
                    batch_updates.append((json.dumps(new_ext, ensure_ascii=False), uuid_val))

            if batch_updates and not dry_run:
                with connection.cursor() as cursor:
                    cursor.executemany(f"""
                        UPDATE {table_name}
                        SET "Json_ext" = %s::jsonb
                        WHERE "UUID" = %s
                    """, batch_updates)

            updated += len(batch_updates)
            processed += len(rows)
            self.stdout.write(f"  Processed {min(processed, total):,}/{total:,} — {updated:,} updated")

            # Safety: if no records were changed in this batch, we'd loop forever
            if not batch_updates:
                break

        action = "would update" if dry_run else "updated"
        self.stdout.write(self.style.SUCCESS(f"\n{label}: {action} {updated:,} of {total:,} records"))

    # ─────────────────────────────────────────────────────────────────────
    # Migrate respondent/head fields from Group json_ext to Individual
    # ─────────────────────────────────────────────────────────────────────

    # Fields to copy from group to the RESPONDENT individual (recipient_type=PRIMARY)
    RESPONDENT_FIELD_MAP = {
        # group key → individual key
        'cni': 'ci',
        'ci_date_emission': 'ci_date_emission',
        'ci_lieu_emission': 'ci_lieu_emission',
        'photo': 'photo',
        'photo_URL': 'photo_URL',
        'cni_photo_recto': 'cni_photo_recto',
        'cni_photo_recto_url': 'cni_photo_recto_url',
        'cni_photo_verso': 'cni_photo_verso',
        'cni_photo_verso_url': 'cni_photo_verso_url',
        'repondant_telephone': 'telephone',
        'repondant_pere': 'pere',
        'repondant_mere': 'mere',
    }

    # Fields to copy from group to the HEAD individual (role=HEAD)
    HEAD_FIELD_MAP = {
        'telephone': 'telephone',
        'telephone_contact': 'telephone_contact',
    }

    def _migrate_respondent_fields(self, batch_size, dry_run):
        self.stdout.write(f"\n{'='*60}")
        self.stdout.write("Migrating respondent/head fields from Group → Individual")
        self.stdout.write(f"{'='*60}")

        # Skip if migration was already done (check if any individual has 'ci' from migration)
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM individual_individual
                WHERE "isDeleted" = false
                  AND "Json_ext" IS NOT NULL
                  AND "Json_ext"::text LIKE '%"ci"%'
                LIMIT 1
            """)
            already_migrated = cursor.fetchone()[0] > 0

        if already_migrated and not dry_run:
            self.stdout.write("Already migrated (individuals have 'ci' field) — skipping")
            return

        # Count groups with respondent data
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM individual_group
                WHERE "isDeleted" = false
                  AND "Json_ext" IS NOT NULL
                  AND "Json_ext"::text LIKE '%repondant_nom%'
            """)
            total = cursor.fetchone()[0]

        self.stdout.write(f"Found {total:,} groups with respondent data to migrate")
        if total == 0:
            return

        updated_respondents = 0
        updated_heads = 0
        offset = 0

        while offset < total:
            # Fetch group + respondent individual + head individual in one query
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        g."UUID" AS group_uuid,
                        g."Json_ext" AS group_ext,
                        resp_i."UUID" AS resp_uuid,
                        resp_i."Json_ext" AS resp_ext,
                        head_i."UUID" AS head_uuid,
                        head_i."Json_ext" AS head_ext
                    FROM individual_group g
                    -- Respondent: recipient_type = PRIMARY
                    LEFT JOIN individual_groupindividual resp_gi
                        ON resp_gi.group_id = g."UUID"
                        AND resp_gi."isDeleted" = false
                        AND resp_gi.recipient_type = 'PRIMARY'
                    LEFT JOIN individual_individual resp_i
                        ON resp_i."UUID" = resp_gi.individual_id
                        AND resp_i."isDeleted" = false
                    -- Head: role = HEAD
                    LEFT JOIN individual_groupindividual head_gi
                        ON head_gi.group_id = g."UUID"
                        AND head_gi."isDeleted" = false
                        AND head_gi.role = 'HEAD'
                    LEFT JOIN individual_individual head_i
                        ON head_i."UUID" = head_gi.individual_id
                        AND head_i."isDeleted" = false
                    WHERE g."isDeleted" = false
                      AND g."Json_ext" IS NOT NULL
                      AND g."Json_ext"::text LIKE '%%repondant_nom%%'
                    ORDER BY g."UUID"
                    LIMIT %s OFFSET %s
                """, [batch_size, offset])
                rows = cursor.fetchall()

            if not rows:
                break

            resp_updates = []  # (new_json, individual_uuid)
            head_updates = []

            for group_uuid, group_ext_raw, resp_uuid, resp_ext_raw, head_uuid, head_ext_raw in rows:
                g_ext = group_ext_raw if isinstance(group_ext_raw, dict) else json.loads(group_ext_raw)

                # Copy to respondent individual
                if resp_uuid and resp_ext_raw:
                    r_ext = resp_ext_raw if isinstance(resp_ext_raw, dict) else json.loads(resp_ext_raw)
                    changed = False
                    for g_key, i_key in self.RESPONDENT_FIELD_MAP.items():
                        g_val = g_ext.get(g_key)
                        if g_val is not None and g_val != '':
                            existing = r_ext.get(i_key)
                            if existing is None or existing == '':
                                r_ext[i_key] = g_val
                                changed = True
                    if changed:
                        resp_updates.append((json.dumps(r_ext, ensure_ascii=False), resp_uuid))

                # Copy to head individual
                if head_uuid and head_ext_raw and head_uuid != resp_uuid:
                    h_ext = head_ext_raw if isinstance(head_ext_raw, dict) else json.loads(head_ext_raw)
                    changed = False
                    for g_key, i_key in self.HEAD_FIELD_MAP.items():
                        g_val = g_ext.get(g_key)
                        if g_val is not None and g_val != '':
                            existing = h_ext.get(i_key)
                            if existing is None or existing == '':
                                h_ext[i_key] = g_val
                                changed = True
                    if changed:
                        head_updates.append((json.dumps(h_ext, ensure_ascii=False), head_uuid))

            if not dry_run:
                if resp_updates:
                    with connection.cursor() as cursor:
                        cursor.executemany("""
                            UPDATE individual_individual
                            SET "Json_ext" = %s::jsonb
                            WHERE "UUID" = %s
                        """, resp_updates)
                if head_updates:
                    with connection.cursor() as cursor:
                        cursor.executemany("""
                            UPDATE individual_individual
                            SET "Json_ext" = %s::jsonb
                            WHERE "UUID" = %s
                        """, head_updates)

            updated_respondents += len(resp_updates)
            updated_heads += len(head_updates)
            offset += len(rows)
            self.stdout.write(
                f"  Processed {min(offset, total):,}/{total:,} — "
                f"{updated_respondents:,} respondents, {updated_heads:,} heads updated"
            )

        action = "would update" if dry_run else "updated"
        self.stdout.write(self.style.SUCCESS(
            f"\nMigration: {action} {updated_respondents:,} respondent individuals "
            f"and {updated_heads:,} head individuals from {total:,} groups"
        ))
