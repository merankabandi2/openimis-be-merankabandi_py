import copy
import logging

from django.db import transaction

from core.models import User
from individual.models import IndividualDataSourceUpload, IndividualDataSource, Group, Individual, GroupIndividual

logger = logging.getLogger(__name__)

# Household fields arrive prefixed with 'hhd_' on the head individual's row.
# They are stripped of the prefix and stored in Group.json_ext.
_group_field_prefix = 'hhd_'

# The social_id uniquely identifies a household and becomes Group.code.
_group_label_field = 'social_id'

# Individual-level fields mapped to model columns (removed from json_ext).
_first_name_field = 'first_name'
_last_name_field = 'last_name'
_dob_field = 'dob'
_head_field = 'head'
_responsable_field = 'responsable'

# Fields that are used for grouping/linking but should not be stored in
# Individual.json_ext (they are either mapped to model fields or redundant).
_individual_pop_fields = {
    _first_name_field, _last_name_field, _dob_field,
    _head_field, _responsable_field,
    # menage_id duplicates social_id at the individual level
    'menage_id',
}


def merankabandi_import_households_workflow(*args, user_uuid=None, upload_uuid=None, **kwargs):
    """
    Burundi-specific household import workflow.

    Processes IndividualDataSource rows from a UI upload and creates:
    - Group records (one per social_id, with hhd_* fields in json_ext)
    - Individual records (with remaining fields in json_ext)
    - GroupIndividual links (with HEAD role and PRIMARY recipient type)

    Individual fields:
        menage_id, individu_id, colline_id, social_id, nom, prenom, ci,
        ci_date_emission, ci_lieu_emission, naissance_lieu, telephone, photo,
        photo_url, cni_photo_recto, cni_photo_recto_url, cni_photo_verso,
        cni_photo_verso_url, naissance_date, age_mariage, sexe, nationalite,
        est_chef, lien, lit, instruction, handicap, type_handicap,
        handicap_empechement, maladie_chro, maladie_chro_type, activite,
        autres_activites, autres_activites_typ, prob_sante, prob_sante_issu,
        fait_soigne, passoigne_raison, soigne_ou, assurance, age, etat_civil,
        repondant, va_ecole, pas_ecole_raison, semaine_ecole,
        semaine_ecole_raison, annee_derniere, termine, pas_termine_raison,
        last_name, first_name, dob, head, responsable, relationship_to_head,
        member_type, pere, mere, role

    Household fields (hhd_ prefixed on head individual's row):
        menage_id, province, commune, colline, colline_id, social_id,
        date_creation, a_adultes, a_enfants, milieu_residence, latitude,
        longitude, distance_ecole, distance_sanitaire, type_menage,
        menage_type_monoparental, menage_mutwa, menage_deplace, ...,
        score_pmt_initial, score_pmt_initial_rural, score_pmt_initial_urbain
    """
    upload = None
    try:
        user = User.objects.get(id=user_uuid)
        upload = IndividualDataSourceUpload.objects.get(id=upload_uuid)

        if upload.status != IndividualDataSourceUpload.Status.TRIGGERED:
            upload.status = IndividualDataSourceUpload.Status.TRIGGERED
            upload.save(username=user.username)

        with transaction.atomic():
            rows = IndividualDataSource.objects.filter(upload=upload)
            groups = {}
            individual_count = 0

            for row in rows:
                data = copy.deepcopy(row.json_ext)
                group_label = str(data.pop(_group_label_field, '')).strip()

                if not group_label:
                    logger.warning("Row %s has empty social_id, skipping", row.id)
                    continue

                group = groups.get(group_label, None)
                if not group:
                    # First row for this household — extract hhd_* fields
                    group_data = {}
                    fields_to_remove = []
                    for key, value in data.items():
                        if key.startswith(_group_field_prefix):
                            group_key = key[len(_group_field_prefix):]
                            group_data[group_key] = value
                            fields_to_remove.append(key)

                    for key in fields_to_remove:
                        data.pop(key)

                    group = Group(code=group_label, json_ext=group_data)
                    group.save(username=user.username)
                    groups[group_label] = group
                else:
                    # Subsequent individuals — just discard any hhd_* fields
                    for key in [k for k in data if k.startswith(_group_field_prefix)]:
                        data.pop(key)

                # Extract model-level individual fields
                first_name = str(data.pop(_first_name_field, '')).strip()
                last_name = str(data.pop(_last_name_field, '')).strip()
                dob = data.pop(_dob_field, None)
                is_head = data.pop(_head_field, False)
                is_responsable = data.pop(_responsable_field, False)

                # Remove redundant fields that shouldn't go into json_ext
                for field in _individual_pop_fields:
                    data.pop(field, None)

                individual = Individual(
                    first_name=first_name,
                    last_name=last_name,
                    dob=dob,
                    json_ext=data,
                )
                individual.save(username=user.username)

                group_individual = GroupIndividual(group=group, individual=individual)
                if is_head:
                    group_individual.role = GroupIndividual.Role.HEAD
                if is_responsable:
                    group_individual.recipient_type = GroupIndividual.RecipientType.PRIMARY
                group_individual.save(username=user.username)

                row.individual = individual
                row.save(username=user.username)

                individual_count += 1

            upload.status = IndividualDataSourceUpload.Status.SUCCESS
            upload.save(username=user.username)

            logger.info(
                "Merankabandi import complete: %d groups, %d individuals from upload %s",
                len(groups), individual_count, upload_uuid,
            )

    except Exception as exc:
        logger.error("Error in merankabandi_import_households_workflow", exc_info=exc)
        if upload:
            upload.status = IndividualDataSourceUpload.Status.FAIL
            upload.error = {'workflow': str(exc)}
            upload.save(username=user.username if 'user' in dir() else 'admin')
        raise
