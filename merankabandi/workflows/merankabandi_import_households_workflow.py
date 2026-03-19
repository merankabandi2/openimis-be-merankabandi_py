import copy
import logging

from django.db import transaction

from core.models import User
from individual.models import IndividualDataSourceUpload, IndividualDataSource, Group, Individual, GroupIndividual

logger = logging.getLogger(__name__)

# Burundi-specific field mappings
_group_field_prefix = 'hhd_'
_group_label_field = 'socialid'
_group_head_field = 'hhd_head'
_group_responsable_field = 'hhd_responsable'
_full_name_field = 'nome'
_dob_field = 'data_de_nascimento'


def merankabandi_import_households_workflow(*args, user_uuid=None, upload_uuid=None, **kwargs):
    """
    Burundi-specific household import workflow.

    Processes IndividualDataSource rows from a UI upload and creates:
    - Group records (one per socialid, with hhd_* fields in json_ext)
    - Individual records (with remaining fields in json_ext)
    - GroupIndividual links (with HEAD role and PRIMARY recipient type)

    Field conventions:
    - 'socialid': group code / household identifier
    - 'hhd_*' prefixed fields: extracted into Group.json_ext (prefix stripped)
    - 'hhd_head': boolean, marks the household head
    - 'hhd_responsable': boolean, marks the primary payment recipient
    - 'nome': full name, split into first_name / last_name
    - 'data_de_nascimento': date of birth
    - All other fields: stored in Individual.json_ext
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
                group_label = str(data.pop(_group_label_field)).strip()

                if not group_label:
                    logger.warning("Row %s has empty socialid, skipping", row.id)
                    continue

                group = groups.get(group_label, None)
                if not group:
                    # Extract hhd_* prefixed fields into group json_ext
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
                    # Remove group fields from subsequent individuals in the same group
                    fields_to_remove = [key for key in data.keys() if key.startswith(_group_field_prefix)]
                    for key in fields_to_remove:
                        data.pop(key)

                # Extract individual identity fields
                full_name = data.pop(_full_name_field, '').strip()
                if ' ' in full_name:
                    first_name, last_name = full_name.split(' ', 1)
                else:
                    first_name = full_name
                    last_name = ''

                dob = data.pop(_dob_field, None)
                is_head = data.pop(_group_head_field, False)
                is_responsable = data.pop(_group_responsable_field, False)

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
