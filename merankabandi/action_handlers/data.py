from merankabandi.action_handlers.base import BaseActionHandler


class IndividualUpdateHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['fields_to_update']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'fields_updated': data.get('fields_to_update', {}), 'updated_by': str(user.id)}


class LocationUpdateHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['new_province', 'new_commune', 'new_colline']

    def execute(self, task, ticket, user, data=None):
        from individual.models import Group
        from location.models import Location
        import json, logging

        data = data or {}
        if isinstance(data, str):
            try: data = json.loads(data)
            except: data = {}
        logging.getLogger('openIMIS').info(f"LocationUpdateHandler data={data}")
        new_colline = data.get('new_colline', '')

        if not new_colline:
            return {'error': 'new_colline is required'}

        # Search by code first, then by name
        location = Location.objects.filter(
            code=new_colline, type='V', validity_to__isnull=True,
        ).first()
        if not location:
            location = Location.objects.filter(
                name__iexact=new_colline, type='V', validity_to__isnull=True,
            ).first()
        if not location:
            return {'error': f'Colline "{new_colline}" not found (searched by code and name)'}

        # Find the individual from the workflow's verify task
        workflow = task.workflow
        verify_task = workflow.tasks.filter(
            step_template__action_type__in=['verify_social_id', 'verify_individual'],
            status='COMPLETED',
        ).first()
        individual_id = (verify_task.result or {}).get('individual_id') if verify_task else None

        if not individual_id:
            return {'error': 'No verified individual found in workflow'}

        group = Group.objects.filter(
            groupindividuals__individual_id=individual_id,
            is_deleted=False,
        ).first()

        if not group:
            return {'error': f'No group found for individual {individual_id}'}

        old_location_code = group.location.code if group.location else None
        group.location = location
        group.save(username=user.username if user else 'Admin')

        return {
            'new_province': data.get('new_province', ''),
            'new_commune': data.get('new_commune', ''),
            'new_colline': new_colline,
            'group_id': str(group.id),
            'old_location_code': old_location_code,
            'action': 'location_updated',
        }


class PhoneNumberSwapHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['old_phone', 'new_phone', 'confirmation']

    def execute(self, task, ticket, user, data=None):
        from individual.models import Individual

        data = data or {}
        old_phone = data.get('old_phone', '')
        new_phone = data.get('new_phone', '')

        if not old_phone or not new_phone:
            return {'error': 'Both old_phone and new_phone are required'}

        old_individual = Individual.objects.filter(
            json_ext__moyen_telecom__msisdn=old_phone,
            is_deleted=False,
        ).first()
        new_individual = Individual.objects.filter(
            json_ext__moyen_telecom__msisdn=new_phone,
            is_deleted=False,
        ).first()

        if not old_individual:
            return {'error': f'No individual found with phone {old_phone}'}
        if not new_individual:
            return {'error': f'No individual found with phone {new_phone}'}

        old_ext = old_individual.json_ext or {}
        new_ext = new_individual.json_ext or {}

        old_ext.setdefault('moyen_telecom', {})['msisdn'] = new_phone
        new_ext.setdefault('moyen_telecom', {})['msisdn'] = old_phone

        old_individual.json_ext = old_ext
        old_individual.save(username=user.username)
        new_individual.json_ext = new_ext
        new_individual.save(username=user.username)

        return {
            'old_phone': old_phone,
            'new_phone': new_phone,
            'old_individual_id': str(old_individual.id),
            'new_individual_id': str(new_individual.id),
            'action': 'phone_swapped',
        }


class AddToCollectionHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['collection_round', 'notes']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'collection_round': data.get('collection_round', ''), 'notes': data.get('notes', ''), 'action': 'added_to_collection'}


class ReRegisterHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['registration_notes']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'registration_notes': data.get('registration_notes', ''), 'action': 're_registered'}
