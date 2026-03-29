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
        data = data or {}
        return {'new_province': data.get('new_province', ''), 'new_commune': data.get('new_commune', ''), 'new_colline': data.get('new_colline', ''), 'action': 'location_updated'}


class PhoneNumberSwapHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['old_phone', 'new_phone', 'confirmation']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'old_phone': data.get('old_phone', ''), 'new_phone': data.get('new_phone', ''), 'action': 'phone_swapped'}


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
