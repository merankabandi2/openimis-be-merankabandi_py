from merankabandi.action_handlers.base import BaseActionHandler


class ManualResolutionHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['resolution_notes']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {
            'resolution_notes': data.get('resolution_notes', ''),
            'resolved_by': str(user.id),
        }


class NotifyCompletionHandler(BaseActionHandler):
    def is_automated(self):
        return True

    def execute(self, task, ticket, user, data=None):
        return {'notification_sent': True}


class ProvideInformationHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['information_provided']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {
            'information_provided': data.get('information_provided', ''),
            'provided_by': str(user.id),
        }


class ExternalReferralHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['referral_type', 'referral_details']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {
            'referral_type': data.get('referral_type', ''),
            'referral_details': data.get('referral_details', ''),
            'referred_by': str(user.id),
        }
