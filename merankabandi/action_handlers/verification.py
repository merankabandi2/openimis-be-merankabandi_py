from merankabandi.action_handlers.base import BaseActionHandler


class VerifySocialIdHandler(BaseActionHandler):
    def is_automated(self):
        return True

    def execute(self, task, ticket, user, data=None):
        from individual.models import Individual
        json_ext = ticket.json_ext or {}
        social_id = (json_ext.get('replacement') or {}).get('replaced_social_id')
        if not social_id:
            social_id = (json_ext.get('reporter') or {}).get('cni_number')
        if not social_id:
            return {'found': False, 'error': 'No social_id in ticket data'}
        individual = Individual.objects.filter(
            json_ext__contains={'social_id': social_id}, is_deleted=False,
        ).first()
        if individual:
            return {
                'found': True, 'individual_id': str(individual.id),
                'first_name': individual.first_name, 'last_name': individual.last_name,
                'social_id': social_id,
            }
        return {'found': False, 'social_id': social_id, 'error': 'Individual not found'}


class VerifyPaymentHistoryHandler(BaseActionHandler):
    def is_automated(self):
        return True

    def execute(self, task, ticket, user, data=None):
        json_ext = ticket.json_ext or {}
        reporter = json_ext.get('reporter') or {}
        return {
            'social_id': reporter.get('cni_number'),
            'payment_check': 'requires_manual_verification',
        }


class VerifyIndividualHandler(BaseActionHandler):
    def is_automated(self):
        return True

    def execute(self, task, ticket, user, data=None):
        return VerifySocialIdHandler().execute(task, ticket, user, data)


class VerifyTargetingHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['targeting_status', 'verification_notes']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {
            'targeting_status': data.get('targeting_status', ''),
            'verification_notes': data.get('verification_notes', ''),
        }


class VerifyPhoneRecordsHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['current_phone', 'correct_phone', 'verification_notes']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {
            'current_phone': data.get('current_phone', ''),
            'correct_phone': data.get('correct_phone', ''),
            'verification_notes': data.get('verification_notes', ''),
        }
