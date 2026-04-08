from merankabandi.action_handlers.base import BaseActionHandler


class VerifySocialIdHandler(BaseActionHandler):
    def is_automated(self):
        return True

    def execute(self, task, ticket, user, data=None):
        from individual.models import Individual
        json_ext = ticket.json_ext or {}
        data = data or {}

        # Try multiple sources for social_id (order: explicit > replacement > suppression > reporter)
        social_id = (
            data.get('social_id')  # User-provided in task completion
            or (json_ext.get('replacement') or {}).get('replaced_social_id')
            or (json_ext.get('suppression') or {}).get('social_id')
            or (json_ext.get('reporter') or {}).get('social_id')
            or (json_ext.get('reporter') or {}).get('cni_number')
        )
        if not social_id:
            return {'found': False, 'error': 'Social ID ou CNI manquant — saisir dans le champ notes'}

        # Search strategy:
        # 1. By social_id on household (Group) → find primary recipient
        # 2. By CNI directly on Individual json_ext
        # 3. By social_id in PreCollecte records

        from individual.models import Group, GroupIndividual
        individual = None

        # 1. Search household by social_id → primary recipient
        group = Group.objects.filter(
            json_ext__contains={'social_id': social_id}, is_deleted=False,
        ).first()
        if group:
            primary = GroupIndividual.objects.filter(
                group=group, role='HEAD', is_deleted=False,
            ).first() or GroupIndividual.objects.filter(
                group=group, recipient_type='PRIMARY', is_deleted=False,
            ).first()
            if primary:
                individual = primary.individual

        # 2. Fallback: search Individual directly by social_id in json_ext
        if not individual:
            individual = Individual.objects.filter(
                json_ext__contains={'social_id': social_id}, is_deleted=False,
            ).first()

        # 3. Fallback: search by CNI number in Individual json_ext
        if not individual:
            individual = Individual.objects.filter(
                json_ext__contains={'numero_cni': social_id}, is_deleted=False,
            ).first()

        # 4. Fallback: search in PreCollecte records
        if not individual:
            from merankabandi.models import PreCollecte
            pc = PreCollecte.objects.filter(social_id=social_id).first()
            if not pc:
                pc = PreCollecte.objects.filter(ci=social_id).first()
            if pc and pc.group:
                gi = GroupIndividual.objects.filter(
                    group=pc.group, role='HEAD', is_deleted=False,
                ).first()
                if gi:
                    individual = gi.individual
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
