from merankabandi.action_handlers.base import BaseActionHandler


class BeneficiaryDeactivateHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['confirmation', 'deactivation_reason']

    def validate(self, task, ticket):
        workflow = task.workflow
        verify_task = workflow.tasks.filter(
            step_template__action_type='verify_social_id', status='COMPLETED',
        ).first()
        if not verify_task or not (verify_task.result or {}).get('found'):
            raise ValueError('Cannot deactivate: no verified individual found')

    def execute(self, task, ticket, user, data=None):
        from social_protection.models import GroupBeneficiary

        data = data or {}
        workflow = task.workflow
        verify_task = workflow.tasks.filter(
            step_template__action_type='verify_social_id', status='COMPLETED',
        ).first()
        individual_id = (verify_task.result or {}).get('individual_id') if verify_task else None
        if not individual_id:
            return {'error': 'No individual to deactivate'}

        beneficiaries = GroupBeneficiary.objects.filter(
            group__individuals__individual_id=individual_id,
            is_deleted=False,
        ).exclude(status='SUSPENDED')
        updated = 0
        for gb in beneficiaries:
            gb.status = 'SUSPENDED'
            gb.save(username=user.username)
            updated += 1

        return {
            'individual_id': individual_id,
            'deactivation_reason': data.get('deactivation_reason', ''),
            'status': 'deactivated',
            'beneficiaries_suspended': updated,
        }


class BeneficiaryReplaceHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['confirmation']

    def execute(self, task, ticket, user, data=None):
        from merankabandi.workflow_models import ReplacementRequest
        replacement = ReplacementRequest.objects.filter(
            ticket=ticket, status=ReplacementRequest.STATUS_APPROVED,
        ).first()
        if not replacement:
            return {'error': 'No approved replacement request found'}
        return {
            'replacement_id': str(replacement.id),
            'new_nom': replacement.new_nom, 'new_prenom': replacement.new_prenom,
            'status': 'new_individual_creation_pending',
        }


class CreateReplacementRequestHandler(BaseActionHandler):
    def is_automated(self):
        return True

    def execute(self, task, ticket, user, data=None):
        from merankabandi.workflow_models import ReplacementRequest
        json_ext = ticket.json_ext or {}
        replacement_data = json_ext.get('replacement') or {}
        new_recipient = replacement_data.get('new_recipient') or {}
        if not replacement_data.get('replaced_social_id'):
            return {'error': 'No replacement data in ticket'}
        existing = ReplacementRequest.objects.filter(ticket=ticket).first()
        if existing:
            return {'replacement_id': str(existing.id), 'status': existing.status}
        rr = ReplacementRequest.objects.create(
            ticket=ticket, task=task,
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
        return {'replacement_id': str(rr.id), 'status': rr.status}
