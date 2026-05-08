from merankabandi.action_handlers.base import BaseActionHandler


class AccountSuspendHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['confirmation', 'account_identifier']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'account_identifier': data.get('account_identifier', ''), 'action': 'suspended', 'suspended_by': str(user.id)}


class AccountReactivateHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['confirmation', 'new_phone_number']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'new_phone_number': data.get('new_phone_number', ''), 'action': 'reactivated', 'reactivated_by': str(user.id)}


class CreateMobileAccountHandler(BaseActionHandler):
    """Configure le compte de paiement after a replacement.

    Sets `moyen_telecom` and `moyen_paiement` on the household's GroupBeneficiary
    (where the payment cycle reads from), and reactivates SUSPENDED beneficiary
    records so the new recipient receives the next payment cycle.

    Required: account_type (PaymentAgency code, e.g. LUMICASH), phone_number.
    """
    def get_required_fields(self):
        return ['account_type', 'phone_number']

    def execute(self, task, ticket, user, data=None):
        from datetime import date
        from social_protection.models import GroupBeneficiary
        from merankabandi.models import PaymentAgency
        from merankabandi.workflow_models import ReplacementRequest

        data = data or {}
        account_type = (data.get('account_type') or '').strip()
        phone_number = (data.get('phone_number') or '').strip()

        # Pre-fill phone from replacement request if operator left it blank
        if not phone_number:
            rr = ReplacementRequest.objects.filter(ticket=ticket).first()
            if rr and rr.new_telephone:
                phone_number = rr.new_telephone.strip()

        if not account_type or not phone_number:
            return {'error': 'account_type et phone_number requis'}

        agency = PaymentAgency.objects.filter(code=account_type, is_active=True).first()
        if not agency:
            return {'error': f'Agence de paiement inconnue: {account_type}'}

        # Find the household via verify_social_id
        verify_task = task.workflow.tasks.filter(
            step_template__action_type='verify_social_id', status='COMPLETED',
        ).first()
        if not verify_task or not (verify_task.result or {}).get('individual_id'):
            return {'error': 'Aucun individu vérifié — étape de vérification manquante'}
        old_individual_id = verify_task.result['individual_id']

        gbs = GroupBeneficiary.objects.filter(
            group__groupindividuals__individual_id=old_individual_id,
            is_deleted=False,
        ).distinct()
        if not gbs.exists():
            return {'error': 'Aucun GroupBeneficiary trouvé pour ce ménage'}

        today = str(date.today())
        moyen_telecom = {
            'msisdn': phone_number,
            'agence': agency.payment_gateway or agency.code,
            'etat': 'ATTRIBUE',
            'status': 'PENDING',
            'requestDate': today,
            'attributedBy': str(user.id) if user else None,
        }
        moyen_paiement = {
            'phoneNumber': phone_number,
            'agence': agency.code,
            'etat': 'ATTRIBUE',
            'status': 'PENDING',
            'requestDate': today,
            'attributedBy': str(user.id) if user else None,
        }

        updated = 0
        reactivated = 0
        username = user.username if user and hasattr(user, 'username') else 'Admin'
        for gb in gbs:
            ext = gb.json_ext or {}
            ext['moyen_telecom'] = moyen_telecom
            ext['moyen_paiement'] = moyen_paiement
            ext['payment_agency_id'] = str(agency.id)
            ext['payment_agency_name'] = agency.name
            gb.json_ext = ext
            if gb.status == 'SUSPENDED':
                gb.status = 'ACTIVE'
                reactivated += 1
            gb.save(username=username)
            updated += 1

        return {
            'account_type': account_type,
            'agency_name': agency.name,
            'phone_number': phone_number,
            'beneficiaries_updated': updated,
            'beneficiaries_reactivated': reactivated,
            'action': 'payment_account_configured',
        }


class UnblockAccountHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['account_identifier', 'unblock_details']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'account_identifier': data.get('account_identifier', ''), 'action': 'unblocked', 'unblock_details': data.get('unblock_details', '')}


class SimAttributionHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['new_sim_number', 'operator']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'new_sim_number': data.get('new_sim_number', ''), 'operator': data.get('operator', ''), 'action': 'sim_attributed'}


class PhoneAttributionHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['phone_model', 'imei']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'phone_model': data.get('phone_model', ''), 'imei': data.get('imei', ''), 'action': 'phone_attributed'}


class PinResetHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['confirmation']

    def execute(self, task, ticket, user, data=None):
        return {'action': 'pin_reset', 'reset_by': str(user.id)}


class PaymentReissueHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['amount', 'payment_details']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'amount': data.get('amount', ''), 'payment_details': data.get('payment_details', ''), 'action': 'payment_reissued'}
