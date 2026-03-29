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
    def get_required_fields(self):
        return ['account_type', 'phone_number']

    def execute(self, task, ticket, user, data=None):
        data = data or {}
        return {'account_type': data.get('account_type', ''), 'phone_number': data.get('phone_number', ''), 'action': 'created'}


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
