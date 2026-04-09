"""
GraphQL mutations and queries for commune payment schedule tracking.
"""
import graphene
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError

from core.gql.gql_mutations.base_mutation import BaseMutation
from core.schema import OpenIMISMutation

from merankabandi.models import (
    CommunePaymentSchedule,
    CommunePaymentScheduleStatus,
    MAX_PAYMENT_ROUNDS,
    STANDARD_TRANSFER_AMOUNT,
)
from merankabandi.payment_schedule_service import PaymentScheduleService


# ─── GQL Types ───────────────────────────────────────────────────

class PaymentRoundType(graphene.ObjectType):
    round_number = graphene.Int()
    status = graphene.String()
    status_display = graphene.String()
    total_beneficiaries = graphene.Int()
    reconciled_count = graphene.Int()
    failed_count = graphene.Int()
    total_amount = graphene.String()
    amount_per_beneficiary = graphene.String()
    created_at = graphene.String()
    payroll_id = graphene.String()


class CommunePaymentStatusType(graphene.ObjectType):
    commune_id = graphene.String()
    total_rounds = graphene.Int()
    reconciled_rounds = graphene.Int()
    remaining_rounds = graphene.Int()
    current_round = graphene.Int()
    current_round_status = graphene.String()
    can_create_next = graphene.Boolean()
    retry_count = graphene.Int()
    rounds = graphene.List(PaymentRoundType)


class CommunePaymentEvolutionType(graphene.ObjectType):
    commune_uuid = graphene.String()
    commune_name = graphene.String()
    total_rounds = graphene.Int()
    reconciled_rounds = graphene.Int()
    total_beneficiaries = graphene.Int()
    total_amount = graphene.String()
    progress_percent = graphene.Float()


class PaymentScheduleValidationType(graphene.ObjectType):
    valid = graphene.Boolean()
    next_round = graphene.Int()
    errors = graphene.List(graphene.String)


# ─── Queries ─────────────────────────────────────────────────────

class PaymentScheduleQuery(graphene.ObjectType):
    commune_payment_status = graphene.Field(
        CommunePaymentStatusType,
        benefit_plan_id=graphene.UUID(required=True),
        commune_id=graphene.UUID(required=True),
    )
    payment_evolution = graphene.List(
        CommunePaymentEvolutionType,
        benefit_plan_id=graphene.UUID(required=True),
    )
    validate_payment_round = graphene.Field(
        PaymentScheduleValidationType,
        benefit_plan_id=graphene.UUID(required=True),
        commune_id=graphene.UUID(required=True),
    )

    def resolve_commune_payment_status(self, info, benefit_plan_id, commune_id):
        service = PaymentScheduleService(info.context.user)
        data = service.get_commune_status(str(benefit_plan_id), str(commune_id))
        return CommunePaymentStatusType(
            commune_id=data['commune_id'],
            total_rounds=data['total_rounds'],
            reconciled_rounds=data['reconciled_rounds'],
            remaining_rounds=data['remaining_rounds'],
            current_round=data['current_round'],
            current_round_status=data['current_round_status'],
            can_create_next=data['can_create_next'],
            retry_count=data['retry_count'],
            rounds=[PaymentRoundType(**r) for r in data['rounds']],
        )

    def resolve_payment_evolution(self, info, benefit_plan_id):
        service = PaymentScheduleService(info.context.user)
        rows = service.get_programme_payment_evolution(str(benefit_plan_id))
        return [
            CommunePaymentEvolutionType(
                commune_uuid=str(r['commune__uuid']),
                commune_name=r['commune__name'],
                total_rounds=r['total_rounds'],
                reconciled_rounds=r['reconciled_rounds'],
                total_beneficiaries=r['total_beneficiaries'] or 0,
                total_amount=str(r['total_amount'] or 0),
                progress_percent=round(
                    (r['reconciled_rounds'] / MAX_PAYMENT_ROUNDS) * 100, 1
                ) if r['reconciled_rounds'] else 0,
            )
            for r in rows
        ]

    def resolve_validate_payment_round(self, info, benefit_plan_id, commune_id):
        service = PaymentScheduleService(info.context.user)
        next_round, errors = service.validate_new_round(
            str(benefit_plan_id), str(commune_id)
        )
        return PaymentScheduleValidationType(
            valid=len(errors) == 0,
            next_round=next_round,
            errors=errors,
        )


# ─── Mutations ───────────────────────────────────────────────────

class CreateCommunePaymentRoundMutation(BaseMutation):
    _mutation_class = "CreateCommunePaymentRoundMutation"
    _mutation_module = "merankabandi"

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)
        commune_id = graphene.UUID(required=True)
        payment_plan_id = graphene.UUID(required=True)
        payment_point_id = graphene.UUID(required=False)
        payment_cycle_id = graphene.UUID(required=False)
        payment_method = graphene.String(required=False)
        amount_per_beneficiary = graphene.Decimal(required=False)

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = PaymentScheduleService(user)
        service.create_payment_round(
            benefit_plan_id=str(data['benefit_plan_id']),
            commune_id=str(data['commune_id']),
            payment_plan_id=str(data['payment_plan_id']),
            payment_point_id=str(data['payment_point_id']) if data.get('payment_point_id') else None,
            payment_cycle_id=str(data['payment_cycle_id']) if data.get('payment_cycle_id') else None,
            payment_method=data.get('payment_method', 'ONLINE'),
            amount_per_beneficiary=data.get('amount_per_beneficiary'),
        )


class CreateRetryPaymentRoundMutation(BaseMutation):
    _mutation_class = "CreateRetryPaymentRoundMutation"
    _mutation_module = "merankabandi"

    class Input(OpenIMISMutation.Input):
        benefit_plan_id = graphene.UUID(required=True)
        commune_id = graphene.UUID(required=True)
        source_round_number = graphene.Int(required=True)
        payment_plan_id = graphene.UUID(required=True)
        payment_point_id = graphene.UUID(required=False)
        payment_cycle_id = graphene.UUID(required=False)
        payment_method = graphene.String(required=False)

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = PaymentScheduleService(user)
        service.create_retry_round(
            benefit_plan_id=str(data['benefit_plan_id']),
            commune_id=str(data['commune_id']),
            source_round_number=data['source_round_number'],
            payment_plan_id=str(data['payment_plan_id']),
            payment_point_id=str(data['payment_point_id']) if data.get('payment_point_id') else None,
            payment_cycle_id=str(data['payment_cycle_id']) if data.get('payment_cycle_id') else None,
            payment_method=data.get('payment_method', 'ONLINE'),
        )


class SyncPaymentScheduleMutation(BaseMutation):
    """Manually sync a payment schedule entry from its linked payroll."""
    _mutation_class = "SyncPaymentScheduleMutation"
    _mutation_module = "merankabandi"

    class Input(OpenIMISMutation.Input):
        payroll_id = graphene.UUID(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = PaymentScheduleService(user)
        service.sync_schedule_from_payroll(str(data['payroll_id']))


# ─── Batch mutations (PaymentCycle workspace) ───────────────────

class CycleInitResultType(graphene.ObjectType):
    created = graphene.Int()
    blocked = graphene.Int()
    skipped = graphene.Int()


class InitializeCycleCommunesMutation(BaseMutation):
    """Create PLANNING CommunePaymentSchedule entries for all communes in selected provinces."""
    _mutation_class = "InitializeCycleCommunesMutation"
    _mutation_module = "merankabandi"

    class Input(OpenIMISMutation.Input):
        payment_cycle_id = graphene.UUID(required=True)
        benefit_plan_id = graphene.UUID(required=True)
        province_ids = graphene.List(graphene.UUID, required=True)
        topup_active = graphene.Boolean(required=False)
        topup_amount = graphene.Int(required=False)

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        import json
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        from payment_cycle.models import PaymentCycle
        cycle = PaymentCycle.objects.get(id=data['payment_cycle_id'])

        # Save topup config to cycle json_ext
        ext = cycle.json_ext or {}
        ext['topup_active'] = data.get('topup_active', False)
        ext['topup_amount'] = data.get('topup_amount', 0)
        ext['benefit_plan_id'] = str(data['benefit_plan_id'])
        ext['province_ids'] = [str(p) for p in data['province_ids']]
        PaymentCycle.objects.filter(id=cycle.id).update(json_ext=ext)

        service = PaymentScheduleService(user)
        result = service.initialize_cycle_communes(
            cycle,
            str(data['benefit_plan_id']),
            [str(p) for p in data['province_ids']],
        )
        return result


class UpdateCommuneDatesBulkMutation(BaseMutation):
    """Bulk-set validity start date for selected communes in a cycle."""
    _mutation_class = "UpdateCommuneDatesBulkMutation"
    _mutation_module = "merankabandi"

    class Input(OpenIMISMutation.Input):
        payment_cycle_id = graphene.UUID(required=True)
        commune_ids = graphene.List(graphene.UUID, required=True)
        date_valid_from = graphene.Date(required=True)

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        service = PaymentScheduleService(user)
        service.update_commune_dates_bulk(
            str(data['payment_cycle_id']),
            [str(c) for c in data['commune_ids']],
            data['date_valid_from'],
        )


class BatchGeneratePayrollsResultType(graphene.ObjectType):
    generated = graphene.Int()
    skipped = graphene.Int()


class BatchGeneratePayrollsMutation(BaseMutation):
    """Batch-create payrolls for all PLANNING schedules in a cycle."""
    _mutation_class = "BatchGeneratePayrollsMutation"
    _mutation_module = "merankabandi"

    class Input(OpenIMISMutation.Input):
        payment_cycle_id = graphene.UUID(required=True)
        payment_plan_id = graphene.UUID(required=True)
        payment_method = graphene.String(required=False)

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError("mutation.authentication_required")

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)

        from payment_cycle.models import PaymentCycle
        cycle = PaymentCycle.objects.get(id=data['payment_cycle_id'])

        service = PaymentScheduleService(user)
        result = service.batch_generate_payrolls(
            cycle,
            str(data['payment_plan_id']),
            data.get('payment_method', 'ONLINE'),
        )
        return result
