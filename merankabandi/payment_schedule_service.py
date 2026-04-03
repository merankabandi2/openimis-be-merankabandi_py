"""
Payment Schedule Service for Merankabandi.

Enforces the operational manual rules for cash transfer payments:
- 12 bimonthly payment rounds per commune per programme (1.2 Transfert Monétaire Régulier)
- Sequential closure: previous round must be RECONCILED before opening the next
- Retry payrolls for failed payments do not count toward the 12-round cap
- Late-enrolled beneficiaries receive cumulative back-pay for missed rounds
"""
import logging
from decimal import Decimal

from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q, Count, Sum, F

from location.models import Location
from payroll.models import Payroll, PayrollStatus, BenefitConsumption, PayrollBenefitConsumption
from payroll.services import PayrollService
from social_protection.models import BenefitPlan, GroupBeneficiary, BeneficiaryStatus

from merankabandi.models import (
    CommunePaymentSchedule,
    CommunePaymentScheduleStatus,
    MAX_PAYMENT_ROUNDS,
    STANDARD_TRANSFER_AMOUNT,
)

logger = logging.getLogger(__name__)


class PaymentScheduleService:

    def __init__(self, user):
        self.user = user

    # ─── Validation ──────────────────────────────────────────────

    def validate_new_round(self, benefit_plan_id, commune_id):
        """
        Validate that a new regular payment round can be created for this
        commune + programme combination.

        Returns (next_round_number, errors[]).
        """
        errors = []
        benefit_plan = BenefitPlan.objects.filter(id=benefit_plan_id, is_deleted=False).first()
        if not benefit_plan:
            return None, ["Programme introuvable."]

        commune = Location.objects.filter(uuid=commune_id, type='W').first()
        if not commune:
            return None, ["Commune introuvable (doit être de type W)."]

        existing = CommunePaymentSchedule.objects.filter(
            benefit_plan=benefit_plan,
            commune=commune,
            is_retry=False,
        ).order_by('round_number')

        completed_count = existing.filter(
            status=CommunePaymentScheduleStatus.RECONCILED
        ).count()
        total_count = existing.count()

        # Cap check
        if total_count >= MAX_PAYMENT_ROUNDS:
            errors.append(
                f"Cette commune a atteint le maximum de {MAX_PAYMENT_ROUNDS} "
                f"tranches de paiement pour ce programme."
            )
            return None, errors

        # Sequential check: last non-reconciled round blocks the next
        last_open = existing.exclude(
            status__in=[
                CommunePaymentScheduleStatus.RECONCILED,
                CommunePaymentScheduleStatus.REJECTED,
            ]
        ).last()

        if last_open:
            errors.append(
                f"La tranche {last_open.round_number} pour {commune.name} "
                f"n'est pas encore clôturée (statut: {last_open.get_status_display()}). "
                f"Veuillez la clôturer avant de créer une nouvelle tranche."
            )
            return None, errors

        next_round = total_count + 1
        return next_round, errors

    def validate_retry(self, benefit_plan_id, commune_id, source_round_number):
        """Validate that a retry can be created for a specific round."""
        errors = []
        benefit_plan = BenefitPlan.objects.filter(id=benefit_plan_id, is_deleted=False).first()
        commune = Location.objects.filter(uuid=commune_id, type='W').first()

        if not benefit_plan or not commune:
            return ["Programme ou commune introuvable."]

        source = CommunePaymentSchedule.objects.filter(
            benefit_plan=benefit_plan,
            commune=commune,
            round_number=source_round_number,
            is_retry=False,
        ).first()

        if not source:
            errors.append(f"Tranche {source_round_number} introuvable pour cette commune.")
        elif source.failed_count == 0:
            errors.append(
                f"La tranche {source_round_number} n'a aucun paiement échoué à réessayer."
            )

        return errors

    # ─── Create ──────────────────────────────────────────────────

    @transaction.atomic
    def create_payment_round(self, benefit_plan_id, commune_id,
                             payment_plan_id, payment_point_id=None,
                             payment_cycle_id=None, payment_method='ONLINE',
                             amount_per_beneficiary=None):
        """
        Create a new regular payment round for a commune.

        1. Validates round cap + sequential closure
        2. Creates the CommunePaymentSchedule record
        3. Creates the Payroll via PayrollService (scoped to commune collines)
        4. Calculates cumulative amounts for late-enrolled beneficiaries
        """
        next_round, errors = self.validate_new_round(benefit_plan_id, commune_id)
        if errors:
            raise ValidationError(errors)

        commune = Location.objects.get(uuid=commune_id, type='W')
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        amount = Decimal(str(amount_per_beneficiary or STANDARD_TRANSFER_AMOUNT))

        # Get all collines under this commune
        colline_uuids = list(
            Location.objects.filter(parent__uuid=commune_id, type='V')
            .values_list('uuid', flat=True)
        )
        if not colline_uuids:
            raise ValidationError(
                f"Aucune colline trouvée pour la commune {commune.name}."
            )

        # Count beneficiaries for this commune
        beneficiary_count = GroupBeneficiary.objects.filter(
            benefit_plan=benefit_plan,
            status=BeneficiaryStatus.ACTIVE,
            is_deleted=False,
            group__location__uuid__in=colline_uuids,
        ).count()

        # Create schedule record
        schedule = CommunePaymentSchedule.objects.create(
            benefit_plan=benefit_plan,
            commune=commune,
            round_number=next_round,
            is_retry=False,
            status=CommunePaymentScheduleStatus.GENERATING,
            amount_per_beneficiary=amount,
            total_beneficiaries=beneficiary_count,
            total_amount=amount * beneficiary_count,
        )

        # Create payroll via upstream service
        payroll_name = f"{benefit_plan.code} - {commune.name} - Tranche {next_round}"
        payroll_data = {
            'name': payroll_name,
            'payment_plan_id': str(payment_plan_id),
            'status': PayrollStatus.GENERATING,
            'payment_method': payment_method,
            'json_ext': {
                'filter_criteria': {
                    'location_ids': [str(u) for u in colline_uuids],
                },
                'payment_schedule_id': str(schedule.id),
                'commune_id': str(commune_id),
                'round_number': next_round,
                'is_retry': False,
            },
        }
        if payment_point_id:
            payroll_data['payment_point_id'] = str(payment_point_id)
        if payment_cycle_id:
            payroll_data['payment_cycle_id'] = str(payment_cycle_id)

        payroll_service = PayrollService(self.user)
        result = payroll_service.create(payroll_data)

        if result.get('success'):
            payroll = Payroll.objects.get(id=result['data']['id'])
            schedule.payroll = payroll
            schedule.save()
        else:
            schedule.status = CommunePaymentScheduleStatus.FAILED
            schedule.save()
            raise ValidationError(
                f"Erreur lors de la création du payroll: {result.get('message', 'Unknown error')}"
            )

        return schedule

    @transaction.atomic
    def create_retry_round(self, benefit_plan_id, commune_id,
                           source_round_number, payment_plan_id,
                           payment_point_id=None, payment_cycle_id=None,
                           payment_method='ONLINE'):
        """
        Create a retry payroll for failed payments from a specific round.
        Does NOT count toward the 12-round cap.
        """
        errors = self.validate_retry(benefit_plan_id, commune_id, source_round_number)
        if errors:
            raise ValidationError(errors)

        commune = Location.objects.get(uuid=commune_id, type='W')
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)

        source_schedule = CommunePaymentSchedule.objects.get(
            benefit_plan=benefit_plan,
            commune=commune,
            round_number=source_round_number,
            is_retry=False,
        )

        # Create retry schedule record (round_number=0 to signal retry)
        schedule = CommunePaymentSchedule.objects.create(
            benefit_plan=benefit_plan,
            commune=commune,
            round_number=source_round_number,
            is_retry=True,
            retry_source=source_schedule,
            status=CommunePaymentScheduleStatus.GENERATING,
            amount_per_beneficiary=source_schedule.amount_per_beneficiary,
        )

        # Create payroll from failed invoices of source payroll
        payroll_name = (
            f"{benefit_plan.code} - {commune.name} - "
            f"Tranche {source_round_number} (Retry)"
        )
        payroll_data = {
            'name': payroll_name,
            'payment_plan_id': str(payment_plan_id),
            'status': PayrollStatus.GENERATING,
            'payment_method': payment_method,
            'from_failed_invoices_payroll_id': str(source_schedule.payroll_id),
            'json_ext': {
                'payment_schedule_id': str(schedule.id),
                'commune_id': str(commune_id),
                'round_number': source_round_number,
                'is_retry': True,
            },
        }
        if payment_point_id:
            payroll_data['payment_point_id'] = str(payment_point_id)
        if payment_cycle_id:
            payroll_data['payment_cycle_id'] = str(payment_cycle_id)

        payroll_service = PayrollService(self.user)
        result = payroll_service.create(payroll_data)

        if result.get('success'):
            payroll = Payroll.objects.get(id=result['data']['id'])
            schedule.payroll = payroll
            schedule.save()
        else:
            schedule.status = CommunePaymentScheduleStatus.FAILED
            schedule.save()

        return schedule

    # ─── Cumulative amount calculation ───────────────────────────

    def calculate_beneficiary_amount(self, beneficiary, commune_id, benefit_plan_id,
                                     current_round):
        """
        Calculate the payment amount for a beneficiary in a given round.

        If the beneficiary was enrolled after round 1, they receive cumulative
        back-pay for all missed rounds.

        Returns: (amount, rounds_covered, is_cumulative)
        """
        benefit_plan = BenefitPlan.objects.get(id=benefit_plan_id)
        commune = Location.objects.get(uuid=commune_id, type='W')

        # Find which rounds this beneficiary has already been paid
        paid_rounds = set()
        beneficiary_payments = BenefitConsumption.objects.filter(
            individual=beneficiary.individual if hasattr(beneficiary, 'individual') else beneficiary,
            status='RECONCILED',
            is_deleted=False,
            payrollbenefitconsumption__payroll__payment_schedules__benefit_plan=benefit_plan,
            payrollbenefitconsumption__payroll__payment_schedules__commune=commune,
            payrollbenefitconsumption__payroll__payment_schedules__is_retry=False,
        ).values_list(
            'payrollbenefitconsumption__payroll__payment_schedules__round_number',
            flat=True
        ).distinct()
        paid_rounds = set(beneficiary_payments)

        # Determine unpaid rounds up to current_round
        all_rounds = set(range(1, current_round + 1))
        unpaid_rounds = all_rounds - paid_rounds

        if not unpaid_rounds:
            # Already fully paid up to this round — standard single-round amount
            unpaid_rounds = {current_round}

        amount = STANDARD_TRANSFER_AMOUNT * len(unpaid_rounds)
        is_cumulative = len(unpaid_rounds) > 1
        rounds_covered = sorted(unpaid_rounds)

        return amount, rounds_covered, is_cumulative

    # ─── Status sync ─────────────────────────────────────────────

    def sync_schedule_from_payroll(self, payroll_id):
        """
        Sync CommunePaymentSchedule status from its linked payroll.
        Called by signals when payroll status changes.
        """
        schedules = CommunePaymentSchedule.objects.filter(payroll_id=payroll_id)
        for schedule in schedules:
            schedule.sync_from_payroll()

        # Also update counts from benefit consumptions
        payroll = Payroll.objects.filter(id=payroll_id).first()
        if not payroll:
            return

        for schedule in schedules:
            benefits = BenefitConsumption.objects.filter(
                payrollbenefitconsumption__payroll=payroll,
                is_deleted=False,
            )
            schedule.total_beneficiaries = benefits.count()
            schedule.reconciled_count = benefits.filter(status='RECONCILED').count()
            schedule.failed_count = benefits.filter(status='REJECTED').count()
            if schedule.total_beneficiaries > 0:
                schedule.total_amount = (
                    schedule.amount_per_beneficiary * schedule.total_beneficiaries
                )
            schedule.save()

    # ─── Queries ─────────────────────────────────────────────────

    def get_commune_schedule(self, benefit_plan_id, commune_id):
        """Get the full payment schedule for a commune."""
        return CommunePaymentSchedule.objects.filter(
            benefit_plan_id=benefit_plan_id,
            commune__uuid=commune_id,
            is_retry=False,
        ).order_by('round_number')

    def get_commune_status(self, benefit_plan_id, commune_id):
        """
        Return a summary of the payment status for a commune.
        """
        schedules = self.get_commune_schedule(benefit_plan_id, commune_id)

        total_rounds = schedules.count()
        reconciled_rounds = schedules.filter(
            status=CommunePaymentScheduleStatus.RECONCILED
        ).count()
        current_round = schedules.exclude(
            status__in=[
                CommunePaymentScheduleStatus.RECONCILED,
                CommunePaymentScheduleStatus.REJECTED,
            ]
        ).first()

        # Get retry stats
        retries = CommunePaymentSchedule.objects.filter(
            benefit_plan_id=benefit_plan_id,
            commune__uuid=commune_id,
            is_retry=True,
        )

        return {
            'commune_id': str(commune_id),
            'total_rounds': total_rounds,
            'reconciled_rounds': reconciled_rounds,
            'remaining_rounds': MAX_PAYMENT_ROUNDS - total_rounds,
            'current_round': current_round.round_number if current_round else None,
            'current_round_status': current_round.get_status_display() if current_round else None,
            'can_create_next': total_rounds < MAX_PAYMENT_ROUNDS and not current_round,
            'retry_count': retries.count(),
            'rounds': [
                {
                    'round_number': s.round_number,
                    'status': s.status,
                    'status_display': s.get_status_display(),
                    'total_beneficiaries': s.total_beneficiaries,
                    'reconciled_count': s.reconciled_count,
                    'failed_count': s.failed_count,
                    'total_amount': str(s.total_amount),
                    'amount_per_beneficiary': str(s.amount_per_beneficiary),
                    'created_at': s.created_at.isoformat() if s.created_at else None,
                    'payroll_id': str(s.payroll_id) if s.payroll_id else None,
                }
                for s in schedules
            ],
        }

    def get_programme_payment_evolution(self, benefit_plan_id):
        """
        Return payment evolution across all communes for a programme.
        Used for the dashboard view.
        """
        schedules = CommunePaymentSchedule.objects.filter(
            benefit_plan_id=benefit_plan_id,
            is_retry=False,
        ).values(
            'commune__uuid', 'commune__name',
        ).annotate(
            total_rounds=Count('id'),
            reconciled_rounds=Count(
                'id',
                filter=Q(status=CommunePaymentScheduleStatus.RECONCILED)
            ),
            total_beneficiaries=Sum('total_beneficiaries'),
            total_amount=Sum('total_amount'),
        ).order_by('commune__name')

        return list(schedules)
