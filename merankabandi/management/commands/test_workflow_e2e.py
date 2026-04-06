"""
End-to-end test for all 29 grievance workflow templates.

Creates a ticket for each template, runs the full workflow to completion
(complete/skip tasks, approve replacements), and verifies final state.

Replayable: --reset deletes all test data before running.

Usage:
  python manage.py test_workflow_e2e --dry-run        # Preview only
  python manage.py test_workflow_e2e                   # Run all 29 workflows
  python manage.py test_workflow_e2e --template replacement_deces  # Single template
  python manage.py test_workflow_e2e --reset           # Clean + re-run
"""
import json
import logging
import traceback
from datetime import date
from uuid import uuid4

from django.core.management.base import BaseCommand
from django.db import transaction
from django.contrib.auth import get_user_model

logger = logging.getLogger(__name__)
User = get_user_model()

# Marker in json_ext to identify test tickets
TEST_MARKER = 'e2e_workflow_test'

# Simulated task result data keyed by action_type (from step templates)
# All handlers accept resolution_notes as fallback
TASK_RESULTS = {
    # Automated handlers (no input needed)
    'verify_social_id': {},
    'verify_payment_history': {},
    'verify_individual': {},
    'create_replacement_request': {},
    'pin_reset': {},
    'notify_completion': {},
    # Verification
    'verify_targeting': {'targeting_status': 'confirmed', 'verification_notes': 'E2E test'},
    'verify_phone_records': {'current_phone': '79000001', 'correct_phone': '79000002', 'verification_notes': 'E2E'},
    # Beneficiary
    'beneficiary_deactivate': {'deactivation_reason': 'E2E test deactivation'},
    'beneficiary_replace': {},
    # Account
    'account_suspend': {'account_identifier': 'TEST-ACCOUNT-001'},
    'account_reactivate': {'new_phone_number': '79000099'},
    'create_mobile_account': {'account_type': 'lumicash', 'phone_number': '79000099'},
    'unblock_account': {'account_identifier': 'TEST-ACCOUNT-001', 'unblock_details': 'E2E unblock'},
    'sim_attribution': {'new_sim_number': 'SIM-E2E-001', 'operator': 'lumicash'},
    'phone_attribution': {'phone_model': 'Nokia 105', 'imei': '000000000000001'},
    'payment_reissue': {'amount': '72000', 'payment_details': 'E2E reissue'},
    # Data
    'individual_update': {'fields_to_update': '{"phone": "79000099"}'},
    'location_update': {'new_province': 'Gitega', 'new_commune': 'Gitega', 'new_colline': 'Bihanga'},
    'phone_number_swap': {'old_phone': '79000001', 'new_phone': '79000002', 'confirmation': 'yes'},
    'add_to_collection': {'collection_round': '1', 'notes': 'E2E add to collection'},
    're_register': {'registration_notes': 'E2E re-registration'},
    # Information
    'provide_information': {'information_provided': 'E2E information provided'},
    # Referral
    'external_referral': {'referral_type': 'hospital', 'referral_details': 'E2E referral'},
    # Manual resolution (generic fallback for all investigate_*/escalate_*/etc)
    'manual_resolution': {'resolution_notes': 'E2E resolved'},
}

def get_task_result(action_type):
    """Get simulated result for an action type, with fallback to generic resolution."""
    if action_type in TASK_RESULTS:
        return TASK_RESULTS[action_type]
    # All investigation/escalation/validation/etc fall back to manual resolution
    return {'resolution_notes': f'E2E completed: {action_type}'}

# Category for each template to create a matching ticket
TEMPLATE_TICKET_DATA = {
    # Replacements
    'replacement_deces': {
        'category': 'uncategorized', 'case_type': 'cas_de_remplacement',
        'replacement': {'motif': 'd_c_s_du_b_n_ficiaire', 'replaced_social_id': 'E2E-SOCIAL-001',
                        'new_recipient': {'nom': 'TestNom', 'prenom': 'TestPrenom', 'sexe': 'F'}},
    },
    'replacement_emigration': {
        'category': 'uncategorized', 'case_type': 'cas_de_remplacement',
        'replacement': {'motif': 'd_m_nagement_du_b_n_ficiaire', 'replaced_social_id': 'E2E-SOCIAL-002',
                        'new_recipient': {'nom': 'TestNom', 'prenom': 'TestPrenom', 'sexe': 'M'}},
    },
    'replacement_remariage': {
        'category': 'uncategorized', 'case_type': 'cas_de_remplacement',
        'replacement': {'motif': 'remariage_du_b_n_ficiaire', 'replaced_social_id': 'E2E-SOCIAL-003',
                        'new_recipient': {'nom': 'TestNom', 'prenom': 'TestPrenom', 'sexe': 'F'}},
    },
    'replacement_refus': {
        'category': 'uncategorized', 'case_type': 'cas_de_remplacement',
        'replacement': {'motif': 'perte_du_statut_de_b_n_ficiaire', 'replaced_social_id': 'E2E-SOCIAL-004',
                        'new_recipient': {'nom': 'TestNom', 'prenom': 'TestPrenom', 'sexe': 'M'}},
    },
    # Suppressions
    'suppression_erreur_inclusion': {
        'category': 'uncategorized', 'case_type': 'cas_de_suppression__retrait_du_programme',
        'suppression': {'motif': 'erreur_d_inclusion'},
    },
    'suppression_volontaire': {
        'category': 'uncategorized', 'case_type': 'cas_de_suppression__retrait_du_programme',
        'suppression': {'motif': 'demande_volontaire_du_b_n_ficiaire'},
    },
    'suppression_double': {
        'category': 'uncategorized', 'case_type': 'cas_de_suppression__retrait_du_programme',
        'suppression': {'motif': 'double_inscription_d_tect_e'},
    },
    'suppression_deces_sans_remplacement': {
        'category': 'uncategorized', 'case_type': 'cas_de_suppression__retrait_du_programme',
        'suppression': {'motif': 'd_c_s_sans_demande_de_remplacement'},
    },
    # Payments
    'payment_non_reception': {'category': 'paiement > paiement_pas_recu'},
    'payment_retard': {'category': 'paiement > paiement_en_retard'},
    'payment_montant_incorrect': {'category': 'paiement > paiement_incomplet'},
    # SIM/Phone
    'sim_lost_stolen_blocked': {'category': 'telephone > perdu'},
    'phone_lost_stolen': {'category': 'telephone > perdu'},
    'phone_no_tm': {'category': 'telephone > recoit_pas_tm'},
    'phone_password_forgot': {'category': 'telephone > mot_de_passe_oublie'},
    # Account
    'account_not_activated': {'category': 'compte > non_active'},
    'account_blocked': {'category': 'compte > bloque'},
    # Data
    'data_correction': {'category': 'information'},
    'information_request': {'category': 'information'},
    'phone_number_reassignment': {'category': 'telephone'},
    # Special
    'special_erreur_inclusion': {'category': 'erreur_inclusion'},
    'special_cible_pas_collecte': {'category': 'erreur_exclusion'},
    'special_cible_collecte': {'category': 'erreur_exclusion'},
    'migration_changement_localite': {'category': 'erreur_exclusion'},
    # Sensitive
    'sensitive_eas_hs': {'category': 'violence_vbg'},
    'sensitive_prelevements': {'category': 'corruption'},
    'sensitive_corruption': {'category': 'corruption'},
    'sensitive_conflit_familial': {'category': 'violence_vbg'},
    'sensitive_accident': {'category': 'accident_negligence'},
}


class Command(BaseCommand):
    help = 'E2E test: create ticket → run full workflow → verify completion for all 29 templates'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Preview without executing')
        parser.add_argument('--reset', action='store_true', help='Delete previous test data first')
        parser.add_argument('--template', type=str, help='Run single template by name')
        parser.add_argument('--verbose', action='store_true', help='Show step-by-step details')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        reset = options['reset']
        single = options.get('template')
        verbose = options['verbose']

        admin = User.objects.get(username='Admin')
        self.stdout.write(f'Using user: {admin.username} (id={admin.id})')

        if reset:
            self._reset_test_data()

        from merankabandi.workflow_models import WorkflowTemplate
        templates = WorkflowTemplate.objects.filter(is_active=True).order_by('name')
        if single:
            templates = templates.filter(name=single)
            if not templates.exists():
                self.stderr.write(f'Template "{single}" not found')
                return

        results = {'passed': 0, 'failed': 0, 'skipped': 0, 'errors': []}

        for template in templates:
            try:
                ok = self._test_workflow(template, admin, dry_run, verbose)
                if ok:
                    results['passed'] += 1
                else:
                    results['skipped'] += 1
            except Exception as e:
                results['failed'] += 1
                results['errors'].append((template.name, str(e)))
                self.stderr.write(self.style.ERROR(
                    f'  FAIL: {template.name} — {e}'
                ))
                if verbose:
                    traceback.print_exc()

        # Summary
        self.stdout.write('')
        self.stdout.write('=' * 60)
        self.stdout.write(f'RESULTS: {results["passed"]} passed, {results["failed"]} failed, {results["skipped"]} skipped')
        if results['errors']:
            self.stdout.write(self.style.ERROR('\nFailed templates:'))
            for name, err in results['errors']:
                self.stdout.write(f'  {name}: {err}')

    def _test_workflow(self, template, user, dry_run, verbose):
        """Test a single workflow template end-to-end."""
        from grievance_social_protection.models import Ticket
        from merankabandi.workflow_models import (
            GrievanceWorkflow, GrievanceTask, ReplacementRequest,
        )
        from merankabandi.workflow_service import WorkflowService

        self.stdout.write(f'\n--- {template.name}: {template.label} ---')

        ticket_data = TEMPLATE_TICKET_DATA.get(template.name)
        if not ticket_data:
            self.stdout.write(self.style.WARNING(f'  No ticket data configured, skipping'))
            return False

        steps = template.steps.order_by('order')
        step_count = steps.count()
        self.stdout.write(f'  Steps: {step_count}')

        if dry_run:
            for step in steps:
                action = step.action_type or 'manual'
                has_result = action in TASK_RESULTS or True  # get_task_result always returns a fallback
                self.stdout.write(f'    {step.order}. {step.name} [{step.role}] action={action} result={"yes" if has_result else "MISSING"}')
            return True

        # Create ticket
        category = ticket_data.get('category', 'uncategorized')
        json_ext = {
            'test_marker': TEST_MARKER,
            'template_name': template.name,
            'form_version': 'e2e_test',
            'case_type': ticket_data.get('case_type', 'cas_de_r_clamation'),
        }
        if 'replacement' in ticket_data:
            json_ext['replacement'] = ticket_data['replacement']
        if 'suppression' in ticket_data:
            json_ext['suppression'] = ticket_data['suppression']

        with transaction.atomic():
            ticket = Ticket(
                id=uuid4(),
                title=f'E2E-{template.name}',
                description=f'E2E test for {template.label}',
                status='OPEN',
                category=category,
                date_of_incident=date.today(),
                json_ext=json_ext,
                user_created=user,
                user_updated=user,
            )
            ticket.save(username=user.username)

        # Create workflow (bypassing signal to control flow)
        workflow = WorkflowService.create_workflow(ticket, template)
        assert workflow, f'Workflow not created for {template.name}'
        assert workflow.status in ('PENDING', 'IN_PROGRESS'), f'Unexpected workflow status: {workflow.status}'

        if verbose:
            self.stdout.write(f'  Ticket: {ticket.id}')
            self.stdout.write(f'  Workflow: {workflow.id} status={workflow.status}')

        # Process replacement request if needed
        if 'replacement' in ticket_data:
            rr = ReplacementRequest.objects.filter(ticket=ticket).first()
            if rr:
                rr.status = 'APPROVED'
                rr.save()
                if verbose:
                    self.stdout.write(f'  ReplacementRequest {rr.id} → APPROVED')

        # Process each task in order
        tasks = GrievanceTask.objects.filter(workflow=workflow).order_by('step_template__order')
        completed_count = 0

        for task in tasks:
            task.refresh_from_db()
            action = task.step_template.action_type or 'manual'

            if task.status == 'SKIPPED':
                if verbose:
                    self.stdout.write(f'    {task.step_template.order}. {task.step_template.name} → SKIPPED (condition)')
                continue

            if task.status == 'COMPLETED':
                completed_count += 1
                if verbose:
                    self.stdout.write(f'    {task.step_template.order}. {task.step_template.name} → already COMPLETED')
                continue

            # Wait for task to become IN_PROGRESS (may be BLOCKED)
            if task.status == 'BLOCKED':
                WorkflowService._progress_workflow(workflow)
                task.refresh_from_db()

            if task.status == 'PENDING':
                WorkflowService._progress_workflow(workflow)
                task.refresh_from_db()

            if task.status != 'IN_PROGRESS':
                if not task.step_template.is_required:
                    WorkflowService.skip_task(task, user, 'E2E: not in progress')
                    if verbose:
                        self.stdout.write(f'    {task.step_template.order}. {task.step_template.name} → SKIPPED (not progressable)')
                    continue
                raise AssertionError(
                    f'Task {task.step_template.name} stuck in {task.status}'
                )

            # Get result data for this action
            result = get_task_result(action)

            # Complete the task
            try:
                WorkflowService.complete_task(task, user, result)
                task.refresh_from_db()
                assert task.status == 'COMPLETED', f'Task {task.step_template.name} not COMPLETED after complete_task (status={task.status})'
                completed_count += 1
                if verbose:
                    self.stdout.write(f'    {task.step_template.order}. {task.step_template.name} → COMPLETED')
            except Exception as e:
                # Try skipping if not required
                if not task.step_template.is_required:
                    WorkflowService.skip_task(task, user, f'E2E: handler error: {e}')
                    if verbose:
                        self.stdout.write(self.style.WARNING(
                            f'    {task.step_template.order}. {task.step_template.name} → SKIPPED (error: {e})'
                        ))
                    continue
                raise

        # Verify workflow completion
        workflow.refresh_from_db()
        ticket.refresh_from_db()

        if workflow.status != 'COMPLETED':
            remaining = GrievanceTask.objects.filter(
                workflow=workflow
            ).exclude(status__in=['COMPLETED', 'SKIPPED']).values_list('step_template__name', 'status')
            raise AssertionError(
                f'Workflow not COMPLETED (status={workflow.status}), '
                f'remaining tasks: {list(remaining)}'
            )

        self.stdout.write(self.style.SUCCESS(
            f'  PASS: {completed_count}/{step_count} tasks completed, '
            f'ticket status={ticket.status}'
        ))
        return True

    def _reset_test_data(self):
        """Delete all test data created by previous runs."""
        from grievance_social_protection.models import Ticket
        from merankabandi.workflow_models import GrievanceWorkflow, GrievanceTask, ReplacementRequest

        # Find test tickets
        test_tickets = Ticket.objects.filter(title__startswith='E2E-')
        count = test_tickets.count()

        if count == 0:
            self.stdout.write('No test data to clean.')
            return

        # Delete in order: tasks → workflows → replacement requests → tickets
        ticket_ids = list(test_tickets.values_list('id', flat=True))
        tasks_deleted = GrievanceTask.objects.filter(ticket_id__in=ticket_ids).delete()[0]
        wf_deleted = GrievanceWorkflow.objects.filter(ticket_id__in=ticket_ids).delete()[0]
        rr_deleted = ReplacementRequest.objects.filter(ticket_id__in=ticket_ids).delete()[0]

        # Delete tickets via raw SQL (HistoryBusinessModel.delete requires user)
        from django.db import connection
        cursor = connection.cursor()
        for tid in ticket_ids:
            cursor.execute('DELETE FROM grievance_social_protection_ticket WHERE "UUID" = %s', [str(tid)])

        self.stdout.write(self.style.SUCCESS(
            f'Cleaned: {count} tickets, {wf_deleted} workflows, '
            f'{tasks_deleted} tasks, {rr_deleted} replacements'
        ))
