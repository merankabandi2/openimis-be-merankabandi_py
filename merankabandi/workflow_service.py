import logging
from datetime import timedelta
from django.db import transaction
from django.db.models import Count, Q
from django.utils import timezone

from merankabandi.workflow_models import (
    WorkflowTemplate,
    GrievanceWorkflow, GrievanceTask, RoleAssignment,
)

logger = logging.getLogger('openIMIS')


class WorkflowService:
    """Orchestrates grievance workflow lifecycle."""

    @classmethod
    def match_template(cls, ticket):
        json_ext = ticket.json_ext or {}
        case_type = json_ext.get('case_type', '')
        templates = []

        if case_type == 'cas_de_remplacement':
            motif = (json_ext.get('replacement') or {}).get('motif', '')
            key = f"remplacement:{motif}"
            tpl = cls._find_template(key)
            if tpl:
                templates.append(tpl)

        elif case_type == 'cas_de_suppression__retrait_du_programme':
            motif = (json_ext.get('suppression') or {}).get('motif', '')
            key = f"suppression:{motif}"
            tpl = cls._find_template(key)
            if tpl:
                templates.append(tpl)

        elif case_type == 'cas_de_r_clamation':
            categorization = json_ext.get('categorization') or {}
            reclamation_type = categorization.get('reclamation_type', '')

            if reclamation_type == 'cas_sensibles':
                for cat in categorization.get('sensitive_categories', []):
                    key = f"reclamation:sensible:{cat}"
                    tpl = cls._find_template(key)
                    if tpl:
                        templates.append(tpl)
            elif reclamation_type == 'cas_sp_ciaux':
                for cat in categorization.get('special_categories', []):
                    key = f"reclamation:speciale:{cat}"
                    tpl = cls._find_template(key)
                    if tpl:
                        templates.append(tpl)
            elif reclamation_type == 'cas_non_sensibles':
                for cat in categorization.get('non_sensitive_categories', []):
                    key = f"reclamation:non_sensible:{cat}"
                    tpl = cls._find_template(key)
                    if tpl:
                        templates.append(tpl)

        return templates

    @classmethod
    def _find_template(cls, case_type_key):
        return WorkflowTemplate.objects.filter(
            case_type=case_type_key, is_active=True
        ).first()

    @classmethod
    @transaction.atomic
    def auto_create_workflow(cls, ticket):
        templates = cls.match_template(ticket)
        workflows = []
        for template in templates:
            workflow = cls.create_workflow(ticket, template)
            workflows.append(workflow)
        return workflows

    @classmethod
    @transaction.atomic
    def create_workflow(cls, ticket, template):
        workflow = GrievanceWorkflow.objects.create(
            ticket=ticket,
            template=template,
            status=GrievanceWorkflow.STATUS_PENDING,
            started_at=timezone.now(),
        )

        steps = template.steps.order_by('order')
        prev_task = None

        for step in steps:
            if step.condition and not cls._evaluate_condition(step.condition, ticket):
                status = GrievanceTask.STATUS_SKIPPED
                blocked_by = None
            elif prev_task and prev_task.status != GrievanceTask.STATUS_SKIPPED:
                status = GrievanceTask.STATUS_BLOCKED
                blocked_by = prev_task
            else:
                status = GrievanceTask.STATUS_PENDING
                blocked_by = None

            task = GrievanceTask.objects.create(
                workflow=workflow,
                step_template=step,
                ticket=ticket,
                assigned_role=step.role,
                status=status,
                blocked_by=blocked_by,
                due_date=cls._calculate_due_date(step),
            )
            prev_task = task

        cls._progress_workflow(workflow)
        return workflow

    @classmethod
    @transaction.atomic
    def complete_task(cls, task, user, result=None):
        from merankabandi.action_handlers import get_handler

        task.status = GrievanceTask.STATUS_COMPLETED
        task.completed_at = timezone.now()

        handler = get_handler(task.step_template.action_type)
        handler_result = {}
        try:
            handler.validate(task, task.ticket)
            handler_result = handler.execute(task, task.ticket, user, data=result) or {}
        except Exception as e:
            logger.error(f"Action handler error for task {task.id}: {e}")
            handler_result = {'error': str(e), 'handler_failed': True}

        merged_result = {**(result or {}), **handler_result}
        task.result = merged_result
        task.save()
        cls._progress_workflow(task.workflow)
        return task

    @classmethod
    @transaction.atomic
    def skip_task(cls, task, user, reason=None):
        if task.step_template.is_required:
            raise ValueError(f"Task {task.id} is required and cannot be skipped")
        task.status = GrievanceTask.STATUS_SKIPPED
        task.completed_at = timezone.now()
        task.result = {"skipped_reason": reason}
        task.save()
        cls._progress_workflow(task.workflow)
        return task

    @classmethod
    def reassign_task(cls, task, new_user):
        task.assigned_user = new_user
        task.save()
        return task

    @classmethod
    def _progress_workflow(cls, workflow):
        tasks = workflow.tasks.order_by('step_template__order')
        all_done = True

        for task in tasks:
            if task.status in (GrievanceTask.STATUS_COMPLETED, GrievanceTask.STATUS_SKIPPED):
                continue

            all_done = False

            if task.status == GrievanceTask.STATUS_BLOCKED:
                if task.blocked_by and task.blocked_by.status in (
                    GrievanceTask.STATUS_COMPLETED, GrievanceTask.STATUS_SKIPPED
                ):
                    task.status = GrievanceTask.STATUS_PENDING
                    task.blocked_by = None
                    task.save()

            if task.status == GrievanceTask.STATUS_PENDING:
                cls._assign_user(task)
                task.status = GrievanceTask.STATUS_IN_PROGRESS
                task.started_at = timezone.now()
                task.save()
                break

        if all_done:
            workflow.status = GrievanceWorkflow.STATUS_COMPLETED
            workflow.completed_at = timezone.now()
            workflow.save()
            cls._check_ticket_completion(workflow.ticket)
        elif workflow.status == GrievanceWorkflow.STATUS_PENDING:
            workflow.status = GrievanceWorkflow.STATUS_IN_PROGRESS
            workflow.save()

    @classmethod
    def _assign_user(cls, task):
        if task.assigned_user:
            return

        ticket_json = task.ticket.json_ext or {}
        location_data = ticket_json.get('location') or {}
        location_codes = [
            location_data.get('colline'),
            location_data.get('zone'),
            location_data.get('commune'),
            location_data.get('province'),
        ]

        for code in location_codes:
            if not code:
                continue
            assignment = RoleAssignment.objects.filter(
                role=task.assigned_role,
                location__code=code,
                is_active=True,
            ).first()
            if assignment:
                task.assigned_user = assignment.user
                return

        assignment = (
            RoleAssignment.objects.filter(
                role=task.assigned_role,
                location__isnull=True,
                is_active=True,
            )
            .annotate(
                active_tasks=Count(
                    'user__grievance_tasks',
                    filter=Q(user__grievance_tasks__status=GrievanceTask.STATUS_IN_PROGRESS),
                )
            )
            .order_by('active_tasks')
            .first()
        )
        if assignment:
            task.assigned_user = assignment.user

    @classmethod
    def _check_ticket_completion(cls, ticket):
        active_workflows = ticket.workflows.exclude(
            status__in=[GrievanceWorkflow.STATUS_COMPLETED, GrievanceWorkflow.STATUS_CANCELLED]
        ).count()
        if active_workflows == 0:
            ticket.status = 'CLOSED'
            ticket.save(username='workflow_engine')

    @classmethod
    def _evaluate_condition(cls, condition, ticket):
        json_ext = ticket.json_ext or {}
        for key, expected in condition.items():
            parts = key.split('__')
            field_path = parts[0]
            operator = parts[1] if len(parts) > 1 else 'exact'

            value = json_ext
            for part in field_path.split('.'):
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
                    break

            if operator == 'exact':
                if value != expected:
                    return False
            elif operator == 'contains':
                if not isinstance(value, list) or expected not in value:
                    return False
            elif operator == 'in':
                if value not in expected:
                    return False
            elif operator == 'exists':
                if not value:
                    return False
        return True

    @classmethod
    def _calculate_due_date(cls, step_template):
        sla = (step_template.json_ext or {}).get('sla_days')
        if sla:
            return (timezone.now() + timedelta(days=sla)).date()
        sla = (step_template.workflow_template.json_ext or {}).get('default_sla_days', 7)
        return (timezone.now() + timedelta(days=sla)).date()
