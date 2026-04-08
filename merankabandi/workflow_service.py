import logging
from datetime import timedelta
from django.db import models, transaction
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

        # Fallback: use ticket.category field if no templates matched from json_ext
        if not templates and hasattr(ticket, 'category') and ticket.category:
            cat_parts = ticket.category.split(' > ')
            # Try the most specific part first (sub-category), then parent
            for part in reversed(cat_parts):
                part = part.strip()
                # Determine sensitivity from category
                from merankabandi.converters.category_resolver import RESTRICTIVENESS
                sensitivity = RESTRICTIVENESS.get(cat_parts[0].strip(), 99)
                if sensitivity <= 5:
                    prefix = 'reclamation:sensible'
                elif sensitivity <= 7:
                    prefix = 'reclamation:speciale'
                else:
                    prefix = 'reclamation:non_sensible'
                key = f"{prefix}:{part}"
                tpl = cls._find_template(key)
                if tpl:
                    templates.append(tpl)
                    break

        return templates

    # Map normalized category names (from module config) to legacy KoBo template keys
    CATEGORY_TO_TEMPLATE_KEY = {
        # Payment
        'paiement_pas_recu': 'probl_me_de_paiement__non_r_ception__mon',
        'paiement_en_retard': 'probl_me_de_paiement__retard',
        'paiement_incomplet': 'probl_me_de_paiement__montant',
        'vole': 'carte_sim__bloqu_e__vol_e__perdue__etc',
        # Phone
        'perdu': 'probl_mes_de_t_l_phone__vol__endommag__n',
        'recoit_pas_tm': 'probl_mes_de_t_l_phone__no_tm',
        'mot_de_passe_oublie': 'probl_mes_de_t_l_phone__mdp',
        # Account
        'non_active': 'probl_mes_de_compte_mobile_money__ecocas',
        'bloque': 'probl_mes_de_compte_mobile_money__bloque',
        # Data
        'information': 'demande_d_information',
        # Sensitive
        'eas_hs__exploitation__abus_sexuel___harc': 'eas_hs__exploitation__abus_sexuel___harc',
        'pr_l_vements_de_fonds': 'pr_l_vements_de_fonds',
        'd_tournement_de_fonds___corruption': 'd_tournement_de_fonds___corruption',
        'conflit_familial': 'conflit_familial',
        'accident_grave_ou_n_gligence_professionn': 'accident_grave_ou_n_gligence_professionn',
        # Special
        'erreur_d_inclusion_potentielle': 'erreur_d_inclusion_potentielle',
        'cibl__mais_pas_collect': 'cibl__mais_pas_collect',
        'cibl__et_collect': 'cibl__et_collect',
        'migration': 'migration',
        # Sub-category from config name → old KoBo key
        'paiement': 'probl_me_de_paiement__non_r_ception__mon',
        'telephone': 'probl_mes_de_t_l_phone__vol__endommag__n',
        'compte': 'probl_mes_de_compte_mobile_money__ecocas',
        # From new form category name → KoBo form value
        'violence_vbg': 'eas_hs__exploitation__abus_sexuel___harc',
        'corruption': 'd_tournement_de_fonds___corruption',
        'accident_negligence': 'accident_grave_ou_n_gligence_professionn',
        'erreur_exclusion': 'cibl__mais_pas_collect',
        'erreur_inclusion': 'erreur_d_inclusion_potentielle',
    }

    @classmethod
    def _find_template(cls, case_type_key):
        # Try exact match first
        tpl = WorkflowTemplate.objects.filter(
            case_type=case_type_key, is_active=True
        ).first()
        if tpl:
            return tpl

        # Try mapping the category part to legacy KoBo key
        parts = case_type_key.rsplit(':', 1)
        if len(parts) == 2:
            prefix, cat = parts
            legacy_cat = cls.CATEGORY_TO_TEMPLATE_KEY.get(cat)
            if legacy_cat and legacy_cat != cat:
                legacy_key = f"{prefix}:{legacy_cat}"
                tpl = WorkflowTemplate.objects.filter(
                    case_type=legacy_key, is_active=True
                ).first()
                if tpl:
                    return tpl

        # Try contains match as last resort
        return WorkflowTemplate.objects.filter(
            case_type__icontains=case_type_key.rsplit(':', 1)[-1],
            is_active=True
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
                order=step.order,
            )
            prev_task = task

        cls._progress_workflow(workflow)
        return workflow

    @classmethod
    @transaction.atomic
    def complete_task(cls, task, user, result=None):
        import json as _json
        from merankabandi.action_handlers import get_handler

        # Ensure result is a dict (may arrive as JSON string from GQL)
        if isinstance(result, str):
            try: result = _json.loads(result)
            except: result = {'resolution_notes': result}
        result = result or {}

        handler = get_handler(task.step_template.action_type)
        handler_result = {}
        handler_error = None
        try:
            handler.validate(task, task.ticket)
            handler_result = handler.execute(task, task.ticket, user, data=result) or {}
        except Exception as e:
            logger.error(f"Action handler error for task {task.id}: {e}")
            handler_error = str(e)
            handler_result = {'error': handler_error, 'handler_failed': True}

        merged_result = {**(result or {}), **handler_result}

        # If handler returned an error, keep task IN_PROGRESS — don't auto-complete
        if handler_result.get('error') or handler_error:
            task.result = merged_result
            task.save()
            logger.warning(
                f"Task {task.step_template.name} not completed — handler error: "
                f"{handler_result.get('error') or handler_error}"
            )
            return task

        # Handler succeeded — mark as completed and progress workflow
        task.status = GrievanceTask.STATUS_COMPLETED
        task.completed_at = timezone.now()
        task.result = merged_result
        task.save()

        # Save back beneficiary identifiers to ticket for quick lookup
        cls._save_beneficiary_to_ticket(task.ticket, merged_result)

        cls._progress_workflow(task.workflow)
        return task

    @classmethod
    def _save_beneficiary_to_ticket(cls, ticket, task_result):
        """
        When a task provides social_id or cni, save it to ticket.json_ext.beneficiary
        so the detail page can look up household/payments without extra queries.
        """
        if not task_result:
            return

        # Extract identifiers from task result
        social_id = task_result.get('social_id') or task_result.get('code_menage')
        cni = task_result.get('cni') or task_result.get('ci') or task_result.get('cni_number')

        if not social_id and not cni:
            return

        # Also check replacement data on ticket
        ext = ticket.json_ext or {}
        if isinstance(ext, str):
            import json
            ext = json.loads(ext)

        beneficiary = ext.get('beneficiary', {})
        updated = False

        if social_id and not beneficiary.get('social_id'):
            beneficiary['social_id'] = social_id
            updated = True
        if cni and not beneficiary.get('cni'):
            beneficiary['cni'] = cni
            updated = True

        if updated:
            ext['beneficiary'] = beneficiary
            ticket.json_ext = ext
            ticket.save(username='Admin', update_fields=['json_ext'])
            logger.info(f"Saved beneficiary identifiers to ticket {ticket.code}: {beneficiary}")

    @classmethod
    @transaction.atomic
    def add_task_to_workflow(cls, workflow, step_template_id, user):
        """Add a task from the global step template pool into an active workflow.
        Inserts after the current IN_PROGRESS task.
        """
        from merankabandi.workflow_models import WorkflowStepTemplate

        current_task = workflow.tasks.filter(
            status=GrievanceTask.STATUS_IN_PROGRESS
        ).order_by('order').first()

        if not current_task:
            raise ValueError("No active task in this workflow — cannot insert")

        step_template = WorkflowStepTemplate.objects.get(id=step_template_id)
        insertion_order = current_task.order + 1

        workflow.tasks.filter(order__gte=insertion_order).update(
            order=models.F('order') + 1
        )

        new_task = GrievanceTask.objects.create(
            workflow=workflow,
            step_template=step_template,
            ticket=workflow.ticket,
            assigned_role=step_template.role,
            status=GrievanceTask.STATUS_PENDING,
            blocked_by=current_task,
            order=insertion_order,
            json_ext={
                'is_additional': True,
                'added_by': str(user.id),
                'added_at': timezone.now().isoformat(),
                'source_template': step_template.workflow_template.name,
            },
        )

        logger.info(
            f"Added task '{step_template.label}' to workflow {workflow.id} "
            f"at order {insertion_order}"
        )
        return new_task

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
            ticket.save(username='Admin')

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
