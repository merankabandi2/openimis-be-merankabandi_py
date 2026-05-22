"""Move beneficiary_deactivate and beneficiary_replace step roles from RIUIRCH to RDO.

RIUIRCH = Responsable des Urgences (emergency response)
RDO = Responsable des Opérations (operations — owns beneficiary lifecycle)

Beneficiary deactivation/replacement are operational tasks, not emergency response,
so the role should be RDO. Updates both the step templates (source of truth for new
workflows) and existing in-flight tasks (assigned_role on GrievanceTask).
"""
from django.db import migrations


def reassign_to_rdo(apps, schema_editor):
    WorkflowStepTemplate = apps.get_model('merankabandi', 'WorkflowStepTemplate')
    GrievanceTask = apps.get_model('merankabandi', 'GrievanceTask')

    WorkflowStepTemplate.objects.filter(
        action_type__in=['beneficiary_deactivate', 'beneficiary_replace'],
        role='RIUIRCH',
    ).update(role='RDO')

    GrievanceTask.objects.filter(
        step_template__action_type__in=['beneficiary_deactivate', 'beneficiary_replace'],
        assigned_role='RIUIRCH',
    ).update(assigned_role='RDO')


def revert(apps, schema_editor):
    WorkflowStepTemplate = apps.get_model('merankabandi', 'WorkflowStepTemplate')
    GrievanceTask = apps.get_model('merankabandi', 'GrievanceTask')

    WorkflowStepTemplate.objects.filter(
        action_type__in=['beneficiary_deactivate', 'beneficiary_replace'],
        role='RDO',
    ).update(role='RIUIRCH')

    GrievanceTask.objects.filter(
        step_template__action_type__in=['beneficiary_deactivate', 'beneficiary_replace'],
        assigned_role='RDO',
    ).update(assigned_role='RIUIRCH')


class Migration(migrations.Migration):
    dependencies = [
        ('merankabandi', '0024_widen_replacement_request_fields'),
    ]
    operations = [
        migrations.RunPython(reassign_to_rdo, revert),
    ]
