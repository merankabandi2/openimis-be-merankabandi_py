"""
Signal handlers that wire Merankabandi workflows to the notification system.
"""
import logging

from django.contrib.contenttypes.models import ContentType

logger = logging.getLogger(__name__)


def _get_actor_name(user):
    if not user:
        return ""
    return f"{user.other_names or ''} {user.last_name or ''}".strip() or str(user)


# ---------------------------------------------------------------------------
# Payment Workflow
# ---------------------------------------------------------------------------

def on_payroll_created(**kwargs):
    """payroll_service.create AFTER — notify approvers."""
    from notification.services import NotificationService, RecipientResolver
    from payroll.models import Payroll
    from tasks_management.models import Task

    result = kwargs.get("result")
    user = kwargs.get("user")
    if not result or not isinstance(result, dict):
        return

    payroll_id = result.get("data", {}).get("id") if isinstance(result.get("data"), dict) else None
    if not payroll_id:
        return

    try:
        payroll = Payroll.objects.get(id=payroll_id)
    except Payroll.DoesNotExist:
        return

    task = Task.objects.filter(
        entity_type=ContentType.objects.get_for_model(Payroll),
        entity_id=str(payroll.id),
    ).first()

    recipients = (
        RecipientResolver.by_task_group(task)
        if task
        else RecipientResolver.by_role(202003)
    )

    NotificationService.notify(
        event_code="payroll.pending_approval",
        actor=user,
        entity=payroll,
        entity_url=f"/payrolls/payroll/{payroll.id}",
        recipients=recipients,
        context={
            "payroll_name": payroll.name or str(payroll.id),
            "payment_point": str(getattr(payroll, "payment_point", "")),
        },
    )


def on_task_completed(**kwargs):
    """task_service.complete_task AFTER — route to payroll or generic task notification."""
    from notification.services import NotificationService, RecipientResolver
    from tasks_management.models import Task
    from core.models import User

    result = kwargs.get("result")
    user = kwargs.get("user")
    if not result:
        return

    task = result.get("data") if isinstance(result, dict) else result
    if not isinstance(task, Task):
        task_id = result.get("data", {}).get("id") if isinstance(result.get("data"), dict) else None
        if not task_id:
            return
        try:
            task = Task.objects.get(id=task_id)
        except Task.DoesNotExist:
            return

    business_event = task.business_event or ""
    task_status = task.status

    source_user = None
    if task.source:
        source_user = User.objects.filter(username=task.source).first()

    if "payroll" in business_event.lower():
        if task_status == Task.Status.COMPLETED:
            if "accept" in business_event.lower():
                event_code = "payroll.approved"
            elif "reject" in business_event.lower():
                event_code = "payroll.rejected"
            elif "reconcil" in business_event.lower():
                event_code = "payroll.reconciled"
            else:
                event_code = "task.completed"
        elif task_status == Task.Status.FAILED:
            event_code = "payroll.rejected"
        else:
            return

        NotificationService.notify(
            event_code=event_code,
            actor=user,
            entity=task.entity if task.entity else task,
            entity_url=f"/payrolls/payroll/{task.entity_id}" if task.entity_id else "",
            recipients=RecipientResolver.by_assignment(source_user) if source_user else [],
            context={
                "payroll_name": str(task.entity_id or ""),
                "actor_name": _get_actor_name(user),
            },
        )
        return

    if task_status == Task.Status.COMPLETED:
        event_code = "task.completed"
    elif task_status == Task.Status.FAILED:
        event_code = "task.failed"
    else:
        return

    NotificationService.notify(
        event_code=event_code,
        actor=user,
        entity=task,
        entity_url="",
        recipients=RecipientResolver.by_assignment(source_user) if source_user else [],
        context={
            "task_description": str(task.data.get("description", "")) if task.data else "",
            "actor_name": _get_actor_name(user),
            "reason": str(task.business_status.get("reason", "")) if task.business_status else "",
        },
    )


def on_task_created(**kwargs):
    """task_service.create AFTER — notify task group executors."""
    from notification.services import NotificationService, RecipientResolver
    from tasks_management.models import Task

    result = kwargs.get("result")
    user = kwargs.get("user")
    if not result:
        return

    task = result.get("data") if isinstance(result, dict) else result
    if not isinstance(task, Task):
        return

    recipients = RecipientResolver.by_task_group(task)
    if not recipients:
        return

    NotificationService.notify(
        event_code="task.assigned",
        actor=user,
        entity=task,
        entity_url="",
        recipients=recipients,
        context={
            "task_description": str(task.data.get("description", "")) if task.data else str(task.business_event or ""),
        },
    )


# ---------------------------------------------------------------------------
# M&E Activity Validation
# ---------------------------------------------------------------------------

def on_activity_validated(**kwargs):
    """Called after KoboDataValidationService validates an activity."""
    from notification.services import NotificationService, RecipientResolver

    user = kwargs.get("user")
    result = kwargs.get("result")
    if not result:
        return

    # The validation service returns (success, activity, error)
    if isinstance(result, (tuple, list)) and len(result) >= 2:
        success, activity = result[0], result[1]
        if not success or not activity:
            return
    else:
        activity = result

    validation_status = getattr(activity, "validation_status", None)
    if validation_status == "VALIDATED":
        event_code = "activity.validated"
    elif validation_status == "REJECTED":
        event_code = "activity.rejected"
    else:
        return

    created_by = getattr(activity, "created_by", None) or getattr(activity, "user_created", None)
    recipients = RecipientResolver.by_assignment(created_by) if created_by else []

    activity_type = type(activity).__name__
    location = str(getattr(activity, "location", ""))

    NotificationService.notify(
        event_code=event_code,
        actor=user,
        entity=activity,
        entity_url="/me/indicators",
        recipients=recipients,
        context={
            "activity_type": activity_type,
            "location": location,
            "actor_name": _get_actor_name(user),
            "comment": getattr(activity, "validation_comment", "") or "",
        },
    )


# ---------------------------------------------------------------------------
# Grievance
# ---------------------------------------------------------------------------

def on_grievance_created(**kwargs):
    """ticket_service.create AFTER — notify assigned staff or role-based fallback."""
    from notification.services import NotificationService, RecipientResolver

    result = kwargs.get("result")
    user = kwargs.get("user")
    if not result:
        return

    ticket = result.get("data") if isinstance(result, dict) else result

    attending_staff = getattr(ticket, "attending_staff", None)
    if attending_staff:
        recipients = RecipientResolver.by_assignment(attending_staff)
    else:
        recipients = RecipientResolver.by_role(191001)

    category_data = getattr(ticket, "category", None) or {}
    category_str = str(category_data) if category_data else ""

    NotificationService.notify(
        event_code="grievance.created",
        actor=user,
        entity=ticket,
        entity_url=f"/grievance/{ticket.id}" if hasattr(ticket, "id") else "",
        recipients=recipients,
        context={
            "ticket_number": str(getattr(ticket, "code", getattr(ticket, "id", ""))),
            "category": category_str,
            "priority": str(getattr(ticket, "priority", "")),
        },
    )


def on_grievance_updated(**kwargs):
    """ticket_service.update AFTER — detect status change or reassignment."""
    from notification.services import NotificationService, RecipientResolver

    result = kwargs.get("result")
    user = kwargs.get("user")
    old_data = kwargs.get("old_data", {})
    if not result:
        return

    ticket = result.get("data") if isinstance(result, dict) else result
    if not ticket:
        return

    ticket_id = getattr(ticket, "id", "")
    ticket_number = str(getattr(ticket, "code", ticket_id))
    entity_url = f"/grievance/{ticket_id}" if ticket_id else ""

    old_status = old_data.get("status") if isinstance(old_data, dict) else None
    new_status = getattr(ticket, "status", None)
    if old_status and new_status and old_status != new_status:
        reporter = getattr(ticket, "reporter", None)
        attending = getattr(ticket, "attending_staff", None)
        recipients = RecipientResolver.merge(
            RecipientResolver.by_assignment(reporter),
            RecipientResolver.by_assignment(attending),
        )
        NotificationService.notify(
            event_code="grievance.status_changed",
            actor=user,
            entity=ticket,
            entity_url=entity_url,
            recipients=recipients,
            context={
                "ticket_number": ticket_number,
                "new_status": str(new_status),
            },
        )

    old_attending = old_data.get("attending_staff_id") if isinstance(old_data, dict) else None
    new_attending = getattr(ticket, "attending_staff", None)
    new_attending_id = new_attending.id if new_attending else None
    if old_attending and new_attending_id and str(old_attending) != str(new_attending_id):
        NotificationService.notify(
            event_code="grievance.assigned",
            actor=user,
            entity=ticket,
            entity_url=entity_url,
            recipients=RecipientResolver.by_assignment(new_attending),
            context={
                "ticket_number": ticket_number,
                "actor_name": _get_actor_name(user),
            },
        )


def on_grievance_comment(**kwargs):
    """comment_service.create AFTER — notify ticket participants + tagged users."""
    from notification.services import NotificationService, RecipientResolver
    from core.models import User
    from merankabandi.workflow_models import RoleAssignment

    result = kwargs.get("result")
    user = kwargs.get("user")
    if not result:
        return

    comment = result.get("data") if isinstance(result, dict) else result
    if not comment:
        return

    ticket = getattr(comment, "ticket", None)
    if not ticket:
        return

    # Base recipients: reporter + attending staff
    reporter = getattr(ticket, "reporter", None)
    attending = getattr(ticket, "attending_staff", None)
    recipients = RecipientResolver.merge(
        RecipientResolver.by_assignment(reporter),
        RecipientResolver.by_assignment(attending),
    )

    # Parse json_ext for tagged users/roles/action assignees
    json_ext = getattr(comment, "json_ext", None) or {}
    if isinstance(json_ext, str):
        import json
        try:
            json_ext = json.loads(json_ext)
        except (ValueError, TypeError):
            json_ext = {}

    tagged_user_id = json_ext.get("tagged_user_id")
    tagged_role = json_ext.get("tagged_role")
    action_assignee_id = json_ext.get("action_assignee_id")

    # Add tagged user to recipients
    if tagged_user_id:
        try:
            tagged_user = User.objects.get(id=tagged_user_id)
            recipients = RecipientResolver.merge(
                recipients, RecipientResolver.by_assignment(tagged_user),
            )
        except User.DoesNotExist:
            pass

    # Add action assignee to recipients
    if action_assignee_id and action_assignee_id != tagged_user_id:
        try:
            assignee = User.objects.get(id=action_assignee_id)
            recipients = RecipientResolver.merge(
                recipients, RecipientResolver.by_assignment(assignee),
            )
        except User.DoesNotExist:
            pass

    # Add all users with tagged role to recipients
    if tagged_role:
        role_assignments = RoleAssignment.objects.filter(
            role=tagged_role, is_active=True,
        ).select_related('user')
        for ra in role_assignments:
            recipients = RecipientResolver.merge(
                recipients, RecipientResolver.by_assignment(ra.user),
            )

    comment_text = str(getattr(comment, "comment", getattr(comment, "content", "")))
    preview = comment_text[:100] + "..." if len(comment_text) > 100 else comment_text

    context = {
        "ticket_number": str(getattr(ticket, "code", getattr(ticket, "id", ""))),
        "comment_preview": preview,
        "actor_name": _get_actor_name(user),
    }
    if tagged_role:
        context["tagged_role"] = tagged_role
    if json_ext.get("action"):
        context["action_required"] = json_ext["action"]
    if json_ext.get("action_assignee_name"):
        context["action_assignee"] = json_ext["action_assignee_name"]

    NotificationService.notify(
        event_code="grievance.comment",
        actor=user,
        entity=ticket,
        entity_url=f"/grievance/detail/{ticket.id}" if hasattr(ticket, "id") else "",
        recipients=recipients,
        context=context,
    )


def on_grievance_reopened(**kwargs):
    """ticket_service.reopen_ticket AFTER — notify assignee."""
    from notification.services import NotificationService, RecipientResolver

    result = kwargs.get("result")
    user = kwargs.get("user")
    if not result:
        return

    ticket = result.get("data") if isinstance(result, dict) else result
    if not ticket:
        return

    attending = getattr(ticket, "attending_staff", None)
    NotificationService.notify(
        event_code="grievance.reopened",
        actor=user,
        entity=ticket,
        entity_url=f"/grievance/{ticket.id}" if hasattr(ticket, "id") else "",
        recipients=RecipientResolver.by_assignment(attending),
        context={
            "ticket_number": str(getattr(ticket, "code", getattr(ticket, "id", ""))),
            "actor_name": _get_actor_name(user),
        },
    )


# ---------------------------------------------------------------------------
# Selection Lifecycle
# ---------------------------------------------------------------------------

def on_quota_selection_completed(**kwargs):
    """applyQuotaSelection mutation AFTER — notify program managers."""
    from notification.services import NotificationService, RecipientResolver

    user = kwargs.get("user")
    result = kwargs.get("result", {})
    if not result:
        return

    NotificationService.notify(
        event_code="selection.quota_completed",
        actor=user,
        entity=None,
        entity_url="",
        recipients=RecipientResolver.by_role(160005),
        context={
            "program_name": result.get("program_name", ""),
            "round": str(result.get("round", "")),
            "selected_count": str(result.get("selected_count", 0)),
        },
    )


def on_community_validation_completed(**kwargs):
    """Bulk validation mutation AFTER — notify program managers."""
    from notification.services import NotificationService, RecipientResolver

    user = kwargs.get("user")
    result = kwargs.get("result", {})
    if not result:
        return

    NotificationService.notify(
        event_code="selection.validation_completed",
        actor=user,
        entity=None,
        entity_url="",
        recipients=RecipientResolver.by_role(160005),
        context={
            "program_name": result.get("program_name", ""),
            "location": result.get("location", ""),
            "validated_count": str(result.get("validated_count", 0)),
            "rejected_count": str(result.get("rejected_count", 0)),
        },
    )


def on_promotion_completed(**kwargs):
    """promoteToBeneficiary mutation AFTER — notify program managers."""
    from notification.services import NotificationService, RecipientResolver

    user = kwargs.get("user")
    result = kwargs.get("result", {})
    if not result:
        return

    NotificationService.notify(
        event_code="selection.promotion_completed",
        actor=user,
        entity=None,
        entity_url="",
        recipients=RecipientResolver.by_role(160005),
        context={
            "program_name": result.get("program_name", ""),
            "promoted_count": str(result.get("promoted_count", 0)),
        },
    )
