"""
Signal handler to auto-create grievance workflows when a ticket is created.
"""
import logging

logger = logging.getLogger('openIMIS')


def on_ticket_created_workflow(**kwargs):
    """ticket_service.create AFTER -- auto-create workflow(s) for the new ticket."""
    from grievance_social_protection.models import Ticket
    from merankabandi.workflow_service import WorkflowService

    result = kwargs.get("result")
    if not result:
        return

    # result is {'success': True, 'data': {'id': uuid, ...}} from BaseService.create()
    ticket_id = None
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, dict):
            ticket_id = data.get("id")
        elif hasattr(data, 'id'):
            ticket_id = data.id
    elif hasattr(result, 'id'):
        ticket_id = result.id

    if not ticket_id:
        return

    try:
        ticket = Ticket.objects.get(id=ticket_id)
        workflows = WorkflowService.auto_create_workflow(ticket)
        if workflows:
            logger.info(
                "Auto-created %d workflow(s) for ticket %s: %s",
                len(workflows), ticket.id,
                [w.template.name for w in workflows],
            )
    except Ticket.DoesNotExist:
        logger.warning("Ticket %s not found for workflow auto-creation", ticket_id)
    except Exception as e:
        logger.error("Failed to auto-create workflow for ticket %s: %s", ticket_id, e)
