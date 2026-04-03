"""
Signal handler to auto-create grievance workflows when a ticket is created.
"""
import logging

logger = logging.getLogger('openIMIS')


def on_ticket_created_workflow(**kwargs):
    """ticket_service.create AFTER -- auto-create workflow(s) for the new ticket."""
    from merankabandi.workflow_service import WorkflowService

    result = kwargs.get("result")
    if not result:
        return

    ticket = result.get("data") if isinstance(result, dict) else result
    if not ticket:
        return

    try:
        workflows = WorkflowService.auto_create_workflow(ticket)
        if workflows:
            logger.info(
                "Auto-created %d workflow(s) for ticket %s",
                len(workflows), ticket.id,
            )
    except Exception as e:
        logger.error("Failed to auto-create workflow for ticket %s: %s", getattr(ticket, 'id', '?'), e)
