"""Auto-execute stuck automated workflow tasks.

Backfill command for tickets created before the auto-execution fix.
Finds IN_PROGRESS tasks whose handlers are automated and have no result,
then runs them through the workflow service so they complete and unblock
the next step. Safe to re-run — the workflow service handles idempotency.
"""
from django.core.management.base import BaseCommand

from merankabandi.action_handlers import get_handler
from merankabandi.workflow_models import GrievanceTask
from merankabandi.workflow_service import WorkflowService


class Command(BaseCommand):
    help = 'Auto-execute IN_PROGRESS automated workflow tasks that were never run'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Show what would run')
        parser.add_argument('--limit', type=int, default=0, help='Process at most N tasks (0 = no limit)')

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        limit = options['limit']

        ip_tasks = (
            GrievanceTask.objects
            .filter(status='IN_PROGRESS', result__isnull=True)
            .select_related('step_template', 'workflow', 'ticket')
            .order_by('workflow_id', 'order')
        )

        candidates = []
        for task in ip_tasks:
            handler = get_handler(task.step_template.action_type)
            if handler.is_automated():
                candidates.append(task)
                if limit and len(candidates) >= limit:
                    break

        self.stdout.write(f'Found {len(candidates)} candidates')

        if dry_run:
            for task in candidates[:20]:
                self.stdout.write(
                    f'  Would execute: ticket={task.ticket.code} '
                    f'step={task.step_template.label} action={task.step_template.action_type}'
                )
            if len(candidates) > 20:
                self.stdout.write(f'  ... and {len(candidates) - 20} more')
            return

        executed = 0
        failed = 0
        for task in candidates:
            user = task.assigned_user or task.ticket.user_created
            try:
                WorkflowService.complete_task(task, user, result={})
                # Re-fetch — task may have been updated (COMPLETED) or kept IN_PROGRESS on error
                task.refresh_from_db()
                if task.status == 'COMPLETED':
                    executed += 1
                else:
                    failed += 1
                    err = (task.result or {}).get('error', 'unknown')
                    self.stdout.write(
                        self.style.WARNING(
                            f'  Task stayed IN_PROGRESS after handler: '
                            f'{task.ticket.code} / {task.step_template.label} — {err}'
                        )
                    )
            except Exception as e:
                failed += 1
                self.stdout.write(
                    self.style.ERROR(
                        f'  Error on {task.ticket.code} / {task.step_template.label}: {e}'
                    )
                )

        self.stdout.write(self.style.SUCCESS(f'\nCompleted: {executed}, failed/blocked: {failed}'))
