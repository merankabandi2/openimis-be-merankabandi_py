"""Seed the notification event types + French templates for the async
account-creation (Finbank) report. Idempotent. Run after deploy:

    python manage.py seed_account_report_notification

Kept in merankabandi (not the upstream notification seed_data) so custom events
stay in the custom module.
"""
from django.core.management.base import BaseCommand


# (code, category, default_channels)
EVENTS = [
    ('report.account_creation_ready', 'report',
     {'in_app': True, 'email': True, 'sms': False}),
    ('report.account_creation_failed', 'report',
     {'in_app': True, 'email': False, 'sms': False}),
]

# code -> (subject, body, sms_body). {filename} is substituted from the task context.
TEMPLATES = {
    'report.account_creation_ready': (
        'Rapport des comptes (Finbank) prêt',
        'Votre rapport des comptes (Finbank) « {filename} » est prêt. '
        'Cliquez sur le lien pour le télécharger.',
        'Rapport comptes Finbank prêt: {filename}',
    ),
    'report.account_creation_failed': (
        'Échec de génération du rapport des comptes',
        'La génération de votre rapport des comptes (Finbank) a échoué. '
        "Veuillez réessayer ou contacter l'administrateur.",
        '',
    ),
}


class Command(BaseCommand):
    help = ("Seed notification event types + FR templates for the account-creation "
            "report (idempotent).")

    def handle(self, *args, **options):
        from notification.models import NotificationEventType, NotificationTemplate

        for code, category, channels in EVENTS:
            NotificationEventType.objects.update_or_create(
                code=code,
                defaults={'category': category, 'default_channels': channels,
                          'is_active': True},
            )
        for code, (subject, body, sms) in TEMPLATES.items():
            event_type = NotificationEventType.objects.get(code=code)
            NotificationTemplate.objects.update_or_create(
                event_type=event_type, language='fr',
                defaults={'subject': subject, 'body': body, 'sms_body': sms},
            )
        self.stdout.write(self.style.SUCCESS(
            f"Seeded {len(EVENTS)} event types + {len(TEMPLATES)} templates "
            "for the account-creation report."))
