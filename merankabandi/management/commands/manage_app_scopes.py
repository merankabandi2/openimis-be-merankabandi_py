from django.core.management.base import BaseCommand
from oauth2_provider.models import Application
from django.conf import settings


class Command(BaseCommand):
    help = 'Manage OAuth2 application scope restrictions'

    def add_arguments(self, parser):
        parser.add_argument(
            '--list',
            action='store_true',
            help='List all applications and their allowed scopes'
        )
        parser.add_argument(
            '--check',
            type=str,
            help='Check which scopes an application can use'
        )

    def handle(self, *args, **options):
        if options['list']:
            self.list_app_scopes()
        elif options['check']:
            self.check_app_scopes(options['check'])
        else:
            self.stdout.write(self.style.ERROR('Please specify --list or --check <app_name>'))

    def list_app_scopes(self):
        """List all applications and their scope restrictions"""
        self.stdout.write("\n" + "="*60)
        self.stdout.write("CONFIGURED SCOPE RESTRICTIONS")
        self.stdout.write("="*60 + "\n")
        
        app_scopes = getattr(settings, 'OAUTH2_APPLICATION_SCOPES', {})
        for app_name, scopes in app_scopes.items():
            self.stdout.write(f"{self.style.SUCCESS(app_name)}:")
            for scope in scopes:
                self.stdout.write(f"  - {scope}")
            self.stdout.write("")
        
        self.stdout.write("\n" + "="*60)
        self.stdout.write("EXISTING APPLICATIONS")
        self.stdout.write("="*60 + "\n")
        
        for app in Application.objects.all():
            if app.name in app_scopes:
                self.stdout.write(f"{self.style.SUCCESS(app.name)} - RESTRICTED")
            else:
                self.stdout.write(f"{self.style.WARNING(app.name)} - UNRESTRICTED (can use any scope)")
            self.stdout.write(f"  Client ID: {app.client_id}")
            self.stdout.write("")

    def check_app_scopes(self, app_name):
        """Check which scopes an application can use"""
        try:
            app = Application.objects.get(name=app_name)
            self.stdout.write(f"\nApplication: {self.style.SUCCESS(app.name)}")
            self.stdout.write(f"Client ID: {app.client_id}")
            
            app_scopes = getattr(settings, 'OAUTH2_APPLICATION_SCOPES', {})
            if app.name in app_scopes:
                self.stdout.write(f"\nAllowed scopes:")
                for scope in app_scopes[app.name]:
                    self.stdout.write(f"  ✓ {scope}")
            else:
                self.stdout.write(f"\n{self.style.WARNING('No restrictions')} - can request any scope")
                
        except Application.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Application '{app_name}' not found"))