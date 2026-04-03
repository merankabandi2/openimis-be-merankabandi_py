"""
Create test users with specific roles for E2E testing.
Usage: python manage.py seed_test_users
"""
from django.core.management.base import BaseCommand
from django.core.cache import cache
from core.models import InteractiveUser, User, Role, UserRole


class Command(BaseCommand):
    help = 'Create test users for E2E testing with specific roles'

    # Maps test user config to role names as they exist in the DB
    TEST_USERS = [
        {
            'username': 'test_ot',
            'password': 'TestOT@2025',
            'last_name': 'TestOT',
            'other_names': 'E2E',
            'roles': ['Operateur de Terrain'],
            'description': 'Operateur de Terrain - collecte, saisie plaintes',
        },
        {
            'username': 'test_rp',
            'password': 'TestRP@2025',
            'last_name': 'TestRP',
            'other_names': 'E2E',
            'roles': ['Responsable Provincial'],
            'description': 'Responsable Provincial - supervision province',
        },
        {
            'username': 'test_stat',
            'password': 'TestSTAT@2025',
            'last_name': 'TestSTAT',
            'other_names': 'E2E',
            'roles': ['Responsable Suivi-Evaluation'],
            'description': 'Statisticien - donnees, rapports, S&E',
        },
        {
            'username': 'test_audit',
            'password': 'TestAUDIT@2025',
            'last_name': 'TestAUDIT',
            'other_names': 'E2E',
            'roles': ['Auditeur'],
            'description': 'Auditeur - lecture seule',
        },
    ]

    def add_arguments(self, parser):
        parser.add_argument(
            '--delete',
            action='store_true',
            help='Delete all test users instead of creating them',
        )

    def handle(self, *args, **options):
        if options['delete']:
            self._delete_test_users()
            return

        self._create_test_users()

    def _create_test_users(self):
        for u in self.TEST_USERS:
            try:
                # Check if interactive user already exists
                i_user = InteractiveUser.objects.filter(
                    login_name=u['username'],
                    validity_to__isnull=True,
                ).first()

                if i_user:
                    self.stdout.write(
                        f"User '{u['username']}' already exists (id={i_user.id}), skipping"
                    )
                    continue

                # Resolve role IDs from role names
                role_ids = []
                for role_name in u['roles']:
                    role = Role.objects.filter(
                        name=role_name,
                        validity_to__isnull=True,
                    ).first()
                    if role:
                        role_ids.append(role.id)
                    else:
                        self.stderr.write(
                            self.style.WARNING(
                                f"  Role '{role_name}' not found in DB, skipping role assignment"
                            )
                        )

                if not role_ids:
                    self.stderr.write(
                        self.style.ERROR(
                            f"No valid roles found for '{u['username']}' "
                            f"(looked for: {u['roles']}). Skipping user creation."
                        )
                    )
                    continue

                # Create InteractiveUser
                i_user = InteractiveUser.objects.create(
                    language_id='fr',
                    last_name=u['last_name'],
                    other_names=u['other_names'],
                    login_name=u['username'],
                    role_id=role_ids[0],
                )

                # Set password using openIMIS hashing (SHA-256 with salt)
                i_user.set_password(u['password'])
                i_user.save()

                # Create core.User linked to the InteractiveUser
                core_user = User.objects.filter(
                    username=u['username'],
                    validity_to__isnull=True,
                ).first()

                if not core_user:
                    core_user = User(
                        username=u['username'],
                        i_user=i_user,
                    )
                    core_user.save()

                # Assign roles via UserRole
                for role_id in role_ids:
                    UserRole.objects.create(
                        user=i_user,
                        role_id=role_id,
                        audit_user_id=1,
                    )

                cache.delete(f'rights_{i_user.id}')
                cache.delete(f'is_admin_{i_user.id}')

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Created user '{u['username']}' with roles {u['roles']} "
                        f"(i_user.id={i_user.id})"
                    )
                )

            except Exception as e:
                self.stderr.write(
                    self.style.ERROR(f"Error creating '{u['username']}': {e}")
                )

        self.stdout.write(self.style.SUCCESS('\nSeed test users complete'))

    def _delete_test_users(self):
        usernames = [u['username'] for u in self.TEST_USERS]
        for username in usernames:
            i_user = InteractiveUser.objects.filter(
                login_name=username,
                validity_to__isnull=True,
            ).first()
            if i_user:
                # Remove role assignments
                UserRole.objects.filter(user=i_user).delete()
                # Remove core User
                User.objects.filter(i_user=i_user).delete()
                # Remove InteractiveUser
                i_user.delete()
                self.stdout.write(f"Deleted user '{username}'")
            else:
                self.stdout.write(f"User '{username}' not found, nothing to delete")

        cache.clear()
        self.stdout.write(self.style.SUCCESS('\nTest user cleanup complete'))
