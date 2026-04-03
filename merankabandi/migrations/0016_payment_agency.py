import uuid
from django.db import migrations, models
import django.db.models.deletion


def migrate_payment_points_to_agencies(apps, schema_editor):
    """
    Migrate data from PaymentPoint -> PaymentAgency
    and ProvincePaymentPoint -> ProvincePaymentAgency.
    """
    PaymentPoint = apps.get_model('payroll', 'PaymentPoint')
    PaymentAgency = apps.get_model('merankabandi', 'PaymentAgency')
    ProvincePaymentPoint = apps.get_model('merankabandi', 'ProvincePaymentPoint')
    ProvincePaymentAgency = apps.get_model('merankabandi', 'ProvincePaymentAgency')
    MonetaryTransfer = apps.get_model('merankabandi', 'MonetaryTransfer')

    # Known gateway mappings
    GATEWAY_MAP = {
        'LUMICASH': 'LUMICASH',
        'INTERBANK': 'INTERBANK',
        'IBB': 'INTERBANK',
        'FINBANK': 'INTERBANK',
        'BANCOBU': 'INTERBANK',
    }

    # 1. Create PaymentAgency from each PaymentPoint
    pp_to_agency = {}
    for pp in PaymentPoint.objects.filter(is_deleted=False):
        code = pp.name.upper().replace(' ', '_')
        agency, created = PaymentAgency.objects.get_or_create(
            code=code,
            defaults={
                'id': uuid.uuid4(),
                'name': pp.name,
                'payment_gateway': GATEWAY_MAP.get(pp.name.upper(), ''),
                'is_active': True,
            }
        )
        pp_to_agency[pp.id] = agency

    # 2. Migrate ProvincePaymentPoint -> ProvincePaymentAgency
    for ppp in ProvincePaymentPoint.objects.filter(is_active=True):
        agency = pp_to_agency.get(ppp.payment_point_id)
        if not agency:
            continue

        # Resolve benefit_plan from payment_plan
        benefit_plan_id = None
        if ppp.payment_plan_id:
            PaymentPlan = apps.get_model('contribution_plan', 'PaymentPlan')
            try:
                payment_plan = PaymentPlan.objects.get(id=ppp.payment_plan_id)
                benefit_plan_id = payment_plan.benefit_plan_id
            except PaymentPlan.DoesNotExist:
                pass

        if benefit_plan_id:
            ProvincePaymentAgency.objects.get_or_create(
                province_id=ppp.province_id,
                benefit_plan_id=benefit_plan_id,
                payment_agency=agency,
                defaults={
                    'id': uuid.uuid4(),
                    'is_active': True,
                }
            )

    # 3. Migrate MonetaryTransfer.payment_agency_old -> payment_agency_new
    for mt in MonetaryTransfer.objects.all():
        agency = pp_to_agency.get(mt.payment_agency_old_id)
        if agency:
            mt.payment_agency_new_id = agency.id
            mt.save(update_fields=['payment_agency_new_id'])


def reverse_migration(apps, schema_editor):
    # Data migration is not reversible
    pass


class Migration(migrations.Migration):

    dependencies = [
        ('merankabandi', '0015_migrate_and_drop_ticket_columns'),
        ('payroll', '0001_initial'),
        ('location', '0001_initial'),
        ('social_protection', '0001_initial'),
        ('contribution_plan', '0001_initial'),
    ]

    operations = [
        # 1. Create PaymentAgency table
        migrations.CreateModel(
            name='PaymentAgency',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('code', models.CharField(max_length=50, unique=True)),
                ('name', models.CharField(max_length=255)),
                ('payment_gateway', models.CharField(blank=True, help_text='Maps to PAYMENT_GATEWAYS env config (e.g. LUMICASH, INTERBANK)', max_length=50, null=True)),
                ('contact_name', models.CharField(blank=True, default='', max_length=255)),
                ('contact_phone', models.CharField(blank=True, default='', max_length=50)),
                ('contact_email', models.CharField(blank=True, default='', max_length=255)),
                ('is_active', models.BooleanField(default=True)),
                ('date_created', models.DateTimeField(auto_now_add=True)),
                ('date_updated', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'merankabandi_payment_agency',
                'ordering': ['name'],
            },
        ),
        # 2. Create ProvincePaymentAgency table
        migrations.CreateModel(
            name='ProvincePaymentAgency',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('is_active', models.BooleanField(default=True)),
                ('date_created', models.DateTimeField(auto_now_add=True)),
                ('date_updated', models.DateTimeField(auto_now=True)),
                ('province', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name='province_payment_agencies', to='location.location')),
                ('benefit_plan', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name='province_payment_agencies', to='social_protection.benefitplan')),
                ('payment_agency', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name='province_assignments', to='merankabandi.paymentagency')),
            ],
            options={
                'db_table': 'merankabandi_province_payment_agency',
                'unique_together': {('province', 'benefit_plan', 'payment_agency')},
            },
        ),
        # 3. Rename MonetaryTransfer.payment_agency -> payment_agency_old
        migrations.RenameField(
            model_name='monetarytransfer',
            old_name='payment_agency',
            new_name='payment_agency_old',
        ),
        # 4. Add new payment_agency FK to PaymentAgency (nullable for migration)
        migrations.AddField(
            model_name='monetarytransfer',
            name='payment_agency_new',
            field=models.ForeignKey(null=True, blank=True, on_delete=django.db.models.deletion.PROTECT, to='merankabandi.paymentagency'),
        ),
        # 5. Run data migration
        migrations.RunPython(migrate_payment_points_to_agencies, reverse_migration),
        # 6. Drop old FK, rename new to final name
        migrations.RemoveField(
            model_name='monetarytransfer',
            name='payment_agency_old',
        ),
        migrations.RenameField(
            model_name='monetarytransfer',
            old_name='payment_agency_new',
            new_name='payment_agency',
        ),
        # 7. Make payment_agency non-nullable
        migrations.AlterField(
            model_name='monetarytransfer',
            name='payment_agency',
            field=models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to='merankabandi.paymentagency'),
        ),
    ]
