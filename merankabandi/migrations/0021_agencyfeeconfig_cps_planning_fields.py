"""
Add AgencyFeeConfig model and planning fields to CommunePaymentSchedule.

- AgencyFeeConfig: fee rate per agency + benefit plan + optional province
- CommunePaymentSchedule: payment_cycle, date_valid_from, topup_amount, PLANNING status
"""
import uuid
from decimal import Decimal
from django.db import migrations, models
import django.db.models.deletion


def seed_agency_fees(apps, schema_editor):
    """Seed known agency fee rates for programme 1.2."""
    AgencyFeeConfig = apps.get_model('merankabandi', 'AgencyFeeConfig')
    PaymentAgency = apps.get_model('merankabandi', 'PaymentAgency')
    BenefitPlan = apps.get_model('social_protection', 'BenefitPlan')

    bp = BenefitPlan.objects.filter(code='1.2', is_deleted=False).first()
    if not bp:
        return

    FEES = {
        'BANCOBU': Decimal('0.0600'),
        'FINBANK': Decimal('0.0347'),
        'INTERBANK': Decimal('0.0550'),
        'LUMICASH': Decimal('0.0300'),
    }

    for agency_code, rate in FEES.items():
        agency = PaymentAgency.objects.filter(code=agency_code, is_active=True).first()
        if not agency:
            continue
        AgencyFeeConfig.objects.update_or_create(
            payment_agency=agency,
            benefit_plan=bp,
            province=None,
            defaults={
                'fee_rate': rate,
                'fee_included': False,
                'is_active': True,
            },
        )


class Migration(migrations.Migration):

    dependencies = [
        ('merankabandi', '0020_add_order_to_grievance_task'),
        ('social_protection', '0022_historicalproject_allows_multiple_enrollments_and_more'),
        ('payment_cycle', '0001_initial'),
    ]

    operations = [
        # AgencyFeeConfig model
        migrations.CreateModel(
            name='AgencyFeeConfig',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('fee_rate', models.DecimalField(decimal_places=4, help_text='Fee rate as decimal (e.g. 0.0550 for 5.5%)', max_digits=6)),
                ('fee_included', models.BooleanField(default=False, help_text='True = fee included in beneficiary amount, False = fee added on top')),
                ('is_active', models.BooleanField(default=True)),
                ('date_created', models.DateTimeField(auto_now_add=True)),
                ('date_updated', models.DateTimeField(auto_now=True)),
                ('payment_agency', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='fee_configs', to='merankabandi.paymentagency')),
                ('benefit_plan', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='agency_fee_configs', to='social_protection.benefitplan')),
                ('province', models.ForeignKey(blank=True, help_text='Province-level override (type D). NULL = default for this agency+plan.', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='agency_fee_configs', to='location.location')),
            ],
            options={
                'db_table': 'merankabandi_agency_fee_config',
                'ordering': ['payment_agency', 'benefit_plan', 'province'],
            },
        ),
        migrations.AddConstraint(
            model_name='agencyfeeconfig',
            constraint=models.UniqueConstraint(fields=('payment_agency', 'benefit_plan', 'province'), name='unique_fee_config_agency_plan_province'),
        ),
        migrations.AddConstraint(
            model_name='agencyfeeconfig',
            constraint=models.UniqueConstraint(condition=models.Q(('province__isnull', True)), fields=('payment_agency', 'benefit_plan'), name='unique_fee_config_agency_plan_default'),
        ),

        # CommunePaymentSchedule new fields
        migrations.AddField(
            model_name='communepaymentschedule',
            name='payment_cycle',
            field=models.ForeignKey(blank=True, help_text='Planning cycle this schedule belongs to', null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='commune_schedules', to='payment_cycle.paymentcycle'),
        ),
        migrations.AddField(
            model_name='communepaymentschedule',
            name='date_valid_from',
            field=models.DateField(blank=True, help_text='Planned payment validity start date', null=True),
        ),
        migrations.AddField(
            model_name='communepaymentschedule',
            name='topup_amount',
            field=models.DecimalField(decimal_places=2, default=0, help_text='One-time top-up/compensatory amount for this round', max_digits=12),
        ),

        # Seed known fee rates
        migrations.RunPython(seed_agency_fees, migrations.RunPython.noop),
    ]
