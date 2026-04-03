import uuid
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('merankabandi', '0017_add_gateway_config_to_payment_agency'),
        ('social_protection', '0022_historicalproject_allows_multiple_enrollments_and_more'),
        ('location', '0019_alter_location_code'),
        ('payroll', '0023_alter_benefitattachment_date_created_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='CommunePaymentSchedule',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('round_number', models.PositiveIntegerField(help_text='Payment round (1-12 for regular, 0 for retry)')),
                ('is_retry', models.BooleanField(default=False, help_text='True if this is a retry payroll for failed payments (does not count toward 12-round cap)')),
                ('status', models.CharField(
                    choices=[
                        ('PENDING', 'Pending'),
                        ('GENERATING', 'Generating'),
                        ('APPROVED', 'Approved'),
                        ('IN_PAYMENT', 'In Payment'),
                        ('RECONCILED', 'Reconciled'),
                        ('FAILED', 'Failed'),
                        ('REJECTED', 'Rejected'),
                    ],
                    default='PENDING',
                    max_length=20,
                )),
                ('amount_per_beneficiary', models.DecimalField(decimal_places=2, default=72000, help_text='Amount per beneficiary for this round (BIF)', max_digits=12)),
                ('total_beneficiaries', models.PositiveIntegerField(default=0, help_text='Number of beneficiaries in this payment round')),
                ('total_amount', models.DecimalField(decimal_places=2, default=0, help_text='Total amount for this round (BIF)', max_digits=15)),
                ('reconciled_count', models.PositiveIntegerField(default=0, help_text='Number of beneficiaries successfully paid')),
                ('failed_count', models.PositiveIntegerField(default=0, help_text='Number of failed payments')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('benefit_plan', models.ForeignKey(
                    help_text='Programme (e.g. 1.2 Transfert Monétaire Régulier)',
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name='payment_schedules',
                    to='social_protection.benefitplan',
                )),
                ('commune', models.ForeignKey(
                    help_text='Commune (location type W)',
                    on_delete=django.db.models.deletion.PROTECT,
                    related_name='payment_schedules',
                    to='location.location',
                )),
                ('payroll', models.ForeignKey(
                    blank=True,
                    help_text='Linked payroll (set when payroll is created)',
                    null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name='payment_schedules',
                    to='payroll.payroll',
                )),
                ('retry_source', models.ForeignKey(
                    blank=True,
                    help_text='Original schedule entry this retry relates to',
                    null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name='retries',
                    to='merankabandi.communepaymentschedule',
                )),
            ],
            options={
                'verbose_name': 'Calendrier de Paiement Commune',
                'verbose_name_plural': 'Calendriers de Paiement Communes',
                'ordering': ['benefit_plan', 'commune', 'round_number'],
            },
        ),
        migrations.AddConstraint(
            model_name='communepaymentschedule',
            constraint=models.UniqueConstraint(
                condition=models.Q(('is_retry', False)),
                fields=('benefit_plan', 'commune', 'round_number'),
                name='unique_regular_round_per_commune',
            ),
        ),
    ]
