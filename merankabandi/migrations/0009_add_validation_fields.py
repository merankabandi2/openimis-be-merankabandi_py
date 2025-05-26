# Generated migration for adding validation fields to KoboToolbox data models

from django.db import migrations, models
import django.db.models.deletion
from django.conf import settings


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('merankabandi', '0008_provincepaymentpoint'),
    ]

    operations = [
        # Add validation fields to SensitizationTraining
        migrations.AddField(
            model_name='sensitizationtraining',
            name='validation_status',
            field=models.CharField(
                max_length=20,
                choices=[
                    ('PENDING', 'Pending Validation'),
                    ('VALIDATED', 'Validated'),
                    ('REJECTED', 'Rejected')
                ],
                default='PENDING',
                verbose_name='Validation Status'
            ),
        ),
        migrations.AddField(
            model_name='sensitizationtraining',
            name='validated_by',
            field=models.ForeignKey(
                null=True,
                blank=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='validated_trainings',
                to=settings.AUTH_USER_MODEL,
                verbose_name='Validated By'
            ),
        ),
        migrations.AddField(
            model_name='sensitizationtraining',
            name='validation_date',
            field=models.DateTimeField(
                null=True,
                blank=True,
                verbose_name='Validation Date'
            ),
        ),
        migrations.AddField(
            model_name='sensitizationtraining',
            name='validation_comment',
            field=models.TextField(
                null=True,
                blank=True,
                verbose_name='Validation Comment'
            ),
        ),
        migrations.AddField(
            model_name='sensitizationtraining',
            name='kobo_submission_id',
            field=models.CharField(
                max_length=255,
                null=True,
                blank=True,
                verbose_name='Kobo Submission ID'
            ),
        ),

        # Add validation fields to BehaviorChangePromotion
        migrations.AddField(
            model_name='behaviorchangepromotion',
            name='validation_status',
            field=models.CharField(
                max_length=20,
                choices=[
                    ('PENDING', 'Pending Validation'),
                    ('VALIDATED', 'Validated'),
                    ('REJECTED', 'Rejected')
                ],
                default='PENDING',
                verbose_name='Validation Status'
            ),
        ),
        migrations.AddField(
            model_name='behaviorchangepromotion',
            name='validated_by',
            field=models.ForeignKey(
                null=True,
                blank=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='validated_behavior_changes',
                to=settings.AUTH_USER_MODEL,
                verbose_name='Validated By'
            ),
        ),
        migrations.AddField(
            model_name='behaviorchangepromotion',
            name='validation_date',
            field=models.DateTimeField(
                null=True,
                blank=True,
                verbose_name='Validation Date'
            ),
        ),
        migrations.AddField(
            model_name='behaviorchangepromotion',
            name='validation_comment',
            field=models.TextField(
                null=True,
                blank=True,
                verbose_name='Validation Comment'
            ),
        ),
        migrations.AddField(
            model_name='behaviorchangepromotion',
            name='kobo_submission_id',
            field=models.CharField(
                max_length=255,
                null=True,
                blank=True,
                verbose_name='Kobo Submission ID'
            ),
        ),

        # Add validation fields to MicroProject
        migrations.AddField(
            model_name='microproject',
            name='validation_status',
            field=models.CharField(
                max_length=20,
                choices=[
                    ('PENDING', 'Pending Validation'),
                    ('VALIDATED', 'Validated'),
                    ('REJECTED', 'Rejected')
                ],
                default='PENDING',
                verbose_name='Validation Status'
            ),
        ),
        migrations.AddField(
            model_name='microproject',
            name='validated_by',
            field=models.ForeignKey(
                null=True,
                blank=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='validated_microprojects',
                to=settings.AUTH_USER_MODEL,
                verbose_name='Validated By'
            ),
        ),
        migrations.AddField(
            model_name='microproject',
            name='validation_date',
            field=models.DateTimeField(
                null=True,
                blank=True,
                verbose_name='Validation Date'
            ),
        ),
        migrations.AddField(
            model_name='microproject',
            name='validation_comment',
            field=models.TextField(
                null=True,
                blank=True,
                verbose_name='Validation Comment'
            ),
        ),
        migrations.AddField(
            model_name='microproject',
            name='kobo_submission_id',
            field=models.CharField(
                max_length=255,
                null=True,
                blank=True,
                verbose_name='Kobo Submission ID'
            ),
        ),
    ]