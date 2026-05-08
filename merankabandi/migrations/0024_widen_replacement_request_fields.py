from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('merankabandi', '0023_agencyfeeconfig_fee_rate_precision'),
    ]

    operations = [
        migrations.AlterField(
            model_name='replacementrequest',
            name='replaced_social_id',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='replacementrequest',
            name='new_telephone',
            field=models.CharField(blank=True, max_length=50, null=True),
        ),
    ]
