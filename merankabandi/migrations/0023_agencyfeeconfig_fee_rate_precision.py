from django.db import migrations, models


class Migration(migrations.Migration):
    """Extend AgencyFeeConfig.fee_rate precision from (6,4) to (8,6).

    Reason: integer-BIF fees (e.g. 1500 BIF on a 72000 base) require rates
    with up to 6 decimal places (1500/72000 = 0.020833...). The previous
    (6,4) precision could only express 0.0208 → 1497.6, missing the target
    by 2.4 BIF per beneficiary.
    """

    dependencies = [
        ('merankabandi', '0022_indicatorachievement_breakdowns'),
    ]

    operations = [
        migrations.AlterField(
            model_name='agencyfeeconfig',
            name='fee_rate',
            field=models.DecimalField(
                decimal_places=6,
                max_digits=8,
                help_text='Fee rate as decimal (e.g. 0.020833 for ~2.0833%). 6dp lets us encode rates derived from integer-BIF fees on a 72K base (e.g. 1500/72000 = 0.020833).',
            ),
        ),
    ]
