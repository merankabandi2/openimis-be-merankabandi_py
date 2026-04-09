from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("merankabandi", "0021_agencyfeeconfig_cps_planning_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="indicatorachievement",
            name="breakdowns",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
