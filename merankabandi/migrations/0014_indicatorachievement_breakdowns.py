from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("merankabandi", "0013_precollecte_pmtformula_selectionquota"),
    ]

    operations = [
        migrations.AddField(
            model_name="indicatorachievement",
            name="breakdowns",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
