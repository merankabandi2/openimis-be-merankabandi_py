# Generated by Django 4.2.20 on 2025-04-04 13:12

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("merankabandi", "0006_monetarytransfer"),
    ]

    operations = [
        migrations.CreateModel(
            name="Indicator",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255)),
                ("pbc", models.CharField(blank=True, max_length=255, null=True)),
                (
                    "baseline",
                    models.DecimalField(decimal_places=2, default=0.0, max_digits=15),
                ),
                (
                    "target",
                    models.DecimalField(decimal_places=2, default=0.0, max_digits=15),
                ),
                ("observation", models.TextField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name="Section",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255)),
            ],
        ),
        migrations.CreateModel(
            name="IndicatorAchievement",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "achieved",
                    models.DecimalField(decimal_places=2, default=0.0, max_digits=15),
                ),
                ("comment", models.TextField(blank=True, null=True)),
                ("timestamp", models.DateTimeField(auto_now_add=True)),
                ("date", models.DateField(blank=True, null=True)),
                (
                    "indicator",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="achievements",
                        to="merankabandi.indicator",
                    ),
                ),
            ],
        ),
        migrations.AddField(
            model_name="indicator",
            name="section",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="indicators",
                to="merankabandi.section",
            ),
        ),
    ]
