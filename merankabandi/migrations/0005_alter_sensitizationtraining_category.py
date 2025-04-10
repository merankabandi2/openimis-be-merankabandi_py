# Generated by Django 4.2.16 on 2025-02-28 19:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("merankabandi", "0004_microproject_livestock_beneficiaries"),
    ]

    operations = [
        migrations.AlterField(
            model_name="sensitizationtraining",
            name="category",
            field=models.CharField(
                blank=True,
                choices=[
                    (
                        "module_mip__mesures_d_inclusio",
                        "Module MIP (Mesures d'Inclusion Productive)",
                    ),
                    (
                        "module_mach__mesures_d_accompa",
                        "Module MACH (Mesures d'Accompagnement pour le développement du Capital Humain)",
                    ),
                ],
                max_length=100,
                null=True,
                verbose_name="Thème",
            ),
        ),
    ]
