"""Set the Merankabandi deployment's FE config so the group-beneficiary export
defaults to xlsx (so the BE registry dispatches to the merankabandi photo-URL
workbook, and the FE names the download .xlsx).

This is the Mera-specific VALUE for the generic hook added in
openimis-fe-social_protection_js BenefitPlanGroupBeneficiariesSearcher
(``getConf('fe-social_protection', 'groupBeneficiaryExportFileFormat', 'csv')``).
Keeping it here keeps social_protection free of Mera-specific divergence — the
fork only reads a generic, csv-defaulted config; the Mera value lives in this module.

The FE reads module config from the BE via the ``moduleConfigurations`` GraphQL
query, which serves ``core.ModuleConfiguration`` rows where is_exposed=True.
"""
import json

from django.db import migrations

MODULE = 'fe-social_protection'
LAYER = 'fe'
KEY = 'groupBeneficiaryExportFileFormat'
VALUE = 'xlsx'


def _load(cfg):
    if not cfg:
        return {}
    try:
        return json.loads(cfg)
    except (ValueError, TypeError):
        return {}


def set_export_xlsx(apps, schema_editor):
    ModuleConfiguration = apps.get_model('core', 'ModuleConfiguration')
    row = ModuleConfiguration.objects.filter(module=MODULE, layer=LAYER).first()
    if row is None:
        ModuleConfiguration.objects.create(
            module=MODULE, layer=LAYER, version='1', is_exposed=True,
            config=json.dumps({KEY: VALUE}),
        )
    else:
        cfg = _load(row.config)
        cfg[KEY] = VALUE
        row.config = json.dumps(cfg)
        row.is_exposed = True
        row.save()


def unset_export_xlsx(apps, schema_editor):
    ModuleConfiguration = apps.get_model('core', 'ModuleConfiguration')
    row = ModuleConfiguration.objects.filter(module=MODULE, layer=LAYER).first()
    if row is None:
        return
    cfg = _load(row.config)
    cfg.pop(KEY, None)
    if cfg:
        row.config = json.dumps(cfg)
        row.save()
    else:
        row.delete()


class Migration(migrations.Migration):
    dependencies = [
        ('merankabandi', '0025_reassign_beneficiary_actions_to_rdo'),
    ]
    operations = [
        migrations.RunPython(set_export_xlsx, unset_export_xlsx),
    ]
