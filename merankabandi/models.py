import uuid
from django.db import models
from django.core.validators import MinValueValidator
from datetime import datetime

from core.models import User, UUIDModel
from location.models import Location
from payroll.models import Payroll, PayrollStatus
from social_protection.models import BenefitPlan

# Host community communes as specified
HOST_COMMUNES = ['Butezi', 'Ruyigi', 'Kiremba', 'Gasorwe', 'Gashoho', 'Muyinga', 'Cankuzo']


class SensitizationTraining(models.Model):
    THEME_CATEGORIES = [
        ('module_mip__mesures_d_inclusio', 'Module MIP (Mesures d\'Inclusion Productive)'),
        ('module_mach__mesures_d_accompa', 'Module MACH (Mesures d\'Accompagnement pour le développement du Capital Humain)')
    ]

    VALIDATION_STATUS_CHOICES = [
        ('PENDING', 'Pending Validation'),
        ('VALIDATED', 'Validated'),
        ('REJECTED', 'Rejected')
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    sensitization_date = models.DateField(verbose_name="Date de la sensibilisation/Formation")

    location = models.ForeignKey('location.Location', on_delete=models.PROTECT)

    category = models.CharField(
        verbose_name="Thème",
        max_length=100,
        choices=THEME_CATEGORIES,
        null=True,
        blank=True
    )
    modules = models.JSONField(
        verbose_name="Modules",
        null=True,
        blank=True,
        help_text="Selected modules"
    )

    facilitator = models.CharField(
        verbose_name="Animateur",
        max_length=255,
        null=True,
        blank=True
    )

    male_participants = models.PositiveIntegerField(
        verbose_name="Participants Hommes",
        validators=[MinValueValidator(0)],
        default=0
    )
    female_participants = models.PositiveIntegerField(
        verbose_name="Participantes Femmes",
        validators=[MinValueValidator(0)],
        default=0
    )
    twa_participants = models.PositiveIntegerField(
        verbose_name="Participants Twa",
        validators=[MinValueValidator(0)],
        default=0
    )

    observations = models.TextField(
        verbose_name="Observations",
        null=True,
        blank=True
    )

    # Validation fields
    validation_status = models.CharField(
        max_length=20,
        choices=VALIDATION_STATUS_CHOICES,
        default='PENDING',
        verbose_name='Validation Status'
    )
    validated_by = models.ForeignKey(
        User,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='validated_trainings',
        verbose_name='Validated By'
    )
    validation_date = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Validation Date'
    )
    validation_comment = models.TextField(
        null=True,
        blank=True,
        verbose_name='Validation Comment'
    )
    kobo_submission_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='Kobo Submission ID'
    )

    class Meta:
        verbose_name = "Sensibilisation/Formation"
        verbose_name_plural = "Sensibilisations/Formations"
        ordering = ['-sensitization_date']

    def __str__(self):
        return f"Formation du {self.sensitization_date} - {self.location.name}"

    @property
    def total_participants(self):
        return (
            self.male_participants +
            self.female_participants
        )

    @classmethod
    def to_data_element_obj(cls, kobo_data, **kwargs):
        # Handle multiple select fields
        modules = []
        mip_modules = kobo_data.get('Module_MIP', []) if kobo_data.get('Module_MIP') else []
        if isinstance(mip_modules, str):
            modules = [mip_modules]

        mach_modules = kobo_data.get('Module_MACH', []) if kobo_data.get('Module_MACH') else []
        if isinstance(mach_modules, str):
            modules = [mach_modules]

        male_participants = int(kobo_data.get('group_zp4mt03/Nombre_dhommes', 0))
        female_participants = int(kobo_data.get('group_zp4mt03/Nombre_de_femmes', 0))
        twa_participants = int(kobo_data.get('group_zp4mt03/Nombre_de_Batwa', 0))

        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7)
                        )[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]
        date = kobo_data.get('Date_de_la_sensibilisation_Formation') or kobo_data.get('start')

        return cls(
            # Metadata
            id=kobo_data.get('_uuid'),
            sensitization_date=datetime.fromisoformat(date).date() if date else None,
            location=Location.objects.filter(code=locationcode).first(),

            # Training details
            category=kobo_data.get('Th_me'),
            modules=modules,
            facilitator=kobo_data.get('Animateur_'),

            male_participants=male_participants,
            female_participants=female_participants,
            twa_participants=twa_participants,

            # Additional fields
            observations=kobo_data.get('Observation'),

            # Kobo metadata
            kobo_submission_id=kobo_data.get('_submission_id') or kobo_data.get('_id'),
        )


class BehaviorChangePromotion(models.Model):
    VALIDATION_STATUS_CHOICES = [
        ('PENDING', 'Pending Validation'),
        ('VALIDATED', 'Validated'),
        ('REJECTED', 'Rejected')
    ]

    # Metadata
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    # Location
    location = models.ForeignKey('location.Location', on_delete=models.PROTECT)
    report_date = models.DateField(verbose_name="Date de l'activité'")

    male_participants = models.PositiveIntegerField(
        verbose_name="Participants Hommes",
        validators=[MinValueValidator(0)],
        default=0
    )
    female_participants = models.PositiveIntegerField(
        verbose_name="Participantes Femmes",
        validators=[MinValueValidator(0)],
        default=0
    )
    twa_participants = models.PositiveIntegerField(
        verbose_name="Participants Twa",
        validators=[MinValueValidator(0)],
        default=0
    )

    comments = models.TextField(
        verbose_name="Commentaires",
        blank=True,
        null=True
    )

    # Validation fields
    validation_status = models.CharField(
        max_length=20,
        choices=VALIDATION_STATUS_CHOICES,
        default='PENDING',
        verbose_name='Validation Status'
    )
    validated_by = models.ForeignKey(
        User,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='validated_behavior_changes',
        verbose_name='Validated By'
    )
    validation_date = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Validation Date'
    )
    validation_comment = models.TextField(
        null=True,
        blank=True,
        verbose_name='Validation Comment'
    )
    kobo_submission_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='Kobo Submission ID'
    )

    class Meta:
        verbose_name = "Promotion du changement de comportement"
        verbose_name_plural = "Promotion du changement de comportement"
        ordering = ['-report_date', 'location']

    def __str__(self):
        return f"Rapport {self.report_date} - {self.location.name}"

    @property
    def total_beneficiaries(self):
        return (
            self.male_participants +
            self.female_participants +
            self.twa_participants
        )

    @classmethod
    def to_data_element_obj(cls, kobo_data, **kwargs):
        male_participants = 0
        female_participants = 0
        twa_participants = 0

        if int(kobo_data.get('group_gy7sg68/Homme', 0)) > 0:
            male_participants = int(kobo_data.get('group_gy7sg68/Homme', 0))
            female_participants = int(kobo_data.get('group_gy7sg68/Femme', 0))
            twa_participants = int(kobo_data.get('group_gy7sg68/Twa', 0))

        # Refugee participants
        if int(kobo_data.get('group_hw5bi20/Femme_001', 0)) > 0:
            female_participants = int(kobo_data.get('group_hw5bi20/Femme_001', 0))
            male_participants = int(kobo_data.get('group_hw5bi20/Homme_001', 0))
        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7)
                        )[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]

        date = kobo_data.get('Date') or kobo_data.get('start')
        return cls(
            # Metadata
            id=kobo_data.get('_uuid'),
            report_date=datetime.fromisoformat(date).date() if date else None,
            location=Location.objects.filter(code=locationcode).first(),

            male_participants=male_participants,
            female_participants=female_participants,
            twa_participants=twa_participants,

            # Additional fields
            comments=kobo_data.get('Commentaires'),

            # Kobo metadata
            kobo_submission_id=kobo_data.get('_submission_id') or kobo_data.get('_id'),
        )


# Micro Project Models
class OtherProjectType(models.Model):
    microproject = models.ForeignKey('MicroProject', on_delete=models.CASCADE, related_name='other_project_types')
    name = models.CharField(max_length=255)
    beneficiary_count = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)

    def __str__(self):
        return f"{self.name} ({self.beneficiary_count} bénéficiaires)"


class MicroProject(models.Model):
    VALIDATION_STATUS_CHOICES = [
        ('PENDING', 'Pending Validation'),
        ('VALIDATED', 'Validated'),
        ('REJECTED', 'Rejected')
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4)

    # Location
    report_date = models.DateField(verbose_name="Date de l'activité'")
    location = models.ForeignKey('location.Location', on_delete=models.PROTECT)

    male_participants = models.PositiveIntegerField(
        verbose_name="Participants Hommes",
        validators=[MinValueValidator(0)],
        default=0
    )
    female_participants = models.PositiveIntegerField(
        verbose_name="Participantes Femmes",
        validators=[MinValueValidator(0)],
        default=0
    )
    twa_participants = models.PositiveIntegerField(
        verbose_name="Participants Twa",
        validators=[MinValueValidator(0)],
        default=0
    )

    # Project type counts
    agriculture_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)
    livestock_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)
    livestock_goat_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)
    livestock_pig_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)
    livestock_rabbit_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)
    livestock_poultry_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)
    livestock_cattle_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)
    commerce_services_beneficiaries = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)

    # Validation fields
    validation_status = models.CharField(
        max_length=20,
        choices=VALIDATION_STATUS_CHOICES,
        default='PENDING',
        verbose_name='Validation Status'
    )
    validated_by = models.ForeignKey(
        User,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='validated_microprojects',
        verbose_name='Validated By'
    )
    validation_date = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name='Validation Date'
    )
    validation_comment = models.TextField(
        null=True,
        blank=True,
        verbose_name='Validation Comment'
    )
    kobo_submission_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        verbose_name='Kobo Submission ID'
    )

    class Meta:
        verbose_name = "Micro-projet"
        verbose_name_plural = "Micro-projets"
        ordering = ['-report_date']

    def __str__(self):
        return f"Micro-projet {self.report_date} - {self.location.name}"

    @classmethod
    def to_data_element_obj(cls, kobo_data, **kwargs):
        male_participants = 0
        female_participants = 0
        twa_participants = 0
        # Regular participants
        if int(kobo_data.get('group_bh77o90/Homme', 0)) > 0:
            male_participants = int(kobo_data.get('group_bh77o90/Homme', 0))
            female_participants = int(kobo_data.get('group_bh77o90/Femme', 0))
            twa_participants = int(kobo_data.get('group_bh77o90/Twa', 0))

        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7)
                        )[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]
        date = kobo_data.get('Date') or kobo_data.get('start')

        micro_project = cls(
            id=kobo_data.get('_uuid'),
            report_date=datetime.fromisoformat(date).date() if date else None,
            location=Location.objects.filter(code=locationcode).first(),

            male_participants=male_participants,
            female_participants=female_participants,
            twa_participants=twa_participants,

            agriculture_beneficiaries=int(kobo_data.get('group_fb09e52/Agriculture', 0)),
            livestock_beneficiaries=int(kobo_data.get('group_fb09e52/Elevage', 0)),
            livestock_goat_beneficiaries=int(kobo_data.get('group_fb09e52/Ch_vres', 0)),
            livestock_pig_beneficiaries=int(kobo_data.get('group_fb09e52/Porcins', 0)),
            livestock_rabbit_beneficiaries=int(kobo_data.get('group_fb09e52/Lapins', 0)),
            livestock_poultry_beneficiaries=int(kobo_data.get('group_fb09e52/Volailles', 0)),
            livestock_cattle_beneficiaries=int(kobo_data.get('group_fb09e52/Bovins', 0)),
            commerce_services_beneficiaries=int(kobo_data.get('group_fb09e52/Commerce_et_services', 0)),

            # Kobo metadata
            kobo_submission_id=kobo_data.get('_submission_id') or kobo_data.get('_id'),
        )

        # Save micro_project first so it has a PK for FK references
        micro_project.save()

        # Handle other project types
        other_projects = kobo_data.get('group_fb09e52/group_mu7lt44', [])
        if isinstance(other_projects, list):
            for project in other_projects:
                if project.get('Autre_pr_ciser'):
                    OtherProjectType.objects.create(
                        microproject=micro_project,
                        name=project.get('Autre_pr_ciser'),
                        beneficiary_count=int(project.get('Effectif', 0))
                    )

        return micro_project


class PaymentAgency(models.Model):
    """
    First-class model for payment service providers (Lumicash, IBB, FINBANK, BANCOBU).
    Replaces upstream PaymentPoint for Merankabandi payment agency management.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    code = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=255)
    payment_gateway = models.CharField(
        max_length=50, blank=True, null=True,
        help_text="Maps to PAYMENT_GATEWAYS env config (e.g. LUMICASH, INTERBANK)"
    )
    gateway_config = models.TextField(
        blank=True, default='',
        help_text="JSON configuration for the payment gateway connector (e.g. class path, credentials key)"
    )
    contact_name = models.CharField(max_length=255, blank=True, default='')
    contact_phone = models.CharField(max_length=50, blank=True, default='')
    contact_email = models.CharField(max_length=255, blank=True, default='')
    is_active = models.BooleanField(default=True)
    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'merankabandi_payment_agency'
        ordering = ['name']

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()

    def __str__(self):
        return f"{self.name} ({self.code})"


class MonetaryTransfer(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    transfer_date = models.DateField(verbose_name="Date des transferts")

    location = models.ForeignKey('location.Location', on_delete=models.PROTECT)
    programme = models.ForeignKey(BenefitPlan, on_delete=models.PROTECT)
    payment_agency = models.ForeignKey(PaymentAgency, on_delete=models.PROTECT)

    # Planned beneficiaries
    planned_women = models.PositiveIntegerField(
        verbose_name="Femmes prévues",
        validators=[MinValueValidator(0)],
        default=0
    )
    planned_men = models.PositiveIntegerField(
        verbose_name="Hommes prévus",
        validators=[MinValueValidator(0)],
        default=0
    )
    planned_twa = models.PositiveIntegerField(
        verbose_name="Twa prévus",
        validators=[MinValueValidator(0)],
        default=0
    )

    # Paid beneficiaries
    paid_women = models.PositiveIntegerField(
        verbose_name="Femmes payées",
        validators=[MinValueValidator(0)],
        default=0
    )
    paid_men = models.PositiveIntegerField(
        verbose_name="Hommes payés",
        validators=[MinValueValidator(0)],
        default=0
    )
    paid_twa = models.PositiveIntegerField(
        verbose_name="Twa payés",
        validators=[MinValueValidator(0)],
        default=0
    )

    # Amount fields
    planned_amount = models.DecimalField(
        verbose_name="Montant prévu",
        max_digits=12,
        decimal_places=2,
        validators=[MinValueValidator(0)],
        default=0,
        help_text="Montant total prévu à distribuer (BIF)"
    )
    transferred_amount = models.DecimalField(
        verbose_name="Montant transféré",
        max_digits=12,
        decimal_places=2,
        validators=[MinValueValidator(0)],
        default=0,
        help_text="Montant effectivement transféré (BIF)"
    )

    class Meta:
        verbose_name = "Transfert Monétaire"
        verbose_name_plural = "Transferts Monétaires"
        ordering = ['-transfer_date']

    def __str__(self):
        return f"Transfert du {self.transfer_date} - {self.location.name}"

    @property
    def total_planned(self):
        return (
            self.planned_women +
            self.planned_men +
            self.planned_twa
        )

    @property
    def total_paid(self):
        return (
            self.paid_women +
            self.paid_men +
            self.paid_twa
        )

    def save(self, *args, **kwargs):
        # check if object has been newly created
        if self.id is None:
            self.id = uuid.uuid4()
        return super().save(*args, **kwargs)

    @classmethod
    def to_data_element_obj(cls, kobo_data, **kwargs):
        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7)
                        )[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]
        date = kobo_data.get('Date_des_transferts') or kobo_data.get('start')

        # Planned beneficiaries
        planned_women = int(kobo_data.get('group_tr1pf23/group_gl1wf27/Femme', 0))
        planned_men = int(kobo_data.get('group_tr1pf23/group_gl1wf27/Homme', 0))
        planned_twa = int(kobo_data.get('group_tr1pf23/group_gl1wf27/Twa', 0))

        # Paid beneficiaries
        paid_women = int(kobo_data.get('group_tr1pf23/group_ee8rm46/Femme_001', 0))
        paid_men = int(kobo_data.get('group_tr1pf23/group_ee8rm46/Homme_001', 0))
        paid_twa = int(kobo_data.get('group_tr1pf23/group_ee8rm46/Twa_001', 0))

        programme_code = '1.1' if kobo_data.get('Nom_du_camp_r_fugi_s') else '1.2'
        payment_agency_name = kobo_data.get('Nom_de_l_agence_de_paiement')

        return cls(
            # Metadata
            id=kobo_data.get('_uuid'),
            transfer_date=datetime.fromisoformat(date).date() if date else None,
            location=Location.objects.filter(code=locationcode).first(),

            # Payment details
            payment_agency=PaymentAgency.objects.filter(name=payment_agency_name).first(),
            programme=BenefitPlan.objects.filter(code=programme_code).first(),

            # Planned beneficiaries
            planned_women=planned_women,
            planned_men=planned_men,
            planned_twa=planned_twa,

            # Paid beneficiaries
            paid_women=paid_women,
            paid_men=paid_men,
            paid_twa=paid_twa,
        )


class Section(models.Model):
    name = models.CharField(max_length=255)

    def update(self, *args, user=None, username=None, save=True, **kwargs):
        obj_data = kwargs.pop('data', {})
        if not obj_data:
            obj_data = kwargs
            kwargs = {}
        for key in obj_data:
            setattr(self, key, obj_data[key])
        if save:
            self.save()
        return self

    def __str__(self):
        return self.name


class Indicator(models.Model):
    section = models.ForeignKey(Section, on_delete=models.CASCADE, related_name="indicators", null=True, blank=True)
    name = models.CharField(max_length=255)
    pbc = models.CharField(max_length=255, blank=True, null=True)
    baseline = models.DecimalField(max_digits=15, decimal_places=2, default=0.00)
    target = models.DecimalField(max_digits=15, decimal_places=2, default=0.00)
    observation = models.TextField(blank=True, null=True)

    def update(self, *args, user=None, username=None, save=True, **kwargs):
        obj_data = kwargs.pop('data', {})
        if not obj_data:
            obj_data = kwargs
            kwargs = {}
        for key in obj_data:
            setattr(self, key, obj_data[key])
        if save:
            self.save()
        return self

    def __str__(self):
        return self.name


class IndicatorAchievement(models.Model):
    indicator = models.ForeignKey(Indicator, on_delete=models.CASCADE, related_name="achievements")
    achieved = models.DecimalField(max_digits=15, decimal_places=2, default=0.00)
    comment = models.TextField(blank=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    date = models.DateField(null=True, blank=True)  # New field to specify the date of the indicator value
    breakdowns = models.JSONField(default=list, blank=True)

    def update(self, *args, user=None, username=None, save=True, **kwargs):
        obj_data = kwargs.pop('data', {})
        if not obj_data:
            obj_data = kwargs
            kwargs = {}
        for key in obj_data:
            setattr(self, key, obj_data[key])
        if save:
            self.save()
        return self

    def __str__(self):
        return f"{self.indicator.name} - {self.achieved} at {self.date}"



class ProvincePaymentAgency(models.Model):
    """
    Association between Province (Location) + BenefitPlan + PaymentAgency.
    Defines which payment agency serves which province for which programme.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    province = models.ForeignKey(Location, on_delete=models.PROTECT, related_name='province_payment_agencies')
    benefit_plan = models.ForeignKey(BenefitPlan, on_delete=models.PROTECT, related_name='province_payment_agencies')
    payment_agency = models.ForeignKey(PaymentAgency, on_delete=models.PROTECT, related_name='province_assignments')
    is_active = models.BooleanField(default=True)
    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'merankabandi_province_payment_agency'
        unique_together = ('province', 'benefit_plan', 'payment_agency')

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()

    def __str__(self):
        return f"{self.province.name} -> {self.payment_agency.name} ({self.benefit_plan.name})"


class ResultFrameworkSnapshot(models.Model):
    """Model for storing result framework snapshots"""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    snapshot_date = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.PROTECT)
    data = models.JSONField()  # Complete snapshot data
    document_path = models.CharField(max_length=500, blank=True)
    status = models.CharField(max_length=20, choices=[
        ('DRAFT', 'Draft'),
        ('FINALIZED', 'Finalized'),
        ('ARCHIVED', 'Archived')
    ], default='DRAFT')

    class Meta:
        verbose_name = "Result Framework Snapshot"
        verbose_name_plural = "Result Framework Snapshots"
        ordering = ['-snapshot_date']

    def __str__(self):
        return f"{self.name} - {self.snapshot_date.strftime('%Y-%m-%d')}"


class PmtFormula(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")
    base_score_urban = models.DecimalField(max_digits=10, decimal_places=4, default=0)
    base_score_rural = models.DecimalField(max_digits=10, decimal_places=4, default=0)
    variables = models.JSONField(
        default=list,
        help_text='List of {field, weight, category, urban_weight, rural_weight}'
    )
    geographic_adjustments = models.JSONField(
        default=dict,
        help_text='Map of province_code -> adjustment value'
    )
    is_active = models.BooleanField(default=True)

    def update(self, *args, user=None, username=None, save=True, **kwargs):
        obj_data = kwargs.pop('data', {})
        if not obj_data:
            obj_data = kwargs
            kwargs = {}
        for key in obj_data:
            setattr(self, key, obj_data[key])
        if save:
            self.save()
        return self

    class Meta:
        verbose_name = "PMT Formula"
        verbose_name_plural = "PMT Formulas"
        ordering = ['name']

    def __str__(self):
        return self.name


class SelectionQuota(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    benefit_plan = models.ForeignKey(BenefitPlan, on_delete=models.PROTECT)
    location = models.ForeignKey(Location, on_delete=models.PROTECT, related_name='selection_quotas')
    targeting_round = models.IntegerField(default=1)
    quota = models.IntegerField()
    collect_multiplier = models.DecimalField(max_digits=4, decimal_places=2, default=2.0)

    def update(self, *args, user=None, username=None, save=True, **kwargs):
        obj_data = kwargs.pop('data', {})
        if not obj_data:
            obj_data = kwargs
            kwargs = {}
        for key in obj_data:
            setattr(self, key, obj_data[key])
        if save:
            self.save()
        return self

    class Meta:
        verbose_name = "Selection Quota"
        verbose_name_plural = "Selection Quotas"
        unique_together = ('benefit_plan', 'location', 'targeting_round')
        ordering = ['benefit_plan', 'location']

    def __str__(self):
        return f"{self.benefit_plan.code} - {self.location.name} (round {self.targeting_round}): {self.quota}"


class PreCollecteStatus(models.TextChoices):
    COLLECTED = "COLLECTED", "Collected"
    LINKED = "LINKED", "Linked"
    DELETED = "DELETED", "Deleted"


class PreCollecte(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    benefit_plan = models.ForeignKey(BenefitPlan, on_delete=models.PROTECT)
    location = models.ForeignKey(
        Location, on_delete=models.PROTECT, related_name='precollecte_records'
    )
    origin_location = models.ForeignKey(
        Location, on_delete=models.PROTECT, null=True, blank=True,
        related_name='precollecte_origin_records'
    )
    nom = models.CharField(max_length=255)
    prenom = models.CharField(max_length=255)
    pere = models.CharField(max_length=255, blank=True, default="")
    mere = models.CharField(max_length=255, blank=True, default="")
    ci = models.CharField(max_length=50, blank=True, default="")
    telephone = models.CharField(max_length=15, blank=True, default="")
    sexe = models.CharField(max_length=1)
    mutwa = models.BooleanField(default=False)
    rapatrie = models.BooleanField(default=False)
    age_handicap = models.BooleanField(default=False)
    social_id = models.CharField(max_length=14, unique=True, blank=True, default="")
    social_id_seq = models.IntegerField(default=0)
    targeting_round = models.IntegerField(default=1)
    kobo_uuid = models.CharField(max_length=255, blank=True, default="")
    device_id = models.CharField(max_length=255, blank=True, default="")
    group = models.ForeignKey(
        'individual.Group', on_delete=models.SET_NULL, null=True, blank=True,
        related_name='precollecte_records'
    )
    status = models.CharField(
        max_length=20, choices=PreCollecteStatus.choices,
        default=PreCollecteStatus.COLLECTED
    )
    json_ext = models.JSONField(null=True, blank=True)

    def update(self, *args, user=None, username=None, save=True, **kwargs):
        obj_data = kwargs.pop('data', {})
        if not obj_data:
            obj_data = kwargs
            kwargs = {}
        for key in obj_data:
            setattr(self, key, obj_data[key])
        if save:
            self.save()
        return self

    class Meta:
        verbose_name = "Pre-Collecte"
        verbose_name_plural = "Pre-Collectes"
        ordering = ['-id']

    def __str__(self):
        return f"{self.social_id} - {self.nom} {self.prenom}"


class IndicatorCalculationRule(models.Model):
    """Configuration for automated indicator calculations"""
    indicator = models.OneToOneField(Indicator, on_delete=models.CASCADE, related_name='calculation_rule')
    calculation_type = models.CharField(max_length=50, choices=[
        ('SYSTEM', 'System Calculated'),
        ('MANUAL', 'Manual Entry'),
        ('MIXED', 'Mixed System/Manual')
    ])
    calculation_method = models.CharField(max_length=100, blank=True, help_text="Method name for system calculations")
    calculation_config = models.JSONField(default=dict, help_text="Configuration for calculations")
    is_active = models.BooleanField(default=True)

    class Meta:
        verbose_name = "Indicator Calculation Rule"
        verbose_name_plural = "Indicator Calculation Rules"

    def __str__(self):
        return f"{self.indicator.name} - {self.calculation_type}"


class CommunePaymentScheduleStatus(models.TextChoices):
    PLANNING = "PLANNING", "Planning"
    PENDING = "PENDING", "Pending"
    GENERATING = "GENERATING", "Generating"
    APPROVED = "APPROVED", "Approved"
    IN_PAYMENT = "IN_PAYMENT", "In Payment"
    RECONCILED = "RECONCILED", "Reconciled"
    FAILED = "FAILED", "Failed"
    REJECTED = "REJECTED", "Rejected"


# Maximum number of regular payment rounds per commune per programme
MAX_PAYMENT_ROUNDS = 12
# Standard bimonthly transfer amount in BIF (36,000 FBU/month × 2 months)
STANDARD_TRANSFER_AMOUNT = 72000

# Vague → province mapping (from RTM presentation Nov 2025)
VAGUE_PROVINCES = {
    1: ['Kirundo', 'Gitega', 'Karuzi', 'Ruyigi'],
    2: ['Ngozi', 'Muyinga', 'Muramvya', 'Mwaro'],
    3: ['Bujumbura Mairie', 'Bubanza', 'Cibitoke', 'Rumonge'],
    4: ['Kayanza', 'Bujumbura Rural', 'Makamba', 'Cankuzo', 'Bururi', 'Rutana'],
}


class AgencyFeeConfig(models.Model):
    """
    Fee rate configuration per PaymentAgency + BenefitPlan + optional Province.

    Lookup order (most specific wins):
      agency + benefit_plan + province  →  agency + benefit_plan  →  agency default

    fee_included=True means the beneficiary amount already includes the fee
    (programme pays amount, agency takes fee from it).
    fee_included=False means fee is added on top
    (programme pays amount + fee, beneficiary receives amount).
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    payment_agency = models.ForeignKey(
        PaymentAgency, on_delete=models.CASCADE,
        related_name='fee_configs',
    )
    benefit_plan = models.ForeignKey(
        BenefitPlan, on_delete=models.CASCADE,
        related_name='agency_fee_configs',
    )
    province = models.ForeignKey(
        Location, on_delete=models.CASCADE,
        null=True, blank=True,
        related_name='agency_fee_configs',
        help_text="Province-level override (type D). NULL = default for this agency+plan."
    )
    fee_rate = models.DecimalField(
        max_digits=8, decimal_places=6,
        help_text="Fee rate as decimal (e.g. 0.020833 for ~2.0833%). 6dp lets us encode rates derived from integer-BIF fees on a 72K base (e.g. 1500/72000 = 0.020833)."
    )
    fee_included = models.BooleanField(
        default=False,
        help_text="True = fee included in beneficiary amount, False = fee added on top"
    )
    is_active = models.BooleanField(default=True)
    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'merankabandi_agency_fee_config'
        ordering = ['payment_agency', 'benefit_plan', 'province']
        constraints = [
            models.UniqueConstraint(
                fields=['payment_agency', 'benefit_plan', 'province'],
                name='unique_fee_config_agency_plan_province',
            ),
            models.UniqueConstraint(
                fields=['payment_agency', 'benefit_plan'],
                condition=models.Q(province__isnull=True),
                name='unique_fee_config_agency_plan_default',
            ),
        ]

    def __str__(self):
        prov = f" / {self.province.name}" if self.province else ""
        incl = "incl." if self.fee_included else "excl."
        return f"{self.payment_agency.code} → {self.benefit_plan.code}{prov}: {self.fee_rate*100:.2f}% ({incl})"

    @classmethod
    def lookup(cls, payment_agency, benefit_plan, province=None):
        """Find the most specific fee config for this combination.

        Returns AgencyFeeConfig or None.
        """
        if province:
            config = cls.objects.filter(
                payment_agency=payment_agency,
                benefit_plan=benefit_plan,
                province=province,
                is_active=True,
            ).first()
            if config:
                return config
        return cls.objects.filter(
            payment_agency=payment_agency,
            benefit_plan=benefit_plan,
            province__isnull=True,
            is_active=True,
        ).first()


class CommunePaymentSchedule(models.Model):
    """
    Tracks payment rounds per commune per benefit plan (programme).

    Enforces:
    - Sequential closure: round N must be RECONCILED before round N+1 can start
    - Cap: max 12 regular rounds per commune per programme
    - Retry payrolls (from failed payments) are tracked but not counted toward the cap
    - Late-enrolled beneficiaries receive cumulative back-pay
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    benefit_plan = models.ForeignKey(
        BenefitPlan, on_delete=models.PROTECT,
        related_name='payment_schedules',
        help_text="Programme (e.g. 1.2 Transfert Monétaire Régulier)"
    )
    commune = models.ForeignKey(
        Location, on_delete=models.PROTECT,
        related_name='payment_schedules',
        help_text="Commune (location type W)"
    )
    round_number = models.PositiveIntegerField(
        help_text="Payment round (1-12 for regular, 0 for retry)"
    )
    payroll = models.ForeignKey(
        Payroll, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='payment_schedules',
        help_text="Linked payroll (set when payroll is created)"
    )
    is_retry = models.BooleanField(
        default=False,
        help_text="True if this is a retry payroll for failed payments (does not count toward 12-round cap)"
    )
    retry_source = models.ForeignKey(
        'self', on_delete=models.SET_NULL, null=True, blank=True,
        related_name='retries',
        help_text="Original schedule entry this retry relates to"
    )
    status = models.CharField(
        max_length=20,
        choices=CommunePaymentScheduleStatus.choices,
        default=CommunePaymentScheduleStatus.PENDING
    )
    amount_per_beneficiary = models.DecimalField(
        max_digits=12, decimal_places=2, default=STANDARD_TRANSFER_AMOUNT,
        help_text="Amount per beneficiary for this round (BIF)"
    )
    total_beneficiaries = models.PositiveIntegerField(
        default=0, help_text="Number of beneficiaries in this payment round"
    )
    total_amount = models.DecimalField(
        max_digits=15, decimal_places=2, default=0,
        help_text="Total amount for this round (BIF)"
    )
    reconciled_count = models.PositiveIntegerField(
        default=0, help_text="Number of beneficiaries successfully paid"
    )
    failed_count = models.PositiveIntegerField(
        default=0, help_text="Number of failed payments"
    )
    payment_cycle = models.ForeignKey(
        'payment_cycle.PaymentCycle', on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='commune_schedules',
        help_text="Planning cycle this schedule belongs to"
    )
    date_valid_from = models.DateField(
        null=True, blank=True,
        help_text="Planned payment validity start date (set during planning)"
    )
    topup_amount = models.DecimalField(
        max_digits=12, decimal_places=2, default=0,
        help_text="One-time top-up/compensatory amount for this round (inherited from cycle, editable)"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Calendrier de Paiement Commune"
        verbose_name_plural = "Calendriers de Paiement Communes"
        ordering = ['benefit_plan', 'commune', 'round_number']
        constraints = [
            models.UniqueConstraint(
                fields=['benefit_plan', 'commune', 'round_number'],
                condition=models.Q(is_retry=False),
                name='unique_regular_round_per_commune'
            ),
        ]

    def __str__(self):
        retry_label = " (retry)" if self.is_retry else ""
        return f"{self.commune.name} - Round {self.round_number}{retry_label} - {self.status}"

    def sync_from_payroll(self):
        """Update status from linked payroll."""
        if not self.payroll:
            return
        status_map = {
            PayrollStatus.GENERATING: CommunePaymentScheduleStatus.GENERATING,
            PayrollStatus.PENDING_APPROVAL: CommunePaymentScheduleStatus.PENDING,
            PayrollStatus.APPROVE_FOR_PAYMENT: CommunePaymentScheduleStatus.APPROVED,
            PayrollStatus.RECONCILED: CommunePaymentScheduleStatus.RECONCILED,
            PayrollStatus.FAILED: CommunePaymentScheduleStatus.FAILED,
            PayrollStatus.REJECTED: CommunePaymentScheduleStatus.REJECTED,
        }
        new_status = status_map.get(self.payroll.status)
        if new_status and new_status != self.status:
            self.status = new_status
            self.save()


# Import workflow models so Django discovers them for migrations
from merankabandi.workflow_models import (  # noqa: E402, F401
    WorkflowTemplate, WorkflowStepTemplate, GrievanceWorkflow,
    GrievanceTask, ReplacementRequest, RoleAssignment,
)
