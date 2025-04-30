import uuid
from django.db import models
from django.core.validators import MinValueValidator
from datetime import datetime

from core.models import User, HistoryModel, UUIDModel
from location.models import Location
from payroll.models import PaymentPoint
from social_protection.models import BenefitPlan
from payment_cycle.models import PaymentCycle
from contribution_plan.models import PaymentPlan

class SensitizationTraining(models.Model):
    THEME_CATEGORIES = [
        ('module_mip__mesures_d_inclusio', 'Module MIP (Mesures d\'Inclusion Productive)'),
        ('module_mach__mesures_d_accompa', 'Module MACH (Mesures d\'Accompagnement pour le développement du Capital Humain)')
    ]

    id = models.UUIDField(primary_key=True)
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

        male_participants = 0
        female_participants = 0
        twa_participants = 0
        # Regular participants
        if int(kobo_data.get('group_bz5vi86/group_nb3yd70/Homme', 0)) > 0:
            male_participants=int(kobo_data.get('group_bz5vi86/group_nb3yd70/Homme', 0))
            female_participants=int(kobo_data.get('group_bz5vi86/group_nb3yd70/Femme', 0))
            twa_participants=int(kobo_data.get('group_bz5vi86/group_nb3yd70/Twa', 0))
        
        # Refugee participants
        if int(kobo_data.get('group_bz5vi86/group_mc86q55/Femme_', 0)) > 0:
            female_participants=int(kobo_data.get('group_bz5vi86/group_mc86q55/Femme_', 0))
            male_participants=int(kobo_data.get('group_bz5vi86/group_mc86q55/Homme_001', 0))

        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]
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
        )


class BehaviorChangePromotion(models.Model):
    # Metadata
    id = models.UUIDField(primary_key=True)
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
            male_participants=int(kobo_data.get('group_gy7sg68/Homme', 0))
            female_participants=int(kobo_data.get('group_gy7sg68/Femme', 0))
            twa_participants=int(kobo_data.get('group_gy7sg68/Twa', 0))
        
        # Refugee participants
        if int(kobo_data.get('group_hw5bi20/Femme_001', 0)) > 0:
            female_participants=int(kobo_data.get('group_hw5bi20/Femme_001', 0))
            male_participants=int(kobo_data.get('group_hw5bi20/Homme_001', 0))
        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]

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
            
        )


# Micro Project Models
class OtherProjectType(models.Model):
    microproject = models.ForeignKey('MicroProject', on_delete=models.CASCADE, related_name='other_project_types')
    name = models.CharField(max_length=255)
    beneficiary_count = models.PositiveIntegerField(validators=[MinValueValidator(0)], default=0)

    def __str__(self):
        return f"{self.name} ({self.beneficiary_count} bénéficiaires)"

class MicroProject(models.Model):
    id = models.UUIDField(primary_key=True)
    
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
            male_participants=int(kobo_data.get('group_bh77o90/Homme', 0))
            female_participants=int(kobo_data.get('group_bh77o90/Femme', 0))
            twa_participants=int(kobo_data.get('group_bh77o90/Twa', 0))

        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]
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
        )
        
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
    

class MonetaryTransfer(models.Model):
    id = models.UUIDField(primary_key=True)
    transfer_date = models.DateField(verbose_name="Date des transferts")
    
    location = models.ForeignKey('location.Location', on_delete=models.PROTECT)
    programme = models.ForeignKey(BenefitPlan, on_delete=models.PROTECT)
    payment_agency = models.ForeignKey(PaymentPoint, on_delete=models.PROTECT)

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
        locationcode = (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[:4] + (str(kobo_data.get('group_ln06g44/Colline')).zfill(7))[5:]
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
        payment_agency_name=kobo_data.get('Nom_de_l_agence_de_paiement')

        return cls(
            # Metadata
            id=kobo_data.get('_uuid'),
            transfer_date=datetime.fromisoformat(date).date() if date else None,
            location=Location.objects.filter(code=locationcode).first(),
        
            # Payment details
            payment_agency=PaymentPoint.objects.filter(name=payment_agency_name).first(),
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

    def __str__(self):
        return self.name

class Indicator(models.Model):
    section = models.ForeignKey(Section, on_delete=models.CASCADE, related_name="indicators", null=True, blank=True)
    name = models.CharField(max_length=255)
    pbc = models.CharField(max_length=255, blank=True, null=True)
    baseline = models.DecimalField(max_digits=15, decimal_places=2, default=0.00)
    target = models.DecimalField(max_digits=15, decimal_places=2, default=0.00)
    observation = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.name

class IndicatorAchievement(models.Model):
    indicator = models.ForeignKey(Indicator, on_delete=models.CASCADE, related_name="achievements")
    achieved = models.DecimalField(max_digits=15, decimal_places=2, default=0.00)
    comment = models.TextField(blank=True, null=True)
    timestamp = models.DateTimeField(auto_now_add=True)
    date = models.DateField(null=True, blank=True)  # New field to specify the date of the indicator value

    def __str__(self):
        return f"{self.indicator.name} - {self.achieved} at {self.timestamp}"

class ProvincePaymentPoint(UUIDModel):
    """
    Model for associating payment points with provinces and benefit plans
    """
    province = models.ForeignKey(Location, on_delete=models.PROTECT, related_name='province_payment_points')
    payment_point = models.ForeignKey(PaymentPoint, on_delete=models.PROTECT, related_name='province_associations')
    payment_plan = models.ForeignKey(PaymentPlan, on_delete=models.PROTECT, null=True, blank=True, related_name='province_payment_points')
    is_active = models.BooleanField(default=True)
    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'merankabandi_province_payment_point'
        unique_together = ('province', 'payment_point', 'payment_plan')
        
    def __str__(self):
        benefit_plan_name = self.payment_plan.benefit_plan.name if self.payment_plan else "All plans"
        return f"{self.province.name} - {self.payment_point.name} - {benefit_plan_name}"
