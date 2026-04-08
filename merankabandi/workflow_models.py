import uuid
from django.db import models

from core.models import User
from grievance_social_protection.models import Ticket


class WorkflowTemplate(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=100, unique=True)
    label = models.CharField(max_length=255)
    case_type = models.CharField(max_length=255, default='', db_index=True)
    description = models.TextField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    json_ext = models.JSONField(null=True, blank=True)

    class Meta:
        db_table = 'merankabandi_workflow_template'

    def __str__(self):
        return self.label

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()


class WorkflowStepTemplate(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    workflow_template = models.ForeignKey(
        WorkflowTemplate,
        on_delete=models.CASCADE,
        related_name='steps'
    )
    name = models.CharField(max_length=100)
    label = models.CharField(max_length=255)
    order = models.PositiveIntegerField(default=0)
    role = models.CharField(max_length=100, null=True, blank=True)
    is_required = models.BooleanField(default=True)
    condition = models.JSONField(null=True, blank=True)
    action_type = models.CharField(max_length=100, null=True, blank=True)
    json_ext = models.JSONField(null=True, blank=True)

    class Meta:
        db_table = 'merankabandi_workflow_step_template'
        ordering = ['order']
        unique_together = [('workflow_template', 'order')]

    def __str__(self):
        return f"{self.workflow_template.name} - {self.label}"

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()


class GrievanceWorkflow(models.Model):
    STATUS_PENDING = 'PENDING'
    STATUS_IN_PROGRESS = 'IN_PROGRESS'
    STATUS_COMPLETED = 'COMPLETED'
    STATUS_CANCELLED = 'CANCELLED'

    STATUS_CHOICES = [
        (STATUS_PENDING, 'Pending'),
        (STATUS_IN_PROGRESS, 'In Progress'),
        (STATUS_COMPLETED, 'Completed'),
        (STATUS_CANCELLED, 'Cancelled'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ticket = models.ForeignKey(
        Ticket,
        on_delete=models.CASCADE,
        related_name='workflows'
    )
    template = models.ForeignKey(
        WorkflowTemplate,
        on_delete=models.PROTECT,
        related_name='instances'
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING
    )
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    json_ext = models.JSONField(null=True, blank=True)

    class Meta:
        db_table = 'merankabandi_grievance_workflow'

    def __str__(self):
        return f"Workflow {self.template.name} for ticket {self.ticket_id}"

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()


class GrievanceTask(models.Model):
    STATUS_PENDING = 'PENDING'
    STATUS_BLOCKED = 'BLOCKED'
    STATUS_IN_PROGRESS = 'IN_PROGRESS'
    STATUS_COMPLETED = 'COMPLETED'
    STATUS_SKIPPED = 'SKIPPED'

    STATUS_CHOICES = [
        (STATUS_PENDING, 'Pending'),
        (STATUS_BLOCKED, 'Blocked'),
        (STATUS_IN_PROGRESS, 'In Progress'),
        (STATUS_COMPLETED, 'Completed'),
        (STATUS_SKIPPED, 'Skipped'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    workflow = models.ForeignKey(
        GrievanceWorkflow,
        on_delete=models.CASCADE,
        related_name='tasks'
    )
    step_template = models.ForeignKey(
        WorkflowStepTemplate,
        on_delete=models.PROTECT,
        related_name='task_instances'
    )
    ticket = models.ForeignKey(
        Ticket,
        on_delete=models.CASCADE,
        related_name='workflow_tasks'
    )
    assigned_role = models.CharField(max_length=100, null=True, blank=True)
    assigned_user = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='grievance_tasks'
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING
    )
    blocked_by = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='blocking'
    )
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    due_date = models.DateField(null=True, blank=True)
    result = models.JSONField(null=True, blank=True)
    json_ext = models.JSONField(null=True, blank=True)
    order = models.PositiveIntegerField(default=0, help_text="Display order within workflow")

    class Meta:
        db_table = 'merankabandi_grievance_task'

    def __str__(self):
        return f"Task {self.step_template.label} ({self.status})"

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()


class ReplacementRequest(models.Model):
    STATUS_PENDING = 'PENDING'
    STATUS_APPROVED = 'APPROVED'
    STATUS_EXECUTED = 'EXECUTED'
    STATUS_REJECTED = 'REJECTED'

    STATUS_CHOICES = [
        (STATUS_PENDING, 'Pending'),
        (STATUS_APPROVED, 'Approved'),
        (STATUS_EXECUTED, 'Executed'),
        (STATUS_REJECTED, 'Rejected'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    ticket = models.ForeignKey(
        Ticket,
        on_delete=models.CASCADE,
        related_name='replacement_requests'
    )
    task = models.ForeignKey(
        GrievanceTask,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='replacement_requests'
    )
    replaced_social_id = models.CharField(max_length=20, null=True, blank=True)
    replaced_individual = models.ForeignKey(
        'individual.Individual',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='replacement_requests_as_replaced'
    )
    motif = models.TextField(null=True, blank=True)
    relationship = models.CharField(max_length=100, null=True, blank=True)
    new_nom = models.CharField(max_length=255, null=True, blank=True)
    new_prenom = models.CharField(max_length=255, null=True, blank=True)
    new_date_naissance = models.DateField(null=True, blank=True)
    new_sexe = models.CharField(max_length=10, null=True, blank=True)
    new_telephone = models.CharField(max_length=20, null=True, blank=True)
    new_cni = models.CharField(max_length=50, null=True, blank=True)
    new_individual = models.ForeignKey(
        'individual.Individual',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='replacement_requests_as_new'
    )
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING
    )
    json_ext = models.JSONField(null=True, blank=True)

    class Meta:
        db_table = 'merankabandi_replacement_request'

    def __str__(self):
        return f"Replacement request for ticket {self.ticket_id} ({self.status})"

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()


class RoleAssignment(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    role = models.CharField(max_length=100)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='role_assignments'
    )
    location = models.ForeignKey(
        'location.Location',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='role_assignments'
    )
    category = models.CharField(max_length=100, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    json_ext = models.JSONField(null=True, blank=True)

    class Meta:
        db_table = 'merankabandi_role_assignment'

    def __str__(self):
        return f"{self.role} - {self.user}"

    def update(self, data):
        [setattr(self, k, v) for k, v in data.items()]
        self.save()
