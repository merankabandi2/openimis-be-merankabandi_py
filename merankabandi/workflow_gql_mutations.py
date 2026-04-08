import graphene
from gettext import gettext as _
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError

from core.gql.gql_mutations.base_mutation import BaseMutation
from core.schema import OpenIMISMutation
from merankabandi.apps import MerankabandiConfig
from merankabandi.workflow_models import (
    GrievanceTask, ReplacementRequest,
    WorkflowTemplate, WorkflowStepTemplate, RoleAssignment,
)
from merankabandi.workflow_service import WorkflowService


class CompleteGrievanceTaskInputType(OpenIMISMutation.Input):
    task_id = graphene.UUID(required=True)
    result = graphene.JSONString(required=False)


class CompleteGrievanceTaskMutation(BaseMutation):
    _mutation_class = "CompleteGrievanceTaskMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        task = GrievanceTask.objects.get(id=data.get('task_id'))
        if task.status != GrievanceTask.STATUS_IN_PROGRESS:
            raise ValidationError(f"Task is not IN_PROGRESS (current: {task.status})")
        WorkflowService.complete_task(task, user, data.get('result'))

    class Input(CompleteGrievanceTaskInputType):
        pass


class SkipGrievanceTaskInputType(OpenIMISMutation.Input):
    task_id = graphene.UUID(required=True)
    reason = graphene.String(required=False)


class SkipGrievanceTaskMutation(BaseMutation):
    _mutation_class = "SkipGrievanceTaskMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        task = GrievanceTask.objects.get(id=data.get('task_id'))
        WorkflowService.skip_task(task, user, data.get('reason'))

    class Input(SkipGrievanceTaskInputType):
        pass


class ReassignGrievanceTaskInputType(OpenIMISMutation.Input):
    task_id = graphene.UUID(required=True)
    user_id = graphene.UUID(required=True)


class ReassignGrievanceTaskMutation(BaseMutation):
    _mutation_class = "ReassignGrievanceTaskMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        from core.models import User as CoreUser
        task = GrievanceTask.objects.get(id=data['task_id'])
        new_user = CoreUser.objects.get(id=data['user_id'])
        WorkflowService.reassign_task(task, new_user)

    class Input(ReassignGrievanceTaskInputType):
        pass


class AddTaskToWorkflowInputType(OpenIMISMutation.Input):
    workflow_id = graphene.UUID(required=True)
    step_template_id = graphene.UUID(required=True)


class AddTaskToWorkflowMutation(BaseMutation):
    _mutation_class = "AddTaskToWorkflowMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        from merankabandi.workflow_models import GrievanceWorkflow
        workflow = GrievanceWorkflow.objects.get(id=data['workflow_id'])
        WorkflowService.add_task_to_workflow(workflow, data['step_template_id'], user)

    class Input(AddTaskToWorkflowInputType):
        pass


class ApproveReplacementRequestInputType(OpenIMISMutation.Input):
    request_id = graphene.UUID(required=True)


class ApproveReplacementRequestMutation(BaseMutation):
    _mutation_class = "ApproveReplacementRequestMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        rr = ReplacementRequest.objects.get(id=data['request_id'])
        rr.status = ReplacementRequest.STATUS_APPROVED
        rr.save()

    class Input(ApproveReplacementRequestInputType):
        pass


class RejectReplacementRequestInputType(OpenIMISMutation.Input):
    request_id = graphene.UUID(required=True)
    reason = graphene.String(required=True)


class RejectReplacementRequestMutation(BaseMutation):
    _mutation_class = "RejectReplacementRequestMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        rr = ReplacementRequest.objects.get(id=data['request_id'])
        rr.status = ReplacementRequest.STATUS_REJECTED
        rr.json_ext = rr.json_ext or {}
        rr.json_ext['rejection_reason'] = data.get('reason')
        rr.save()

    class Input(RejectReplacementRequestInputType):
        pass


class CreateWorkflowTemplateInputType(OpenIMISMutation.Input):
    name = graphene.String(required=True)
    label = graphene.String(required=True)
    case_type = graphene.String(required=True)
    description = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)
    json_ext = graphene.JSONString(required=False)


class CreateWorkflowTemplateMutation(BaseMutation):
    _mutation_class = "CreateWorkflowTemplateMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        WorkflowTemplate.objects.create(**data)

    class Input(CreateWorkflowTemplateInputType):
        pass


class UpdateWorkflowTemplateInputType(CreateWorkflowTemplateInputType):
    id = graphene.UUID(required=True)


class UpdateWorkflowTemplateMutation(BaseMutation):
    _mutation_class = "UpdateWorkflowTemplateMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        tpl_id = data.pop('id')
        WorkflowTemplate.objects.filter(id=tpl_id).update(**data)

    class Input(UpdateWorkflowTemplateInputType):
        pass


class DeleteWorkflowTemplateInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class DeleteWorkflowTemplateMutation(BaseMutation):
    _mutation_class = "DeleteWorkflowTemplateMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        WorkflowTemplate.objects.filter(id__in=data.get('ids', [])).delete()

    class Input(DeleteWorkflowTemplateInputType):
        pass


# --- Admin CRUD: WorkflowStepTemplate ---

class CreateWorkflowStepTemplateInputType(OpenIMISMutation.Input):
    workflow_template_id = graphene.UUID(required=True)
    name = graphene.String(required=True)
    label = graphene.String(required=True)
    order = graphene.Int(required=True)
    role = graphene.String(required=True)
    action_type = graphene.String(required=True)
    is_required = graphene.Boolean(required=False)
    condition = graphene.JSONString(required=False)
    json_ext = graphene.JSONString(required=False)


class CreateWorkflowStepTemplateMutation(BaseMutation):
    _mutation_class = "CreateWorkflowStepTemplateMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        tpl_id = data.pop('workflow_template_id')
        tpl = WorkflowTemplate.objects.get(id=tpl_id)
        WorkflowStepTemplate.objects.create(workflow_template=tpl, **data)

    class Input(CreateWorkflowStepTemplateInputType):
        pass


class UpdateWorkflowStepTemplateInputType(CreateWorkflowStepTemplateInputType):
    id = graphene.UUID(required=True)
    workflow_template_id = graphene.UUID(required=False)


class UpdateWorkflowStepTemplateMutation(BaseMutation):
    _mutation_class = "UpdateWorkflowStepTemplateMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        step_id = data.pop('id')
        data.pop('workflow_template_id', None)
        WorkflowStepTemplate.objects.filter(id=step_id).update(**data)

    class Input(UpdateWorkflowStepTemplateInputType):
        pass


class DeleteWorkflowStepTemplateInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class DeleteWorkflowStepTemplateMutation(BaseMutation):
    _mutation_class = "DeleteWorkflowStepTemplateMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        WorkflowStepTemplate.objects.filter(id__in=data.get('ids', [])).delete()

    class Input(DeleteWorkflowStepTemplateInputType):
        pass


class CreateRoleAssignmentInputType(OpenIMISMutation.Input):
    role = graphene.String(required=True)
    user_id = graphene.UUID(required=True)
    location_id = graphene.Int(required=False)
    category = graphene.String(required=False)
    is_active = graphene.Boolean(required=False)


class CreateRoleAssignmentMutation(BaseMutation):
    _mutation_class = "CreateRoleAssignmentMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        from core.models import User as CoreUser
        from location.models import Location
        user_obj = CoreUser.objects.get(id=data.pop('user_id'))
        location_id = data.pop('location_id', None)
        location = Location.objects.get(id=location_id) if location_id else None
        RoleAssignment.objects.create(user=user_obj, location=location, **data)

    class Input(CreateRoleAssignmentInputType):
        pass


class UpdateRoleAssignmentInputType(CreateRoleAssignmentInputType):
    id = graphene.UUID(required=True)


class UpdateRoleAssignmentMutation(BaseMutation):
    _mutation_class = "UpdateRoleAssignmentMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        ra_id = data.pop('id')
        ra = RoleAssignment.objects.get(id=ra_id)
        user_id = data.pop('user_id', None)
        location_id = data.pop('location_id', None)
        if user_id:
            from core.models import User as CoreUser
            ra.user = CoreUser.objects.get(id=user_id)
        if location_id is not None:
            from location.models import Location
            ra.location = Location.objects.get(id=location_id) if location_id else None
        for k, v in data.items():
            setattr(ra, k, v)
        ra.save()

    class Input(UpdateRoleAssignmentInputType):
        pass


class DeleteRoleAssignmentInputType(OpenIMISMutation.Input):
    ids = graphene.List(graphene.UUID, required=True)


class DeleteRoleAssignmentMutation(BaseMutation):
    _mutation_class = "DeleteRoleAssignmentMutation"
    _mutation_module = MerankabandiConfig.name

    @classmethod
    def _validate_mutation(cls, user, **data):
        if isinstance(user, AnonymousUser) or not user.id:
            raise ValidationError(_("mutation.authentication_required"))

    @classmethod
    def _mutate(cls, user, **data):
        data.pop('client_mutation_id', None)
        data.pop('client_mutation_label', None)
        RoleAssignment.objects.filter(id__in=data.get('ids', [])).delete()

    class Input(DeleteRoleAssignmentInputType):
        pass
