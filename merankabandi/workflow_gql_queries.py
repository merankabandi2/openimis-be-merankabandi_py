import graphene
from graphene_django import DjangoObjectType
from core import ExtendedConnection
from merankabandi.workflow_models import (
    WorkflowTemplate, WorkflowStepTemplate,
    GrievanceWorkflow, GrievanceTask,
    ReplacementRequest, RoleAssignment,
)


class WorkflowTemplateGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')

    class Meta:
        model = WorkflowTemplate
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            'id': ['exact'],
            'name': ['exact', 'icontains'],
            'case_type': ['exact', 'icontains'],
            'is_active': ['exact'],
        }
        connection_class = ExtendedConnection


class WorkflowStepTemplateGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')

    class Meta:
        model = WorkflowStepTemplate
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            'id': ['exact'],
            'workflow_template__id': ['exact'],
            'role': ['exact'],
            'action_type': ['exact'],
        }
        connection_class = ExtendedConnection


class GrievanceWorkflowGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')
    template_name = graphene.String()
    template_label = graphene.String()

    def resolve_template_name(self, info):
        return self.template.name if self.template else None

    def resolve_template_label(self, info):
        return self.template.label if self.template else None

    class Meta:
        model = GrievanceWorkflow
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            'id': ['exact'],
            'ticket__id': ['exact'],
            'status': ['exact', 'in'],
        }
        connection_class = ExtendedConnection


class GrievanceTaskGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')
    step_name = graphene.String()
    step_label = graphene.String()
    action_type = graphene.String()
    is_automated = graphene.Boolean()
    required_fields = graphene.List(graphene.String)
    assigned_user_name = graphene.String()
    blocked_by_id = graphene.UUID()

    def resolve_step_name(self, info):
        return self.step_template.name if self.step_template else None

    def resolve_step_label(self, info):
        return self.step_template.label if self.step_template else None

    def resolve_action_type(self, info):
        return self.step_template.action_type if self.step_template else None

    def resolve_is_automated(self, info):
        from merankabandi.action_handlers import get_handler
        if self.step_template:
            handler = get_handler(self.step_template.action_type)
            return handler.is_automated()
        return False

    def resolve_required_fields(self, info):
        from merankabandi.action_handlers import get_handler
        if self.step_template:
            handler = get_handler(self.step_template.action_type)
            return handler.get_required_fields()
        return []

    def resolve_assigned_user_name(self, info):
        if self.assigned_user:
            return f"{self.assigned_user.other_names or ''} {self.assigned_user.last_name or ''}".strip()
        return None

    def resolve_blocked_by_id(self, info):
        return self.blocked_by_id

    class Meta:
        model = GrievanceTask
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            'id': ['exact'],
            'ticket__id': ['exact'],
            'workflow__id': ['exact'],
            'assigned_user__id': ['exact'],
            'assigned_role': ['exact'],
            'status': ['exact', 'in'],
        }
        connection_class = ExtendedConnection


class ReplacementRequestGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')

    class Meta:
        model = ReplacementRequest
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            'id': ['exact'],
            'ticket__id': ['exact'],
            'status': ['exact', 'in'],
            'replaced_social_id': ['exact', 'icontains'],
        }
        connection_class = ExtendedConnection


class RoleAssignmentGQLType(DjangoObjectType):
    uuid = graphene.String(source='id')
    user_name = graphene.String()

    def resolve_user_name(self, info):
        if self.user:
            return f"{self.user.other_names or ''} {self.user.last_name or ''}".strip()
        return None

    class Meta:
        model = RoleAssignment
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            'id': ['exact'],
            'role': ['exact'],
            'user__id': ['exact'],
            'location__id': ['exact'],
            'is_active': ['exact'],
        }
        connection_class = ExtendedConnection
