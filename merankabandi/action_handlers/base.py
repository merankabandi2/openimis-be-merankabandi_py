class BaseActionHandler:
    def validate(self, task, ticket):
        pass

    def execute(self, task, ticket, user, data=None):
        raise NotImplementedError

    def is_automated(self):
        return False

    def get_required_fields(self):
        return []
