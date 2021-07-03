import json


class RuntimeEnvContext:
    """A context used to describe the created runtime env."""

    def __init__(self, conda_env_name=None):
        self.conda_env_name = conda_env_name

    def serialize(self) -> str:
        # serialize the context to json string.
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))
