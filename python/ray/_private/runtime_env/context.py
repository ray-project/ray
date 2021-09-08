import json


class RuntimeEnvContext:
    """A context used to describe the created runtime env."""

    def __init__(self,
                 session_dir: str,
                 conda_env_name: str = None,
                 working_dir: str = None):
        self.conda_env_name: str = conda_env_name
        self.session_dir: str = session_dir
        self.working_dir: str = working_dir

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))
