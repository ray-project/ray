import logging

from ray._private.runtime_env_prototype.sdk.runtime_env import RuntimeEnvBase


default_logger = logging.getLogger(__name__)


class Pip(RuntimeEnvBase):
    def __init__(self, packages=None, pip_check=False):
        super().__init__()
        self.packages = packages
        self.pip_check = pip_check

    def to_jsonable_type(self):
        jsonable_data = dict()
        jsonable_data["packages"] = self.packages
        jsonable_data["pip_check"] = self.pip_check
        return jsonable_data

    @staticmethod
    def from_jsonable_type(jsonable_data) -> "Pip":
        if not isinstance(jsonable_data, dict):
            raise TypeError
        pip_runtime_env = Pip(
            jsonable_data.get("packages", None), jsonable_data.get("pip_check", False)
        )
        return pip_runtime_env
