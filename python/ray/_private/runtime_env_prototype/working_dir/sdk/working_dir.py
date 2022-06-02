import logging

from ray._private.runtime_env_prototype.sdk.runtime_env import RuntimeEnvBase

default_logger = logging.getLogger(__name__)


class WorkingDir(RuntimeEnvBase):
    def __init__(self, working_dir: str):
        super().__init__()
        self.working_dir = working_dir

    def to_jsonable_type(self):
        return self.working_dir

    @staticmethod
    def from_jsonable_type(jsonable_data) -> "WorkingDir":
        if not isinstance(jsonable_data, str):
            raise TypeError
        working_dir = WorkingDir(jsonable_data)
        return working_dir
