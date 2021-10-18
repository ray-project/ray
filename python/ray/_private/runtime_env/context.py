import json
import logging
import os
import sys
from typing import Dict, List, Optional

from ray.util.annotations import DeveloperAPI
from ray.core.generated.common_pb2 import Language

logger = logging.getLogger(__name__)


@DeveloperAPI
class RuntimeEnvContext:
    """A context used to describe the created runtime env."""

    def __init__(self,
                 command_prefix: List[str] = None,
                 env_vars: Dict[str, str] = None,
                 py_executable: Optional[str] = None,
                 resources_dir: Optional[str] = None):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
        # TODO(edoakes): this should not be in the context but just passed to
        # the per-resource manager constructor. However, it's currently used in
        # the legacy Ray client codepath to pass the resources dir to the shim
        # process. We should remove it once Ray client uses the agent.
        self.resources_dir: str = resources_dir

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))

    def exec_worker(self, passthrough_args: List[str], language: Language):
        os.environ.update(self.env_vars)

        if language == Language.PYTHON and sys.platform == "win32":
            executable = f'"{self.py_executable}"'  # Path may contain spaces
        elif language == Language.PYTHON:
            executable = f"exec {self.py_executable}"
        elif sys.platform == "win32":
            executable = ""
        else:
            executable = "exec "

        exec_command = " ".join([f"{executable}"] + passthrough_args)
        command_str = " && ".join(self.command_prefix + [exec_command])

        if sys.platform == "win32":
            os.system(command_str)
        else:
            os.execvp(file="bash", args=["bash", "-c", command_str])

        logger.info(f"Exec'ing worker with command: {command_str}")
