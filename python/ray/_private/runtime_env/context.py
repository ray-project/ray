import json
import logging
import os
import sys
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


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
        self.resources_dir: str = resources_dir

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))

    def exec_worker(self, passthrough_args: List[str]):
        os.environ.update(self.env_vars)
        exec_command = " ".join([f"exec {self.py_executable}"] +
                                passthrough_args)
        command_str = " && ".join(self.command_prefix + [exec_command])
        logger.info(f"Exec'ing worker with command: {command_str}")
        os.execvp("bash", ["bash", "-c", command_str])
