import json
import logging
import os
import shlex
import subprocess
import sys
from typing import Any, Dict, List, Optional

from ray._private.runtime_env.protocol import RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR
from ray._private.services import get_ray_jars_dir
from ray._private.utils import update_envs
from ray.core.generated.common_pb2 import Language
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Node credentials that containerized workers must not inherit. The auth token
# reaches them as a read-only file mount instead.
_NODE_SECRET_ENV_VARS = {"RAY_AUTH_TOKEN", RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR}


@DeveloperAPI
class RuntimeEnvContext:
    """A context used to describe the created runtime env."""

    def __init__(
        self,
        command_prefix: List[str] = None,
        env_vars: Dict[str, str] = None,
        py_executable: Optional[str] = None,
        override_worker_entrypoint: Optional[str] = None,
        java_jars: List[str] = None,
        container: Optional[Dict[str, Any]] = None,
    ):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
        self.override_worker_entrypoint: Optional[str] = override_worker_entrypoint
        self.java_jars = java_jars or []
        self.container = container or {}

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))

    def exec_worker(self, passthrough_args: List[str], language: Language):
        launcher_env = os.environ.copy()
        update_envs(self.env_vars)

        if language == Language.PYTHON and sys.platform == "win32":
            executable = [self.py_executable]
        elif language == Language.PYTHON:
            executable = ["exec", self.py_executable]
        elif language == Language.JAVA:
            executable = ["java"]
            ray_jars = os.path.join(get_ray_jars_dir(), "*")

            local_java_jars = []
            for java_jar in self.java_jars:
                local_java_jars.append(f"{java_jar}/*")
                local_java_jars.append(java_jar)

            class_path_args = ["-cp", ray_jars + ":" + str(":".join(local_java_jars))]
            passthrough_args = class_path_args + passthrough_args
        elif sys.platform == "win32":
            executable = []
        else:
            executable = ["exec"]

        # By default, raylet uses the path to default_worker.py on host.
        # However, the path to default_worker.py inside the container
        # can be different. We need the user to specify the path to
        # default_worker.py inside the container.
        if self.override_worker_entrypoint:
            logger.debug(
                f"Changing the worker entrypoint from {passthrough_args[0]} to "
                f"{self.override_worker_entrypoint}."
            )
            passthrough_args[0] = self.override_worker_entrypoint

        if self.container and language == Language.PYTHON:
            self._exec_container_worker(launcher_env, passthrough_args)

        if sys.platform == "win32":

            def quote(s):
                s = s.replace("&", "%26")
                return s

            passthrough_args = [quote(s) for s in passthrough_args]

            cmd = [*self.command_prefix, *executable, *passthrough_args]
            logger.debug(f"Exec'ing worker with command: {cmd}")
            subprocess.Popen(cmd, shell=True).wait()
        else:
            # We use shlex to do the necessary shell escape
            # of special characters in passthrough_args.
            passthrough_args = [shlex.quote(s) for s in passthrough_args]
            cmd = [*self.command_prefix, *executable, *passthrough_args]
            # TODO(SongGuyang): We add this env to command for macOS because it doesn't
            # work for the C++ process of `os.execvp`. We should find a better way to
            # fix it.
            MACOS_LIBRARY_PATH_ENV_NAME = "DYLD_LIBRARY_PATH"
            if MACOS_LIBRARY_PATH_ENV_NAME in os.environ:
                cmd.insert(
                    0,
                    f"{MACOS_LIBRARY_PATH_ENV_NAME}="
                    f"{os.environ[MACOS_LIBRARY_PATH_ENV_NAME]}",
                )
            logger.debug(f"Exec'ing worker with command: {cmd}")
            # PyCharm will monkey patch the os.execvp at
            # .pycharm_helpers/pydev/_pydev_bundle/pydev_monkey.py
            # The monkey patched os.execvp function has a different
            # signature. So, we use os.execvp("executable", args=[])
            # instead of os.execvp(file="executable", args=[])
            os.execvp("bash", args=["bash", "-c", " ".join(cmd)])

    def _exec_container_worker(
        self, launcher_env: Dict[str, str], passthrough_args: List[str]
    ):
        # The job's env_vars go to the container only: podman runs with the
        # raylet's environment, so job values such as LD_PRELOAD can't affect
        # the host launcher. RAY_* vars are passed by name, so podman reads
        # their values from that environment.
        container_env: Dict[str, Optional[str]] = {
            name: None
            for name in launcher_env
            if name.startswith("RAY_") and name not in _NODE_SECRET_ENV_VARS
        }
        # Raw values: expanding ${VAR} against the host env would let a job
        # copy node secrets into its container.
        container_env.update(self.env_vars)
        container_env.update(self.container["env_vars"])

        cmd = list(self.container["command"])
        for name, value in container_env.items():
            cmd.extend(["--env", name if value is None else f"{name}={value}"])
        cmd.extend(
            [
                "--entrypoint",
                self.container["entrypoint"],
                self.container["image"],
                *passthrough_args,
            ]
        )
        logger.debug(f"Exec'ing worker in container image {self.container['image']}.")
        os.execvpe(cmd[0], cmd, launcher_env)
