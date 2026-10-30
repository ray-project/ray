import json
import logging
import os
import re
import shlex
import subprocess
import sys
from typing import Any, Dict, List, NoReturn, Optional

from ray._private.services import get_ray_jars_dir
from ray._private.utils import update_envs
from ray.core.generated.common_pb2 import Language
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

_RUN_OPTION_REFERENCE = re.compile(
    r"\$(?:(\$)|([A-Za-z_][A-Za-z0-9_]*)|\{([A-Za-z_][A-Za-z0-9_]*)\}|(\{))"
)


def expand_run_option(run_option: str, env: Dict[str, str]) -> str:
    """Expands `$NAME` and `${NAME}` in a `container.run_options` item.

    Each item is passed to podman as exactly one argument, so it is never
    split and an expanded value can't add arguments. Nothing is executed:
    `$(...)` and backticks stay literal. `$$` gives a literal `$`, an unset
    name expands to an empty string, and any other `$` stays literal.

    Args:
        run_option: One `container.run_options` item.
        env: The variables `$NAME` and `${NAME}` expand from.

    Returns:
        The podman argument.

    Raises:
        ValueError: On a `${` that isn't followed by a plain name and `}`.
    """

    def expand(match: re.Match) -> str:
        literal_dollar, name, braced_name, bad_brace = match.groups()
        if bad_brace:
            raise ValueError(
                f"Unsupported expansion in run_option {run_option!r}: only "
                "$NAME and ${NAME} are expanded; use $$ for a literal $."
            )
        if literal_dollar:
            return "$"
        return env.get(name or braced_name, "")

    return _RUN_OPTION_REFERENCE.sub(expand, run_option)


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
        fork_server_socket_path: Optional[str] = None,
    ):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
        self.override_worker_entrypoint: Optional[str] = override_worker_entrypoint
        self.java_jars = java_jars or []
        self.container = container or {}
        self.fork_server_socket_path: Optional[str] = fork_server_socket_path

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
    ) -> NoReturn:
        # The job's env_vars go to the container only: podman runs with the
        # raylet's environment, so job values such as LD_PRELOAD can't affect
        # the host launcher. RAY_* vars are passed by name, so podman reads
        # their values from that environment and the --env flags carry none;
        # a run_options item that references one still puts its value on argv.
        container_env: Dict[str, Optional[str]] = {
            name: None for name in launcher_env if name.startswith("RAY_")
        }
        # Passed as written, without expanding ${VAR} against the host env.
        # TODO(ray-project/ray#67216): move job values off argv (--env-file).
        container_env.update(self.env_vars)
        container_env.update(self.container["env_vars"])

        cmd = list(self.container["command"])
        for name, value in container_env.items():
            cmd.extend(["--env", name if value is None else f"{name}={value}"])
        # After the --env flags, so a user's `--env NAME=VALUE` wins, as podman
        # applies the last value given for a name. Expanded from the raylet's
        # environment plus the values the container gets, with the job's
        # env_vars as written, without the ${VAR} expansion update_envs applies.
        run_option_env = {**launcher_env, **self.env_vars, **self.container["env_vars"]}
        for run_option in self.container["run_options"]:
            cmd.append(expand_run_option(run_option, run_option_env))
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
