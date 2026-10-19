import json
import logging
import os
import re
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

_VAR_REFERENCE = re.compile(r"\$(?:([A-Za-z_]\w*)|\{([^}]*)\})", re.ASCII)
_VAR_NAME = re.compile(r"[A-Za-z_]\w*", re.ASCII)
_ARG_SEPARATORS = " \t\n"


def split_run_option(run_option: str, env: Dict[str, str]) -> List[str]:
    """Splits a `container.run_options` item into podman arguments, treating
    quotes, backslashes, `$NAME`, `${NAME}` and a leading `~` like a POSIX
    shell does, with values taken from `env`.

    Unlike a shell, nothing is executed: `$(...)` and backticks stay literal.
    An expanded value is never split, so it can't add arguments. As in a
    shell, single quotes and a backslash keep `$` literal, and an unset
    variable expands to an empty string. Other shell expansions, such as
    `~user`, `$1` and `$'...'`, stay literal.

    Args:
        run_option: One `container.run_options` item.
        env: The variables `$NAME` and `${NAME}` expand from.

    Returns:
        The podman arguments.

    Raises:
        ValueError: On an unclosed quote, a trailing backslash, or a `${...}`
            that isn't a plain variable name.
    """
    run_option = _remove_line_continuations(run_option)
    args: List[str] = []
    current: List[str] = []
    in_arg = False
    at_word_start = True
    quote = None
    i = 0
    while i < len(run_option):
        char = run_option[i]
        starts_word, at_word_start = at_word_start, False
        if quote == "'":
            if char == "'":
                quote = None
            else:
                current.append(char)
        elif char == "\\":
            i += 1
            if i == len(run_option):
                raise ValueError(f"Trailing backslash in run_option {run_option!r}.")
            escaped = run_option[i]
            # Inside double quotes, a backslash only escapes these characters.
            if quote == '"' and escaped not in '$`"\\':
                current.append(char)
            current.append(escaped)
            in_arg = True
        elif run_option.startswith("${", i) and not _VAR_REFERENCE.match(run_option, i):
            raise ValueError(f"Unclosed '${{' in run_option {run_option!r}.")
        elif char == "$" and (match := _VAR_REFERENCE.match(run_option, i)):
            name = match.group(1) or match.group(2)
            if not _VAR_NAME.fullmatch(name):
                raise ValueError(
                    f"Unsupported expansion {match.group(0)!r} in run_option "
                    f"{run_option!r}: only $NAME and ${{NAME}} are expanded."
                )
            value = env.get(name, "")
            current.append(value)
            # An unquoted variable that expands to nothing adds no argument.
            in_arg = in_arg or bool(value) or quote is not None
            i = match.end()
            continue
        elif char in "'\"" and quote is None:
            quote = char
            in_arg = True
        elif char == '"' and quote == '"':
            quote = None
        elif (
            char == "~"
            and quote is None
            and starts_word
            and run_option[i + 1 : i + 2] in ("", "/", *_ARG_SEPARATORS)
            and "HOME" in env
        ):
            current.append(env["HOME"])
            in_arg = True
        elif char in _ARG_SEPARATORS and quote is None:
            at_word_start = True
            if in_arg:
                args.append("".join(current))
                current = []
                in_arg = False
        else:
            current.append(char)
            in_arg = True
        i += 1
    if quote is not None:
        raise ValueError(f"No closing quotation in run_option {run_option!r}.")
    if in_arg:
        args.append("".join(current))
    return args


def _remove_line_continuations(text: str) -> str:
    # A shell drops backslash-newline before reading words or names, except
    # inside single quotes or when the backslash is itself escaped.
    kept: List[str] = []
    quote = None
    i = 0
    while i < len(text):
        char = text[i]
        if quote == "'":
            quote = None if char == "'" else quote
        elif char == "\\":
            if text[i + 1 : i + 2] == "\n":
                i += 2
                continue
            kept.append(text[i : i + 2])
            i += 2
            continue
        elif char in "'\"":
            quote = None if quote == char else (quote or char)
        kept.append(char)
        i += 1
    return "".join(kept)


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
        # their values from that environment. Node secrets are dropped from it,
        # so run_options such as `--env-host` can't forward them either.
        launcher_env = {
            name: value
            for name, value in launcher_env.items()
            if name not in _NODE_SECRET_ENV_VARS
        }
        container_env: Dict[str, Optional[str]] = {
            name: None for name in launcher_env if name.startswith("RAY_")
        }
        # Raw values, as master passed them single-quoted: expanding ${VAR}
        # against os.environ would let a job copy node secrets.
        container_env.update(self.env_vars)
        container_env.update(self.container["env_vars"])

        cmd = list(self.container["command"])
        for name, value in container_env.items():
            cmd.extend(["--env", name if value is None else f"{name}={value}"])
        # After the --env flags, so a user's `--env NAME=VALUE` wins, as podman
        # applies the last value given for a name. Expanded from what a shell
        # running podman would have seen, minus the node secrets.
        run_option_env = {**launcher_env, **self.env_vars}
        for run_option in self.container["run_options"]:
            cmd.extend(split_run_option(run_option, run_option_env))
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
