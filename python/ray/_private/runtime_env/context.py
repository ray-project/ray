import json
import logging
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional

from ray.util.annotations import DeveloperAPI
from ray.core.generated.common_pb2 import Language
from ray._private.services import get_ray_jars_dir, get_ray_native_library_dir

logger = logging.getLogger(__name__)

_LINUX = sys.platform.startswith("linux")
_MACOS = sys.platform.startswith("darwin")
if _LINUX:
    _LIBRARY_PATH_ENV_NAME = "LD_LIBRARY_PATH"
elif _MACOS:
    _LIBRARY_PATH_ENV_NAME = "DYLD_LIBRARY_PATH"
else:
    # Win32
    _LIBRARY_PATH_ENV_NAME = "PATH"


@DeveloperAPI
class RuntimeEnvContext:
    """A context used to describe the created runtime env."""

    def __init__(
        self,
        command_prefix: List[str] = None,
        env_vars: Dict[str, str] = None,
        py_executable: Optional[str] = None,
        resources_dir: Optional[str] = None,
        container: Dict[str, Any] = None,
        java_jars: List[str] = None,
        py_modules: List[str] = None,
        native_libraries: List[str] = None,
    ):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
        # TODO(edoakes): this should not be in the context but just passed to
        # the per-resource manager constructor. However, it's currently used in
        # the legacy Ray client codepath to pass the resources dir to the shim
        # process. We should remove it once Ray client uses the agent.
        self.resources_dir: str = resources_dir
        self.container = container or {}
        self.java_jars = java_jars or []
        self.py_modules = py_modules or []
        self.native_libraries = native_libraries or []

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))

    def exec_worker(self, passthrough_args: List[str], language: Language):
        os.environ.update(self.env_vars)

        if language == Language.PYTHON:
            if sys.platform == "win32":
                executable = self.py_executable
            else:
                executable = f"exec {self.py_executable}"

            if self.py_modules:
                passthrough_args += [
                    "--code-search-path",
                    "'" + ":".join(self.py_modules) + "'",
                ]

        elif language == Language.JAVA:
            executable = "java"
            ray_jars = os.path.join(get_ray_jars_dir(), "*")

            local_java_jars = []
            for java_jar in self.java_jars:
                local_java_jars.append(f"{java_jar}/*")
                local_java_jars.append(java_jar)

            class_path_args = [
                "-cp",
                # TODO(SongGuyang): Remove java jars from class path. We should
                # use code-search-path.
                ray_jars + ":" + str(":".join(local_java_jars)),
                # "-Dray.job.code-search-path=" + str(":".join(local_java_jars)),
            ]
            passthrough_args = class_path_args + passthrough_args
        elif language == Language.CPP:
            executable = "exec "
            all_library_paths = get_ray_native_library_dir()
            if self.native_libraries:
                passthrough_args += [
                    "--ray_code_search_path",
                    ":".join(self.native_libraries),
                ]
                all_library_paths += ":"
                all_library_paths += ":".join(self.native_libraries)
            os_paths = os.environ.get(_LIBRARY_PATH_ENV_NAME, None)
            if os_paths:
                all_library_paths += ":"
                all_library_paths += os_paths
            os.environ[_LIBRARY_PATH_ENV_NAME] = all_library_paths
        elif sys.platform == "win32":
            executable = ""
        else:
            executable = "exec "

        exec_command = " ".join([f"{executable}"] + passthrough_args)
        command_str = " && ".join(self.command_prefix + [exec_command])
        # TODO(SongGuyang): We add this env to command for macOS because it doesn't
        # work for the C++ process of `os.execvp`. We should find a better way to
        # fix it.
        MACOS_LIBRARY_PATH_ENV_NAME = "DYLD_LIBRARY_PATH"
        if MACOS_LIBRARY_PATH_ENV_NAME in os.environ:
            command_str = (
                MACOS_LIBRARY_PATH_ENV_NAME
                + "="
                + os.environ.get(MACOS_LIBRARY_PATH_ENV_NAME)
                + " "
                + command_str
            )
        logger.debug(f"Exec'ing worker with command: {command_str}")
        if sys.platform == "win32":
            subprocess.run([executable, *passthrough_args])
        else:
            # PyCharm will monkey patch the os.execvp at
            # .pycharm_helpers/pydev/_pydev_bundle/pydev_monkey.py
            # The monkey patched os.execvp function has a different
            # signature. So, we use os.execvp("executable", args=[])
            # instead of os.execvp(file="executable", args=[])
            os.execvp("bash", args=["bash", "-c", command_str])
