import json
import logging
import os
import subprocess
import shlex
import sys
from typing import Dict, List, Optional

from ray.util.annotations import DeveloperAPI
from ray.core.generated.common_pb2 import Language
from ray._private.services import get_ray_jars_dir
from ray._private.utils import (
    update_envs,
    try_update_code_search_path,
    try_update_ld_preload,
    try_update_ld_library_path,
)

logger = logging.getLogger(__name__)


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
        working_dir: Optional[str] = None,
        symlink_paths_to_working_dir: List[str] = None,
        native_libraries: List[Dict[str, str]] = None,
        preload_libraries: List[str] = None,
    ):
        self.command_prefix = command_prefix or []
        self.env_vars = env_vars or {}
        self.py_executable = py_executable or sys.executable
        self.override_worker_entrypoint: Optional[str] = override_worker_entrypoint
        self.java_jars = java_jars or []
        self.working_dir = working_dir
        # `symlink_paths_to_working_dir` provides a way that we can link any content to
        # the working directory of the workers. Note that the dirs in this list
        # will not be linked directly. Instead, we will walk through each dir
        # and link each content to the working directory.
        # For example, if the tree of a dir named "resource_dir1" is:
        # resource_dir1
        # ├── file1.txt
        # └── txt_dir
        #     └── file2.txt
        # And the tree of another dir named "resource_dir2" is:
        # resource_dir2
        # └── file3.txt
        # If you append these two dirs into `symlink_paths_to_working_dir`:
        # self.symlink_paths_to_working_dir.append("/xxx/xxx/resource_dir1")
        # self.symlink_paths_to_working_dir.append("/xxx/xxx/resource_dir2")
        # The final working directory of the worker process will be:
        # /xxx/xxx/working_dirs/{worker_id}
        # ├── file1.txt
        # └── txt_dir
        #     └── file2.txt
        # └── file3.txt
        # Note that if there are conflict file or sub dir names in different
        # resource dirs, some contents will be covered and we don't guarantee it.
        self.symlink_paths_to_working_dir = symlink_paths_to_working_dir or []
        self.native_libraries = native_libraries or {
            "lib_path": [],
            "code_search_path": [],
        }
        self.preload_libraries = preload_libraries or []

    def serialize(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))

    def exec_worker(self, passthrough_args: List[str], language: Language):
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
            # try update code search path
            passthrough_args = try_update_code_search_path(
                passthrough_args, language, self.java_jars, self.native_libraries
            )
            passthrough_args = class_path_args + passthrough_args
        elif language == Language.CPP:
            executable = ["exec"]
            # try update code search path
            passthrough_args = try_update_code_search_path(
                passthrough_args, language, self.java_jars, self.native_libraries
            )

        elif sys.platform == "win32":
            executable = []
        else:
            executable = ["exec"]

        # try update ld_library path
        try_update_ld_library_path(language, self.native_libraries, self.working_dir)

        # try update ld_preload
        try_update_ld_preload(self.preload_libraries)

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
            logger.info(f"Exec'ing worker with command: {cmd}")
            # PyCharm will monkey patch the os.execvp at
            # .pycharm_helpers/pydev/_pydev_bundle/pydev_monkey.py
            # The monkey patched os.execvp function has a different
            # signature. So, we use os.execvp("executable", args=[])
            # instead of os.execvp(file="executable", args=[])
            os.execvp("bash", args=["bash", "-c", " ".join(cmd)])
