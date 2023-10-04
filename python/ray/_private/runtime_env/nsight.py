import os
import logging
import subprocess
from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.packaging import (
    delete_package,
    get_local_dir_from_uri,
)
from ray._private.utils import (
    get_directory_size_bytes,
    try_to_create_directory,
)

default_logger = logging.getLogger(__name__)


def check_if_nsys_installed():
    try:
        result = subprocess.run(
            ["nsys", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


class NsightPlugin(RuntimeEnvPlugin):
    name = "nsight"

    def __init__(self, resources_dir: str):
        self.nsys_cmd = []
        self._resources_dir = os.path.join(resources_dir, "nsight")
        try_to_create_directory(self._resources_dir)

        # replace this with better way to get logs dir
        session_dir, runtime_dir = os.path.split(resources_dir)
        self._logs_dir = os.path.join(session_dir + "/logs/nsight")
        try_to_create_directory(self._logs_dir)

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info("Got request to delete nsight URI %s", uri)
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        # move package to head node
        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        nsight_flags = runtime_env.get("nsight")
        if not nsight_flags:
            return 0
        if not check_if_nsys_installed():
            logger.warning(
                "Nsys is not installed, running worker process without `nsys profile`"
            )
            return 0

        self.nsys_cmd = [
            "nsys",
            "profile",
            "--sample=cpu",
            "--cudabacktrace=true",
            "--stop-on-exit=true",
            "-o",
            f"{self._logs_dir}/worker_process_%p",
        ]
        self.nsys_cmd += ["-t " + ",".join(nsight_flags)]
        return 0

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        logger.info("Running nsys profiler")
        context.py_executable = " ".join(self.nsys_cmd) + " python"
