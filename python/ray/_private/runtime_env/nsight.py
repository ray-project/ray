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
        result = subprocess.run(["nsys", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

class NsightPlugin(RuntimeEnvPlugin):
    name = "nsight"

    def __init__(self, resource_dir: str):
        self.nsys_cmd = []
        self._resources_dir = os.path.join(resource_dir, "nsight")
        try_to_create_directory(self._resources_dir)
    
    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info(f"Got request to delete URI {uri}")
        protocol, hash = parse_uri(uri)
        if protocol != Protocol.CONDA:
            raise ValueError(
                "CondaPlugin can only delete URIs with protocol "
                f"conda.  Received protocol {protocol}, URI {uri}"
            )

        conda_env_path = self._get_path_from_hash(hash)
        local_dir_size = get_directory_size_bytes(conda_env_path)

        with FileLock(self._installs_and_deletions_file_lock):
            successful = delete_conda_env(prefix=conda_env_path, logger=logger)
        if not successful:
            logger.warning(f"Error when deleting nsight env {conda_env_path}. ")
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
            logger.warning("Nsys is not installed, running worker process without `nsys profile`")
            return 0

        self.nsys_cmd = ["nsys", "profile", "--sample=cpu", "--cudabacktrace=true", \
        "--stop-on-exit=true", 	"-o", f"{self._resources_dir}/worker_process_%p"]
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
