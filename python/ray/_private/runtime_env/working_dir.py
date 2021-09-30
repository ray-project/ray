import logging
import os
from typing import Optional

from ray.experimental.internal_kv import _internal_kv_initialized
from ray._private.runtime_env import RuntimeEnvContext
from ray._private.packaging import download_and_unpack_package, delete_package

default_logger = logging.getLogger(__name__)


class WorkingDirManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = resources_dir
        assert _internal_kv_initialized()

    def delete_uri(self,
                   uri: str,
                   logger: Optional[logging.Logger] = default_logger) -> bool:

        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")

        return deleted

    def setup(self,
              runtime_env: dict,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):
        if not runtime_env.get("uris"):
            return

        working_dir = download_and_unpack_package(
            runtime_env["uris"][0], self._resources_dir, logger=logger)
        context.command_prefix += [f"cd {working_dir}"]

        # Insert the working_dir as the first entry in PYTHONPATH. This is
        # compatible with users providing their own PYTHONPATH in env_vars.
        python_path = working_dir
        if "PYTHONPATH" in context.env_vars:
            python_path += os.pathsep + context.env_vars["PYTHONPATH"]
        context.env_vars["PYTHONPATH"] = python_path
