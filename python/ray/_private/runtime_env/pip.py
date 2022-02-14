import os
import json
import logging
import hashlib
import shutil

from pathlib import Path
from typing import Optional, List, Dict

from ray._private.runtime_env.conda_utils import exec_cmd_stream_to_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import Protocol, parse_uri
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.utils import try_to_create_directory

default_logger = logging.getLogger(__name__)

RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP = "RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP"


def _get_pip_hash(pip_list: List[str]) -> str:
    serialized_pip_spec = json.dumps(pip_list, sort_keys=True)
    hash = hashlib.sha1(serialized_pip_spec.encode("utf-8")).hexdigest()
    return hash


def _install_pip_list_to_dir(
        pip_list: List[str],
        target_dir: str,
        logger: Optional[logging.Logger] = default_logger):
    try_to_create_directory(target_dir)
    try:
        pip_requirements_file = os.path.join(target_dir, "requirements.txt")
        with open(pip_requirements_file, "w") as file:
            for line in pip_list:
                file.write(line + "\n")
        exit_code, output = exec_cmd_stream_to_logger(
            [
                "pip", "install", f"--target={target_dir}", "-r",
                pip_requirements_file
            ],
            logger,
        )
        if exit_code != 0:
            shutil.rmtree(target_dir)
            raise RuntimeError(
                f"Failed to install pip requirements:\n{output}")
    finally:
        if os.path.exists(pip_requirements_file):
            os.remove(pip_requirements_file)


def get_uri(runtime_env: Dict) -> Optional[str]:
    """Return `"pip://<hashed_dependencies>"`, or None if no GC required."""
    pip = runtime_env.get("pip")
    if pip is not None:
        if isinstance(pip, list):
            uri = "pip://" + _get_pip_hash(pip_list=pip)
        else:
            raise TypeError("pip field received by RuntimeEnvAgent must be "
                            f"list, not {type(pip).__name__}.")
    else:
        uri = None
    return uri


class PipManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "pip")
        try_to_create_directory(self._resources_dir)

    def _get_path_from_hash(self, hash: str) -> str:
        """Generate a path from the hash of a pip spec.

        Example output:
            /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
                /pip/ray-9a7972c3a75f55e976e620484f58410c920db091
        """
        return os.path.join(self._resources_dir, hash)

    def delete_uri(self,
                   uri: str,
                   logger: Optional[logging.Logger] = default_logger) -> bool:
        logger.info(f"Got request to delete URI {uri}")
        protocol, hash = parse_uri(uri)
        if protocol != Protocol.PIP:
            raise ValueError("PipManager can only delete URIs with protocol "
                             f"pip. Received protocol {protocol}, URI {uri}")

        pip_env_path = self._get_path_from_hash(hash)
        try:
            shutil.rmtree(pip_env_path)
            successful = True
        except OSError:
            successful = False
            logger.warning(f"Error when deleting pip env {pip_env_path}.")
        return successful

    def setup(self,
              runtime_env: RuntimeEnv,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):
        if not runtime_env.has_pip():
            return

        logger.debug(f"Setting up pip for runtime_env: {runtime_env}")
        pip_packages: List[str] = runtime_env.pip_packages()
        target_dir = self._get_path_from_hash(_get_pip_hash(pip_packages))

        _install_pip_list_to_dir(pip_packages, target_dir, logger=logger)

        # Despite Ray being removed from the input pip list during validation,
        # other packages in the pip list (for example, xgboost_ray) may
        # themselves include Ray as a dependency.  In this case, we will have
        # inadvertently installed the latest Ray version in the target_dir,
        # which may cause Ray version mismatch issues. Uninstall it here, if it
        # exists, to make the workers use the Ray that is already
        # installed in the cluster.
        #
        # In the case where the user explicitly wants to include Ray in their
        # pip list (and signals this by setting the environment variable below)
        # then we don't want this deletion logic, so we skip it.
        if os.environ.get(RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP) != 1:
            ray_path = Path(target_dir) / "ray"
            if ray_path.exists() and ray_path.is_dir():
                shutil.rmtree(ray_path)

        # Insert the target directory into the PYTHONPATH.
        python_path = target_dir
        if "PYTHONPATH" in context.env_vars:
            python_path += os.pathsep + context.env_vars["PYTHONPATH"]
        context.env_vars["PYTHONPATH"] = python_path
