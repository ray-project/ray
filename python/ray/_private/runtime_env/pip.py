import os
import json
import logging
import hashlib
import shutil

from pathlib import Path
from typing import Optional, List, Dict
from filelock import FileLock

from ray._private.runtime_env.conda_utils import exec_cmd_stream_to_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    Protocol,
    get_local_dir_from_uri,
    parse_uri,
)
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)

RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP = "RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP"


def _get_pip_hash(pip_list: List[str]) -> str:
    serialized_pip_spec = json.dumps(pip_list, sort_keys=True)
    hash = hashlib.sha1(serialized_pip_spec.encode("utf-8")).hexdigest()
    return hash


def _install_pip_list_to_dir(
    pip_list: List[str],
    target_dir: str,
    logger: Optional[logging.Logger] = default_logger,
):
    try_to_create_directory(target_dir)
    exit_code, output = exec_cmd_stream_to_logger(
        ["pip", "install", f"--target={target_dir}"] + pip_list, logger
    )
    if exit_code != 0:
        shutil.rmtree(target_dir)
        raise RuntimeError(f"Failed to install pip requirements:\n{output}")


def get_uri(runtime_env: Dict) -> Optional[str]:
    """Return `"pip://<hashed_dependencies>"`, or None if no GC required."""
    pip = runtime_env.get("pip")
    if pip is not None:
        if isinstance(pip, list):
            uri = "pip://" + _get_pip_hash(pip_list=pip)
        else:
            raise TypeError(
                "pip field received by RuntimeEnvAgent must be "
                f"list, not {type(pip).__name__}."
            )
    else:
        uri = None
    return uri


class PipManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "pip")
        try_to_create_directory(self._resources_dir)

        # Concurrent pip installs are unsafe.  This lock prevents concurrent
        # installs (and deletions).
        self._installs_and_deletions_file_lock = os.path.join(
            self._resources_dir, "ray-pip-installs-and-deletions.lock"
        )

    def _get_path_from_hash(self, hash: str) -> str:
        """Generate a path from the hash of a pip spec.

        Example output:
            /tmp/ray/session_2021-11-03_16-33-59_356303_41018/runtime_resources
                /pip/ray-9a7972c3a75f55e976e620484f58410c920db091
        """
        return os.path.join(self._resources_dir, hash)

    def get_uri(self, runtime_env: RuntimeEnv) -> Optional[str]:
        """Return the pip URI from the RuntimeEnv if it exists, else None."""
        pip_uri = runtime_env.pip_uri()
        if pip_uri != "":
            return pip_uri
        return None

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info(f"Got request to delete pip URI {uri}")
        protocol, hash = parse_uri(uri)
        if protocol != Protocol.PIP:
            raise ValueError(
                "PipManager can only delete URIs with protocol "
                f"pip. Received protocol {protocol}, URI {uri}"
            )

        pip_env_path = self._get_path_from_hash(hash)
        local_dir_size = get_directory_size_bytes(pip_env_path)
        try:
            with FileLock(self._installs_and_deletions_file_lock):
                shutil.rmtree(pip_env_path)
        except OSError as e:
            logger.warning(f"Error when deleting pip env {pip_env_path}: {str(e)}")
            return 0

        return local_dir_size

    def create(
        self,
        uri: str,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        logger.debug("Setting up pip for runtime_env: " f"{runtime_env.serialize()}")
        protocol, hash = parse_uri(uri)
        target_dir = self._get_path_from_hash(hash)

        pip_packages: List[str] = runtime_env.pip_packages()
        with FileLock(self._installs_and_deletions_file_lock):
            _install_pip_list_to_dir(pip_packages, target_dir, logger=logger)

            # Despite Ray being removed from the input pip list during
            # validation, other packages in the pip list (for example,
            # xgboost_ray) may themselves include Ray as a dependency.  In this
            # case, we will have inadvertently installed the latest Ray version
            # in the target_dir, which may cause Ray version mismatch issues.
            # Uninstall it here, if it exists, to make the workers use the Ray
            # that is already installed in the cluster.
            #
            # In the case where the user explicitly wants to include Ray in
            # their pip list (and signals this by setting the environment
            # variable below) then we don't want this deletion logic, so we
            # skip it.
            if os.environ.get(RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP) != 1:
                ray_path = Path(target_dir) / "ray"
                if ray_path.exists() and ray_path.is_dir():
                    shutil.rmtree(ray_path)
        return get_directory_size_bytes(target_dir)

    def modify_context(
        self,
        uri: str,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.has_pip():
            return
        # Insert the target directory into the PYTHONPATH.
        protocol, hash = parse_uri(uri)
        target_dir = get_local_dir_from_uri(uri, self._resources_dir)
        if not target_dir.exists():
            raise ValueError(
                f"Local directory {target_dir} for URI {uri} does "
                "not exist on the cluster. Something may have gone wrong while "
                "installing the runtime_env `pip` packages."
            )
        python_path = str(target_dir)
        if "PYTHONPATH" in context.env_vars:
            python_path += os.pathsep + context.env_vars["PYTHONPATH"]
        context.env_vars["PYTHONPATH"] = python_path
