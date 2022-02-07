import logging
import os
from typing import Any, Dict, Optional
from pathlib import Path

from ray.experimental.internal_kv import _internal_kv_initialized
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    download_and_unpack_package,
    delete_package,
    get_local_dir_from_uri,
    get_uri_for_directory,
    get_uri_for_package,
    upload_package_to_gcs,
    parse_uri,
    Protocol,
    upload_package_if_needed,
)
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)


def upload_working_dir_if_needed(
    runtime_env: Dict[str, Any],
    scratch_dir: str,
    logger: Optional[logging.Logger] = default_logger,
) -> Dict[str, Any]:
    """Uploads the working_dir and replaces it with a URI.

    If the working_dir is already a URI, this is a no-op.
    """
    working_dir = runtime_env.get("working_dir")
    if working_dir is None:
        return runtime_env

    if not isinstance(working_dir, str) and not isinstance(working_dir, Path):
        raise TypeError(
            "working_dir must be a string or Path (either a local path "
            f"or remote URI), got {type(working_dir)}."
        )

    if isinstance(working_dir, Path):
        working_dir = str(working_dir)

    # working_dir is already a URI -- just pass it through.
    try:
        protocol, path = parse_uri(working_dir)
    except ValueError:
        protocol, path = None, None

    if protocol is not None:
        if protocol in Protocol.remote_protocols() and not path.endswith(".zip"):
            raise ValueError("Only .zip files supported for remote URIs.")
        return runtime_env

    excludes = runtime_env.get("excludes", None)
    try:
        working_dir_uri = get_uri_for_directory(working_dir, excludes=excludes)
    except ValueError:  # working_dir is not a directory
        package_path = Path(working_dir)
        if not package_path.exists() or package_path.suffix != ".zip":
            raise ValueError(
                f"directory {package_path} must be an existing "
                "directory or a zip package"
            )

        pkg_uri = get_uri_for_package(package_path)
        upload_package_to_gcs(pkg_uri, package_path.read_bytes())
        runtime_env["working_dir"] = pkg_uri
        return runtime_env

    upload_package_if_needed(
        working_dir_uri,
        scratch_dir,
        working_dir,
        include_parent_dir=False,
        excludes=excludes,
        logger=logger,
    )
    runtime_env["working_dir"] = working_dir_uri
    return runtime_env


class WorkingDirManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "working_dir_files")
        try_to_create_directory(self._resources_dir)
        assert _internal_kv_initialized()

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    def get_uri(self, runtime_env: RuntimeEnv) -> Optional[str]:
        working_dir_uri = runtime_env.working_dir()
        if working_dir_uri != "":
            return working_dir_uri
        return None

    def create(
        self,
        uri: str,
        runtime_env: dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        local_dir = download_and_unpack_package(uri, self._resources_dir, logger=logger)
        return get_directory_size_bytes(local_dir)

    def modify_context(
        self, uri: Optional[str], runtime_env_dict: Dict, context: RuntimeEnvContext
    ):
        if uri is None:
            return

        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        if not local_dir.exists():
            raise ValueError(
                f"Local directory {local_dir} for URI {uri} does "
                "not exist on the cluster. Something may have gone wrong while "
                "downloading or unpacking the working_dir."
            )

        context.command_prefix += [f"cd {local_dir}"]

        # Insert the working_dir as the first entry in PYTHONPATH. This is
        # compatible with users providing their own PYTHONPATH in env_vars.
        python_path = str(local_dir)
        if "PYTHONPATH" in context.env_vars:
            python_path += os.pathsep + context.env_vars["PYTHONPATH"]
        context.env_vars["PYTHONPATH"] = python_path
