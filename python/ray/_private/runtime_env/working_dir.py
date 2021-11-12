import logging
import os
from typing import Any, Dict, Optional

from ray.experimental.internal_kv import _internal_kv_initialized
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    download_and_unpack_package, delete_package, get_uri_for_directory,
    parse_uri, Protocol, upload_package_if_needed)

default_logger = logging.getLogger(__name__)


def upload_working_dir_if_needed(
        runtime_env: Dict[str, Any],
        scratch_dir: str,
        logger: Optional[logging.Logger] = default_logger) -> Dict[str, Any]:
    """Uploads the working_dir and replaces it with a URI.

    If the working_dir is already a URI, this is a no-op.
    """
    working_dir = runtime_env.get("working_dir")
    if working_dir is None:
        return runtime_env

    if not isinstance(working_dir, str):
        raise TypeError(
            "working_dir must be a string (either a local path or remote "
            f"URI), got {type(working_dir)}.")

    # working_dir is already a URI -- just pass it through.
    try:
        protocol, path = parse_uri(working_dir)
    except ValueError:
        protocol, path = None, None

    if protocol is not None:
        if protocol in Protocol.remote_protocols(
        ) and not path.endswith(".zip"):
            raise ValueError("Only .zip files supported for remote URIs.")
        return runtime_env

    excludes = runtime_env.get("excludes", None)
    working_dir_uri = get_uri_for_directory(working_dir, excludes=excludes)
    upload_package_if_needed(
        working_dir_uri,
        scratch_dir,
        working_dir,
        include_parent_dir=False,
        excludes=excludes,
        logger=logger)
    runtime_env["working_dir"] = working_dir_uri
    return runtime_env


class WorkingDirManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "working_dir_files")
        if not os.path.isdir(self._resources_dir):
            os.makedirs(self._resources_dir)
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
        if not runtime_env.get("working_dir"):
            return

        working_dir = download_and_unpack_package(
            runtime_env["working_dir"], self._resources_dir, logger=logger)
        context.command_prefix += [f"cd {working_dir}"]

        # Insert the working_dir as the first entry in PYTHONPATH. This is
        # compatible with users providing their own PYTHONPATH in env_vars.
        python_path = working_dir
        if "PYTHONPATH" in context.env_vars:
            python_path += os.pathsep + context.env_vars["PYTHONPATH"]
        context.env_vars["PYTHONPATH"] = python_path
