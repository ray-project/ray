import logging
import os
from types import ModuleType
from typing import Any, Dict, List, Optional
from pathlib import Path

from ray.experimental.internal_kv import _internal_kv_initialized
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    download_and_unpack_package,
    delete_package,
    get_local_dir_from_uri,
    get_uri_for_directory,
    parse_uri,
    Protocol,
    upload_package_if_needed,
)
from ray._private.utils import get_directory_size_bytes
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.utils import try_to_create_directory

default_logger = logging.getLogger(__name__)


def _check_is_uri(s: str) -> bool:
    try:
        protocol, path = parse_uri(s)
    except ValueError:
        protocol, path = None, None

    if protocol in Protocol.remote_protocols() and not path.endswith(".zip"):
        raise ValueError("Only .zip files supported for remote URIs.")

    return protocol is not None


def upload_py_modules_if_needed(
    runtime_env: Dict[str, Any],
    scratch_dir: str,
    logger: Optional[logging.Logger] = default_logger,
) -> Dict[str, Any]:
    """Uploads the entries in py_modules and replaces them with a list of URIs.

    For each entry that is already a URI, this is a no-op.
    """
    py_modules = runtime_env.get("py_modules")
    if py_modules is None:
        return runtime_env

    if not isinstance(py_modules, list):
        raise TypeError(
            "py_modules must be a List of local paths, imported modules, or "
            f"URIs, got {type(py_modules)}."
        )

    py_modules_uris = []
    for module in py_modules:
        if isinstance(module, str):
            # module_path is a local path or a URI.
            module_path = module
        elif isinstance(module, Path):
            module_path = str(module)
        elif isinstance(module, ModuleType):
            # NOTE(edoakes): Python allows some installed Python packages to
            # be split into multiple directories. We could probably handle
            # this, but it seems tricky & uncommon. If it's a problem for
            # users, we can add this support on demand.
            if len(module.__path__) > 1:
                raise ValueError(
                    "py_modules only supports modules whose " "__path__ has length 1."
                )
            [module_path] = module.__path__
        else:
            raise TypeError(
                "py_modules must be a list of file paths, URIs, "
                f"or imported modules, got {type(module)}."
            )

        if _check_is_uri(module_path):
            module_uri = module_path
        else:
            # module_path is a local path.
            excludes = runtime_env.get("excludes", None)
            module_uri = get_uri_for_directory(module_path, excludes=excludes)
            upload_package_if_needed(
                module_uri,
                scratch_dir,
                module_path,
                excludes=excludes,
                include_parent_dir=True,
                logger=logger,
            )

        py_modules_uris.append(module_uri)

    # TODO(architkulkarni): Expose a single URI for py_modules.  This plugin
    # should internally handle the "sub-URIs", the individual modules.

    runtime_env["py_modules"] = py_modules_uris
    return runtime_env


class PyModulesManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "py_modules_files")
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

    def get_uris(self, runtime_env: dict) -> Optional[List[str]]:
        return runtime_env.py_modules()

    def create(
        self,
        uri: str,
        runtime_env: RuntimeEnv,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        module_dir = download_and_unpack_package(
            uri, self._resources_dir, logger=logger
        )
        return get_directory_size_bytes(module_dir)

    def modify_context(
        self,
        uris: Optional[List[str]],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if uris is None:
            return
        module_dirs = []
        for uri in uris:
            module_dir = get_local_dir_from_uri(uri, self._resources_dir)
            if not module_dir.exists():
                raise ValueError(
                    f"Local directory {module_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading or unpacking the py_modules files."
                )
            module_dirs.append(str(module_dir))
        # Insert the py_modules directories into the PYTHONPATH.
        python_path = os.pathsep.join(module_dirs)
        if "PYTHONPATH" in context.env_vars:
            python_path += os.pathsep + context.env_vars["PYTHONPATH"]
        context.env_vars["PYTHONPATH"] = python_path
