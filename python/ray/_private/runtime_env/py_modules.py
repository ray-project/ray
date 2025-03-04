import logging
import os
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    Protocol,
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
    get_uri_for_file,
    get_uri_for_directory,
    get_uri_for_package,
    install_wheel_package,
    is_whl_uri,
    package_exists,
    parse_uri,
    upload_package_if_needed,
    upload_package_to_gcs,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.working_dir import set_pythonpath_in_context
from ray._private.utils import get_directory_size_bytes, try_to_create_directory
from ray.exceptions import RuntimeEnvSetupError

default_logger = logging.getLogger(__name__)


def _check_is_uri(s: str) -> bool:
    try:
        protocol, path = parse_uri(s)
    except ValueError:
        protocol, path = None, None

    if (
        protocol in Protocol.remote_protocols()
        and not path.endswith(".zip")
        and not path.endswith(".whl")
    ):
        raise ValueError("Only .zip or .whl files supported for remote URIs.")

    return protocol is not None


def upload_py_modules_if_needed(
    runtime_env: Dict[str, Any],
    scratch_dir: Optional[str] = os.getcwd(),
    logger: Optional[logging.Logger] = default_logger,
    upload_fn=None,
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
            if not hasattr(module, "__path__"):
                # This is a single-file module.
                module_path = module.__file__
            else:
                # NOTE(edoakes): Python allows some installed Python packages to
                # be split into multiple directories. We could probably handle
                # this, but it seems tricky & uncommon. If it's a problem for
                # users, we can add this support on demand.
                if len(module.__path__) > 1:
                    raise ValueError(
                        "py_modules only supports modules whose __path__"
                        " has length 1 or those who are single-file."
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
            if Path(module_path).is_dir() or Path(module_path).suffix == ".py":
                is_dir = Path(module_path).is_dir()
                excludes = runtime_env.get("excludes", None)
                if is_dir:
                    module_uri = get_uri_for_directory(module_path, excludes=excludes)
                else:
                    module_uri = get_uri_for_file(module_path)
                if upload_fn is None:
                    try:
                        upload_package_if_needed(
                            module_uri,
                            scratch_dir,
                            module_path,
                            excludes=excludes,
                            include_parent_dir=is_dir,
                            logger=logger,
                        )
                    except Exception as e:
                        from ray.util.spark.utils import is_in_databricks_runtime

                        if is_in_databricks_runtime():
                            raise RuntimeEnvSetupError(
                                f"Failed to upload module {module_path} to the Ray "
                                f"cluster, please ensure there are only files under "
                                f"the module path, notebooks under the path are "
                                f"not allowed, original exception: {e}"
                            ) from e
                        raise RuntimeEnvSetupError(
                            f"Failed to upload module {module_path} to the Ray "
                            f"cluster: {e}"
                        ) from e
                else:
                    upload_fn(module_path, excludes=excludes)
            elif Path(module_path).suffix == ".whl":
                module_uri = get_uri_for_package(Path(module_path))
                if upload_fn is None:
                    if not package_exists(module_uri):
                        try:
                            upload_package_to_gcs(
                                module_uri, Path(module_path).read_bytes()
                            )
                        except Exception as e:
                            raise RuntimeEnvSetupError(
                                f"Failed to upload {module_path} to the Ray "
                                f"cluster: {e}"
                            ) from e
                else:
                    upload_fn(module_path, excludes=None, is_file=True)
            else:
                raise ValueError(
                    "py_modules entry must be a .py file, "
                    "a directory, or a .whl file; "
                    f"got {module_path}"
                )

        py_modules_uris.append(module_uri)

    # TODO(architkulkarni): Expose a single URI for py_modules.  This plugin
    # should internally handle the "sub-URIs", the individual modules.

    runtime_env["py_modules"] = py_modules_uris
    return runtime_env


class PyModulesPlugin(RuntimeEnvPlugin):

    name = "py_modules"

    def __init__(
        self, resources_dir: str, gcs_aio_client: "GcsAioClient"  # noqa: F821
    ):
        self._resources_dir = os.path.join(resources_dir, "py_modules_files")
        self._gcs_aio_client = gcs_aio_client
        try_to_create_directory(self._resources_dir)

    def _get_local_dir_from_uri(self, uri: str):
        return get_local_dir_from_uri(uri, self._resources_dir)

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info("Got request to delete pymodule URI %s", uri)
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    def get_uris(self, runtime_env) -> List[str]:
        return runtime_env.py_modules()

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:

        module_dir = await download_and_unpack_package(
            uri, self._resources_dir, self._gcs_aio_client, logger=logger
        )

        if is_whl_uri(uri):
            wheel_uri = module_dir
            module_dir = self._get_local_dir_from_uri(uri)
            await install_wheel_package(
                wheel_uri=wheel_uri, target_dir=module_dir, logger=logger
            )

        return get_directory_size_bytes(module_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        module_dirs = []
        for uri in uris:
            module_dir = self._get_local_dir_from_uri(uri)
            if not module_dir.exists():
                raise ValueError(
                    f"Local directory {module_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading, unpacking or installing the py_modules files."
                )
            module_dirs.append(str(module_dir))
        set_pythonpath_in_context(os.pathsep.join(module_dirs), context)
