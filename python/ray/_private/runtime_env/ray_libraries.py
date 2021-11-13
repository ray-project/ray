import logging
import os
from shutil import rmtree
from types import ModuleType
from typing import Any, Dict, Optional

import ray
from ray.experimental.internal_kv import _internal_kv_initialized
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (download_and_unpack_package,
                                                get_uri_for_directory,
                                                upload_package_if_needed)

default_logger = logging.getLogger(__name__)


def upload_ray_libraries_if_needed(
        runtime_env: Dict[str, Any],
        scratch_dir: str,
        logger: Optional[logging.Logger] = default_logger) -> Dict[str, Any]:
    """Uploads the libraries and replaces them with a dict of name->URI."""
    libraries = runtime_env.get("ray_libraries")
    if libraries is None:
        return runtime_env

    if isinstance(libraries, (str, ModuleType)):
        libraries = [libraries]
    elif not isinstance(libraries, list):
        raise TypeError("ray_libraries must be a str, module, or list of strs "
                        "and modules.")

    [ray_path] = ray.__path__
    libraries_uris = {}
    for library in libraries:
        if isinstance(library, str):
            library_path = library
        elif isinstance(library, ModuleType):
            if len(library.__path__) > 1:
                raise ValueError("ray_libraries only supports modules whose "
                                 "__path__ has length 1.")
            [library_path] = library.__path__
        else:
            raise TypeError("ray_libraries must be a list of file paths or "
                            f"imported modules, got {type(library)}.")

        if not library_path.startswith("/"):
            relative_path = library_path
        elif not library_path.startswith(ray_path):
            raise ValueError("ray_libraries entries must be a sub-directory "
                             f"of Ray (should start with '{ray_path}'), got "
                             f"{library_path}.")
        else:
            relative_path = os.path.relpath(library_path, ray_path)

        full_path = os.path.join(ray_path, relative_path)
        library_uri = get_uri_for_directory(full_path)
        upload_package_if_needed(
            library_uri, scratch_dir, full_path, logger=logger)

        libraries_uris[relative_path] = library_uri

    runtime_env["ray_libraries"] = libraries_uris
    return runtime_env


class RayLibrariesManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir,
                                           "ray_libraries_files")
        os.makedirs(self._resources_dir)
        assert _internal_kv_initialized()

    def setup(self,
              runtime_env: dict,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):
        if not runtime_env.get("ray_libraries"):
            return

        [ray_path] = ray.__path__
        for relative_path, uri in runtime_env["ray_libraries"].items():
            target_path = os.path.join(ray_path, relative_path)
            logger.info(f"Unpacking URI {uri} to {target_path}.")
            library_dir = download_and_unpack_package(
                uri, self._resources_dir, logger=logger)
            if os.path.islink(target_path):
                os.unlink(target_path)
            else:
                rmtree(target_path)
            os.rename(library_dir, target_path)
