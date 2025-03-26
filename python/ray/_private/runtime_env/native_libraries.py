import logging
import os
from typing import Dict, List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
    is_zip_uri,
    is_tar_uri,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)


class NativeLibrariesPlugin(RuntimeEnvPlugin):

    name = "native_libraries"

    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "native_libraries_files")
        try_to_create_directory(self._resources_dir)

    def _get_local_dir_from_uri(self, uri: str):
        return get_local_dir_from_uri(uri, self._resources_dir)

    async def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        deleted = await delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    def get_uris(self, runtime_env: dict) -> List[str]:
        libraries = []
        for lib_info in runtime_env.get("native_libraries", []):
            if isinstance(lib_info, str):
                libraries.append(lib_info)
            else:
                libraries.append(lib_info["url"])
        return libraries

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        if not uri:
            return 0
        # NOTE: (Jacky) if uri endwith zip or tar, we download and unpack uri file to local dir.
        # Instead, we do not decompress, but directly move the file to local_dir
        local_dir = await download_and_unpack_package(
            uri, self._resources_dir, None, logger=logger
        )

        return get_directory_size_bytes(local_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        def _check_library_dir(library_dir, uri):
            """Check local library_dir if exist"""
            if not library_dir.exists():
                raise ValueError(
                    f"Local directory {library_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading, unpacking or installing the native library files."
                )

        for lib_info in runtime_env_dict.get("native_libraries", []):
            uri = str(lib_info)
            if isinstance(lib_info, str):
                library_dir = self._get_local_dir_from_uri(lib_info)
                _check_library_dir(library_dir, uri)
                context.native_libraries["lib_path"].append(str(library_dir))
                context.native_libraries["code_search_path"].append(str(library_dir))
            else:
                library_dir = self._get_local_dir_from_uri(lib_info["url"])
                for path in lib_info.get("lib_path", ["./"]):
                    if isinstance(path, dict):
                        rel_lib_path = path.get("path", "./")
                        abs_lib_path = library_dir / rel_lib_path
                        _check_library_dir(abs_lib_path, uri)
                        env_var_name = path.get("env_var_name", "LD_LIBRARY_PATH")
                        if env_var_name == "LD_LIBRARY_PATH":
                            context.native_libraries["lib_path"].append(
                                str(abs_lib_path)
                            )
                        elif env_var_name == "LD_PRELOAD":
                            for lib_path in abs_lib_path.glob("*.so*"):
                                context.preload_libraries.append(str(lib_path))
                    elif isinstance(path, str):
                        rel_lib_path = path if path else "./"
                        abs_lib_path = library_dir / rel_lib_path
                        _check_library_dir(abs_lib_path, uri)
                        context.native_libraries["lib_path"].append(str(abs_lib_path))

                for rel_code_search_path in lib_info.get("code_search_path", ["./"]):
                    abs_code_search_path = library_dir / rel_code_search_path
                    _check_library_dir(abs_code_search_path, uri)
                    context.native_libraries["code_search_path"].append(
                        str(abs_code_search_path)
                    )
