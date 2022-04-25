import logging
import os
from types import ModuleType
from typing import Any, Dict, List, Optional
from pathlib import Path
import asyncio

from ray.experimental.internal_kv import _internal_kv_initialized
from ray._private.runtime_env.conda_utils import exec_cmd_stream_to_logger
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    download_and_unpack_package,
    delete_package,
    get_local_dir_from_uri,
    get_uri_for_directory,
    get_uri_for_package,
    package_exists,
    parse_uri,
    is_jar_uri,
    Protocol,
    upload_package_if_needed,
    upload_package_to_gcs,
)
from ray._private.utils import get_directory_size_bytes
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


class JavaJarsManager:
    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "java_jars_files")
        try_to_create_directory(self._resources_dir)
        assert _internal_kv_initialized()

    def _get_local_dir_from_uri(self, uri: str):
        return get_local_dir_from_uri(uri, self._resources_dir)

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
        return runtime_env.java_jars()

    def _download_jars(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ):
        """Download a jar URI."""
        jar_file = download_and_unpack_package(
            uri, self._resources_dir, logger=logger
        )
        module_dir = self._get_local_dir_from_uri(uri)
        return module_dir

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        def _create():
            if is_jar_uri(uri):
                module_dir = self._download_jars(uri=uri, logger=logger)
            else:
                module_dir = download_and_unpack_package(
                    uri, self._resources_dir, logger=logger
                )

            return get_directory_size_bytes(module_dir)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _create)

    def modify_context(
        self,
        uris: Optional[List[str]],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if uris is None:
            return
        for uri in uris:
            module_dir = self._get_local_dir_from_uri(uri)
            if not module_dir.exists():
                raise ValueError(
                    f"Local directory {module_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading, unpacking or installing the java jar files."
                )
            context.java_jars.append(str(module_dir))
