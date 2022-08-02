import logging
import os
from typing import Dict, List, Optional

from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    delete_package,
    download_and_unpack_package,
    get_local_path_from_uri,
    parse_uri,
    UriType,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)


class NativeLibrariesPlugin(RuntimeEnvPlugin):

    name = "native_libraries"

    def __init__(self, resources_dir: str, gcs_aio_client: GcsAioClient):
        self._resources_dir = os.path.join(resources_dir, "native_libraries_files")
        self._gcs_aio_client = gcs_aio_client
        try_to_create_directory(self._resources_dir)

    def _get_local_dir_from_uri(self, uri: str):
        return get_local_path_from_uri(uri, self._resources_dir)

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        parsed_uri = parse_uri(uri)
        if parsed_uri.uri_type is not UriType.REMOTE:
            return 0
        local_dir = get_local_path_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    def get_uris(self, runtime_env: dict) -> List[str]:
        return runtime_env.get("native_libraries", [])

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        if not uri:
            return 0
        parsed_uri = parse_uri(uri)
        if parsed_uri.uri_type is not UriType.REMOTE:
            return 0
        library_dir = await download_and_unpack_package(
            uri, self._resources_dir, self._gcs_aio_client, logger=logger
        )

        return get_directory_size_bytes(library_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        for uri in uris:
            library_dir = self._get_local_dir_from_uri(uri)
            if not library_dir.exists():
                raise ValueError(
                    f"Local directory {library_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading, unpacking or installing the native library files."
                )
            context.native_libraries.append(str(library_dir))
