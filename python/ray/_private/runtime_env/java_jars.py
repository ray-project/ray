import logging
import os
from typing import Dict, List, Optional

from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
    is_jar_uri,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes, try_to_create_directory
from ray.exceptions import RuntimeEnvSetupError

default_logger = logging.getLogger(__name__)


class JavaJarsPlugin(RuntimeEnvPlugin):

    name = "java_jars"

    def __init__(self, resources_dir: str, gcs_aio_client: GcsAioClient):
        self._resources_dir = os.path.join(resources_dir, "java_jars_files")
        self._gcs_aio_client = gcs_aio_client
        try_to_create_directory(self._resources_dir)

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

    def get_uris(self, runtime_env: dict) -> List[str]:
        return runtime_env.java_jars()

    async def _download_jars(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ):
        """Download a jar URI."""
        try:
            jar_file = await download_and_unpack_package(
                uri, self._resources_dir, self._gcs_aio_client, logger=logger
            )
        except Exception as e:
            raise RuntimeEnvSetupError(
                "Failed to download jar file: {}".format(e)
            ) from e
        module_dir = self._get_local_dir_from_uri(uri)
        logger.debug(f"Succeeded to download jar file {jar_file} .")
        return module_dir

    async def create(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ) -> int:
        if not uri:
            return 0
        if is_jar_uri(uri):
            module_dir = await self._download_jars(uri=uri, logger=logger)
        else:
            try:
                module_dir = await download_and_unpack_package(
                    uri, self._resources_dir, self._gcs_aio_client, logger=logger
                )
            except Exception as e:
                raise RuntimeEnvSetupError(
                    "Failed to download jar file: {}".format(e)
                ) from e

        return get_directory_size_bytes(module_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        for uri in uris:
            module_dir = self._get_local_dir_from_uri(uri)
            if not module_dir.exists():
                raise ValueError(
                    f"Local directory {module_dir} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading, unpacking or installing the java jar files."
                )
            context.java_jars.append(str(module_dir))
            context.symlink_paths_to_working_dir.append(str(module_dir))
