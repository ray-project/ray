import json
import logging
import os
from typing import Dict, List, Optional, Union

from ray._common.utils import try_to_create_directory
from ray._private.runtime_env.constants import (
    RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR,
)
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.validation import parse_and_validate_archives
from ray._private.utils import get_directory_size_bytes

default_logger = logging.getLogger(__name__)


class ArchivesPlugin(RuntimeEnvPlugin):
    name = "archives"

    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "archives_files")
        try_to_create_directory(self._resources_dir)

    @staticmethod
    def validate(runtime_env_dict: dict) -> None:
        parse_and_validate_archives(runtime_env_dict[ArchivesPlugin.name])
        if RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR in runtime_env_dict.get(
            "env_vars", {}
        ):
            raise ValueError(
                f"{RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR!r} is managed by the "
                "archives runtime environment and cannot be set in env_vars."
            )

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        archives = runtime_env.archives()
        if isinstance(archives, str):
            return [archives] if archives else []
        if isinstance(archives, dict):
            uris = []
            seen_uris = set()
            for uri in archives.values():
                if isinstance(uri, str) and uri and uri not in seen_uris:
                    uris.append(uri)
                    seen_uris.add(uri)
            return uris
        return []

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        local_dir = await download_and_unpack_package(
            uri,
            self._resources_dir,
            gcs_client=None,
            logger=logger,
        )
        return get_directory_size_bytes(local_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> None:
        if not uris:
            return

        archives = runtime_env.archives()
        local_paths: Union[str, Dict[str, str]]
        if isinstance(archives, str):
            local_paths = self._get_local_dir(archives)
        else:
            local_paths = {
                name: self._get_local_dir(uri) for name, uri in archives.items()
            }

        logger.info("Adding archives paths to the worker context.")
        context.env_vars[RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR] = json.dumps(
            local_paths, sort_keys=True
        )

    def delete_uri(self, uri: str, logger: logging.Logger = default_logger) -> int:
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)
        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning("Tried to delete nonexistent archives URI: %s", uri)
            return 0
        return local_dir_size

    def _get_local_dir(self, uri: str) -> str:
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        if not local_dir.exists():
            raise ValueError(
                f"Local directory {local_dir} for archives URI {uri} does not "
                "exist on the cluster. Something may have gone wrong while "
                "downloading or unpacking the archive."
            )
        return str(local_dir)
