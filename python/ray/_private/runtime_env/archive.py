import logging
import os
import json
from typing import Dict, List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.packaging import (
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes, try_to_create_directory

default_logger = logging.getLogger(__name__)


def get_context():
    """Return value example

    If archives plugin value is a instance of dict, like
        {"url1": "https://xxx.zip", "url2": "https://ooo.zip"},
    archives context value is like
        {"url1": "{resources_dir}/https_xxx/", "url2": "{resources_dir}/https_ooo/"}.

    If value is a instance of str, like "s3://xxx.zip", archives context value is like "{resources_dir}/s3_xxx/".
    """
    return json.loads(os.environ["RAY_ARCHIVE_PATH"])


def set_archive_path_in_context(
    local_archive_uris: str, context: RuntimeEnvContext, logger: logging.Logger
):
    """Insert the path in RAY_ARCHIVE_PATH in the runtime env."""
    logger.info(f"Setting archive path {local_archive_uris} to context {context}.")
    context.env_vars["RAY_ARCHIVE_PATH"] = local_archive_uris


class DownloadAndUnpackArchivePlugin(RuntimeEnvPlugin):

    name = "archives"

    def __init__(self, resources_dir: str):
        self._resources_dir = os.path.join(resources_dir, "archive_files")
        try_to_create_directory(self._resources_dir)

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

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        if self.name not in runtime_env:
            return []
        archive_uris = runtime_env[self.name]
        if isinstance(archive_uris, str):
            # NOTE(Jacky): archives uris pattern is like "archives": "s3://xxx.zip".
            if archive_uris != "":
                return [archive_uris]
            return []
        elif isinstance(archive_uris, dict):
            # NOTE(Jacky): archives uris pattern is like "archives": {"url1": "https://xxx.zip", "url2": "https://ooo.zip"}.
            return list(archive_uris.values())
        else:
            raise TypeError(f"Except dict or str, got {type(archive_uris)}")

    async def create(
        self,
        uri: Optional[str],
        runtime_env: dict,
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        local_dir = await download_and_unpack_package(
            uri,
            self._resources_dir,
            None,
            logger=logger,
        )
        return get_directory_size_bytes(local_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not uris:
            return

        archive_uris = runtime_env_dict[self.name]
        if isinstance(archive_uris, str):
            local_archive_uris = get_local_dir_from_uri(
                archive_uris, self._resources_dir
            )
            if not local_archive_uris.exists():
                raise ValueError(
                    f"Local directory {local_archive_uris} for URI {uri} does "
                    "not exist on the cluster. Something may have gone wrong while "
                    "downloading or unpacking the archive package."
                )
            local_archive_uris = str(local_archive_uris)
            # TODO(Jacky): The Working Dir plugin has not been revamped, so this interface is blocked for now
            # context.symlink_dirs_to_cwd.append(local_archive_uris)
        elif isinstance(archive_uris, dict):
            local_archive_uris = dict()
            for key, uri in archive_uris.items():
                local_dir = get_local_dir_from_uri(uri, self._resources_dir)
                if not local_dir.exists():
                    raise ValueError(
                        f"Local directory {local_dir} for URI {uri} does "
                        "not exist on the cluster. Something may have gone wrong while "
                        "downloading or unpacking the archive package."
                    )
                local_archive_uris[key] = str(local_dir)
                # TODO(Jacky): The Working Dir plugin has not been revamped, so this interface is blocked for now
                # context.symlink_dirs_to_cwd.append(str(local_dir))
        else:
            # NOTE(Jacky): archive_uris can not be instance of list, cause users need to find
            # the local directory corresponding to the downloaded file based on the index.
            raise TypeError(f"Except dict or str, got {type(archive_uris)}")
        set_archive_path_in_context(
            local_archive_uris=json.dumps(local_archive_uris),
            context=context,
            logger=logger,
        )
