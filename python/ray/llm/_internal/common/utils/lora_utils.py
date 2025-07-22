"""
Generic LoRA utilities and abstractions.

This module provides the canonical LoRA functionality for both serve and batch components.
It serves as the single source of truth for LoRA-related operations, replacing duplicated
functions across the codebase.
"""

import asyncio
import json
import os
import subprocess
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

from ray.llm._internal.common.models import DiskMultiplexConfig, global_id_manager
from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    CloudMirrorConfig,
    LoraMirrorConfig,
    is_remote_path,
    remote_object_cache,
)
from ray.llm._internal.common.utils.download_utils import (
    CloudModelDownloader,
)
from ray.llm._internal.serve.configs.constants import (
    CLOUD_OBJECT_EXISTS_EXPIRE_S,
    CLOUD_OBJECT_MISSING_EXPIRE_S,
    LORA_ADAPTER_CONFIG_NAME,
)
from ray.llm._internal.serve.deployments.utils.server_utils import make_async

# Type variable for the retry decorator
T = TypeVar("T")

logger = get_logger(__name__)

CLOUD_OBJECT_MISSING = object()


def get_base_model_id(model_id: str) -> str:
    """Get base model id for a given model id.

    A LoRA fine-tuned model_id is expected to be in the format of
        base_model_id:lora_id
        e.g. meta-llama/Llama-2-7b-chat-hf:my_suffix:aBc1234

    The returned base model id is in the format of
        base_model_id
        e.g. meta-llama/Llama-2-7b-chat-hf

    This function can safely take any string.
    """
    return model_id.split(":")[0]


def get_lora_id(lora_model_id: str) -> str:
    """Get lora id for a given lora model id.

    A LoRA fine-tuned model_id is expected to be in the format of
        base_model_id:lora_id
        e.g. meta-llama/Llama-2-7b-chat-hf:my_suffix:aBc1234

    The returned lora id is in the format of
        lora_id
        e.g. my_suffix:aBc1234

    This function can safely take any string.
    """
    return ":".join(lora_model_id.split(":")[1:])


def clean_model_id(model_id: str) -> str:
    """Clean model ID for filesystem usage by replacing slashes with dashes."""
    return model_id.replace("/", "--")


def clear_directory(dir: str) -> None:
    """Clear a directory recursively, ignoring missing directories."""
    try:
        subprocess.run(f"rm -r {dir}", shell=True, check=False)
    except FileNotFoundError:
        pass


def retry_with_exponential_backoff(
    max_tries: int,
    exception_to_check: type[Exception],
    base_delay: float = 1,
    max_delay: float = 32,
    exponential_base: float = 2,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry decorator with exponential backoff.

    Args:
        max_tries: Maximum number of retry attempts
        exception_to_check: Exception type to catch and retry on
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exponential_base: Base for exponential calculation

    Returns:
        A decorator function that applies retry logic with exponential backoff
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = base_delay
            last_exception = None

            for attempt in range(max_tries):
                try:
                    return func(*args, **kwargs)
                except exception_to_check as e:
                    last_exception = e
                    if attempt == max_tries - 1:  # Last attempt
                        raise last_exception

                    # Log the failure and retry
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_tries} failed: {str(e)}. "
                        f"Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                    # Calculate next delay with exponential backoff
                    delay = min(delay * exponential_base, max_delay)

            # This should never be reached due to the raise in the loop
            raise last_exception if last_exception else RuntimeError(
                "Unexpected error in retry logic"
            )

        return wrapper

    return decorator


@make_async
def _get_object_from_cloud(object_uri: str) -> Union[str, object]:
    """Gets an object from the cloud.

    Don't call this function directly. Use get_object_from_cloud() instead, so
    the results can be cached.

    Return: Returns the body of the object. If the object doesn't exist,
        returns a sentinel CLOUD_OBJECT_MISSING object instead.
    """
    if object_uri.endswith("/"):
        raise ValueError(f'object_uri {object_uri} must not end with a "/".')

    body_str = CloudFileSystem.get_file(object_uri)

    if body_str is None:
        logger.info(f"{object_uri} does not exist.")
        return CLOUD_OBJECT_MISSING
    else:
        return body_str


@remote_object_cache(
    max_size=4096,
    missing_expire_seconds=CLOUD_OBJECT_MISSING_EXPIRE_S,
    exists_expire_seconds=CLOUD_OBJECT_EXISTS_EXPIRE_S,
    missing_object_value=CLOUD_OBJECT_MISSING,
)
async def get_object_from_cloud(object_uri: str) -> Union[str, object]:
    """Gets an object from the cloud with caching.

    The cache will store missing objects for a short time and existing objects for
    a longer time. This prevents unnecessary cloud API calls when objects don't exist
    while ensuring we don't cache missing objects for too long in case they get created.

    Args:
        object_uri: The URI of the object to retrieve from cloud storage

    Returns:
        The body of the object if it exists, or CLOUD_OBJECT_MISSING if it doesn't.
    """
    return await _get_object_from_cloud(object_uri)


async def get_lora_finetuned_context_length(bucket_uri: str):
    """Gets the sequence length used to tune the LoRA adapter.

    Return: Returns the max sequence length for the adapter, if it exists.
    """
    config_uri = f"{bucket_uri}/{LORA_ADAPTER_CONFIG_NAME}"
    config_body = await get_object_from_cloud(config_uri)

    if config_body == CLOUD_OBJECT_MISSING:
        return None

    try:
        if isinstance(config_body, str):
            config = json.loads(config_body)
            return config.get("max_length", None)
        else:
            return None
    except (json.JSONDecodeError, KeyError):
        logger.warning(f"Failed to parse config from {config_uri}")
        return None


def get_lora_model_ids(
    dynamic_lora_loading_path: str,
    base_model_id: str,
) -> List[str]:
    """Get all LoRA model IDs from the dynamic loading path.

    This is generic logic that can be used by serve and other components.

    Args:
        dynamic_lora_loading_path: The base path where LoRA adapters are stored
        base_model_id: The base model ID to filter by

    Returns:
        List of LoRA model IDs in the format base_model_id:lora_id
    """
    from ray.llm._internal.common.utils.cloud_utils import CloudFileSystem

    # Ensure that the dynamic_lora_loading_path has no trailing slash.
    dynamic_lora_loading_path = dynamic_lora_loading_path.rstrip("/")

    try:
        # List subfolders directly from the dynamic_lora_loading_path
        # The path should already point to the correct model-specific directory
        lora_subfolders = CloudFileSystem.list_subfolders(dynamic_lora_loading_path)
    except Exception as e:
        logger.warning(
            f"Failed to list LoRA subfolders from {dynamic_lora_loading_path}: {e}. "
            "Returning empty list."
        )
        return []

    lora_model_ids = []
    for subfolder in lora_subfolders:
        # Each subfolder represents a LoRA adapter for the base model
        # Create the full LoRA model ID by combining base_model_id with the subfolder name
        lora_model_id = f"{base_model_id}:{subfolder}"
        lora_model_ids.append(lora_model_id)

    return lora_model_ids


async def download_multiplex_config_info(
    model_id: str, base_path: str
) -> Tuple[str, int]:
    """Download multiplex configuration info for a LoRA model.

    This is a wrapper that forwards to the correct implementation.
    The actual implementation is in multiplex/utils.py.

    Args:
        model_id: The LoRA model ID
        base_path: The base path where the model is stored

    Returns:
        Tuple of (bucket_uri, max_total_tokens)
    """
    # Import here to avoid circular imports
    from ray.llm._internal.serve.deployments.llm.multiplex.utils import (
        download_multiplex_config_info as _actual_download_multiplex_config_info,
    )

    return await _actual_download_multiplex_config_info(model_id, base_path)


def download_lora_adapter(
    lora_name: str,
    remote_path: Optional[str] = None,
    lora_root: Optional[str] = None,
    download_timeout_s: Optional[float] = None,
    max_tries: int = 1,
) -> str:
    """Download a LoRA adapter from remote storage to local directory.

    This function supports both simple downloading (for backward compatibility)
    and advanced downloading with caching and retry functionality.

    Args:
        lora_name: The lora name.
        remote_path: The remote path to the lora. If specified, the remote_path will be
            used as the base path to load the lora.
        lora_root: Path to directory where LoRA weights will be cached (for advanced mode).
        download_timeout_s: Download timeout in seconds (for advanced mode).
        max_tries: Number of retry attempts (for advanced mode).

    Returns:
        The local path to the lora if remote_path is specified, otherwise the lora name.
    """
    assert not is_remote_path(
        lora_name
    ), "lora_name cannot be a remote path (s3:// or gs://)"

    if remote_path is None:
        return lora_name

    # Use the working implementation consistently
    lora_path = os.path.join(remote_path, lora_name)

    # Always use CloudModelDownloader which properly downloads all files including tokenizers
    # This ensures consistency between serve and batch paths
    mirror_config = CloudMirrorConfig(bucket_uri=lora_path)

    # If lora_root is specified, use it for caching; otherwise use default behavior
    if lora_root is not None:
        # Create a local cache directory for this specific lora
        local_cache_dir = os.path.join(lora_root, lora_name)
        os.makedirs(local_cache_dir, exist_ok=True)

        # Use CloudModelDownloader with custom cache location
        downloader = CloudModelDownloader(
            local_cache_dir,  # Use the cache directory as the "model_id" for local storage
            mirror_config,
        )
        return downloader.get_model(tokenizer_only=False)
    else:
        # Original behavior for backward compatibility
        downloader = CloudModelDownloader(lora_name, mirror_config)
        return downloader.get_model(tokenizer_only=False)


class _LoraModelLoader:
    """Download Lora weights from remote, and manage a CPU memory cache.

    This entire downloader is sync.

    Args:
        lora_root: Path to directory where LoRA weights will be cached.
        download_timeout_s: How much time the download subprocess has to download
            a single LoRA before a timeout. None means no timeout.
        max_tries: Number of times to try downloading a LoRA model if
            the download subprocess fails.
    """

    def __init__(
        self,
        lora_root: Optional[str] = None,
        download_timeout_s: Optional[float] = None,
        max_tries: int = 1,
    ):
        self.lora_root = lora_root or "/tmp/ray/llm/lora/cache"
        self.disk_cache: Dict[str, DiskMultiplexConfig] = {}
        self.active_syncing_tasks: Dict[str, asyncio.Task[DiskMultiplexConfig]] = {}
        if download_timeout_s is not None and download_timeout_s <= 0:
            raise ValueError(
                f"download_timeout_s must be None or >0, got {download_timeout_s}"
            )
        self.download_timeout_s = download_timeout_s
        if max_tries < 1:
            raise ValueError(f"max_tries must be >=1, got {max_tries}")
        self.max_tries = max_tries

    async def load_model(
        self, lora_model_id: str, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        """Load a model.

        This function will load a Lora model from s3 and cache it on disk and in memory.
        This function runs in a separate thread because it does synchronous disk operations.
        """
        if lora_model_id in self.disk_cache:
            return self.disk_cache[lora_model_id]

        if lora_model_id not in self.active_syncing_tasks:
            # Cannot use _load_model directly in create_task
            # due to TypeError: a coroutine was expected, got <Future...
            task = asyncio.create_task(self._load_model_async(lora_mirror_config))
            task.add_done_callback(
                lambda result: self.active_syncing_tasks.pop(lora_model_id, None)
            )
            self.active_syncing_tasks[lora_model_id] = task
        else:
            task = self.active_syncing_tasks[lora_model_id]

        # Ensure that cancellation of the current request doesn't
        # affect other requests
        disk_config = await asyncio.shield(task)

        # If we are successful, add the result to the disk cache
        # This will not be reached if the task raises an exception
        self.disk_cache[lora_model_id] = disk_config

        return disk_config

    async def _load_model_async(
        self, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        return await self._load_model(lora_mirror_config)

    @make_async
    def _load_model(
        self, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        return self._load_model_sync(lora_mirror_config)

    @make_async
    def clear_cache(self):
        """Clear the disk cache

        Note: clear_disk_cache currently blindly clears the disk cache and is not
         thread / process safe because another process
         may be reading the cache as it is being cleared.

         TODO(tchordia): come up with a way to clear the Lora Disk cache.
        """
        clear_directory(self.lora_root)

    def _download_lora(self, lora_mirror_config: "LoraMirrorConfig") -> str:
        # Revert to the original working implementation approach
        # The bucket_uri in LoraMirrorConfig already contains the full path to the lora
        lora_id = get_lora_id(clean_model_id(lora_mirror_config.lora_model_id))

        # Create local directory for the lora model
        model_local_path = os.path.join(self.lora_root, lora_id)
        os.makedirs(model_local_path, exist_ok=True)

        # Use CloudFileSystem.download_files directly like in the original implementation
        # This is more reliable than trying to force it through download_lora_adapter
        logger.info(
            "Downloading %s to %s", lora_mirror_config.bucket_uri, model_local_path
        )

        try:
            CloudFileSystem.download_files(
                path=model_local_path,
                bucket_uri=lora_mirror_config.bucket_uri,
            )
        except Exception as e:
            logger.error(
                "Failed to sync model (%s) from %s to %s",
                str(e),
                lora_mirror_config.bucket_uri,
                model_local_path,
            )
            raise

        return model_local_path

    def _load_model_sync(
        self, lora_mirror_config: "LoraMirrorConfig"
    ) -> DiskMultiplexConfig:
        """Load a model from the given mirror configuration."""

        # Apply retry decorator to _download_lora at runtime with instance parameters
        download_with_retries = retry_with_exponential_backoff(
            max_tries=self.max_tries,
            exception_to_check=Exception,  # Catch any exception from CloudFileSystem
        )(lambda config: self._download_lora(config))

        local_path = download_with_retries(lora_mirror_config)
        # the lora_assigned_id is consistent for the lifetime of the disk cache entry
        # If the disk cache is cleared, a new id will be generated.
        return DiskMultiplexConfig.model_validate(
            {
                "model_id": lora_mirror_config.lora_model_id,
                "max_total_tokens": lora_mirror_config.max_total_tokens,
                "local_path": local_path,
                "lora_assigned_int_id": global_id_manager.next(),
            }
        )
