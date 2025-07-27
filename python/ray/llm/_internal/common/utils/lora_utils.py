"""
Generic LoRA utilities and abstractions.

This module provides canonical LoRA utility functions for both serve and batch components.
It serves as the single source of truth for LoRA operations and builds on the generic
download primitives from download_utils.py.
"""

import asyncio
import json
import os
import subprocess
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from ray.llm._internal.common.constants import (
    CLOUD_OBJECT_EXISTS_EXPIRE_S,
    CLOUD_OBJECT_MISSING_EXPIRE_S,
    LORA_ADAPTER_CONFIG_NAME,
)

# Import the global ID manager from common models
from ray.llm._internal.common.models import global_id_manager, make_async
from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    LoraMirrorConfig,
    remote_object_cache,
)
from ray.llm._internal.serve.configs.server_models import DiskMultiplexConfig

logger = get_logger(__name__)

CLOUD_OBJECT_MISSING = object()
DEFAULT_LORA_MAX_TOTAL_TOKENS = 4096
T = TypeVar("T")


def get_base_model_id(model_id: str) -> str:
    """Get base model id for a given model id."""
    return model_id.split(":")[0]


def get_lora_id(lora_model_id: str) -> str:
    """Get lora id for a given lora model id."""
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
    """Retry decorator with exponential backoff."""

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


def sync_files_with_lock(
    bucket_uri: str,
    local_path: str,
    timeout: Optional[float] = None,
    substrings_to_include: Optional[List[str]] = None,
) -> None:
    """Sync files from bucket_uri to local_path with file locking."""
    from filelock import FileLock

    logger.info("Downloading %s to %s", bucket_uri, local_path)

    with FileLock(local_path + ".lock", timeout=timeout or -1):
        try:
            CloudFileSystem.download_files(
                path=local_path,
                bucket_uri=bucket_uri,
                substrings_to_include=substrings_to_include,
            )
        except Exception as e:
            logger.error(
                "Failed to sync files from %s to %s: %s",
                bucket_uri,
                local_path,
                str(e),
            )
            raise


@make_async
def _get_object_from_cloud(object_uri: str) -> Union[str, object]:
    """Gets an object from the cloud."""
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
    """Gets an object from the cloud with caching."""
    return await _get_object_from_cloud(object_uri)


async def get_lora_finetuned_context_length(bucket_uri: str) -> Optional[int]:
    """Gets the sequence length used to tune the LoRA adapter."""
    if bucket_uri.endswith("/"):
        bucket_uri = bucket_uri.rstrip("/")
    object_uri = f"{bucket_uri}/{LORA_ADAPTER_CONFIG_NAME}"

    object_str_or_missing_message = await get_object_from_cloud(object_uri)

    if object_str_or_missing_message is CLOUD_OBJECT_MISSING:
        logger.debug(f"LoRA adapter config file not found at {object_uri}")
        return None

    try:
        adapter_config_str = object_str_or_missing_message
        adapter_config = json.loads(adapter_config_str)
        return adapter_config.get("max_length")
    except (json.JSONDecodeError, AttributeError) as e:
        logger.warning(f"Failed to parse LoRA adapter config at {object_uri}: {e}")
        return None


def get_lora_model_ids(
    dynamic_lora_loading_path: str,
    base_model_id: str,
) -> List[str]:
    """Get the model IDs of all the LoRA models.

    The dynamic_lora_loading_path is expected to hold subfolders each for
    a different lora checkpoint. Each subfolder name will correspond to
    the unique identifier for the lora checkpoint. The lora model is
    accessible via <base_model_id>:<lora_id>. Therefore, we prepend
    the base_model_id to each subfolder name.

    Args:
        dynamic_lora_loading_path: the cloud folder that contains all the LoRA
            weights.
        base_model_id: model ID of the base model.

    Returns:
        List of LoRA fine-tuned model IDs. Does not include the base model
        itself.
    """
    lora_subfolders = CloudFileSystem.list_subfolders(dynamic_lora_loading_path)

    lora_model_ids = []
    for subfolder in lora_subfolders:
        lora_model_ids.append(f"{base_model_id}:{subfolder}")

    return lora_model_ids


def download_lora_adapter(
    lora_name: str,
    remote_path: Optional[str] = None,
) -> str:
    """Download a LoRA adapter from remote storage.

    This maintains backward compatibility with existing code.
    """
    from ray.llm._internal.common.utils.cloud_utils import is_remote_path
    from ray.llm._internal.common.utils.download_utils import (
        CloudMirrorConfig,
        CloudModelDownloader,
    )

    assert not is_remote_path(
        lora_name
    ), "lora_name cannot be a remote path (s3:// or gs://)"

    if remote_path is None:
        return lora_name

    lora_path = os.path.join(remote_path, lora_name)
    mirror_config = CloudMirrorConfig(bucket_uri=lora_path)
    downloader = CloudModelDownloader(lora_name, mirror_config)
    return downloader.get_model(tokenizer_only=False)


class LoraModelLoader:
    """Download LoRA weights from remote storage and manage disk cache."""

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
        self, lora_model_id: str, lora_mirror_config: LoraMirrorConfig
    ) -> DiskMultiplexConfig:
        """Load a LoRA model."""
        if lora_model_id in self.disk_cache:
            return self.disk_cache[lora_model_id]

        if lora_model_id not in self.active_syncing_tasks:
            task = asyncio.create_task(self._load_model_async(lora_mirror_config))
            task.add_done_callback(
                lambda result: self.active_syncing_tasks.pop(lora_model_id, None)
            )
            self.active_syncing_tasks[lora_model_id] = task
        else:
            task = self.active_syncing_tasks[lora_model_id]

        disk_config = await asyncio.shield(task)
        self.disk_cache[lora_model_id] = disk_config
        return disk_config

    async def _load_model_async(
        self, lora_mirror_config: LoraMirrorConfig
    ) -> DiskMultiplexConfig:
        return await self._load_model(lora_mirror_config)

    @make_async
    def _load_model(self, lora_mirror_config: LoraMirrorConfig) -> DiskMultiplexConfig:
        return self._load_model_sync(lora_mirror_config)

    @make_async
    def clear_cache(self):
        """Clear the disk cache."""
        clear_directory(self.lora_root)

    def _model_dir_path(self, model_id: str) -> str:
        """Construct the path for the lora weight."""
        lora_id = get_lora_id(clean_model_id(model_id))
        path = os.path.join(self.lora_root, lora_id)
        os.makedirs(path, exist_ok=True)
        return path

    def _download_lora(self, lora_mirror_config: LoraMirrorConfig) -> str:
        """Download LoRA weights using generic download primitives."""
        model_local_path = self._model_dir_path(lora_mirror_config.lora_model_id)
        sync_files_with_lock(
            lora_mirror_config.bucket_uri,
            model_local_path,
            timeout=self.download_timeout_s,
        )
        return model_local_path

    def _load_model_sync(
        self, lora_mirror_config: LoraMirrorConfig
    ) -> DiskMultiplexConfig:
        """Load a model from the given mirror configuration."""
        download_with_retries = retry_with_exponential_backoff(
            max_tries=self.max_tries,
            exception_to_check=Exception,
        )(lambda config: self._download_lora(config))

        local_path = download_with_retries(lora_mirror_config)
        return DiskMultiplexConfig.model_validate(
            {
                "model_id": lora_mirror_config.lora_model_id,
                "max_total_tokens": lora_mirror_config.max_total_tokens,
                "local_path": local_path,
                "lora_assigned_int_id": global_id_manager.next(),
            }
        )
