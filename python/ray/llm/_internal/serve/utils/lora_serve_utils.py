"""
Serve-specific LoRA utilities that use generic abstractions from lora_utils.py.

This module provides serve-specific functionality while using the generic
LoRA abstractions from common/lora_utils.py. This ensures clean separation
between generic and serve-specific concerns.
"""

import asyncio
import json
import os
from typing import Any, Dict, Optional

from fastapi import HTTPException

from ray.llm._internal.common.constants import LORA_ADAPTER_CONFIG_NAME
from ray.llm._internal.common.models import global_id_manager, make_async
from ray.llm._internal.common.utils.cloud_utils import (
    LoraMirrorConfig,
)
from ray.llm._internal.common.utils.lora_utils import (
    CLOUD_OBJECT_MISSING,
    clean_model_id,
    clear_directory,
    get_base_model_id,
    get_lora_id,
    get_object_from_cloud,
    retry_with_exponential_backoff,
    sync_files_with_lock,
)
from ray.llm._internal.serve.core.configs.llm_config import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


async def get_lora_finetuned_context_length(bucket_uri: str) -> Optional[int]:
    """Gets the sequence length used to tune the LoRA adapter.

    Return: Returns the max sequence length for the adapter, if it exists.

    Raises: HTTPException if the LoRA adapter config file isn't available
        in the cloud storage repository.
    """
    if bucket_uri.endswith("/"):
        bucket_uri = bucket_uri.rstrip("/")
    object_uri = f"{bucket_uri}/{LORA_ADAPTER_CONFIG_NAME}"

    object_str_or_missing_message = await get_object_from_cloud(object_uri)

    if object_str_or_missing_message is CLOUD_OBJECT_MISSING:
        raise HTTPException(
            404,
            f"Unable to find LoRA adapter config file "
            f'"{LORA_ADAPTER_CONFIG_NAME}" in folder {bucket_uri}. '
            "Check that the file exists and that you have read permissions.",
        )
    else:
        adapter_config_str = object_str_or_missing_message
        adapter_config = json.loads(adapter_config_str)
        return adapter_config.get("max_length")


async def download_multiplex_config_info(
    model_id: str, base_path: str
) -> tuple[str, Optional[int]]:
    """Downloads info needed to create a multiplex config.

    Downloads objects using cloud storage provider APIs.

    Returns: 2-tuple containing
        1. A bucket_uri for the bucket containing LoRA weights and config.
        2. The maximum LoRA sequence length.

    Raises: HTTPException if the LoRA adapter config file isn't available
        in the cloud storage repository.
    """
    bucket_uri = f"{base_path}/{model_id}"
    ft_context_length = await get_lora_finetuned_context_length(bucket_uri)
    return bucket_uri, ft_context_length


async def get_lora_model_metadata(
    model_id: str, llm_config: LLMConfig
) -> Dict[str, Any]:
    """Get the lora model metadata for a given model id and llm config.

    This is used to get the metadata for the model with the given model id.
    """
    # Note (genesu): `model_id` passed is a lora model id where it's in a form of
    #     base_model_id:suffix:id
    base_model_id = get_base_model_id(model_id)
    lora_id = get_lora_id(model_id)
    base_path = llm_config.lora_config.dynamic_lora_loading_path

    # Examples of the variables:
    #   model_id: "meta-llama/Meta-Llama-3.1-8B-Instruct:my_suffix:aBc1234"
    #   base_path: "s3://ray-llama-weights"
    #   bucket_uri: "s3://ray-llama-weights/my_suffix:aBc1234"
    (
        bucket_uri,
        ft_context_length,
    ) = await download_multiplex_config_info(lora_id, base_path)

    return {
        "model_id": model_id,
        "base_model_id": base_model_id,
        "max_request_context_length": ft_context_length,
        # Note (genesu): `bucket_uri` affects where the lora weights are downloaded
        # from remote location.
        "bucket_uri": bucket_uri,
    }


async def get_lora_mirror_config(
    model_id: str,
    llm_config: LLMConfig,
) -> LoraMirrorConfig:
    """Get LoRA mirror configuration for serve-specific LLM config."""
    metadata = await get_lora_model_metadata(model_id, llm_config)

    return LoraMirrorConfig(
        lora_model_id=model_id,
        bucket_uri=metadata["bucket_uri"],
        max_total_tokens=metadata["max_request_context_length"],
        sync_args=None,
    )


class LoraModelLoader:
    """Download LoRA weights from remote storage and manage disk cache.

    This class is serve-specific as it depends on DiskMultiplexConfig and
    other serve-specific concepts.
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

    async def load_model_from_config(
        self, lora_model_id: str, llm_config
    ) -> DiskMultiplexConfig:
        """Load a LoRA model by first fetching its mirror config from S3."""
        lora_mirror_config = await get_lora_mirror_config(lora_model_id, llm_config)
        return await self.load_model(lora_model_id, lora_mirror_config)

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
