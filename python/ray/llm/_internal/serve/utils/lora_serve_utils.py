"""
Serve-specific LoRA utilities that use generic abstractions from lora_utils.py.

This module provides serve-specific functionality while using the generic
LoRA abstractions from common/lora_utils.py. This ensures clean separation
between generic and serve-specific concerns.
"""

import json
from typing import Any, Dict, List

from fastapi import HTTPException

from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    LoraMirrorConfig,
)
from ray.llm._internal.common.utils.lora_utils import (
    CLOUD_OBJECT_MISSING,
    get_base_model_id,
    get_lora_id,
    get_object_from_cloud,
)
from ray.llm._internal.serve.configs.constants import LORA_ADAPTER_CONFIG_NAME
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


async def get_lora_finetuned_context_length(bucket_uri: str):
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
        return adapter_config.get("context_length")


async def download_multiplex_config_info(
    model_id: str, base_path: str
) -> tuple[str, int]:
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
