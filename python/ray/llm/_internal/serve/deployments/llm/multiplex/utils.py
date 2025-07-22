"""
Serve-specific LoRA utilities that use generic abstractions from lora_utils.py.

This module provides serve-specific functionality while using the generic
LoRA abstractions from common/lora_utils.py. This ensures clean separation
between generic and serve-specific concerns.
"""

from typing import Any, Dict, Optional

from filelock import FileLock

from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    LoraMirrorConfig,
)
from ray.llm._internal.common.utils.lora_utils import (
    download_multiplex_config_info,
    get_base_model_id,
    get_lora_id,
)
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger

CLOUD_OBJECT_MISSING = object()

logger = get_logger(__name__)


# These utility functions are now imported from the canonical location
# in lora_utils.py to avoid duplication


def sync_model(
    bucket_uri: str,
    local_path: str,
    timeout: Optional[float] = None,
):
    """Sync from bucket_uri to local_path.

    This method isn't re-entrant and will block (up to timeout) if already syncing
    at a given path.
    """

    logger.info("Downloading %s to %s", bucket_uri, local_path)

    with FileLock(local_path + ".lock", timeout=timeout or -1):
        try:
            # Use CloudFileSystem.download_files for the sync operation
            CloudFileSystem.download_files(
                path=local_path,
                bucket_uri=bucket_uri,
            )
        except Exception as e:
            logger.error(
                "Failed to sync model (%s) from %s to %s",
                str(e),
                bucket_uri,
                local_path,
            )
            raise


# These utility functions are now imported from the canonical location
# in lora_utils.py to avoid duplication


# download_multiplex_config_info is now imported from lora_utils.py


async def get_lora_model_metadata(
    model_id: str, llm_config: LLMConfig
) -> Dict[str, Any]:
    """Get metadata for a LoRA model.

    This is serve-specific logic that uses generic LoRA utilities.

    Args:
        model_id: The LoRA model ID
        llm_config: The LLM configuration

    Returns:
        Dictionary containing model metadata
    """
    if (
        not llm_config.lora_config
        or not llm_config.lora_config.dynamic_lora_loading_path
    ):
        return {}

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
    """Get LoRA mirror configuration for a model.

    This is serve-specific logic that creates LoRA mirror configs
    using the generic LoraMirrorConfig class.

    Args:
        model_id: The LoRA model ID
        llm_config: The LLM configuration

    Returns:
        LoraMirrorConfig for the model
    """
    if (
        not llm_config.lora_config
        or not llm_config.lora_config.dynamic_lora_loading_path
    ):
        raise ValueError("No LoRA configuration available")

    base_path = llm_config.lora_config.dynamic_lora_loading_path
    lora_id = get_lora_id(model_id)
    bucket_uri = f"{base_path}/{lora_id}"

    # Get metadata to determine max tokens
    metadata = await get_lora_model_metadata(model_id, llm_config)
    max_total_tokens = metadata.get("max_request_context_length", 4096)

    return LoraMirrorConfig(
        lora_model_id=model_id,
        bucket_uri=bucket_uri,
        max_total_tokens=max_total_tokens,
    )
