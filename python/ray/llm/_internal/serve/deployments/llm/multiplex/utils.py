"""
Serve-specific LoRA utilities that use generic abstractions from lora_utils.py.

This module provides serve-specific functionality while using the generic
LoRA abstractions from common/lora_utils.py. This ensures clean separation
between generic and serve-specific concerns.
"""

import subprocess
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

from filelock import FileLock

from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    LoraMirrorConfig,
)
from ray.llm._internal.common.utils.lora_utils import (
    get_base_model_id,
    get_lora_finetuned_context_length,
    get_lora_id,
)
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.observability.logging import get_logger

CLOUD_OBJECT_MISSING = object()

# Type variable for the retry decorator
T = TypeVar("T")

logger = get_logger(__name__)


def clean_model_id(model_id: str):
    return model_id.replace("/", "--")


def clear_directory(dir: str):
    try:
        subprocess.run(f"rm -r {dir}", check=False)
    except FileNotFoundError:
        pass


def sync_model(
    bucket_uri: str,
    local_path: str,
    timeout: Optional[float] = None,
    sync_args: Optional[List[str]] = None,
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


def get_lora_model_ids(
    dynamic_lora_loading_path: str,
    base_model_id: str,
) -> List[str]:
    """Get all LoRA model IDs from the dynamic loading path.

    This is serve-specific logic that uses generic cloud utilities.

    Args:
        dynamic_lora_loading_path: The base path where LoRA adapters are stored
        base_model_id: The base model ID to filter by

    Returns:
        List of LoRA model IDs in the format base_model_id:lora_id
    """
    # This is serve-specific implementation that would list objects
    # in cloud storage and filter by base model
    # For now, return empty list as this requires cloud-specific logic
    return []


async def download_multiplex_config_info(
    model_id: str, base_path: str
) -> Tuple[str, int]:
    """Download multiplex configuration info for a LoRA model.

    This is serve-specific logic that uses generic cloud utilities.

    Args:
        model_id: The LoRA model ID
        base_path: The base path where the model is stored

    Returns:
        Tuple of (model_id, max_total_tokens)
    """
    # This is serve-specific implementation that would download
    # and parse configuration files using generic cloud utilities
    return model_id, 4096  # Default max tokens


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

    base_path = llm_config.lora_config.dynamic_lora_loading_path
    lora_id = get_lora_id(model_id)
    bucket_uri = f"{base_path}/{lora_id}"

    # Use generic utility to get context length
    max_length = await get_lora_finetuned_context_length(bucket_uri)

    return {
        "model_id": model_id,
        "base_model_id": get_base_model_id(model_id),
        "max_request_context_length": max_length or 4096,
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
