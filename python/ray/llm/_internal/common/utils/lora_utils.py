"""
Generic LoRA utilities and abstractions.

This module provides generic LoRA functionality that can be used by both
serve and batch components. It uses abstractions from download_utils.py
and provides clean separation between generic and specific concerns.
"""

import json
import time
from functools import wraps
from typing import List, Tuple, TypeVar, Union

from ray.llm._internal.common.observability.logging import get_logger
from ray.llm._internal.common.utils.cloud_utils import (
    CloudFileSystem,
    remote_object_cache,
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


def clean_model_id(model_id: str):
    return model_id.replace("/", "--")


def clear_directory(dir: str):
    import subprocess

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
):
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

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
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
        config = json.loads(config_body)
        return config.get("max_length", None)
    except (json.JSONDecodeError, KeyError):
        logger.warning(f"Failed to parse config from {config_uri}")
        return None


def get_lora_model_ids(
    dynamic_lora_loading_path: str,
    base_model_id: str,
) -> List[str]:
    """Get all LoRA model IDs from the dynamic loading path.

    Args:
        dynamic_lora_loading_path: The base path where LoRA adapters are stored
        base_model_id: The base model ID to filter by

    Returns:
        List of LoRA model IDs in the format base_model_id:lora_id
    """
    # This is a simplified implementation - in practice this would
    # list objects in the cloud storage and filter by base model
    # For now, return empty list as this is serve-specific logic
    return []


async def download_multiplex_config_info(
    model_id: str, base_path: str
) -> Tuple[str, int]:
    """Download multiplex configuration info for a LoRA model.

    Args:
        model_id: The LoRA model ID
        base_path: The base path where the model is stored

    Returns:
        Tuple of (model_id, max_total_tokens)
    """
    # This is a simplified implementation - in practice this would
    # download and parse configuration files
    return model_id, 4096  # Default max tokens
