import json
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple, Union, TypeVar, Callable
from functools import wraps

from fastapi import HTTPException
from filelock import FileLock

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.deployments.utils.cloud_utils import (
    CloudFileSystem,
    remote_object_cache,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LoraMirrorConfig,
)
from ray.llm._internal.serve.configs.constants import (
    CLOUD_OBJECT_MISSING_EXPIRE_S,
    CLOUD_OBJECT_EXISTS_EXPIRE_S,
    LORA_ADAPTER_CONFIG_NAME,
)
from ray.llm._internal.serve.deployments.utils.server_utils import make_async

CLOUD_OBJECT_MISSING = object()

# Type variable for the retry decorator
T = TypeVar("T")

logger = get_logger(__name__)


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

    Returns:
        The body of the object if it exists, or CLOUD_OBJECT_MISSING if it doesn't.
    """
    return await _get_object_from_cloud(object_uri)


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


async def download_multiplex_config_info(
    model_id: str, base_path: str
) -> Tuple[str, int]:
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
    metadata = await get_lora_model_metadata(model_id, llm_config)

    return LoraMirrorConfig(
        lora_model_id=model_id,
        bucket_uri=metadata["bucket_uri"],
        max_total_tokens=metadata["max_request_context_length"],
    )
