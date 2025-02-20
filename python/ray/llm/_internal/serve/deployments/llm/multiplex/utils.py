import json
import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple, Union

# TODO (genesu): remove dependency on asyncache.
from asyncache import cached
from cachetools import TLRUCache
from fastapi import HTTPException
from filelock import FileLock

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.deployments.utils.cloud_utils import (
    GCP_EXECUTABLE,
    AWS_EXECUTABLE,
    get_file_from_gcs,
    get_file_from_s3,
    list_subfolders_gcs,
    list_subfolders_s3,
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


def _get_aws_sync_command(
    bucket_uri: str,
    local_path: str,
    sync_args: Optional[List[str]] = None,
    executable: str = AWS_EXECUTABLE,
) -> List[str]:
    sync_args = sync_args or []
    return [executable, "s3", "sync"] + sync_args + [bucket_uri, local_path]


def _get_gcp_sync_command(
    bucket_uri: str,
    local_path: str,
    sync_args: Optional[List[str]] = None,
    executable: str = GCP_EXECUTABLE,
) -> List[str]:
    sync_args = sync_args or []
    return [executable, "storage", "rsync"] + sync_args + [bucket_uri, local_path]


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
    if bucket_uri.startswith("s3://"):
        command = _get_aws_sync_command(bucket_uri, local_path, sync_args=sync_args)
    elif bucket_uri.startswith("gs://"):
        command = _get_gcp_sync_command(bucket_uri, local_path, sync_args=sync_args)
    else:
        raise ValueError(
            'bucket_uri must start with "s3://" or "gs://". '
            f'Got "{bucket_uri}" instead.'
        )
    logger.info(
        "Downloading %s to %s using %s, timeout=%ss",
        bucket_uri,
        local_path,
        command,
        timeout,
    )
    with FileLock(local_path + ".lock", timeout=timeout or -1):
        try:
            subprocess.run(command, check=True, capture_output=True, timeout=timeout)
        except Exception as e:
            # Not using logger.exception since we raise anyway.
            if isinstance(
                e, (subprocess.TimeoutExpired, subprocess.CalledProcessError)
            ):
                stdout_txt = f"\nSTDOUT: {e.stdout.decode()}" if e.stdout else ""
                stderr_txt = f"\nSTDERR: {e.stderr.decode()}" if e.stderr else ""
            else:
                stdout_txt = ""
                stderr_txt = ""
            logger.error(
                "Failed to sync model (%s) from %s to %s using %s.%s%s",
                str(e),
                bucket_uri,
                local_path,
                command,
                stdout_txt,
                stderr_txt,
            )
            raise


def _validate_model_ttu(key, value, now):
    # Return the expiration time depending on value
    # (now + some offset)
    # For get_lora_finetuned_context_length, we want to periodically re-check if
    # the files are available if they weren't before (because they
    # might have been uploaded in the meantime).
    # If they were uploaded and we cached the sequence length response,
    # then we just need to guard against users deleting the files
    # from the bucket, which shouldn't happen often.
    if value is CLOUD_OBJECT_MISSING:
        return now + CLOUD_OBJECT_MISSING_EXPIRE_S
    else:
        return now + CLOUD_OBJECT_EXISTS_EXPIRE_S


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

    if object_uri.startswith("s3://"):
        body_str = get_file_from_s3(object_uri=object_uri)
    elif object_uri.startswith("gs://"):
        body_str = get_file_from_gcs(object_uri=object_uri)
    else:
        raise ValueError(
            f'object_uri "{object_uri}" must start with "s3://" or "gs://".'
        )

    if body_str is None:
        logger.info(f"{object_uri} does not exist.")
        return CLOUD_OBJECT_MISSING
    else:
        return body_str


@cached(
    cache=TLRUCache(
        maxsize=4096,
        getsizeof=lambda x: 1,
        ttu=_validate_model_ttu,
        timer=time.monotonic,
    )
)
async def get_object_from_cloud(object_uri: str) -> Union[str, object]:
    """Calls _get_object_from_cloud with caching.

    We separate the caching logic from the implementation, so the
    implementation can be faked while testing the caching logic in unit tests.
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

    Args:
        dynamic_lora_loading_path: the cloud folder that contains all the LoRA
            weights.
        base_model_id: model ID of the base model.

    Returns:
        List of LoRA fine-tuned model IDs. Does not include the base model
        itself.
    """

    # The organization hosting the model. E.g. this would be "google" for the
    # model "google/gemma-2-9b-it".
    model_provider_organization = base_model_id.split("/")[0]

    # Ensure that the dynamic_lora_loading_path has no trailing slash.
    dynamic_lora_loading_path = dynamic_lora_loading_path.rstrip("/")

    # This folder contains all the LoRA subfolders.
    lora_folder = f"{dynamic_lora_loading_path}/{model_provider_organization}/"

    if "s3://" in dynamic_lora_loading_path:
        lora_subfolders = list_subfolders_s3(lora_folder)
    elif "gs://" in dynamic_lora_loading_path:
        lora_subfolders = list_subfolders_gcs(lora_folder)
    else:
        logger.warning(
            'Expected a path that starts with "s3://" or "gs://" for '
            "dynamic_lora_loading_path when getting LoRA model IDs. "
            f'Got "{lora_folder}" instead.'
        )
        return []

    lora_model_ids = []
    for subfolder in lora_subfolders:
        # There may be multiple models from the same provider. All their LoRAs
        # will be in these lora_subfolders. We must select only the LoRA
        # adapters for the passed-in base_model_id.
        model = base_model_id.split("/")[1]
        if subfolder.startswith(model):
            lora_model_ids.append(f"{model_provider_organization}/{subfolder}")

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
