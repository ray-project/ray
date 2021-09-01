import ray
import grpc
import os
from typing import List, Union

from ray._private.client_mode_hook import client_mode_hook

redis = os.environ.get("RAY_KV_USE_GCS", "0") == "0"
_initialized = False


def _initialize_internal_kv(gcs_client: "ray._raylet.GcsClient" = None):
    """Initialize the internal KV for use in other function calls.

    We try to use a GCS client, either passed in here or from the Ray worker
    global scope. If that is not possible or it's feature-flagged off with the
    RAY_KV_USE_GCS env variable, we fall back to using the Ray worker redis
    client directly.
    """
    global global_gcs_client, _initialized

    if not _initialized:
        if gcs_client is not None:
            global_gcs_client = gcs_client
        elif redis:
            global_gcs_client = None
        else:
            try:
                ray.worker.global_worker.gcs_client.kv_exists("dummy")
                global_gcs_client = ray.worker.global_worker.gcs_client
            except grpc.RpcError:
                global_gcs_client = None

    _initialized = True
    return global_gcs_client


@client_mode_hook
def _internal_kv_initialized():
    gcs_client = _initialize_internal_kv()

    worker = ray.worker.global_worker
    if gcs_client is not None:
        return True
    else:
        return hasattr(worker, "mode") and worker.mode is not None


@client_mode_hook
def _internal_kv_get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""
    gcs_client = _initialize_internal_kv()

    if gcs_client is not None:
        return gcs_client.kv_get(key)
    else:
        return ray.worker.global_worker.redis_client.hget(key, "value")


@client_mode_hook
def _internal_kv_exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""
    gcs_client = _initialize_internal_kv()
    if gcs_client is not None:
        return gcs_client.kv_exists(key)
    else:
        return ray.worker.global_worker.redis_client.hexists(key, "value")


@client_mode_hook
def _internal_kv_put(key: Union[str, bytes],
                     value: Union[str, bytes],
                     overwrite: bool = True) -> bool:
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns:
        already_exists (bool): whether the value already exists.
    """
    gcs_client = _initialize_internal_kv()
    if gcs_client is not None:
        return not gcs_client.kv_put(key, value, overwrite)
    else:
        if overwrite:
            updated = ray.worker.global_worker.redis_client.hset(
                key, "value", value)
        else:
            updated = ray.worker.global_worker.redis_client.hsetnx(
                key, "value", value)
        return updated == 0  # already exists


@client_mode_hook
def _internal_kv_del(key: Union[str, bytes]):
    gcs_client = _initialize_internal_kv()
    if gcs_client is not None:
        return gcs_client.kv_del(key)
    else:
        return ray.worker.global_worker.redis_client.delete(key)


@client_mode_hook
def _internal_kv_list(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix.
    """
    gcs_client = _initialize_internal_kv()
    if gcs_client is not None:
        return gcs_client.kv_keys(prefix)
    else:
        if isinstance(prefix, bytes):
            pattern = prefix + b"*"
        else:
            pattern = prefix + "*"
        return ray.worker.global_worker.redis_client.keys(pattern=pattern)
