import ray
import grpc
import os
from typing import List, Union

from ray._private.client_mode_hook import client_mode_hook

redis = os.environ.get("RAY_KV_USE_GCS", "0") == "0"
initialized = False
global_gcs_client = None


def _initialize_internal_kv(gcs_client: "ray._raylet.GcsClient" = None):
    global global_gcs_client, initialized

    if initialized:
        return

    if gcs_client is not None:
        global_gcs_client = gcs_client
    elif not redis:
        try:
            _internal_kv_exists("dummy")
            global_gcs_client = ray.worker.global_worker.gcs_client
        except grpc.RpcError:
            pass

    initialized = True


@client_mode_hook
def _internal_kv_initialized():
    _initialize_internal_kv()

    worker = ray.worker.global_worker
    if global_gcs_client is not None:
        return True
    elif not (hasattr(worker, "mode") and worker.mode is not None):
        return False
    else:
        return True


@client_mode_hook
def _internal_kv_get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""
    _initialize_internal_kv()

    if global_gcs_client is not None:
        return global_gcs_client.kv_get(key)
    else:
        return ray.worker.global_worker.redis_client.hget(key, "value")


@client_mode_hook
def _internal_kv_exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""
    _initialize_internal_kv()
    if global_gcs_client is not None:
        return global_gcs_client.kv_exists(key)
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
    _initialize_internal_kv()
    if global_gcs_client is not None:
        return not global_gcs_client.kv_put(key, value, overwrite)
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
    _initialize_internal_kv()
    if global_gcs_client is not None:
        return global_gcs_client.kv_del(key)
    else:
        return ray.worker.global_worker.redis_client.delete(key)


@client_mode_hook
def _internal_kv_list(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix.
    """
    _initialize_internal_kv()
    if global_gcs_client is not None:
        return global_gcs_client.kv_keys(prefix)
    else:
        if isinstance(prefix, bytes):
            pattern = prefix + b"*"
        else:
            pattern = prefix + "*"
        return ray.worker.global_worker.redis_client.keys(pattern=pattern)
