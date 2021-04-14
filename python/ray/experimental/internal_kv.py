import ray
import grpc
import os
from typing import List, Union

from ray._private.client_mode_hook import client_mode_hook

redis = os.environ.get("RAY_KV_USE_GCS", "0") == "0"


@client_mode_hook
def _internal_kv_initialized():
    global redis
    worker = ray.worker.global_worker
    if not (hasattr(worker, "mode") and worker.mode is not None):
        return False
    if not redis:
        try:
            _internal_kv_exists("dummy")
        except grpc.RpcError:
            redis = True
    return True


@client_mode_hook
def _internal_kv_get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""
    if redis:
        return ray.worker.global_worker.redis_client.hget(key, "value")
    else:
        return ray.worker.global_worker.gcs_client.kv_get(key)


@client_mode_hook
def _internal_kv_exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""
    if redis:
        return ray.worker.global_worker.redis_client.hexists(key, "value")
    else:
        return ray.worker.global_worker.gcs_client.kv_exists(key)


@client_mode_hook
def _internal_kv_put(key: Union[str, bytes],
                     value: Union[str, bytes],
                     overwrite: bool = True) -> bool:
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns:
        already_exists (bool): whether the value already exists.
    """
    if redis:
        if overwrite:
            updated = ray.worker.global_worker.redis_client.hset(
                key, "value", value)
        else:
            updated = ray.worker.global_worker.redis_client.hsetnx(
                key, "value", value)
        return updated == 0  # already exists
    else:
        return not ray.worker.global_worker.gcs_client.kv_put(
            key, value, overwrite)


@client_mode_hook
def _internal_kv_del(key: Union[str, bytes]):
    if redis:
        return ray.worker.global_worker.redis_client.delete(key)
    else:
        return ray.worker.global_worker.gcs_client.kv_del(key)


@client_mode_hook
def _internal_kv_list(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix."""
    if redis:
        if isinstance(prefix, bytes):
            pattern = prefix + b"*"
        else:
            pattern = prefix + "*"
        return ray.worker.global_worker.redis_client.keys(pattern=pattern)
    else:
        return ray.worker.global_worker.gcs_client.kv_keys(prefix)
