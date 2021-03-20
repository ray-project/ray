import ray
from typing import List, Union

from ray._private.client_mode_hook import client_mode_hook
from collections import namedtuple
from ray._raylet import connect_to_gcs, GcsClient

@client_mode_hook
def _internal_kv_initialized(target=None):
    worker = ray.worker.global_worker
    return hasattr(worker, "mode") and worker.mode is not None


@client_mode_hook
def _internal_kv_get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""
    return ray.worker.global_worker.gcs_client.kv_get(key)


@client_mode_hook
def _internal_kv_exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""
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

    return ray.worker.global_worker.gcs_client.kv_put(key, value, overwrite)


@client_mode_hook
def _internal_kv_del(key: Union[str, bytes]):
    return ray.worker.global_worker.gcs_client.kv_del(key)


@client_mode_hook
def _internal_kv_list(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix."""
    return ray.worker.global_worker.gcs_client.kv_keys(prefix)
