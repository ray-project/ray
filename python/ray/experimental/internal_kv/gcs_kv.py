import ray
from typing import List, Union

from ray._private.client_mode_hook import client_mode_hook
from collections import namedtuple
from ray._raylet import connect_to_gcs, GcsClient

gcs_client = None

Target = namedtuple("Target", ["ip", "port", "password"])


@client_mode_hook
def _internal_kv_initialized(target=None):
    global gcs_client
    if gcs_client is not None:
        return True
    if target is None:
        worker = ray.worker.global_worker
        if worker.core_worker is not None:
            gcs_client = worker.core_worker.get_gcs_client()
        print(gcs_client)
    elif isinstance(target, GcsClient):
        gcs_client = target
    else:
        gcs_client = connect_to_gcs(target.ip, str(target.port),
                                    target.password)
    return gcs_client is not None


@client_mode_hook
def _internal_kv_get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""
    return gcs_client.kv_get(key)


@client_mode_hook
def _internal_kv_exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""
    return gcs_client.kv_exists(key)


@client_mode_hook
def _internal_kv_put(key: Union[str, bytes],
                     value: Union[str, bytes],
                     overwrite: bool = True) -> bool:
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns:
        already_exists (bool): whether the value already exists.
    """

    return gcs_client.kv_put(key, value, overwrite)


@client_mode_hook
def _internal_kv_del(key: Union[str, bytes]):
    return gcs_client.kv_del(key)


@client_mode_hook
def _internal_kv_list(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix."""
    return gcs_client.kv_keys(prefix)
