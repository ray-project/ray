import ray
import grpc
from typing import List, Union

from ray._private.client_mode_hook import client_mode_hook

_initialized = False


def _initialize_internal_kv(gcs_client: "ray._raylet.GcsClient" = None):
    """Initialize the internal KV for use in other function calls.

    We try to use a GCS client, either passed in here or from the Ray worker
    global scope.
    """
    global global_gcs_client, _initialized

    if not _initialized:
        if gcs_client is not None:
            global_gcs_client = gcs_client
        else:
            try:
                ray.worker.global_worker.gcs_client.kv_exists("dummy")
                global_gcs_client = ray.worker.global_worker.gcs_client
            except grpc.RpcError:
                raise OSError("Could not connect to gcs, retry later.")

    _initialized = True
    return global_gcs_client


@client_mode_hook
def _internal_kv_initialized():
    gcs_client = _initialize_internal_kv()

    return gcs_client is not None


@client_mode_hook
def _internal_kv_get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""
    gcs_client = _initialize_internal_kv()

    return gcs_client.kv_get(key)


@client_mode_hook
def _internal_kv_exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""
    gcs_client = _initialize_internal_kv()
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
    gcs_client = _initialize_internal_kv()
    return not gcs_client.kv_put(key, value, overwrite)


@client_mode_hook
def _internal_kv_del(key: Union[str, bytes]):
    gcs_client = _initialize_internal_kv()
    return gcs_client.kv_del(key)


@client_mode_hook
def _internal_kv_list(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix.
    """
    gcs_client = _initialize_internal_kv()
    return gcs_client.kv_keys(prefix)
