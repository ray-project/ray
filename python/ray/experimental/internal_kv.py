from typing import List, Union
from ray._private.utils import deprecated
from ray._private.client_mode_hook import client_mode_hook
from ray._private.gcs_utils import GcsClient

_initialized = False
global_gcs_client = None


def reset():
    global global_gcs_client, _initialized
    global_gcs_client = None
    _initialized = False


def get_gcs_client():
    return global_gcs_client


def initialize(gcs_client: GcsClient):
    """Initialize the internal KV for use in other function calls.
    """
    global global_gcs_client, _initialized
    global_gcs_client = gcs_client
    _initialized = True


@client_mode_hook(auto_init=False)
def is_initialized():
    return global_gcs_client is not None


@client_mode_hook(auto_init=False)
def get(key: Union[str, bytes]) -> bytes:
    """Fetch the value of a binary key."""

    if isinstance(key, str):
        key = key.encode()
    assert isinstance(key, bytes)
    return global_gcs_client.internal_kv_get(key)


@client_mode_hook(auto_init=False)
def exists(key: Union[str, bytes]) -> bool:
    """Check key exists or not."""

    if isinstance(key, str):
        key = key.encode()
    assert isinstance(key, bytes)
    return global_gcs_client.internal_kv_exists(key)


@client_mode_hook(auto_init=False)
def put(key: Union[str, bytes],
        value: Union[str, bytes],
        overwrite: bool = True) -> bool:
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns:
        already_exists (bool): whether the value already exists.
    """

    if isinstance(key, str):
        key = key.encode()

    if isinstance(value, str):
        value = value.encode()

    assert isinstance(key, bytes) and isinstance(value, bytes) and isinstance(
        overwrite, bool)
    return global_gcs_client.internal_kv_put(key, value, overwrite) == 0


@client_mode_hook(auto_init=False)
def delete(key: Union[str, bytes]):
    if isinstance(key, str):
        key = key.encode()
    assert isinstance(key, bytes)
    return global_gcs_client.internal_kv_del(key)


@client_mode_hook(auto_init=False)
def keys(prefix: Union[str, bytes]) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix.
    """
    if isinstance(prefix, str):
        prefix = prefix.encode()
    return global_gcs_client.internal_kv_keys(prefix)


_internal_kv_reset = deprecated(
    reset,
    "Plase use `internal_kv.reset`.",
    removal_release="2.0",
    warn_once=True)
_internal_kv_get_gcs_client = deprecated(
    get_gcs_client,
    "Please use `internal_kv.get_gcs_client`.",
    removal_release="2.0",
    warn_once=True)
_initialize_internal_kv = deprecated(
    initialize,
    "Please use `internal_kv.initialize`.",
    removal_release="2.0",
    warn_once=True)
_internal_kv_initialized = deprecated(
    is_initialized,
    "Please use `internal_kv.is_initialized`.",
    removal_release="2.0",
    warn_once=True)
_internal_kv_get = deprecated(
    get,
    "Please use `internal_kv.get`.",
    removal_release="2.0",
    warn_once=True)
_internal_kv_exists = deprecated(
    exists,
    "Please use `internal_kv.exists`",
    removal_release="2.0",
    warn_once=True)
_internal_kv_put = deprecated(
    put, "Please use `internal_kv.put`", removal_release="2.0", warn_once=True)
_internal_kv_del = deprecated(
    delete,
    "Please use `internal_kv.delete`",
    removal_release="2.0",
    warn_once=True)
_internal_kv_list = deprecated(
    keys,
    "Please use `internal_kv.keys`",
    removal_release="2.0",
    warn_once=True)
