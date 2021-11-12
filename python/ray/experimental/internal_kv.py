from typing import List, Union, Optional, Tuple

from ray._private.client_mode_hook import client_mode_hook
from ray._private.gcs_utils import GcsClient

_initialized = False
global_gcs_client = None


def _internal_kv_reset():
    global global_gcs_client, _initialized
    global_gcs_client = None
    _initialized = False


def _internal_kv_get_gcs_client():
    return global_gcs_client


def _initialize_internal_kv(gcs_client: GcsClient):
    """Initialize the internal KV for use in other function calls.
    """
    global global_gcs_client, _initialized
    global_gcs_client = gcs_client
    _initialized = True


# b'@:' will be the leading characters for namespace
# If the key in storage has this, it'll contain namespace
__NS_START_CHAR = b"@"


def __make_key(namespace: Optional[str], key: bytes) -> bytes:
    if namespace is None:
        if key.startswith(__NS_START_CHAR + b":"):
            raise ValueError("key is not allowed to start with '@:'")
        return key
    assert isinstance(namespace, str)
    assert isinstance(key, bytes)
    return b":".join([__NS_START_CHAR, namespace.encode(), key])


def __parse_key(key: bytes) -> Tuple[Optional[str], bytes]:
    assert isinstance(key, bytes)
    if not key.startswith(__NS_START_CHAR + b":"):
        return (None, key)

    _, ns, key = key.split(b":", 2)
    return (ns.decode(), key)


@client_mode_hook(auto_init=False)
def _internal_kv_initialized():
    return global_gcs_client is not None


@client_mode_hook(auto_init=False)
def _internal_kv_get(key: Union[str, bytes],
                     *,
                     namespace: Optional[str] = None) -> bytes:
    """Fetch the value of a binary key."""

    if isinstance(key, str):
        key = key.encode()
    assert isinstance(key, bytes)
    key = __make_key(namespace, key)
    return global_gcs_client.internal_kv_get(key)


@client_mode_hook(auto_init=False)
def _internal_kv_exists(key: Union[str, bytes],
                        *,
                        namespace: Optional[str] = None) -> bool:
    """Check key exists or not."""

    if isinstance(key, str):
        key = key.encode()
    assert isinstance(key, bytes)
    key = __make_key(namespace, key)
    return global_gcs_client.internal_kv_exists(key)


@client_mode_hook(auto_init=False)
def _internal_kv_put(key: Union[str, bytes],
                     value: Union[str, bytes],
                     overwrite: bool = True,
                     *,
                     namespace: Optional[str] = None) -> bool:
    """Globally associates a value with a given binary key.

    This only has an effect if the key does not already have a value.

    Returns:
        already_exists (bool): whether the value already exists.
    """

    if isinstance(key, str):
        key = key.encode()
    if isinstance(value, str):
        value = value.encode()
    key = __make_key(namespace, key)
    assert isinstance(key, bytes) and isinstance(value, bytes) and isinstance(
        overwrite, bool)
    return global_gcs_client.internal_kv_put(key, value, overwrite) == 0


@client_mode_hook(auto_init=False)
def _internal_kv_del(key: Union[str, bytes],
                     *,
                     namespace: Optional[str] = None):
    if isinstance(key, str):
        key = key.encode()
    key = __make_key(namespace, key)
    assert isinstance(key, bytes)
    return global_gcs_client.internal_kv_del(key)


@client_mode_hook(auto_init=False)
def _internal_kv_list(prefix: Union[str, bytes],
                      *,
                      namespace: Optional[str] = None) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix.
    """
    if isinstance(prefix, str):
        prefix = prefix.encode()
    prefix = __make_key(namespace, prefix)
    return [
        __parse_key(key)[1]
        for key in global_gcs_client.internal_kv_keys(prefix)
    ]
