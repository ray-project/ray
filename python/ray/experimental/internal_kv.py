from typing import List, Optional, Union

from ray._private.client_mode_hook import client_mode_hook
from ray._raylet import GcsClient

_initialized = False
global_gcs_client = None


def _internal_kv_reset():
    global global_gcs_client, _initialized
    global_gcs_client = None
    _initialized = False


def internal_kv_get_gcs_client():
    return global_gcs_client


def _initialize_internal_kv(gcs_client: GcsClient):
    """Initialize the internal KV for use in other function calls."""
    global global_gcs_client, _initialized
    assert gcs_client is not None
    global_gcs_client = gcs_client
    _initialized = True


@client_mode_hook
def _internal_kv_initialized():
    return global_gcs_client is not None


@client_mode_hook
def _internal_kv_get(
    key: Union[str, bytes], *, namespace: Optional[Union[str, bytes]] = None
) -> bytes:
    """Fetch the value of a binary key."""

    if isinstance(key, str):
        key = key.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()
    assert isinstance(key, bytes)
    return global_gcs_client.internal_kv_get(key, namespace)


@client_mode_hook
def _internal_kv_exists(
    key: Union[str, bytes], *, namespace: Optional[Union[str, bytes]] = None
) -> bool:
    """Check key exists or not."""

    if isinstance(key, str):
        key = key.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()
    assert isinstance(key, bytes)
    return global_gcs_client.internal_kv_exists(key, namespace)


@client_mode_hook
def _pin_runtime_env_uri(uri: str, *, expiration_s: int) -> None:
    """Pin a runtime_env URI for expiration_s."""
    return global_gcs_client.pin_runtime_env_uri(uri, expiration_s)


@client_mode_hook
def _internal_kv_put(
    key: Union[str, bytes],
    value: Union[str, bytes],
    overwrite: bool = True,
    *,
    namespace: Optional[Union[str, bytes]] = None,
) -> bool:
    """Globally associates a value with a given binary key.

    Args:
        key: The binary key to associate the value with.
        value: The binary value to store under the key.
        overwrite: Whether to overwrite an existing value for the key. If
            False and the key already exists, the existing value is left
            unchanged.
        namespace: Optional namespace under which the key is scoped.

    Returns:
        True if the key already existed prior to this call; False if this
        call newly created the key. When ``overwrite=False``, True means
        the value was therefore NOT written.

        This polarity is historical and easy to misread as "put succeeded".
        New compare-and-set callers should use
        ``_internal_kv_put_if_absent``, which returns True iff this call
        created the key.
    """

    if isinstance(key, str):
        key = key.encode()
    if isinstance(value, str):
        value = value.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()
    assert (
        isinstance(key, bytes)
        and isinstance(value, bytes)
        and isinstance(overwrite, bool)
    )
    return global_gcs_client.internal_kv_put(key, value, overwrite, namespace) == 0


def _internal_kv_put_if_absent(
    key: Union[str, bytes],
    value: Union[str, bytes],
    *,
    namespace: Optional[Union[str, bytes]] = None,
) -> bool:
    """Put ``value`` only if ``key`` does not already exist.

    Compare-and-set helper for callers that want a natural "did I create
    the key?" return value. Unlike ``_internal_kv_put(..., overwrite=False)``,
    True means this call won the race and wrote the value.

    Args:
        key: The binary key to associate the value with.
        value: The binary value to store under the key.
        namespace: Optional namespace under which the key is scoped.

    Returns:
        True if this call created the key; False if the key already existed
        and nothing was written.
    """
    # invert the legacy "already existed" return from `_internal_kv_put`.
    # no @client_mode_hook here: `_internal_kv_put` already redirects under
    # ray client, so this wrapper stays correct in both modes.
    return not _internal_kv_put(key, value, overwrite=False, namespace=namespace)


@client_mode_hook
def _internal_kv_del(
    key: Union[str, bytes],
    *,
    del_by_prefix: bool = False,
    namespace: Optional[Union[str, bytes]] = None,
) -> int:
    if isinstance(key, str):
        key = key.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()
    assert isinstance(key, bytes)
    return global_gcs_client.internal_kv_del(key, del_by_prefix, namespace)


@client_mode_hook
def _internal_kv_list(
    prefix: Union[str, bytes], *, namespace: Optional[Union[str, bytes]] = None
) -> List[bytes]:
    """List all keys in the internal KV store that start with the prefix."""
    if isinstance(prefix, str):
        prefix = prefix.encode()
    if isinstance(namespace, str):
        namespace = namespace.encode()
    return global_gcs_client.internal_kv_keys(prefix, namespace)
