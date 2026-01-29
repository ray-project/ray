import os
import sys
from collections.abc import Iterable, Sequence
from functools import lru_cache
from typing import Optional, TypeVar

from lance import LanceNamespaceStorageOptionsProvider
from lance.namespace import LanceNamespace

T = TypeVar("T")

# Cache size for namespace clients per worker, configurable via environment variable
_NAMESPACE_CACHE_SIZE = int(os.environ.get("LANCE_RAY_NAMESPACE_CACHE_SIZE", "16"))


def has_namespace_params(
    namespace_impl: Optional[str],
    table_id: Optional[list[str]],
) -> bool:
    """Check if namespace parameters are provided.

    Only namespace_impl and table_id are required; namespace_properties can be None.

    Args:
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        table_id: The table identifier as a list of strings.

    Returns:
        True if both namespace_impl and table_id are provided, False otherwise.
    """
    return namespace_impl is not None and table_id is not None


def validate_uri_or_namespace(
    uri: Optional[str],
    namespace_impl: Optional[str],
    table_id: Optional[list[str]],
) -> None:
    """Validate that either uri OR (namespace_impl + table_id) is provided.

    Args:
        uri: The URI of the dataset.
        namespace_impl: The namespace implementation type.
        table_id: The table identifier.

    Raises:
        ValueError: If both uri and namespace params are provided, or neither.
    """
    has_ns = has_namespace_params(namespace_impl, table_id)

    if uri is not None and has_ns:
        raise ValueError(
            "Cannot provide both 'uri' and namespace parameters. "
            "Use either 'uri' OR ('namespace_impl' + 'table_id')."
        )

    if uri is None and not has_ns:
        raise ValueError(
            "Must provide either 'uri' OR ('namespace_impl' + 'table_id')."
        )


@lru_cache(maxsize=_NAMESPACE_CACHE_SIZE)
def _get_cached_namespace(
    namespace_impl: str,
    namespace_properties_tuple: Optional[tuple[tuple[str, str], ...]],
) -> LanceNamespace:
    """Internal cached namespace loader. Use get_or_create_namespace() instead."""
    import lance_namespace as ln

    namespace_properties = (
        dict(namespace_properties_tuple) if namespace_properties_tuple else {}
    )
    return ln.connect(namespace_impl, namespace_properties)


def get_or_create_namespace(
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
):
    """Get or create a cached namespace client.

    This function loads a namespace client from cache or creates a new one.
    The namespace client is cached per-worker using lru_cache. Module-level state
    persists across task invocations within the same Ray worker process, avoiding
    redundant network calls to recreate namespace connections.

    Args:
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        namespace_properties: Properties for connecting to the namespace (can be None).

    Returns:
        A namespace client instance, or None if namespace_impl is not provided.
    """
    if namespace_impl is None:
        return None

    # Convert dict to hashable tuple for lru_cache (None if no properties)
    namespace_properties_tuple = (
        tuple(sorted(namespace_properties.items())) if namespace_properties else None
    )
    return _get_cached_namespace(namespace_impl, namespace_properties_tuple)


def create_storage_options_provider(
    namespace_impl: Optional[str],
    namespace_properties: Optional[dict[str, str]],
    table_id: Optional[list[str]],
) -> Optional["LanceNamespaceStorageOptionsProvider"]:
    """Create a LanceNamespaceStorageOptionsProvider if namespace parameters are provided.

    This function reconstructs a namespace connection and creates a storage options
    provider for credential refresh in distributed workers. Workers receive serializable
    namespace_impl/properties/table_id instead of the non-serializable namespace object.

    The namespace client is cached per-worker to avoid redundant connection overhead
    across multiple task invocations within the same Ray worker.

    Args:
        namespace_impl: The namespace implementation type (e.g., "rest", "dir").
        namespace_properties: Properties for connecting to the namespace (can be None).
        table_id: The table identifier as a list of strings.

    Returns:
        LanceNamespaceStorageOptionsProvider if namespace_impl and table_id are provided,
        None otherwise.
    """
    if not has_namespace_params(namespace_impl, table_id):
        return None

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return None

    return LanceNamespaceStorageOptionsProvider(namespace=namespace, table_id=table_id)


if sys.version_info >= (3, 12):
    from itertools import batched

    def array_split(iterable: Iterable[T], n: int) -> list[Sequence[T]]:
        """Split iterable into n chunks."""
        items = list(iterable)
        chunk_size = (len(items) + n - 1) // n
        return list(batched(items, chunk_size))

else:
    from more_itertools import divide

    def array_split(iterable: Iterable[T], n: int) -> list[Sequence[T]]:
        return list(map(list, divide(n, iterable)))
