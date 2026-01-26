import os
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

_NAMESPACE_CACHE_SIZE = int(os.environ.get("LANCE_RAY_NAMESPACE_CACHE_SIZE", "16"))


def _has_namespace_params(
    namespace_impl: Optional[str],
    table_id: Optional[List[str]],
) -> bool:
    return namespace_impl is not None and table_id is not None


@lru_cache(maxsize=_NAMESPACE_CACHE_SIZE)
def _get_cached_namespace(
    namespace_impl: str,
    namespace_properties_tuple: Optional[Tuple[Tuple[str, str], ...]],
):
    import lance_namespace as ln

    namespace_properties = (
        dict(namespace_properties_tuple) if namespace_properties_tuple else {}
    )
    return ln.connect(namespace_impl, namespace_properties)


def get_or_create_namespace(
    namespace_impl: Optional[str],
    namespace_properties: Optional[Dict[str, str]],
):
    """Get or create a cached namespace client for the worker."""
    if namespace_impl is None:
        return None

    namespace_properties_tuple = (
        tuple(sorted(namespace_properties.items())) if namespace_properties else None
    )
    return _get_cached_namespace(namespace_impl, namespace_properties_tuple)


def create_storage_options_provider(
    namespace_impl: Optional[str],
    namespace_properties: Optional[Dict[str, str]],
    table_id: Optional[List[str]],
):
    """Create a LanceNamespaceStorageOptionsProvider if namespace params exist."""
    if not _has_namespace_params(namespace_impl, table_id):
        return None

    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return None

    from lance import LanceNamespaceStorageOptionsProvider

    return LanceNamespaceStorageOptionsProvider(namespace=namespace, table_id=table_id)
