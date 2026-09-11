import os
from functools import lru_cache
from inspect import signature
from typing import Any, Callable, Dict, List, Optional, Tuple

from packaging.version import InvalidVersion, Version

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

    assert table_id is not None
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return None

    from lance import LanceNamespaceStorageOptionsProvider

    return LanceNamespaceStorageOptionsProvider(namespace=namespace, table_id=table_id)


def get_lance_namespace_kwargs(
    target: Callable[..., Any],
    namespace_impl: Optional[str],
    namespace_properties: Optional[Dict[str, str]],
    table_id: Optional[List[str]],
) -> Dict[str, Any]:
    """Return the namespace arguments accepted by a Lance API target.

    Lance 6 replaced ``storage_options_provider`` with ``namespace_client`` and
    ``table_id``. Keep supporting both APIs, and omit namespace arguments
    altogether for regular URI writes.
    """
    if not _has_namespace_params(namespace_impl, table_id):
        return {}

    assert table_id is not None
    namespace = get_or_create_namespace(namespace_impl, namespace_properties)
    if namespace is None:
        return {}

    try:
        has_namespace_client = "namespace_client" in signature(target).parameters
    except (TypeError, ValueError):
        docstring = getattr(target, "__doc__", "") or ""
        if "namespace_client" in docstring:
            has_namespace_client = True
        elif "storage_options_provider" in docstring:
            has_namespace_client = False
        else:
            import lance

            try:
                has_namespace_client = Version(
                    getattr(lance, "__version__", "")
                ) >= Version("6.0.0")
            except (InvalidVersion, TypeError):
                has_namespace_client = False

    if has_namespace_client:
        return {"namespace_client": namespace, "table_id": table_id}

    return {
        "storage_options_provider": create_storage_options_provider(
            namespace_impl, namespace_properties, table_id
        )
    }
