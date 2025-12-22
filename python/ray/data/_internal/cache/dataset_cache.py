"""Public API and decorators for Ray Data caching.

This module provides decorators for caching Dataset operation results and
invalidating cache entries when transformations are applied.
"""

import functools
from typing import Any, Callable, Dict, List, Optional

from ray.data._internal.cache.core_cache import CacheStats, DatasetCache
from ray.data.context import DataContext

_global_cache: Optional[DatasetCache] = None


def _get_cache() -> DatasetCache:
    """Get or create the global cache instance."""
    global _global_cache
    if _global_cache is None:
        from ray.data._internal.cache.core_cache import CacheConfiguration
        from ray.data.context import DataContext

        context = DataContext.get_current()
        config = CacheConfiguration.from_data_context(context)
        _global_cache = DatasetCache(config)

    return _global_cache


def cache_result(operation_name: str, include_params: Optional[List[str]] = None):
    """Decorator to cache Dataset operation results.

    Caches the result of a Dataset operation based on the logical plan and
    operation parameters. If the same operation is called again with the same
    logical plan, the cached result is returned.

    The decorator checks the DataContext to determine if caching is enabled.
    If disabled, the original function is called without caching.

    Args:
        operation_name: Name of the operation (e.g., "count", "schema").
        include_params: List of parameter names to include in cache key.
                       For example, ["limit"] for take(limit=10).

    Returns:
        Decorator function that wraps the original method.

    Example:
        >>> @cache_result("count")
        ... def count(self):
        ...     return self._count()
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            context = DataContext.get_current()
            if not getattr(context, "enable_dataset_caching", True):
                return func(self, *args, **kwargs)

            cache_params = {}
            if include_params:
                for i, param_name in enumerate(include_params):
                    if i < len(args):
                        cache_params[param_name] = args[i]
                    elif param_name in kwargs:
                        cache_params[param_name] = kwargs[param_name]

            cache = _get_cache()
            cached_result = cache.get(
                self._logical_plan, operation_name, **cache_params
            )
            if cached_result is not None:
                return cached_result

            result = func(self, *args, **kwargs)
            cache.put(self._logical_plan, operation_name, result, **cache_params)
            return result

        return wrapper

    return decorator


def invalidate_cache_on_transform(operation_name: str):
    """Decorator to update cache when transformations are applied.

    When a Dataset transformation is applied, this decorator updates the cache
    by preserving valid entries and computing new values where possible.

    The decorator checks the DataContext to determine if caching is enabled.
    It also verifies that the result has a _logical_plan attribute before
    attempting cache invalidation.

    Args:
        operation_name: Name of the transformation operation.

    Returns:
        Decorator function that wraps the original method.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            original_plan = self._logical_plan
            result = func(self, *args, **kwargs)

            context = DataContext.get_current()
            if getattr(context, "enable_dataset_caching", True) and hasattr(
                result, "_logical_plan"
            ):
                cache = _get_cache()
                transform_params = {"args": args, "kwargs": kwargs}
                cache.invalidate_for_transform(
                    operation_name,
                    original_plan,
                    result._logical_plan,
                    **transform_params,
                )

            return result

        return wrapper

    return decorator


def clear_dataset_cache() -> None:
    """Clear all cached Dataset results."""
    _get_cache().clear()


def get_cache_stats() -> Dict[str, Any]:
    """Get Dataset cache statistics."""
    stats = _get_cache().get_stats()
    return {
        "hit_count": stats.hit_count,
        "miss_count": stats.miss_count,
        "hit_rate": stats.hit_rate,
        "local_entries": stats.local_entries,
        "ray_entries": stats.ray_entries,
        "total_entries": stats.total_entries,
    }


def disable_dataset_caching():
    """Context manager to temporarily disable dataset caching.

    Useful for testing or when you need to bypass caching for specific operations.
    The previous caching state is restored when exiting the context.

    Returns:
        Context manager that disables caching within its scope.

    Example:
        >>> with disable_dataset_caching():
        ...     result = dataset.count()  # This won't be cached
    """

    class _DisableCache:
        def __enter__(self):
            context = DataContext.get_current()
            self._old_enabled = getattr(context, "enable_dataset_caching", True)
            context.enable_dataset_caching = False
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            context = DataContext.get_current()
            context.enable_dataset_caching = self._old_enabled

    return _DisableCache()


__all__ = [
    "DatasetCache",
    "CacheStats",
    "cache_result",
    "invalidate_cache_on_transform",
    "clear_dataset_cache",
    "get_cache_stats",
    "disable_dataset_caching",
]
