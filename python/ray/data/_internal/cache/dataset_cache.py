"""
Simple, elegant caching for Ray Data operations.
"""

import functools
from typing import Any, Callable, Dict, List, Optional

from .constants import CacheableOperation
from .core_cache import CacheStats, DatasetCache
from ray.data.context import DataContext

# Global cache instance
_global_cache: Optional[DatasetCache] = None


def _get_cache() -> DatasetCache:
    """Get global cache instance."""
    global _global_cache
    if _global_cache is None:
        _global_cache = DatasetCache()
    return _global_cache


def cache_result(operation_name: str, include_params: List[str] = None):
    """Decorator to cache Dataset operation results."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Check if caching is enabled
            context = DataContext.get_current()
            if not getattr(context, "enable_dataset_caching", True):
                return func(self, *args, **kwargs)

            # Simple parameter extraction
            cache_params = {}
            if include_params and args:
                # Just use positional args for simplicity
                for i, param_name in enumerate(include_params):
                    if i < len(args):
                        cache_params[param_name] = args[i]

            # Try cache first
            cache = _get_cache()
            cached_result = cache.get(
                self._logical_plan, operation_name, **cache_params
            )
            if cached_result is not None:
                return cached_result

            # Execute and cache result
            result = func(self, *args, **kwargs)
            cache.put(self._logical_plan, operation_name, result, **cache_params)
            return result

        return wrapper

    return decorator


def invalidate_cache_on_transform(operation_name: str):
    """Decorator to update cache when transformations are applied."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            original_plan = self._logical_plan
            result = func(self, *args, **kwargs)

            # Update cache if enabled
            context = DataContext.get_current()
            if getattr(context, "enable_dataset_caching", True) and hasattr(
                result, "_logical_plan"
            ):

                cache = _get_cache()
                # Simple parameter extraction
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


# Public API functions
def clear_dataset_cache() -> None:
    """Clear all cached Dataset results."""
    _get_cache().clear()


def get_cache_stats() -> Dict[str, Any]:
    """Get Dataset cache statistics."""
    stats = _get_cache().get_stats()
    # Convert dataclass to dict for backward compatibility
    return {
        "hit_count": stats.hit_count,
        "miss_count": stats.miss_count,
        "hit_rate": stats.hit_rate,
        "local_entries": stats.local_entries,
        "ray_entries": stats.ray_entries,
        "total_entries": stats.total_entries,
    }


def disable_dataset_caching():
    """Context manager to temporarily disable dataset caching."""

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


# Re-export for backward compatibility
__all__ = [
    "DatasetCache",
    "CacheStats",
    "CacheableOperation",
    "cache_result",
    "invalidate_cache_on_transform",
    "clear_dataset_cache",
    "get_cache_stats",
    "disable_dataset_caching",
]
