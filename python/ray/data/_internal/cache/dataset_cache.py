"""
Public API and decorators for Ray Data caching.

This module provides the user-facing caching API and decorators used
internally by Dataset methods. It wraps the core DatasetCache implementation
with convenient decorators and utility functions.

Public API Functions:
    - clear_dataset_cache(): Clear all cached results
    - get_cache_stats(): Get cache performance statistics
    - disable_dataset_caching(): Context manager to temporarily disable caching

Decorators (for internal use):
    - @cache_result: Decorator to cache Dataset operation results
    - @invalidate_cache_on_transform: Decorator to update cache after transformations

Example Usage:
    ```python
    import ray.data as rd

    # Use datasets normally - caching is automatic
    ds = ray.data.range(100)
    count1 = ds.count()  # Executes and caches
    count2 = ds.count()  # Returns cached result

    # Check cache stats
    stats = rd.get_cache_stats()
    print(f"Hit rate: {stats['hit_rate']:.2%}")

    # Temporarily disable caching
    with rd.disable_dataset_caching():
        count3 = ds.count()  # Not cached
    ```
"""

import functools
from typing import Any, Callable, Dict, List, Optional

from .constants import CacheableOperation
from .core_cache import CacheStats, DatasetCache
from ray.data.context import DataContext

# =============================================================================
# GLOBAL CACHE INSTANCE
# =============================================================================

# Single global cache instance shared by all Datasets
# This is initialized lazily on first use
_global_cache: Optional[DatasetCache] = None


def _get_cache() -> DatasetCache:
    """Get or create the global cache instance.

    This function ensures we have a single global cache instance that's
    properly configured from the DataContext.

    Returns:
        The global DatasetCache instance

    Note:
        The cache is initialized lazily on first use to avoid startup overhead
        and to ensure DataContext is available.
    """
    global _global_cache
    if _global_cache is None:
        # Import here to avoid circular dependencies
        from .core_cache import CacheConfiguration
        from ray.data.context import DataContext

        # Get configuration from DataContext
        context = DataContext.get_current()
        config = CacheConfiguration.from_data_context(context)

        # Create global cache instance
        _global_cache = DatasetCache(config)

    return _global_cache


# =============================================================================
# CACHE_RESULT DECORATOR
# =============================================================================


def cache_result(operation_name: str, include_params: Optional[List[str]] = None):
    """Decorator to cache Dataset operation results.

    This decorator is applied to Dataset methods that return expensive-to-compute
    results. It automatically caches results and returns cached values when available.

    Args:
        operation_name: Name of the operation (e.g., "count", "schema")
        include_params: List of parameter names to include in cache key
                       (e.g., ["column"] for sum(column))

    Returns:
        Decorator function

    Usage:
        ```python
        @cache_result("count")
        def count(self) -> int:
            return self._execute_count()

        @cache_result("sum", include_params=["on"])
        def sum(self, on: str) -> float:
            return self._execute_sum(on)
        ```

    How It Works:
        1. Check if caching is enabled (DataContext.enable_dataset_caching)
        2. Extract parameters from both args and kwargs (for cache key)
        3. Try to get result from cache
        4. If cache miss, execute function and cache result
        5. Return result (cached or newly computed)

    Example:
        ds = ray.data.range(100)
        count1 = ds.count()  # Cache miss → executes, caches, returns 100
        count2 = ds.count()  # Cache hit → returns 100 instantly!
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # -----------------------------------------------------------------
            # Check if caching is enabled
            # -----------------------------------------------------------------
            context = DataContext.get_current()
            if not getattr(context, "enable_dataset_caching", True):
                # Caching disabled, just execute function normally
                return func(self, *args, **kwargs)

            # -----------------------------------------------------------------
            # Extract parameters for cache key
            # -----------------------------------------------------------------
            # We need to extract parameters from both positional args and
            # keyword args to create a unique cache key.
            cache_params = {}
            if include_params:
                for i, param_name in enumerate(include_params):
                    # First try positional args (by index)
                    if i < len(args):
                        cache_params[param_name] = args[i]
                    # Then try keyword args (by name)
                    elif param_name in kwargs:
                        cache_params[param_name] = kwargs[param_name]

            # -----------------------------------------------------------------
            # Try cache first
            # -----------------------------------------------------------------
            cache = _get_cache()
            cached_result = cache.get(
                self._logical_plan, operation_name, **cache_params
            )
            if cached_result is not None:
                # Cache hit! Return cached value
                return cached_result

            # -----------------------------------------------------------------
            # Cache miss - execute function and cache result
            # -----------------------------------------------------------------
            result = func(self, *args, **kwargs)
            cache.put(self._logical_plan, operation_name, result, **cache_params)
            return result

        return wrapper

    return decorator


# =============================================================================
# INVALIDATE_CACHE_ON_TRANSFORM DECORATOR
# =============================================================================


def invalidate_cache_on_transform(operation_name: str):
    """Decorator to update cache when transformations are applied.

    This decorator is applied to Dataset transformation methods (map, filter,
    limit, etc.). It updates the cache to preserve or compute new values
    based on the transformation type.

    Args:
        operation_name: Name of the transformation (e.g., "map", "limit")

    Returns:
        Decorator function

    Usage:
        ```python
        @invalidate_cache_on_transform("limit")
        def limit(self, limit: int) -> "Dataset":
            # ... create new dataset with limit ...
            return new_dataset
        ```

    How It Works:
        1. Execute transformation function (creates new dataset)
        2. If caching enabled, call cache.invalidate_for_transform()
        3. Smart updater preserves/computes cache values based on operation type
        4. Return new dataset

    Examples:
        # Reordering operation - preserves everything!
        ds = ray.data.range(100)
        ds.count()  # Caches count=100
        sorted_ds = ds.sort("id")
        sorted_ds.count()  # Returns 100 instantly (preserved!)

        # Limit operation - smart-computes count
        ds = ray.data.range(100)
        ds.count()  # Caches count=100
        limited_ds = ds.limit(10)
        limited_ds.count()  # Returns 10 instantly (computed: min(100, 10))
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Save original logical plan
            original_plan = self._logical_plan

            # Execute transformation (creates new dataset)
            result = func(self, *args, **kwargs)

            # -----------------------------------------------------------------
            # Update cache if enabled
            # -----------------------------------------------------------------
            context = DataContext.get_current()
            if getattr(context, "enable_dataset_caching", True) and hasattr(
                result, "_logical_plan"
            ):
                cache = _get_cache()

                # Package transformation parameters
                # We pass both args and kwargs so smart updater can extract
                # operation-specific parameters (e.g., limit value, column names)
                transform_params = {"args": args, "kwargs": kwargs}

                # Delegate to smart updater for cache preservation/computation
                cache.invalidate_for_transform(
                    operation_name,
                    original_plan,
                    result._logical_plan,
                    **transform_params,
                )

            return result

        return wrapper

    return decorator


# =============================================================================
# PUBLIC API FUNCTIONS
# =============================================================================


def clear_dataset_cache() -> None:
    """Clear all cached Dataset results.

    This removes all entries from both local and Ray caches. Use this if:
    - You want to free memory
    - You suspect cached values are stale
    - You're debugging cache behavior

    Example:
        >>> import ray.data as rd
        >>> ds = ray.data.range(100)
        >>> ds.count()  # Caches result
        100
        >>> rd.clear_dataset_cache()  # Clear all cached results
        >>> ds.count()  # Recomputes (cache was cleared)
        100
    """
    _get_cache().clear()


def get_cache_stats() -> Dict[str, Any]:
    """Get Dataset cache statistics.

    Returns a dictionary with cache performance metrics:
    - hit_count: Number of cache hits
    - miss_count: Number of cache misses
    - hit_rate: Cache hit rate (0.0 to 1.0)
    - local_entries: Number of entries in local cache
    - ray_entries: Number of entries in Ray cache
    - total_entries: Total number of cached entries

    Returns:
        Dictionary with cache statistics

    Example:
        >>> import ray.data as rd
        >>> ds = ray.data.range(100)
        >>> ds.count()
        100
        >>> ds.count()  # Cache hit
        100
        >>> stats = rd.get_cache_stats()
        >>> print(f"Hit rate: {stats['hit_rate']:.2%}")
        Hit rate: 50.00%
    """
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
    """Context manager to temporarily disable dataset caching.

    This is useful for testing or when you want to ensure operations are
    executed fresh without using cached values.

    Returns:
        Context manager that disables caching within its scope

    Example:
        >>> import ray.data as rd
        >>> ds = ray.data.range(100)
        >>> ds.count()  # Cached
        100
        >>> ds.count()  # Uses cache
        100
        >>> with rd.disable_dataset_caching():
        ...     ds.count()  # Not cached (recomputed)
        100
        >>> ds.count()  # Uses cache again
        100
    """

    class _DisableCache:
        """Internal context manager class for disabling caching."""

        def __enter__(self):
            """Disable caching on context entry."""
            context = DataContext.get_current()
            # Save current state
            self._old_enabled = getattr(context, "enable_dataset_caching", True)
            # Disable caching
            context.enable_dataset_caching = False
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            """Restore caching state on context exit."""
            context = DataContext.get_current()
            # Restore previous state
            context.enable_dataset_caching = self._old_enabled

    return _DisableCache()


# =============================================================================
# EXPORTS
# =============================================================================

# Re-export for backward compatibility and convenience
__all__ = [
    # Core classes
    "DatasetCache",
    "CacheStats",
    "CacheableOperation",
    # Decorators
    "cache_result",
    "invalidate_cache_on_transform",
    # Public API functions
    "clear_dataset_cache",
    "get_cache_stats",
    "disable_dataset_caching",
]
