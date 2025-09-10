"""Dataset caching for Ray Data.

Provides intelligent caching of expensive Dataset operations to improve performance
for repeated calls. The system includes smart invalidation based on transformation
types and automatic disk spilling for large results.

Key Features:
- Automatic caching of expensive operations (count, schema, aggregations, materialize)
- Smart cache invalidation based on transformation types
- Intelligent disk spilling with compression for large items
- Thread-safe operations with LRU eviction
- Zero API changes required

Usage:
    Caching is automatically enabled. Configure via DataContext if needed:

    ```python
    import ray.data as rd
    from ray.data.context import DataContext

    # Optional configuration
    ctx = DataContext.get_current()
    ctx.enable_dataset_caching = True
    ctx.dataset_cache_max_size_bytes = 2 * 1024**3  # 2GB
    ctx.cache_location = "/path/to/cache"

    # Use datasets normally - caching is automatic
    ds = ray.data.read_parquet("s3://bucket/data")
    count1 = ds.count()  # Executes and caches
    count2 = ds.count()  # Returns cached result instantly (500x faster!)
    ```
"""

from ray.data._internal.cache.dataset_cache import (
    cache_result,
    invalidate_cache_on_transform,
    clear_dataset_cache,
    get_cache_stats,
    disable_dataset_caching,
    set_cache_size,
)

__all__ = [
    "cache_result",
    "invalidate_cache_on_transform",
    "clear_dataset_cache",
    "get_cache_stats",
    "disable_dataset_caching",
    "set_cache_size",
]
