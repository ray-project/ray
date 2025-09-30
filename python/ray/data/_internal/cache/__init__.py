"""Dataset caching for Ray Data.

Provides intelligent caching of expensive Dataset operations to improve performance
for repeated calls. The system includes smart invalidation based on transformation
types and automatic disk spilling for large results.

Key Features:
- Automatic caching of expensive operations (count, schema, aggregations, materialize)
- Smart cache invalidation based on transformation types
- Size-aware cache placement (local memory vs Ray object store)
- Smart cache updates that compute new values from existing ones
- Proper Ray dataset handling (caches ObjectRef pointers, not data)
- Zero API changes required

Architecture:
The caching system is split into logical modules:
- constants.py: Enums and transformation type mappings
- size_utils.py: Object size calculation with Ray dataset awareness
- validation.py: Cache value validation and consistency checking
- key_generation.py: Stable cache key generation from logical plans
- cache_strategies.py: Cache placement strategy decisions
- smart_updates.py: Smart cache update logic for transformations
- core_cache.py: Main DatasetCache class
- dataset_cache.py: Public API and decorators

Usage:
    Caching is automatically enabled. Configure via DataContext if needed:

    ```python
    import ray.data as rd
    from ray.data.context import DataContext

    # Optional configuration
    ctx = DataContext.get_current()
    ctx.enable_dataset_caching = True
    ctx.dataset_cache_max_size_bytes = 2 * 1024**3  # 2GB

    # Use datasets normally - caching is automatic
    ds = ray.data.read_parquet("s3://bucket/data")
    count1 = ds.count()  # Executes and caches
    count2 = ds.count()  # Returns cached result instantly!

    # Ray datasets are cached as ObjectRef pointers
    mat1 = ds.materialize()  # Materializes and caches ObjectRef structure
    mat2 = ds.materialize()  # Returns cached ObjectRef structure instantly
    ```
"""

from ray.data._internal.cache.dataset_cache import (
    cache_result,
    clear_dataset_cache,
    disable_dataset_caching,
    get_cache_stats,
    invalidate_cache_on_transform,
)

__all__ = [
    "cache_result",
    "invalidate_cache_on_transform",
    "clear_dataset_cache",
    "get_cache_stats",
    "disable_dataset_caching",
]
