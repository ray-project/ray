"""
Cache placement strategy for Ray Data caching.

This module determines where to cache objects based on their size:
- Small objects (< 50KB): Local memory cache (fastest access)
- Medium objects (50KB - 10MB): Ray object store (automatic spilling to disk)
- Large objects (> 10MB): Not cached (too expensive)

The strategy is simple and effective:
1. Count, schema, column lists → Local cache
2. MaterializedDataset → Ray object store
3. Very large results → Skip caching
"""

from enum import Enum
from typing import Any

from .constants import LOCAL_CACHE_THRESHOLD_BYTES, RAY_CACHE_THRESHOLD_BYTES
from .size_utils import get_object_size


class CacheStrategy(Enum):
    """Cache placement strategies.

    Attributes:
        LOCAL: Cache in local Python memory for instant access (best for small objects)
        RAY: Cache in Ray object store with automatic disk spilling (good for medium objects)
        NONE: Don't cache (for large objects that would waste memory)
    """

    LOCAL = "local"  # Local memory cache (fastest)
    RAY = "ray"  # Ray object store (automatic spilling)
    NONE = "none"  # Don't cache (too large or unsafe)


def get_cache_strategy(operation_name: str, result: Any) -> CacheStrategy:
    """Determine where to cache a result based on its size.

    This is the core decision function for cache placement. It uses simple
    size thresholds to decide between local memory, Ray object store, or
    no caching.

    Decision Logic:
        - Size < 50KB → LOCAL cache (instant access)
        - 50KB ≤ Size < 10MB → RAY cache (with disk spilling)
        - Size ≥ 10MB → NONE (too expensive to cache)

    Examples:
        - count() result (int, ~28 bytes) → LOCAL
        - schema() result (~5KB) → LOCAL
        - columns() list (~1KB) → LOCAL
        - MaterializedDataset (ObjectRefs, ~100KB) → RAY
        - Large pandas DataFrame (>10MB) → NONE (don't cache)

    Args:
        operation_name: Name of the operation (e.g., "count", "schema", "materialize")
                       Currently unused but available for future operation-specific logic
        result: The object to be cached (will be measured for size)

    Returns:
        CacheStrategy enum indicating where to cache (LOCAL, RAY, or NONE)

    Note:
        For Ray objects (MaterializedDataset), we only measure the ObjectRef
        structure size, not the actual data size, so they're usually cached.
    """

    # Measure the size of the result object
    # For Ray objects, this returns only the pointer size, not data size
    size_bytes = get_object_size(result)

    # Small objects go to local Python memory cache (fastest access)
    if size_bytes < LOCAL_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.LOCAL

    # Medium objects go to Ray object store (automatic disk spilling)
    elif size_bytes < RAY_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.RAY

    # Large objects don't get cached (would waste memory)
    else:
        return CacheStrategy.NONE
