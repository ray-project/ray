"""
Simple cache placement strategy for Ray Data caching.
"""

from enum import Enum
from typing import Any

from .constants import LOCAL_CACHE_THRESHOLD_BYTES, RAY_CACHE_THRESHOLD_BYTES
from .size_utils import get_object_size


class CacheStrategy(Enum):
    """Cache placement strategies."""

    LOCAL = "local"  # Cache in local memory (fastest)
    RAY = "ray"  # Cache in Ray object store (automatic spilling)
    NONE = "none"  # Don't cache (too large or unsafe)


def get_cache_strategy(operation_name: str, result: Any) -> CacheStrategy:
    """Determine where to cache based on operation and size.

    Args:
        operation_name: Name of the operation being cached
        result: The result object to be cached

    Returns:
        CacheStrategy enum value
    """

    # Get size of result
    size = get_object_size(result)

    # Small objects go to local cache
    if size < LOCAL_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.LOCAL

    # Medium objects go to Ray object store
    elif size < RAY_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.RAY

    # Large objects don't get cached
    else:
        return CacheStrategy.NONE
