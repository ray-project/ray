"""Cache placement strategy for Ray Data caching.

Determines where to cache objects based on size:
- Small objects (< 50KB): Local memory cache
- Medium objects (50KB - 10MB): Ray object store
- Large objects (> 10MB): Not cached
"""

import sys
from enum import Enum
from typing import Any

from .constants import LOCAL_CACHE_THRESHOLD_BYTES, RAY_CACHE_THRESHOLD_BYTES


class CacheStrategy(Enum):
    """Cache placement strategies."""

    LOCAL = "local"
    RAY = "ray"
    NONE = "none"


def get_cache_strategy(operation_name: str, result: Any) -> CacheStrategy:
    """Determine where to cache a result based on its size."""
    size_bytes = sys.getsizeof(result)

    if size_bytes < LOCAL_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.LOCAL
    elif size_bytes < RAY_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.RAY
    else:
        return CacheStrategy.NONE
