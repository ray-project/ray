"""Dataset caching for Ray Data."""

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
