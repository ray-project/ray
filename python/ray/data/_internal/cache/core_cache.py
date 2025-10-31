"""Thread-safe core cache implementation for Ray Data.

Manages two caches: local in-memory dict for small objects (< 50KB) and
Ray object store for medium objects (50KB - 10MB).
"""

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from .cache_strategies import CacheStrategy, get_cache_strategy
from .constants import (
    DEFAULT_MAX_CACHE_SIZE_BYTES,
    LOCAL_CACHE_THRESHOLD_BYTES,
    MAX_CACHE_ENTRIES,
    RAY_CACHE_THRESHOLD_BYTES,
)
from .key_generation import make_cache_key
from .smart_updates import SmartCacheUpdater
from .validation import validate_cached_value
from ray.data._internal.logical.interfaces import LogicalPlan

if TYPE_CHECKING:
    from ray.data import DataContext


@dataclass
class CacheConfiguration:
    """Configuration for dataset cache behavior."""

    max_entries: int = MAX_CACHE_ENTRIES
    local_threshold_bytes: int = LOCAL_CACHE_THRESHOLD_BYTES
    ray_threshold_bytes: int = RAY_CACHE_THRESHOLD_BYTES
    max_size_bytes: int = DEFAULT_MAX_CACHE_SIZE_BYTES
    enable_smart_updates: bool = True

    @classmethod
    def from_data_context(cls, context: "DataContext") -> "CacheConfiguration":
        """Create cache configuration from DataContext."""
        return cls(
            max_size_bytes=getattr(
                context, "dataset_cache_max_size_bytes", DEFAULT_MAX_CACHE_SIZE_BYTES
            ),
        )


@dataclass
class CacheStats:
    """Cache performance statistics."""

    hit_count: int = 0
    miss_count: int = 0
    local_entries: int = 0
    ray_entries: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate as a percentage."""
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0

    @property
    def total_entries(self) -> int:
        """Total number of cached entries across both caches."""
        return self.local_entries + self.ray_entries


class DatasetCache:
    """Thread-safe cache for Dataset operations."""

    def __init__(self, config: Optional[CacheConfiguration] = None):
        """Initialize the dataset cache."""
        self._config = config or CacheConfiguration()
        self._local_cache: OrderedDict[str, Any] = OrderedDict()
        self._ray_cache: OrderedDict[str, Any] = OrderedDict()
        self._hit_count = 0
        self._miss_count = 0
        self._smart_updater = SmartCacheUpdater(self._local_cache, self._ray_cache)
        self._lock = threading.RLock()

    def get(
        self, logical_plan: LogicalPlan, operation_name: str, **params
    ) -> Optional[Any]:
        """Get cached result in a thread-safe manner.

        Checks local cache first, then Ray cache. For Ray cache, ray.get()
        is called outside the lock to avoid blocking.
        """
        cache_key = make_cache_key(logical_plan, operation_name, **params)

        with self._lock:
            if cache_key in self._local_cache:
                result = self._local_cache[cache_key]
                if validate_cached_value(operation_name, result):
                    self._local_cache.move_to_end(cache_key)
                    self._hit_count += 1
                    return result
                else:
                    self._local_cache.pop(cache_key, None)

            object_ref = None
            if cache_key in self._ray_cache:
                try:
                    import ray

                    object_ref = self._ray_cache[cache_key]
                except Exception:
                    self._ray_cache.pop(cache_key, None)
                    self._miss_count += 1
                    return None
            else:
                self._miss_count += 1
                return None

        # Call ray.get() outside lock to avoid blocking
        try:
            result = ray.get(object_ref)

            with self._lock:
                if cache_key in self._ray_cache and validate_cached_value(
                    operation_name, result
                ):
                    self._ray_cache.move_to_end(cache_key)
                    self._hit_count += 1
                    return result
                else:
                    self._ray_cache.pop(cache_key, None)
                    return None
        except Exception:
            with self._lock:
                self._ray_cache.pop(cache_key, None)
            return None

    def put(
        self, logical_plan: LogicalPlan, operation_name: str, result: Any, **params
    ) -> None:
        """Cache a result in a thread-safe manner.

        Determines where to cache (local vs Ray) based on size, with LRU eviction.
        """
        cache_key = make_cache_key(logical_plan, operation_name, **params)
        strategy = get_cache_strategy(operation_name, result)

        if strategy == CacheStrategy.LOCAL:
            with self._lock:
                while len(self._local_cache) >= self._config.max_entries:
                    self._local_cache.popitem(last=False)
                self._local_cache[cache_key] = result

        elif strategy == CacheStrategy.RAY:
            try:
                import ray

                object_ref = ray.put(result)

                with self._lock:
                    while len(self._ray_cache) >= self._config.max_entries:
                        self._ray_cache.popitem(last=False)

                    self._ray_cache[cache_key] = object_ref
            except Exception:
                pass

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_plan: LogicalPlan,
        target_plan: LogicalPlan,
        **transform_params,
    ) -> None:
        """Update cache for a transformation in a thread-safe manner."""
        source_key_prefix = make_cache_key(source_plan, "")
        target_key_prefix = make_cache_key(target_plan, "")

        with self._lock:
            self._smart_updater.invalidate_for_transform(
                operation_name, source_key_prefix, target_key_prefix, **transform_params
            )

    def clear(self) -> None:
        """Clear all cache entries in a thread-safe manner."""
        with self._lock:
            self._local_cache.clear()
            self._ray_cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics as a dataclass in a thread-safe manner."""
        with self._lock:
            return CacheStats(
                hit_count=self._hit_count,
                miss_count=self._miss_count,
                local_entries=len(self._local_cache),
                ray_entries=len(self._ray_cache),
            )
