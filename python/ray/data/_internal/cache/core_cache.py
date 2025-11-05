"""Thread-safe core cache implementation for Ray Data.

Manages two caches: local in-memory dict for small objects (< 50KB) and
Ray object store for medium objects (50KB - 10MB).
"""

import sys
import threading
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from ray.data._internal.cache.constants import (
    DEFAULT_MAX_CACHE_SIZE_BYTES,
    LOCAL_CACHE_THRESHOLD_BYTES,
    MAX_CACHE_ENTRIES,
    RAY_CACHE_THRESHOLD_BYTES,
)
from ray.data._internal.cache.key_generation import make_cache_key
from ray.data._internal.cache.smart_updates import SmartCacheUpdater
from ray.data._internal.logical.interfaces import LogicalPlan

if TYPE_CHECKING:
    from ray.data import DataContext


class CacheStrategy(Enum):
    """Cache placement strategies."""

    LOCAL = "local"
    RAY = "ray"
    NONE = "none"


def _get_cache_strategy(operation_name: str, result: Any) -> CacheStrategy:
    """Determine where to cache a result based on its size."""
    size_bytes = sys.getsizeof(result)

    if size_bytes < LOCAL_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.LOCAL
    elif size_bytes < RAY_CACHE_THRESHOLD_BYTES:
        return CacheStrategy.RAY
    else:
        return CacheStrategy.NONE


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

        Args:
            logical_plan: The logical plan to generate cache key from.
            operation_name: Name of the operation (e.g., "count", "schema").
            **params: Additional parameters to include in cache key.

        Returns:
            Cached result if found, None otherwise.
        """
        cache_key = make_cache_key(logical_plan, operation_name, **params)

        with self._lock:
            if cache_key in self._local_cache:
                result = self._local_cache[cache_key]
                self._local_cache.move_to_end(cache_key)
                self._hit_count += 1
                return result

            if cache_key in self._ray_cache:
                object_ref = self._ray_cache[cache_key]
            else:
                self._miss_count += 1
                return None

        # Call ray.get() outside lock to avoid blocking other threads.
        # Ray.get() may block on network I/O, so we release the lock first.
        # See: https://docs.ray.io/en/latest/ray-core/api/doc/ray.html#ray.get
        try:
            import ray

            result = ray.get(object_ref)

            with self._lock:
                # Re-check cache key exists (may have been evicted during ray.get()).
                if cache_key in self._ray_cache:
                    self._ray_cache.move_to_end(cache_key)
                    self._hit_count += 1
                    return result
                return None
        except Exception:
            # Object may have been lost or evicted from Ray object store.
            # Remove from cache and return None.
            with self._lock:
                self._ray_cache.pop(cache_key, None)
            self._miss_count += 1
            return None

    def put(
        self, logical_plan: LogicalPlan, operation_name: str, result: Any, **params
    ) -> None:
        """Cache a result in a thread-safe manner.

        Determines where to cache (local vs Ray) based on size, with LRU eviction.
        Large objects (> 10MB) are not cached.

        Args:
            logical_plan: The logical plan to generate cache key from.
            operation_name: Name of the operation (e.g., "count", "schema").
            result: The result to cache.
            **params: Additional parameters to include in cache key.
        """
        cache_key = make_cache_key(logical_plan, operation_name, **params)
        strategy = _get_cache_strategy(operation_name, result)

        if strategy == CacheStrategy.LOCAL:
            with self._lock:
                self._evict_if_full(self._local_cache)
                self._local_cache[cache_key] = result
        elif strategy == CacheStrategy.RAY:
            self._put_in_ray_cache(cache_key, result)

    def _evict_if_full(self, cache: OrderedDict) -> None:
        """Evict oldest entry if cache is at capacity.

        Uses LRU (Least Recently Used) eviction: removes the oldest entry
        (first item) when cache reaches max_entries. OrderedDict maintains
        insertion order, and move_to_end() updates recency.

        Args:
            cache: OrderedDict cache to evict from (local or ray cache).
        """
        while len(cache) >= self._config.max_entries:
            # popitem(last=False) removes the oldest (first) entry.
            cache.popitem(last=False)

    def _put_in_ray_cache(self, cache_key: str, result: Any) -> None:
        """Put result in Ray object store cache.

        Stores the result in Ray's distributed object store and keeps a reference
        in the cache. The object reference allows retrieving the value later.

        See: https://docs.ray.io/en/latest/ray-core/api/doc/ray.html#ray.put
        """
        try:
            import ray

            # Store object in Ray's distributed object store.
            # Returns an ObjectRef that can be used to retrieve the value.
            object_ref = ray.put(result)

            with self._lock:
                # Evict oldest entries if cache is full (LRU eviction).
                self._evict_if_full(self._ray_cache)
                # Store object reference in cache.
                self._ray_cache[cache_key] = object_ref
        except Exception:
            # If Ray.put() fails (e.g., Ray not initialized), silently skip caching.
            # This allows the code to work even if Ray object store is unavailable.
            pass

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_plan: LogicalPlan,
        target_plan: LogicalPlan,
        **transform_params,
    ) -> None:
        """Update cache for a transformation in a thread-safe manner.

        Preserves valid cache entries after transformation and computes
        new values where possible (e.g., count after limit).

        Args:
            operation_name: Name of the transformation operation.
            source_plan: Logical plan of the source dataset.
            target_plan: Logical plan of the target dataset.
            **transform_params: Transformation parameters.
        """
        source_key_prefix = make_cache_key(source_plan, "")
        target_key_prefix = make_cache_key(target_plan, "")

        with self._lock:
            self._smart_updater.invalidate_for_transform(
                operation_name, source_key_prefix, target_key_prefix, **transform_params
            )

    def clear(self) -> None:
        """Clear all cache entries in a thread-safe manner.

        Removes all entries from both local and Ray caches.
        """
        with self._lock:
            self._local_cache.clear()
            self._ray_cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics as a dataclass in a thread-safe manner.

        Returns:
            CacheStats object containing hit count, miss count, hit rate,
            and entry counts for both caches.
        """
        with self._lock:
            return CacheStats(
                hit_count=self._hit_count,
                miss_count=self._miss_count,
                local_entries=len(self._local_cache),
                ray_entries=len(self._ray_cache),
            )
