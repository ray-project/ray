"""
Thread-safe core cache implementation for Ray Data.

This module implements the main DatasetCache class that manages two separate caches:
1. Local cache: In-memory Python dict for small objects (< 50KB)
2. Ray cache: Ray object store for medium objects (50KB - 10MB)

Key Design Decisions:
- Thread-safe: All operations use locks to support concurrent access
- LRU eviction: Least Recently Used items are evicted when cache is full
- Smart updates: Transformations can preserve/compute cache values intelligently
- Ray integration: Medium objects stored in Ray object store with automatic disk spilling

The cache is optimized for Dataset operations:
- count(), schema(), columns() → Local cache (instant access)
- materialize() → Ray cache (stores ObjectRef structure, not data)
- Large results (>10MB) → Not cached
"""

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from ray.data import DataContext

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

# =============================================================================
# CONFIGURATION
# =============================================================================


@dataclass
class CacheConfiguration:
    """Configuration for dataset cache behavior.

    This dataclass holds all cache configuration settings. It can be created
    from a DataContext object to pick up user-specified settings.

    Attributes:
        max_entries: Maximum number of cache entries per cache type (local/Ray)
        local_threshold_bytes: Size threshold for local cache (< this → local)
        ray_threshold_bytes: Size threshold for Ray cache (< this → Ray)
        max_size_bytes: Total maximum cache size across both caches
        enable_smart_updates: Whether to use smart cache preservation logic
    """

    max_entries: int = MAX_CACHE_ENTRIES  # 1000 entries per cache
    local_threshold_bytes: int = LOCAL_CACHE_THRESHOLD_BYTES  # 50KB
    ray_threshold_bytes: int = RAY_CACHE_THRESHOLD_BYTES  # 10MB
    max_size_bytes: int = DEFAULT_MAX_CACHE_SIZE_BYTES  # 1GB
    enable_smart_updates: bool = True  # Enable intelligent cache updates

    @classmethod
    def from_data_context(cls, context: "DataContext") -> "CacheConfiguration":
        """Create cache configuration from DataContext.

        This pulls configuration from the DataContext, allowing users to
        customize cache behavior via context settings.

        Args:
            context: DataContext object with cache settings

        Returns:
            CacheConfiguration with settings from context
        """
        return cls(
            max_entries=MAX_CACHE_ENTRIES,
            local_threshold_bytes=LOCAL_CACHE_THRESHOLD_BYTES,
            ray_threshold_bytes=RAY_CACHE_THRESHOLD_BYTES,
            max_size_bytes=getattr(
                context, "dataset_cache_max_size_bytes", DEFAULT_MAX_CACHE_SIZE_BYTES
            ),
            enable_smart_updates=True,
        )


# =============================================================================
# STATISTICS
# =============================================================================


@dataclass
class CacheStats:
    """Cache performance statistics.

    Tracks hit/miss rates and cache utilization to help debug performance.

    Attributes:
        hit_count: Number of cache hits (found in cache)
        miss_count: Number of cache misses (had to compute)
        local_entries: Current number of entries in local cache
        ray_entries: Current number of entries in Ray cache
    """

    hit_count: int = 0  # How many times we found value in cache
    miss_count: int = 0  # How many times we had to compute value
    local_entries: int = 0  # Current local cache size
    ray_entries: int = 0  # Current Ray cache size

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate as a percentage.

        Returns:
            Hit rate from 0.0 to 1.0 (0% to 100%)
        """
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0

    @property
    def total_entries(self) -> int:
        """Total number of cached entries across both caches.

        Returns:
            Sum of local_entries and ray_entries
        """
        return self.local_entries + self.ray_entries


# =============================================================================
# MAIN CACHE CLASS
# =============================================================================


class DatasetCache:
    """Thread-safe cache for Dataset operations.

    This is the main cache implementation. It manages two separate caches
    (local and Ray) and provides thread-safe get/put/invalidate operations.

    Architecture:
        - Local cache: OrderedDict for LRU eviction + thread lock
        - Ray cache: OrderedDict of ObjectRefs + thread lock
        - Smart updater: Handles cache preservation for transformations
        - Lock: RLock for thread safety (allows recursive locking)

    Thread Safety:
        All public methods acquire the lock before accessing cache data.
        Ray operations (ray.get, ray.put) are performed OUTSIDE the lock
        to avoid blocking other operations.
    """

    def __init__(self, config: Optional[CacheConfiguration] = None):
        """Initialize the dataset cache.

        Args:
            config: Cache configuration (or None for defaults)
        """
        # Store configuration
        self._config = config or CacheConfiguration()

        # Initialize the two caches as ordered dicts (for LRU eviction)
        self._local_cache: OrderedDict[str, Any] = OrderedDict()
        self._ray_cache: OrderedDict[str, Any] = OrderedDict()

        # Initialize statistics counters
        self._hit_count = 0
        self._miss_count = 0

        # Initialize smart cache updater for transformation handling
        self._smart_updater = SmartCacheUpdater(self._local_cache, self._ray_cache)

        # Thread safety: RLock allows the same thread to acquire lock multiple times
        self._lock = threading.RLock()

    # =========================================================================
    # GET: Retrieve cached value
    # =========================================================================

    def get(
        self, logical_plan: LogicalPlan, operation_name: str, **params
    ) -> Optional[Any]:
        """Get cached result in a thread-safe manner.

        This is the main cache lookup method. It checks local cache first,
        then Ray cache. For Ray cache, ray.get() is called outside the lock.

        Args:
            logical_plan: Logical plan of the dataset
            operation_name: Name of the operation (e.g., "count", "schema")
            **params: Additional operation parameters (e.g., column names)

        Returns:
            Cached result if found and valid, None otherwise

        Algorithm:
            1. Generate cache key from logical plan + operation + params
            2. Try local cache (with lock)
               - If found and valid → return (hit!)
               - If found but invalid → evict and return None
            3. Try Ray cache (with lock to get ObjectRef)
            4. Call ray.get() OUTSIDE lock (to avoid blocking)
            5. Validate result and return
        """
        # Generate unique cache key for this operation
        cache_key = make_cache_key(logical_plan, operation_name, **params)

        with self._lock:
            # ---------------------------------------------------------------------
            # Try local cache first (fastest)
            # ---------------------------------------------------------------------
            if cache_key in self._local_cache:
                result = self._local_cache[cache_key]

                # Validate the cached value
                if validate_cached_value(operation_name, result):
                    # Valid! Move to end (LRU) and return
                    self._local_cache.move_to_end(cache_key)
                    self._hit_count += 1
                    return result
                else:
                    # Invalid! Remove from cache
                    self._local_cache.pop(cache_key, None)

            # ---------------------------------------------------------------------
            # Try Ray cache (holds ObjectRefs)
            # ---------------------------------------------------------------------
            if cache_key in self._ray_cache:
                # Get the ObjectRef while holding the lock
                try:
                    import ray

                    object_ref = self._ray_cache[cache_key]
                except Exception:
                    # Ray not available or ObjectRef corrupted
                    self._ray_cache.pop(cache_key, None)
                    self._miss_count += 1
                    return None
            else:
                # Not in Ray cache either - miss
                self._miss_count += 1
                return None

        # -------------------------------------------------------------------------
        # Call ray.get() OUTSIDE the lock to avoid blocking other operations
        # -------------------------------------------------------------------------
        # This is important! ray.get() can be slow if data needs to be fetched
        # from another node or deserialized. We don't want to hold the lock
        # during this time, as it would block all other cache operations.
        try:
            result = ray.get(object_ref)

            with self._lock:
                # Re-check that cache_key still exists (could be evicted while getting)
                if cache_key in self._ray_cache and validate_cached_value(
                    operation_name, result
                ):
                    # Still valid! Move to end (LRU) and return
                    self._ray_cache.move_to_end(cache_key)
                    self._hit_count += 1
                    return result
                else:
                    # Cache entry was evicted or result is invalid
                    self._ray_cache.pop(cache_key, None)
                    return None
        except Exception:
            # ray.get() failed - remove corrupted ObjectRef
            with self._lock:
                self._ray_cache.pop(cache_key, None)
            return None

    # =========================================================================
    # PUT: Store value in cache
    # =========================================================================

    def put(
        self, logical_plan: LogicalPlan, operation_name: str, result: Any, **params
    ) -> None:
        """Cache a result in a thread-safe manner.

        This determines where to cache the result (local vs Ray) based on size,
        then stores it with LRU eviction if the cache is full.

        Args:
            logical_plan: Logical plan of the dataset
            operation_name: Name of the operation
            result: The result to cache
            **params: Additional operation parameters

        Algorithm:
            1. Generate cache key
            2. Determine cache strategy (LOCAL/RAY/NONE) based on size
            3. For LOCAL: Insert with lock, evict LRU if full
            4. For RAY: Call ray.put() outside lock, then insert ObjectRef
        """
        # Generate cache key
        cache_key = make_cache_key(logical_plan, operation_name, **params)

        # Determine where to cache based on size
        strategy = get_cache_strategy(operation_name, result)

        # ---------------------------------------------------------------------
        # LOCAL cache: Store in Python memory
        # ---------------------------------------------------------------------
        if strategy == CacheStrategy.LOCAL:
            with self._lock:
                # Evict least recently used entries if cache is full
                while len(self._local_cache) >= self._config.max_entries:
                    # popitem(last=False) removes the oldest item (FIFO/LRU)
                    self._local_cache.popitem(last=False)

                # Insert the new result
                self._local_cache[cache_key] = result

        # ---------------------------------------------------------------------
        # RAY cache: Store in Ray object store
        # ---------------------------------------------------------------------
        elif strategy == CacheStrategy.RAY:
            try:
                import ray

                # Call ray.put() OUTSIDE lock to avoid blocking
                # This transfers the object to the Ray object store
                object_ref = ray.put(result)

                with self._lock:
                    # Collect ObjectRefs to evict (we'll delete them outside the lock)
                    refs_to_delete = []
                    while len(self._ray_cache) >= self._config.max_entries:
                        old_key, old_ref = self._ray_cache.popitem(last=False)
                        refs_to_delete.append(old_ref)

                    # Insert the new ObjectRef
                    self._ray_cache[cache_key] = object_ref

                # Clean up old ObjectRefs OUTSIDE the lock to avoid blocking
                for old_ref in refs_to_delete:
                    try:
                        # Ray internal API for explicit ObjectRef cleanup
                        # See: https://github.com/ray-project/ray/blob/master/python/ray/_private/worker.py
                        if ray.is_initialized():
                            ray._private.worker.global_worker.core_worker.delete_objects(
                                [old_ref]
                            )
                    except (AttributeError, RuntimeError):
                        # Ray cleanup failed, ignore (ObjectRef will be garbage collected)
                        pass
            except Exception:
                # Ray not available or ray.put() failed, skip caching
                pass

    # =========================================================================
    # INVALIDATE: Update cache for transformation
    # =========================================================================

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_plan: LogicalPlan,
        target_plan: LogicalPlan,
        **transform_params,
    ) -> None:
        """Update cache for a transformation in a thread-safe manner.

        This is called when a transformation is applied to a dataset. It uses
        the SmartCacheUpdater to preserve/compute cache values intelligently.

        Args:
            operation_name: Name of the transformation (e.g., "limit", "sort")
            source_plan: Logical plan before transformation
            target_plan: Logical plan after transformation
            **transform_params: Transformation parameters (e.g., limit value)

        Example:
            ds = ray.data.range(100)  # source_plan
            ds.count()  # Caches count=100
            limited = ds.limit(10)  # target_plan
            → Smart updater computes: count=min(100, 10)=10
        """
        # Generate cache key prefixes for source and target datasets
        source_key_prefix = make_cache_key(source_plan, "")
        target_key_prefix = make_cache_key(target_plan, "")

        with self._lock:
            # Delegate to smart updater for cache preservation logic
            self._smart_updater.invalidate_for_transform(
                operation_name, source_key_prefix, target_key_prefix, **transform_params
            )

    # =========================================================================
    # CLEAR: Remove all cache entries
    # =========================================================================

    def clear(self) -> None:
        """Clear all cache entries in a thread-safe manner.

        This is called when the user explicitly clears the cache via
        ray.data.clear_dataset_cache().

        Note: Ray ObjectRefs are cleaned up outside the lock to avoid blocking.
        """
        with self._lock:
            # Get all Ray ObjectRefs to clean up (before clearing)
            ray_refs = list(self._ray_cache.values())

            # Clear both caches
            self._local_cache.clear()
            self._ray_cache.clear()

        # Clean up Ray ObjectRefs OUTSIDE the lock to avoid blocking
        for object_ref in ray_refs:
            try:
                import ray

                # Ray internal API for explicit ObjectRef cleanup
                # See: https://github.com/ray-project/ray/blob/master/python/ray/_private/worker.py
                if ray.is_initialized():
                    ray._private.worker.global_worker.core_worker.delete_objects(
                        [object_ref]
                    )
            except (ImportError, AttributeError, RuntimeError):
                # Ray cleanup failed, ignore (ObjectRefs will be garbage collected)
                pass

    # =========================================================================
    # STATS: Get cache statistics
    # =========================================================================

    def get_stats(self) -> CacheStats:
        """Get cache statistics as a dataclass in a thread-safe manner.

        Returns:
            CacheStats dataclass with current statistics
        """
        with self._lock:
            return CacheStats(
                hit_count=self._hit_count,
                miss_count=self._miss_count,
                local_entries=len(self._local_cache),
                ray_entries=len(self._ray_cache),
            )
