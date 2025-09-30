"""
Simple core cache implementation for Ray Data caching.
"""

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Optional

from .cache_strategies import CacheStrategy, get_cache_strategy
from .key_generation import make_cache_key
from .smart_updates import SmartCacheUpdater
from .validation import validate_cached_value
from ray.data._internal.logical.interfaces import LogicalPlan

# Configuration constants

# Maximum number of entries in each cache (local and Ray).
# This prevents unbounded cache growth in terms of number of items.
MAX_CACHE_ENTRIES = 1000


@dataclass
class CacheConfiguration:
    """Configuration for dataset cache behavior."""

    max_entries: int = MAX_CACHE_ENTRIES
    local_threshold_bytes: int = 50 * 1024  # 50KB
    ray_threshold_bytes: int = 10 * 1024 * 1024  # 10MB
    enable_smart_updates: bool = True


@dataclass
class CacheStats:
    """Cache performance statistics."""

    hit_count: int = 0
    miss_count: int = 0
    local_entries: int = 0
    ray_entries: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0

    @property
    def total_entries(self) -> int:
        return self.local_entries + self.ray_entries


class DatasetCache:
    """Simple cache for Dataset operations."""

    def __init__(self, config: Optional[CacheConfiguration] = None):
        self._config = config or CacheConfiguration()
        self._local_cache: OrderedDict[str, Any] = OrderedDict()
        self._ray_cache: OrderedDict[str, Any] = OrderedDict()
        self._hit_count = 0
        self._miss_count = 0
        self._smart_updater = SmartCacheUpdater(self._local_cache, self._ray_cache)

    def get(
        self, logical_plan: LogicalPlan, operation_name: str, **params
    ) -> Optional[Any]:
        """Get cached result."""
        cache_key = make_cache_key(logical_plan, operation_name, **params)

        # Try local cache first
        if cache_key in self._local_cache:
            result = self._local_cache[cache_key]
            if validate_cached_value(operation_name, result):
                self._local_cache.move_to_end(cache_key)
                self._hit_count += 1
                return result
            else:
                self._local_cache.pop(cache_key, None)

        # Try Ray cache
        if cache_key in self._ray_cache:
            try:
                import ray

                object_ref = self._ray_cache[cache_key]
                result = ray.get(object_ref)
                if validate_cached_value(operation_name, result):
                    self._ray_cache.move_to_end(cache_key)
                    self._hit_count += 1
                    return result
                else:
                    self._ray_cache.pop(cache_key, None)
            except Exception:
                self._ray_cache.pop(cache_key, None)

        self._miss_count += 1
        return None

    def put(
        self, logical_plan: LogicalPlan, operation_name: str, result: Any, **params
    ) -> None:
        """Cache a result."""
        cache_key = make_cache_key(logical_plan, operation_name, **params)
        strategy = get_cache_strategy(operation_name, result)

        if strategy == CacheStrategy.LOCAL:
            # Evict if needed
            while len(self._local_cache) >= self._config.max_entries:
                self._local_cache.popitem(last=False)
            self._local_cache[cache_key] = result

        elif strategy == CacheStrategy.RAY:
            try:
                import ray

                object_ref = ray.put(result)
                # Evict if needed
                while len(self._ray_cache) >= self._config.max_entries:
                    old_key, old_ref = self._ray_cache.popitem(last=False)
                    try:
                        if ray.is_initialized():
                            ray._private.worker.global_worker.core_worker.delete_objects(
                                [old_ref]
                            )
                    except (AttributeError, RuntimeError):
                        pass
                self._ray_cache[cache_key] = object_ref
            except Exception:
                pass  # Ray not available, skip caching

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_plan: LogicalPlan,
        target_plan: LogicalPlan,
        **transform_params,
    ) -> None:
        """Update cache for transformation."""
        source_key_prefix = make_cache_key(source_plan, "")
        target_key_prefix = make_cache_key(target_plan, "")

        self._smart_updater.invalidate_for_transform(
            operation_name, source_key_prefix, target_key_prefix, **transform_params
        )

    def clear(self) -> None:
        """Clear all cache entries."""
        # Clean up Ray ObjectRefs
        for object_ref in self._ray_cache.values():
            try:
                import ray

                if ray.is_initialized():
                    ray._private.worker.global_worker.core_worker.delete_objects(
                        [object_ref]
                    )
            except (ImportError, AttributeError, RuntimeError):
                pass

        self._local_cache.clear()
        self._ray_cache.clear()

    def get_stats(self) -> CacheStats:
        """Get cache statistics as a dataclass."""
        return CacheStats(
            hit_count=self._hit_count,
            miss_count=self._miss_count,
            local_entries=len(self._local_cache),
            ray_entries=len(self._ray_cache),
        )
