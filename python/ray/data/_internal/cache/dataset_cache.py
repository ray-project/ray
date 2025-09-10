"""Intelligent caching system for Ray Data operations.

This module provides comprehensive caching for expensive Dataset operations including
smart invalidation, disk spilling, and performance optimizations. The system is
designed to be powerful yet simple, focusing on Ray Data-specific optimizations.
"""

import contextlib
import functools
import gzip
import inspect
import logging
import os
import pickle
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


class CacheableOperation(Enum):
    """Operations that can be cached."""

    COUNT = "count"
    SCHEMA = "schema"
    SIZE_BYTES = "size_bytes"
    STATS = "stats"
    INPUT_FILES = "input_files"
    COLUMNS = "columns"
    TAKE_ALL = "take_all"
    TAKE_BATCH = "take_batch"
    TAKE = "take"
    MATERIALIZE = "materialize"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    STD = "std"
    UNIQUE = "unique"


class TransformationType(Enum):
    """Categories of transformations and their cache effects."""

    SCHEMA_PRESERVING_COUNT_CHANGING = (
        "schema_preserving_count_changing"  # limit, repartition
    )
    ROW_PRESERVING_SCHEMA_CHANGE = (
        "row_preserving_schema_change"  # map, add_column, etc.
    )
    ROW_CHANGING_NO_SCHEMA_CHANGE = "row_changing_no_schema_change"  # filter
    ROW_CHANGING_SCHEMA_CHANGE = "row_changing_schema_change"  # map_batches, flat_map
    REORDERING_ONLY = "reordering_only"  # sort, random_shuffle
    COMBINING = "combining"  # union, join
    GROUPING = "grouping"  # groupby


# Smart invalidation matrix
CACHE_VALIDITY_MATRIX = {
    TransformationType.SCHEMA_PRESERVING_COUNT_CHANGING: {
        # limit, repartition - preserve schema/columns but NOT count (limit changes count!)
        CacheableOperation.SCHEMA,
        CacheableOperation.COLUMNS,
        # COUNT is invalidated because limit() changes the row count
        # SIZE_BYTES is invalidated because limit() changes dataset size
        # TAKE_ALL, MATERIALIZE invalidated because data subset changes
    },
    TransformationType.ROW_PRESERVING_SCHEMA_CHANGE: {
        CacheableOperation.COUNT,
    },
    TransformationType.ROW_CHANGING_NO_SCHEMA_CHANGE: {
        CacheableOperation.SCHEMA,
        CacheableOperation.COLUMNS,
    },
    TransformationType.ROW_CHANGING_SCHEMA_CHANGE: set(),
    TransformationType.REORDERING_ONLY: {
        CacheableOperation.COUNT,
        CacheableOperation.SCHEMA,
        CacheableOperation.COLUMNS,
        CacheableOperation.SIZE_BYTES,
        CacheableOperation.SUM,
        CacheableOperation.MIN,
        CacheableOperation.MAX,
        CacheableOperation.MEAN,
        CacheableOperation.STD,
        CacheableOperation.UNIQUE,
    },
    TransformationType.COMBINING: set(),
    TransformationType.GROUPING: set(),
}


def get_transformation_type(operation_name: str) -> TransformationType:
    """Get transformation type for cache invalidation.

    Args:
        operation_name: Name of the transformation operation (e.g., "map", "filter").

    Returns:
        The transformation type for determining cache invalidation behavior.
    """
    operation_map = {
        "limit": TransformationType.SCHEMA_PRESERVING_COUNT_CHANGING,
        "repartition": TransformationType.SCHEMA_PRESERVING_COUNT_CHANGING,
        "map": TransformationType.ROW_PRESERVING_SCHEMA_CHANGE,
        "add_column": TransformationType.ROW_PRESERVING_SCHEMA_CHANGE,
        "drop_columns": TransformationType.ROW_PRESERVING_SCHEMA_CHANGE,
        "select_columns": TransformationType.ROW_PRESERVING_SCHEMA_CHANGE,
        "rename_columns": TransformationType.ROW_PRESERVING_SCHEMA_CHANGE,
        "filter": TransformationType.ROW_CHANGING_NO_SCHEMA_CHANGE,
        "map_batches": TransformationType.ROW_CHANGING_SCHEMA_CHANGE,
        "flat_map": TransformationType.ROW_CHANGING_SCHEMA_CHANGE,
        "sort": TransformationType.REORDERING_ONLY,
        "random_shuffle": TransformationType.REORDERING_ONLY,
        "union": TransformationType.COMBINING,
        "join": TransformationType.COMBINING,
        "groupby": TransformationType.GROUPING,
    }
    return operation_map.get(
        operation_name, TransformationType.ROW_CHANGING_SCHEMA_CHANGE
    )


class _CacheKey:
    """Cache key based on dataset logical plan and operation parameters.

    This class creates deterministic cache keys by hashing the logical plan
    structure and operation parameters. Cache keys are used to identify
    identical operations for caching purposes.

    Args:
        logical_plan: The dataset's logical plan for cache key generation.
        operation_name: Name of the operation being cached.
        **kwargs: Additional parameters to include in the cache key.
    """

    def __init__(self, logical_plan: LogicalPlan, operation_name: str, **kwargs):
        self.logical_plan = logical_plan
        self.operation_name = operation_name
        self.parameters = kwargs
        self._hash = None

    def __hash__(self) -> int:
        if self._hash is None:
            plan_hash = self._hash_logical_plan(self.logical_plan.dag)
            param_hash = hash(frozenset(self.parameters.items()))
            op_hash = hash(self.operation_name)
            self._hash = hash((plan_hash, param_hash, op_hash))
        return self._hash

    def __eq__(self, other) -> bool:
        if not isinstance(other, _CacheKey):
            return False
        return (
            hash(self) == hash(other)
            and self.operation_name == other.operation_name
            and self.parameters == other.parameters
        )

    def _hash_logical_plan(self, operator) -> int:
        """Create deterministic hash of logical plan DAG structure."""
        op_info = (
            type(operator).__name__,
            getattr(operator, "_name", ""),
            str(getattr(operator, "_params", {})),
        )

        input_hashes = []
        for input_op in getattr(operator, "input_dependencies", []):
            input_hashes.append(self._hash_logical_plan(input_op))

        combined = (*op_info, *sorted(input_hashes))
        return hash(combined)


class _CacheEntry:
    """Cached result with metadata.

    Stores a cached operation result along with metadata for cache management
    including size estimation, access tracking, and LRU eviction.

    Args:
        key: The cache key for this entry.
        result: The cached operation result.
        size_bytes: Estimated size of the cached result in bytes.
    """

    def __init__(self, key: _CacheKey, result: Any, size_bytes: int = 0):
        self.key = key
        self.result = result
        self.size_bytes = size_bytes
        self.created_at = time.time()
        self.last_accessed = time.time()
        self.access_count = 0

    def access(self):
        """Record access to this cache entry."""
        self.last_accessed = time.time()
        self.access_count += 1


class _DiskSpillManager:
    """Manages disk spilling for large cache items."""

    def __init__(
        self, cache_location: Optional[str] = None, max_size_bytes: int = 10 * 1024**3
    ):
        self.max_size_bytes = max_size_bytes
        self.current_size_bytes = 0

        # Setup cache directory
        if cache_location is None:
            cache_location = os.path.join(os.path.expanduser("~"), ".ray_data_cache")

        self.cache_dir = Path(cache_location)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._entries: OrderedDict[str, Dict] = OrderedDict()
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="disk_cache"
        )

        self._load_existing_entries()

    def _load_existing_entries(self):
        """Load existing cache entries from disk."""
        try:
            metadata_file = self.cache_dir / "cache_metadata.pkl"
            if metadata_file.exists():
                with open(metadata_file, "rb") as f:
                    saved_entries = pickle.load(f)

                for key, entry in saved_entries.items():
                    file_path = Path(entry["file_path"])
                    if file_path.exists():
                        self._entries[key] = entry
                        self.current_size_bytes += entry["size_bytes"]
                    else:
                        # Clean up missing files
                        with contextlib.suppress(FileNotFoundError):
                            file_path.unlink()

        except Exception as e:
            logger.debug(f"Could not load disk cache metadata: {e}")

    def get(self, key: str) -> Optional[Any]:
        """Get item from disk cache."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None

            file_path = Path(entry["file_path"])
            if not file_path.exists():
                # Clean up missing entry
                del self._entries[key]
                return None

            # Move to end (LRU)
            self._entries.move_to_end(key)
            entry["last_accessed"] = time.time()

        # Load from disk
        try:
            with open(file_path, "rb") as f:
                data = f.read()

            if entry.get("compressed", False):
                data = gzip.decompress(data)

            return pickle.loads(data)

        except Exception as e:
            logger.warning(f"Failed to load disk cache entry {key}: {e}")
            # Clean up corrupted entry
            with self._lock:
                self._entries.pop(key, None)
                with contextlib.suppress(FileNotFoundError):
                    file_path.unlink()
            return None

    def put(self, key: str, data: Any) -> None:
        """Store item in disk cache asynchronously."""
        self._executor.submit(self._put_sync, key, data)

    def _put_sync(self, key: str, data: Any) -> None:
        """Synchronously store item in disk cache."""
        try:
            # Serialize and compress
            serialized = pickle.dumps(data)
            compressed = gzip.compress(serialized)

            file_path = self.cache_dir / f"{key}.cache"

            # Atomic write
            temp_path = file_path.with_suffix(".tmp")
            with open(temp_path, "wb") as f:
                f.write(compressed)
                f.flush()
                os.fsync(f.fileno())

            temp_path.replace(file_path)

            # Update metadata
            with self._lock:
                # Remove old entry if exists
                if key in self._entries:
                    old_entry = self._entries[key]
                    self.current_size_bytes -= old_entry["size_bytes"]

                # Add new entry
                entry_data = {
                    "file_path": str(file_path),
                    "size_bytes": len(compressed),
                    "compressed": True,
                    "created_at": time.time(),
                    "last_accessed": time.time(),
                }

                # Evict if necessary
                while (
                    self.current_size_bytes + entry_data["size_bytes"]
                    > self.max_size_bytes
                    and self._entries
                ):
                    self._evict_lru()

                self._entries[key] = entry_data
                self.current_size_bytes += entry_data["size_bytes"]

                # Save metadata periodically
                if len(self._entries) % 50 == 0:
                    self._save_metadata()

        except Exception as e:
            logger.warning(f"Failed to store disk cache entry {key}: {e}")

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._entries:
            return

        key, entry = self._entries.popitem(last=False)
        self.current_size_bytes -= entry["size_bytes"]

        # Remove file
        with contextlib.suppress(FileNotFoundError):
            Path(entry["file_path"]).unlink()

    def _save_metadata(self):
        """Save metadata to disk."""
        try:
            metadata_file = self.cache_dir / "cache_metadata.pkl"
            with open(metadata_file, "wb") as f:
                pickle.dump(dict(self._entries), f)
        except Exception as e:
            logger.debug(f"Failed to save disk cache metadata: {e}")

    def clear(self) -> None:
        """Clear all disk cache entries."""
        with self._lock:
            for entry in self._entries.values():
                with contextlib.suppress(FileNotFoundError):
                    Path(entry["file_path"]).unlink()

            self._entries.clear()
            self.current_size_bytes = 0

            # Remove metadata file
            with contextlib.suppress(FileNotFoundError):
                (self.cache_dir / "cache_metadata.pkl").unlink()


class _DatasetCacheManager:
    """Manages intelligent caching for Dataset operations.

    Provides comprehensive caching with memory management, disk spilling,
    and smart invalidation based on transformation types. The cache manager
    handles both small results in memory and large results spilled to disk.

    Args:
        max_size_bytes: Maximum size in bytes for the memory cache.
    """

    def __init__(self, max_size_bytes: int = 1024 * 1024 * 1024):
        self.max_size_bytes = max_size_bytes
        self.current_size_bytes = 0
        self._cache: OrderedDict[_CacheKey, _CacheEntry] = OrderedDict()
        self._lock = threading.RLock()
        self._hit_count = 0
        self._miss_count = 0

        # Disk spilling for large items
        context = DataContext.get_current()
        cache_location = getattr(context, "cache_location", None)
        self._disk_spill_threshold = getattr(
            context, "memory_spill_threshold_bytes", 100 * 1024**2
        )

        # Use reasonable default for disk cache size (10GB)
        max_disk_size = 10 * 1024**3
        self._disk_cache = _DiskSpillManager(cache_location, max_disk_size)

    def get(self, key: _CacheKey) -> Optional[Any]:
        """Get cached result for the given key."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is not None:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                entry.access()
                self._hit_count += 1
                return entry.result

            # Try disk cache
            disk_key = str(hash(key))
            disk_result = self._disk_cache.get(disk_key)
            if disk_result is not None:
                self._hit_count += 1
                return disk_result

            self._miss_count += 1
            return None

    def put(self, key: _CacheKey, result: Any) -> None:
        """Store result in cache with intelligent spilling."""

        # Special handling for MaterializedDataset - cache the object itself, not raw data
        if hasattr(result, "_plan") and hasattr(result, "num_blocks"):
            # This is a MaterializedDataset - cache the object with ObjectRef[Block] references
            # This is memory-efficient as we're not duplicating the actual data
            size_bytes = self._estimate_materialized_dataset_size(result)
            logger.debug(
                f"Caching MaterializedDataset with {result.num_blocks()} blocks, estimated size: {size_bytes:,} bytes"
            )
        else:
            size_bytes = self._estimate_size(result)

        # Large items go directly to disk (but MaterializedDataset should usually stay in memory)
        if size_bytes > self._disk_spill_threshold and not hasattr(
            result, "num_blocks"
        ):
            disk_key = str(hash(key))
            self._disk_cache.put(disk_key, result)
            return

        with self._lock:
            # Check if we need to evict entries
            while (
                self.current_size_bytes + size_bytes > self.max_size_bytes
                and self._cache
            ):
                self._evict_lru()

            # Store in memory cache
            entry = _CacheEntry(key, result, size_bytes)
            if key in self._cache:
                old_entry = self._cache[key]
                self.current_size_bytes -= old_entry.size_bytes

            self._cache[key] = entry
            self.current_size_bytes += size_bytes

    def invalidate_for_transformation(
        self, base_logical_plan: LogicalPlan, transformation_type: TransformationType
    ) -> None:
        """Selectively invalidate cache based on transformation type."""
        valid_operations = CACHE_VALIDITY_MATRIX.get(transformation_type, set())

        with self._lock:
            keys_to_remove = []
            for cache_key in self._cache.keys():
                if self._is_related_plan(cache_key.logical_plan, base_logical_plan):
                    try:
                        operation_enum = CacheableOperation(cache_key.operation_name)
                        if operation_enum not in valid_operations:
                            keys_to_remove.append(cache_key)
                    except ValueError:
                        # Unknown operation, invalidate conservatively
                        keys_to_remove.append(cache_key)

            # Remove invalidated entries
            for key in keys_to_remove:
                entry = self._cache.pop(key)
                self.current_size_bytes -= entry.size_bytes

    def _is_related_plan(self, plan1: LogicalPlan, plan2: LogicalPlan) -> bool:
        """Check if two logical plans are related."""
        # Simple check using string representation
        plan1_str = str(plan1.dag)
        plan2_str = str(plan2.dag)
        return plan2_str in plan1_str or plan1_str in plan2_str

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
            self.current_size_bytes = 0

        self._disk_cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        with self._lock:
            total_requests = self._hit_count + self._miss_count
            hit_rate = self._hit_count / total_requests if total_requests > 0 else 0

            return {
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_rate": hit_rate,
                "cache_size_bytes": self.current_size_bytes,
                "cache_entries": len(self._cache),
                "disk_cache_size_bytes": self._disk_cache.current_size_bytes,
                "disk_cache_entries": len(self._disk_cache._entries),
                "max_size_bytes": self.max_size_bytes,
                "disk_spill_threshold_bytes": self._disk_spill_threshold,
            }

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return

        key, entry = self._cache.popitem(last=False)
        self.current_size_bytes -= entry.size_bytes

    def _estimate_materialized_dataset_size(self, materialized_dataset) -> int:
        """Estimate size of MaterializedDataset object (not the data itself).

        MaterializedDataset contains ObjectRef[Block] references, not the actual data.
        We estimate the size of the object structure itself, which is small.
        """
        try:
            # MaterializedDataset object overhead
            base_size = 1024  # Base object size

            # Size of block references and metadata (not the actual block data)
            num_blocks = materialized_dataset.num_blocks()
            ref_overhead = num_blocks * 64  # ~64 bytes per ObjectRef + metadata

            # Logical plan overhead
            plan_overhead = 2048  # Estimated logical plan size

            total_size = base_size + ref_overhead + plan_overhead

            logger.debug(
                f"MaterializedDataset size estimate: {num_blocks} blocks -> {total_size:,} bytes overhead"
            )
            return total_size

        except Exception as e:
            logger.debug(f"Failed to estimate MaterializedDataset size: {e}")
            return 4096  # Conservative fallback

    def _estimate_size(self, obj: Any) -> int:
        """Estimate memory size of an object."""
        if hasattr(obj, "__sizeof__"):
            return obj.__sizeof__()
        elif isinstance(obj, (list, tuple)):
            if len(obj) > 1000:
                # Sample large collections
                sample_size = min(100, len(obj) // 10)
                sample_total = sum(
                    self._estimate_size(obj[i])
                    for i in range(0, len(obj), len(obj) // sample_size)
                )
                return sample_total * len(obj) // sample_size
            else:
                return sum(self._estimate_size(item) for item in obj)
        elif isinstance(obj, dict):
            return sum(
                self._estimate_size(k) + self._estimate_size(v) for k, v in obj.items()
            )
        else:
            return 1024  # 1KB fallback


# Global cache manager
_global_cache_manager: Optional[_DatasetCacheManager] = None
_cache_lock = threading.Lock()


def _get_cache_manager() -> _DatasetCacheManager:
    """Get the global cache manager instance."""
    global _global_cache_manager
    with _cache_lock:
        if _global_cache_manager is None:
            context = DataContext.get_current()
            max_cache_size = getattr(
                context, "dataset_cache_max_size_bytes", 1024 * 1024 * 1024
            )
            _global_cache_manager = _DatasetCacheManager(max_cache_size)
        return _global_cache_manager


def cache_result(operation_name: str, include_params: List[str] = None):
    """Decorator to cache Dataset operation results.

    Args:
        operation_name: Name of the operation being cached.
        include_params: List of parameter names to include in cache key.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Check if caching is enabled
            context = DataContext.get_current()
            if not getattr(context, "enable_dataset_caching", True):
                return func(self, *args, **kwargs)

            # Create cache key
            cache_params = {}
            if include_params:

                sig = inspect.signature(func)
                bound_args = sig.bind(self, *args, **kwargs)
                bound_args.apply_defaults()

                for param_name in include_params:
                    if param_name in bound_args.arguments:
                        cache_params[param_name] = bound_args.arguments[param_name]

            cache_key = _CacheKey(self._logical_plan, operation_name, **cache_params)

            # Try to get from cache
            cache_manager = _get_cache_manager()
            cached_result = cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result

            # Execute operation and cache result
            result = func(self, *args, **kwargs)
            cache_manager.put(cache_key, result)

            return result

        return wrapper

    return decorator


def invalidate_cache_on_transform(operation_name: str):
    """Decorator to invalidate cache when transformations are applied.

    Args:
        operation_name: Name of the transformation operation.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Get original logical plan
            original_logical_plan = self._logical_plan

            # Execute transformation
            result = func(self, *args, **kwargs)

            # Apply selective invalidation if caching enabled
            context = DataContext.get_current()
            if getattr(context, "enable_dataset_caching", True) and hasattr(
                result, "_logical_plan"
            ):
                try:
                    transformation_type = get_transformation_type(operation_name)
                    cache_manager = _get_cache_manager()
                    cache_manager.invalidate_for_transformation(
                        original_logical_plan, transformation_type
                    )
                except Exception as e:
                    logger.debug(f"Cache invalidation failed for {operation_name}: {e}")

            return result

        return wrapper

    return decorator


# Context manager for cache configuration
class _CacheConfig:
    """Context manager for temporary cache configuration."""

    def __init__(self, enabled: bool = True, max_size_bytes: int = None):
        self.enabled = enabled
        self.max_size_bytes = max_size_bytes
        self._old_enabled = None
        self._old_max_size = None

    def __enter__(self):
        context = DataContext.get_current()
        self._old_enabled = getattr(context, "enable_dataset_caching", True)
        context.enable_dataset_caching = self.enabled

        if self.max_size_bytes is not None:
            global _global_cache_manager
            if _global_cache_manager is not None:
                self._old_max_size = _global_cache_manager.max_size_bytes
                _global_cache_manager.max_size_bytes = self.max_size_bytes

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        context = DataContext.get_current()
        context.enable_dataset_caching = self._old_enabled

        if self._old_max_size is not None:
            global _global_cache_manager
            if _global_cache_manager is not None:
                _global_cache_manager.max_size_bytes = self._old_max_size


# Public API functions
def clear_dataset_cache() -> None:
    """Clear all cached Dataset results."""
    cache_manager = _get_cache_manager()
    cache_manager.clear()


def get_cache_stats() -> Dict[str, Any]:
    """Get Dataset cache statistics."""
    cache_manager = _get_cache_manager()
    return cache_manager.get_stats()


def disable_dataset_caching():
    """Context manager to temporarily disable dataset caching."""
    return _CacheConfig(enabled=False)


def set_cache_size(max_size_bytes: int):
    """Context manager to temporarily set cache size limit."""
    return _CacheConfig(max_size_bytes=max_size_bytes)
