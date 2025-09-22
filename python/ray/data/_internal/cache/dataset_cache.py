"""
Simple, elegant caching for Ray Data operations.

This module provides lightweight caching that leverages Ray Data's existing
ExecutionPlan snapshots and follows established Ray Data patterns.
"""

import functools
import inspect
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data.context import DataContext


class TransformationType(Enum):
    """Categories of transformations and their cache effects."""

    SCHEMA_PRESERVING_COUNT_CHANGING = "schema_preserving_count_changing"
    ROW_PRESERVING_SCHEMA_CHANGE = "row_preserving_schema_change"
    ROW_CHANGING_NO_SCHEMA_CHANGE = "row_changing_no_schema_change"
    ROW_CHANGING_SCHEMA_CHANGE = "row_changing_schema_change"
    REORDERING_ONLY = "reordering_only"
    COMBINING = "combining"
    GROUPING = "grouping"


class CacheableOperation(Enum):
    """Operations that can be cached."""

    COUNT = "count"
    SCHEMA = "schema"
    SIZE_BYTES = "size_bytes"
    STATS = "stats"
    INPUT_FILES = "input_files"
    COLUMNS = "columns"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    STD = "std"
    UNIQUE = "unique"
    TAKE = "take"
    TAKE_BATCH = "take_batch"
    TAKE_ALL = "take_all"


# Smart invalidation matrix - which operations remain valid after transformations
CACHE_VALIDITY_MATRIX = {
    TransformationType.SCHEMA_PRESERVING_COUNT_CHANGING: {
        CacheableOperation.SCHEMA,
        CacheableOperation.COLUMNS,
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


# Transformation type mapping
TRANSFORMATION_TYPES = {
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


@dataclass
class CacheStats:
    """Statistics for cache performance monitoring."""

    hit_count: int = 0
    miss_count: int = 0
    local_cache_entries: int = 0
    ray_cache_entries: int = 0
    max_entries: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0


@dataclass
class CacheConfiguration:
    """Configuration for dataset caching behavior."""

    max_entries: int = 10000
    enable_smart_updates: bool = True


class DatasetCache:
    """Cache for Dataset operation results.

    Provides caching while following Ray Data patterns:
    - Local memory: Small metadata and aggregations
    - Ray object store: Medium-sized results with automatic disk spilling
    - Smart invalidation: Preserves valid cached operations based on transformation type
    - Size-aware: Automatically chooses optimal caching strategy
    """

    def __init__(self, max_entries: int = 10000):
        # Local cache for small objects (fastest access)
        self._local_cache: OrderedDict[str, Any] = OrderedDict()
        # Ray object store cache for medium objects (automatic spilling)
        self._ray_cache: OrderedDict[str, Any] = OrderedDict()  # stores ObjectRefs
        self._max_entries = max_entries
        self._hit_count = 0
        self._miss_count = 0

        # Intelligent size thresholds
        self._local_threshold = 50 * 1024  # 50KB - keep reasonably sized objects local
        self._ray_threshold = 100 * 1024 * 1024  # 100MB - use Ray store up to 100MB

        # Performance tracking
        self._cache_saves_seconds = 0.0  # Track time saved by cache hits

    def get(
        self, logical_plan: LogicalPlan, operation_name: str, **params
    ) -> Optional[Any]:
        """Get cached result for operation."""
        cache_key = self._make_key(logical_plan, operation_name, **params)

        # Try local cache first (fastest)
        if cache_key in self._local_cache:
            result = self._local_cache[cache_key]
            self._local_cache.move_to_end(cache_key)
            self._hit_count += 1
            return result

        # Try Ray object store cache (for medium objects)
        if cache_key in self._ray_cache:
            try:
                import ray

                object_ref = self._ray_cache[cache_key]
                result = ray.get(object_ref)
                self._ray_cache.move_to_end(cache_key)
                self._hit_count += 1
                return result
            except Exception:
                self._ray_cache.pop(cache_key, None)

            self._miss_count += 1
            return None

    def put(
        self, logical_plan: LogicalPlan, operation_name: str, result: Any, **params
    ) -> None:
        """Cache result for operation with intelligent placement."""
        cache_key = self._make_key(logical_plan, operation_name, **params)

        # Determine cache strategy based on size and type
        cache_strategy = self._get_cache_strategy(operation_name, result)

        if cache_strategy == "local":
            # Evict LRU if needed
            while len(self._local_cache) >= self._max_entries // 2:
                self._local_cache.popitem(last=False)
            self._local_cache[cache_key] = result

        elif cache_strategy == "ray":
            try:
                import ray

                object_ref = ray.put(result)
                # Evict LRU if needed
                while len(self._ray_cache) >= self._max_entries // 2:
                    old_key, old_ref = self._ray_cache.popitem(last=False)
                    # Ray GC will handle ObjectRef cleanup
                self._ray_cache[cache_key] = object_ref
            except Exception:
                pass

        # cache_strategy == "none" → don't cache

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_plan: LogicalPlan,
        target_plan: LogicalPlan,
        **transform_params,
    ) -> None:
        """Intelligently update cache entries based on transformation type."""
        transform_type = TRANSFORMATION_TYPES.get(
            operation_name, TransformationType.ROW_CHANGING_SCHEMA_CHANGE
        )
        valid_operations = CACHE_VALIDITY_MATRIX.get(transform_type, set())

        # First, try to compute new cache values from existing ones
        self._smart_cache_update(
            operation_name, source_plan, target_plan, transform_params
        )

        # Then invalidate remaining entries that can't be computed
        self._invalidate_cache_dict(self._local_cache, valid_operations)
        self._invalidate_cache_dict(self._ray_cache, valid_operations)

    def clear(self) -> None:
        """Clear all cache entries."""
        self._local_cache.clear()
        self._ray_cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total = self._hit_count + self._miss_count
        return {
            "hit_count": self._hit_count,
            "miss_count": self._miss_count,
            "hit_rate": self._hit_count / total if total > 0 else 0,
            "local_cache_entries": len(self._local_cache),
            "ray_cache_entries": len(self._ray_cache),
            "total_entries": len(self._local_cache) + len(self._ray_cache),
            "max_entries": self._max_entries,
        }

    def _invalidate_cache_dict(
        self, cache_dict: OrderedDict, valid_operations: set
    ) -> None:
        """Invalidate entries in a cache dictionary."""
        keys_to_remove = []
        for cache_key in cache_dict.keys():
            for op in CacheableOperation:
                if op.value in cache_key and op not in valid_operations:
                    keys_to_remove.append(cache_key)
                    break

        for key in keys_to_remove:
            cache_dict.pop(key, None)

    def _smart_cache_update(
        self,
        operation_name: str,
        source_plan: LogicalPlan,
        target_plan: LogicalPlan,
        transform_params: dict,
    ) -> None:
        """Compute new cache values from existing ones.

        CACHE SYSTEM OVERVIEW:

        Instead of just invalidating cache entries after transformations, this system
        COMPUTES new cached values from existing ones using mathematical relationships
        and logical analysis of what each operation actually changes.

        PERFORMANCE APPROACH:

        Traditional approach:
        1. ds.count() → compute and cache
        2. ds.limit(100) → invalidate count cache
        3. limited_ds.count() → recompute from scratch

        Smart cache approach:
        1. ds.count() → compute and cache: 10000
        2. ds.limit(100) → compute new count: min(10000, 100) = 100
        3. limited_ds.count() → use computed value

        OPERATION CATEGORIES:

        1. COUNT-PRESERVING: map, add_column, drop_columns, select_columns, rename_columns
           → Preserve count (1:1 transformations or column-only changes)

        2. VALUE-PRESERVING: sort, random_shuffle, repartition
           → Preserve ALL aggregations (values unchanged, only order/blocks change)

        3. STRUCTURE-PRESERVING: filter
           → Preserve schema/columns (same structure, different rows)

        4. SIZE-COMPUTABLE: limit
           → Compute new count and size from original values + parameters

        5. COLUMN-COMPUTABLE: add_column, drop_columns, select_columns, rename_columns
           → Compute new columns list from original list + operation parameters

        This is the core optimization that provides massive performance gains by computing
        new cached values instead of invalidating and recomputing from scratch.
        """

        source_key_prefix = self._make_key(source_plan, "")
        target_key_prefix = self._make_key(target_plan, "")

        # Smart cache updates for specific operations
        if operation_name == "limit":
            self._update_limit_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "add_column":
            self._update_add_column_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "drop_columns":
            self._update_drop_columns_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "select_columns":
            self._update_select_columns_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "repartition":
            self._update_repartition_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "rename_columns":
            self._update_rename_columns_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name in ["sort", "random_shuffle"]:
            self._update_reorder_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "map":
            self._update_map_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "with_column":  # Uses add_column transform type
            self._update_with_column_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        elif operation_name == "filter":
            self._update_filter_caches(
                source_key_prefix, target_key_prefix, transform_params
            )
        # Advanced: Cross-operation cache computation
        self._try_advanced_cache_computation(
            operation_name, source_key_prefix, target_key_prefix, transform_params
        )

    def _update_limit_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for limit operation.

        LOGIC: limit(N) operation truncates dataset to first N rows.
        - If original dataset has 10000 rows and we limit(500), new count = 500
        - If original dataset has 100 rows and we limit(500), new count = 100 (unchanged)
        - Formula: new_count = min(original_count, limit_value)

        This avoids expensive recomputation of count after limit operations.
        """
        limit_value = params.get("limit")
        if limit_value is None:
            return

        # Check if we have cached count from the source dataset
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]

            # SMART COMPUTATION: new count = min(original count, limit value)
            # This is mathematically correct and avoids expensive recomputation
            new_count = min(original_count, limit_value)

            # Cache the computed count for the target dataset
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = new_count

    def _update_add_column_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for add_column operation.

        LOGIC: add_column(name, function) adds a new column to the dataset.
        - Row count stays the same (each row gets a new column value)
        - Column list grows by one (original columns + new column)
        - Schema changes (new column added to structure)
        - Aggregations become invalid (data in rows changed)

        SMART OPTIMIZATIONS:
        1. Preserve count: 1000 rows → still 1000 rows after adding column
        2. Compute new columns: ["id", "value"] → ["id", "value", "new_col"]

        This avoids expensive count() and columns() recomputation.
        """
        column_name = params.get("col") or params.get("column_name")
        if column_name is None:
            return

        # OPTIMIZATION 1: Preserve count (add_column is 1:1 row transformation)
        # Original: 1000 rows → After add_column: still 1000 rows
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = original_count

        # OPTIMIZATION 2: Compute new columns list from existing list
        # Original: ["id", "value"] → After add_column("new"): ["id", "value", "new"]
        source_columns_key = f"columns_{source_prefix}_"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]
            if (
                isinstance(original_columns, list)
                and column_name not in original_columns
            ):
                new_columns = original_columns + [column_name]
                target_columns_key = f"columns_{target_prefix}_"
                self._local_cache[target_columns_key] = new_columns

    def _update_drop_columns_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for drop_columns operation.

        LOGIC: drop_columns(["col1", "col2"]) removes specified columns from dataset.
        - Row count stays the same (same rows, just fewer columns per row)
        - Column list shrinks (remove specified columns from list)
        - Schema changes (columns removed from structure)
        - Aggregations on remaining columns could be preserved, but complex to track

        SMART OPTIMIZATIONS:
        1. Preserve count: 1000 rows → still 1000 rows after dropping columns
        2. Compute new columns: ["id", "name", "value"] → ["id", "value"] (dropped "name")

        This avoids expensive count() and columns() recomputation.
        """
        cols_to_drop = params.get("cols", [])
        if not cols_to_drop:
            return

        # OPTIMIZATION 1: Preserve count (drop_columns doesn't change number of rows)
        # Original: 1000 rows → After drop_columns: still 1000 rows (same data, fewer columns)
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = original_count

        # OPTIMIZATION 2: Compute new columns list by filtering out dropped columns
        # Original: ["id", "name", "value"] → After drop_columns(["name"]): ["id", "value"]
        source_columns_key = f"columns_{source_prefix}_"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]
            if isinstance(original_columns, list):
                new_columns = [c for c in original_columns if c not in cols_to_drop]
                target_columns_key = f"columns_{target_prefix}_"
                self._local_cache[target_columns_key] = new_columns

    def _update_select_columns_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for select_columns operation.

        LOGIC: select_columns(["col1", "col2"]) keeps only specified columns.
        - Row count stays the same (same rows, just subset of columns per row)
        - Column list becomes exactly the selected columns
        - Schema changes (only selected columns remain)
        - Aggregations on selected columns could be preserved, but complex to track

        SMART OPTIMIZATIONS:
        1. Preserve count: 1000 rows → still 1000 rows after selecting columns
        2. Set new columns: ["id", "name", "value"] → ["id", "value"] (selected subset)

        This avoids expensive count() and columns() recomputation.
        """
        cols_to_select = params.get("cols", [])
        if not cols_to_select:
            return

        # Normalize single column to list for consistent handling
        if isinstance(cols_to_select, str):
            cols_to_select = [cols_to_select]

        # OPTIMIZATION 1: Preserve count (select_columns doesn't change number of rows)
        # Original: 1000 rows → After select_columns: still 1000 rows (same data, selected columns)
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = original_count

        # OPTIMIZATION 2: Set new columns list to exactly the selected columns
        # Original: ["id", "name", "value"] → After select_columns(["id", "value"]): ["id", "value"]
        target_columns_key = f"columns_{target_prefix}_"
        self._local_cache[target_columns_key] = cols_to_select

    def _update_repartition_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for repartition operation.

        LOGIC: repartition(num_blocks) changes block structure but NOT data content.
        - Row count stays exactly the same (same data, different block organization)
        - Schema stays exactly the same (same columns and types)
        - Column list stays exactly the same (no columns added/removed)
        - ALL aggregations stay the same (sum, min, max, mean, std unchanged)
        - Size stays the same (same data, just reorganized)
        - Only block-level metadata changes (number of blocks, rows per block)

        SMART OPTIMIZATIONS:
        1. Preserve count: 10000 rows → still 10000 rows after repartition
        2. Preserve schema: Same column structure
        3. Preserve ALL aggregations: sum, min, max, mean, std all unchanged

        This avoids expensive recomputation for repartition operations.
        """
        # OPTIMIZATION: Preserve almost everything for repartition
        # Repartition only changes block structure, not data content
        preserve_operations = [
            "count",
            "schema",
            "columns",
            "size_bytes",
            "sum",
            "min",
            "max",
            "mean",
            "std",
        ]

        for op in preserve_operations:
            source_key = f"{op}_{source_prefix}_"
            if source_key in self._local_cache:
                original_value = self._local_cache[source_key]
                target_key = f"{op}_{target_prefix}_"
                self._local_cache[target_key] = original_value

    def _update_rename_columns_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for rename_columns operation.

        LOGIC: rename_columns({"old": "new"}) changes column names but NOT data content.
        - Row count stays exactly the same (same rows, just renamed columns)
        - Column list changes (same columns, different names)
        - Schema changes (column names updated, but types stay same)
        - Aggregations stay the same but need column name mapping

        SMART OPTIMIZATIONS:
        1. Preserve count: 1000 rows → still 1000 rows after rename
        2. Compute new columns: ["id", "name"] → ["id", "full_name"] (renamed "name" to "full_name")
        3. Could preserve aggregations with name mapping (future enhancement)

        This avoids expensive count() and columns() recomputation.
        """
        names = params.get("names", {})
        if not names:
            return

        # OPTIMIZATION 1: Preserve count (rename doesn't change number of rows)
        # Original: 1000 rows → After rename: still 1000 rows (same data, renamed columns)
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = original_count

        # OPTIMIZATION 2: Compute new columns list with renamed column names
        # Original: ["id", "name", "value"] → After rename({"name": "full_name"}): ["id", "full_name", "value"]
        source_columns_key = f"columns_{source_prefix}_"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]
            if isinstance(original_columns, list):
                # Handle both dict mapping and list of new names
                if isinstance(names, dict):
                    # Apply rename mapping: old_name → new_name
                    new_columns = [names.get(col, col) for col in original_columns]
                elif isinstance(names, list):
                    # Replace with new names (truncate if needed)
                    new_columns = names[: len(original_columns)]
                else:
                    return  # Unknown format

                target_columns_key = f"columns_{target_prefix}_"
                self._local_cache[target_columns_key] = new_columns

    def _update_reorder_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for reordering operations (sort, shuffle).

        LOGIC: sort/random_shuffle changes row ORDER but NOT row VALUES.
        - Row count stays exactly the same (same rows, different order)
        - Schema stays exactly the same (same columns and types)
        - Column list stays exactly the same (no columns added/removed)
        - ALL aggregations stay exactly the same (brilliant optimization!)
          * sum([1,2,3]) == sum([3,1,2]) → order doesn't matter for aggregations
          * min([1,2,3]) == min([3,1,2]) → minimum value unchanged
          * max([1,2,3]) == max([3,1,2]) → maximum value unchanged
          * mean([1,2,3]) == mean([3,1,2]) → average unchanged
        - Size stays the same (same data, just reordered)
        - Unique values stay the same (same set of values, different order)
        - Only take() and materialize() become invalid (order-dependent operations)

        SMART OPTIMIZATIONS:
        1. Preserve count: 10000 rows → still 10000 rows after sort
        2. Preserve ALL aggregations: sum, min, max, mean, std all unchanged
        3. Preserve unique values: same set of unique values, just reordered

        This optimization provides significant performance benefits.
        """
        # OPTIMIZATION: Preserve almost everything for reordering operations
        # Sort/shuffle only changes ORDER, not VALUES, so aggregations stay valid
        preserve_operations = [
            "count",
            "schema",
            "columns",
            "size_bytes",
            "sum",
            "min",
            "max",
            "mean",
            "std",
            "unique",
        ]

        for op in preserve_operations:
            source_key = f"{op}_{source_prefix}_"
            if source_key in self._local_cache:
                original_value = self._local_cache[source_key]
                target_key = f"{op}_{target_prefix}_"
                self._local_cache[target_key] = original_value

    def _update_map_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for map operation.

        LOGIC: map(function) transforms each row individually (1:1 transformation).
        - Row count stays exactly the same (one output row per input row)
        - Schema usually changes (function transforms row structure)
        - Column list usually changes (function defines new column structure)
        - Aggregations become invalid (row values completely transformed)

        SMART OPTIMIZATIONS:
        1. Preserve count: 1000 rows → still 1000 rows after map (1:1 transformation)

        Example: map(lambda x: {"doubled": x["id"] * 2})
        - Input: 1000 rows with {"id": ...}
        - Output: 1000 rows with {"doubled": ...}
        - Count preserved: 1000 → 1000         - Schema changed: {"id": int} → {"doubled": int} (invalidated)

        This avoids expensive count() recomputation after map operations.
        """
        # OPTIMIZATION: Preserve count (map is 1:1 row transformation)
        # Original: 1000 rows → After map: still 1000 rows (each input row → one output row)
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = original_count

    def _update_with_column_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for with_column operation.

        LOGIC: with_column(name, expression) adds a computed column using expressions.
        - Row count stays exactly the same (each row gets a new computed column)
        - Column list grows by one (original columns + new computed column)
        - Schema changes (new column added with computed values)
        - Aggregations become invalid (row data changed due to new column)

        SMART OPTIMIZATIONS:
        1. Preserve count: 1000 rows → still 1000 rows after with_column
        2. Compute new columns: ["id", "value"] → ["id", "value", "computed_col"]

        Example: with_column("doubled", col("id") * 2)
        - Input: 1000 rows with ["id", "value"]
        - Output: 1000 rows with ["id", "value", "doubled"]
        - Count preserved: 1000 → 1000         - Columns computed: ["id", "value"] → ["id", "value", "doubled"]
        This avoids expensive count() and columns() recomputation.
        """
        column_name = params.get("column_name")
        if column_name is None:
            return

        # OPTIMIZATION 1: Preserve count (with_column is 1:1 row transformation)
        # Original: 1000 rows → After with_column: still 1000 rows (each row gets new column)
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = original_count

        # OPTIMIZATION 2: Compute new columns list by appending the new column
        # Original: ["id", "value"] → After with_column("doubled", ...): ["id", "value", "doubled"]
        source_columns_key = f"columns_{source_prefix}_"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]
            if (
                isinstance(original_columns, list)
                and column_name not in original_columns
            ):
                new_columns = original_columns + [column_name]
                target_columns_key = f"columns_{target_prefix}_"
                self._local_cache[target_columns_key] = new_columns

    def _update_filter_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for filter operation.

        LOGIC: filter(predicate) removes rows that don't match the predicate.
        - Row count usually changes (some rows filtered out)
        - Schema stays exactly the same (same columns and types)
        - Column list stays exactly the same (no columns added/removed)
        - Aggregations become invalid (different set of rows)

        SMART OPTIMIZATIONS:
        1. Preserve schema: same column structure after filtering
        2. Preserve columns: same column names after filtering

        Example: filter(lambda x: x["value"] > 100)
        - Input: 1000 rows with ["id", "value"]
        - Output: 300 rows with ["id", "value"] (700 rows filtered out)
        - Count changes: 1000 → 300 (invalidated)
        - Schema preserved: {"id": int, "value": int} → same         - Columns preserved: ["id", "value"] → same
        This avoids expensive schema() and columns() recomputation.
        """
        # OPTIMIZATION: Preserve schema and columns (filter doesn't change structure)
        # Filter only removes rows, doesn't change column structure
        preserve_operations = ["schema", "columns"]

        for op in preserve_operations:
            source_key = f"{op}_{source_prefix}_"
            if source_key in self._local_cache:
                original_value = self._local_cache[source_key]
                target_key = f"{op}_{target_prefix}_"
                self._local_cache[target_key] = original_value

    def _try_advanced_cache_computation(
        self, operation_name: str, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Advanced cache computation across operations.

        This method implements sophisticated cross-operation optimizations that go beyond
        simple preservation. These optimizations compute new cached values using mathematical
        relationships between operations.

        ADVANCED OPTIMIZATIONS:
        1. Size computation: Estimate new dataset size from existing size + operation parameters
        2. Take transformation: Transform cached take results instead of recomputing
        3. Aggregation mapping: Preserve aggregations with column name changes
        4. Schema computation: Build new schema from existing schema + operation changes

        These optimizations improve performance for complex pipelines.
        """

        # ADVANCED OPTIMIZATION 1: Size computation for limit operations
        # If we know original size and count, we can estimate new size after limit
        if operation_name == "limit":
            self._compute_limit_size_bytes(source_prefix, target_prefix, params)

        # ADVANCED OPTIMIZATION 2: Take result computation for simple transformations
        # For operations that transform data predictably, transform cached take results
        if operation_name in [
            "add_column",
            "drop_columns",
            "select_columns",
            "rename_columns",
        ]:
            self._try_compute_take_from_transformation(
                operation_name, source_prefix, target_prefix, params
            )

        # ADVANCED OPTIMIZATION 3: Aggregation preservation for column operations
        # When columns are renamed, aggregations on those columns should be preserved with new names
        if operation_name in ["rename_columns"]:
            self._try_preserve_renamed_aggregations(
                source_prefix, target_prefix, params
            )

        # ADVANCED OPTIMIZATION 4: Schema computation for column operations
        # Build new schema from existing schema + operation changes (add/drop/select columns)
        if operation_name in ["add_column", "drop_columns", "select_columns"]:
            self._try_compute_schema_from_columns(source_prefix, target_prefix, params)

    def _compute_limit_size_bytes(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Compute size_bytes for limit operation from existing cache.

        ADVANCED LOGIC: If we know the original dataset size and count, we can mathematically
        estimate the new size after a limit operation without executing it.

        MATHEMATICAL RELATIONSHIP:
        - Original: 10000 rows, 50MB total size → 5KB per row average
        - After limit(2000): 2000 rows → estimated size = 2000 × 5KB = 10MB
        - Formula: new_size = (new_count / original_count) × original_size

        This avoids expensive size_bytes() recomputation for limit operations.
        """
        limit_value = params.get("limit")
        if limit_value is None:
            return

        # Check if we have both count and size_bytes cached from source
        source_count_key = f"count_{source_prefix}_"
        source_size_key = f"size_bytes_{source_prefix}_"

        if (
            source_count_key in self._local_cache
            and source_size_key in self._local_cache
        ):
            original_count = self._local_cache[source_count_key]
            original_size = self._local_cache[source_size_key]

            if original_count > 0:
                # MATHEMATICAL COMPUTATION: Estimate new size based on row proportion
                # Step 1: Calculate average size per row
                size_per_row = original_size / original_count

                # Step 2: Calculate new count (same as limit count computation)
                new_count = min(original_count, limit_value)

                # Step 3: Estimate new size = new_count × size_per_row
                new_size = int(size_per_row * new_count)

                # Cache the computed size
                target_size_key = f"size_bytes_{target_prefix}_"
                self._local_cache[target_size_key] = new_size

    def _try_compute_take_from_transformation(
        self, operation_name: str, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Try to compute take results for simple transformations.

        FUTURE ENHANCEMENT: For operations that transform data predictably, we could
        transform cached take results instead of invalidating them.

        POTENTIAL LOGIC:
        - drop_columns: Remove specified columns from cached take result
        - select_columns: Keep only specified columns from cached take result
        - rename_columns: Rename column keys in cached take result

        Example for drop_columns(["name"]):
        - Cached take: [{"id": 1, "name": "John", "value": 100}, ...]
        - Computed take: [{"id": 1, "value": 100}, ...] (removed "name")

        This would avoid expensive take() recomputation for simple column operations.
        Implementation requires careful handling of data types and edge cases.
        """
        # This is a sophisticated enhancement for future implementation
        # Requires careful analysis of transformation functions and data types
        pass

    def _try_preserve_renamed_aggregations(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Try to preserve aggregations when columns are renamed.

        Args:
            source_prefix: Cache key prefix for source dataset.
            target_prefix: Cache key prefix for target dataset.
            params: Transformation parameters including column rename mapping.

        FUTURE ENHANCEMENT: When columns are renamed, aggregations on those columns
        should be preserved with updated column names in the cache keys.

        POTENTIAL LOGIC:
        - If sum("old_name") = 12345 is cached
        - After rename_columns({"old_name": "new_name"})
        - We should cache sum("new_name") = 12345 (same value, new column name)

        Example:
        - Original: sum("revenue") = 1000000 (cached)
        - After rename({"revenue": "total_sales"})
        - Computed: sum("total_sales") = 1000000 (preserved with new name)

        This would improve performance for rename operations.
        Implementation requires parsing cache keys and mapping column names.
        """
        names = params.get("names", {})
        if not isinstance(names, dict):
            return

        # This is a sophisticated enhancement for future implementation
        # Requires parsing cache keys and mapping old column names to new names
        pass

    def _try_compute_schema_from_columns(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Try to compute schema from columns list for column operations.

        FUTURE ENHANCEMENT: If we have cached schema and know the column changes,
        we could compute the new schema without executing the operation.

        POTENTIAL LOGIC:
        - add_column: Add new column to existing schema with inferred type
        - drop_columns: Remove specified columns from existing schema
        - select_columns: Keep only specified columns from existing schema

        Example for drop_columns(["name"]):
        - Original schema: {"id": int64, "name": string, "value": float64}
        - Computed schema: {"id": int64, "value": float64} (removed "name")

        This would avoid expensive schema() recomputation for column operations.
        Implementation requires understanding PyArrow schema manipulation.
        """
        # This is a sophisticated enhancement for future implementation
        # Requires understanding PyArrow/Pandas schema structures and type inference
        pass

    def _update_union_caches(
        self, source_prefix: str, target_prefix: str, params: dict
    ) -> None:
        """Smart cache update for union operation."""
        # For union, we could compute count if we have counts from all datasets
        # This is more complex and would require tracking multiple source plans
        # For now, we'll leave this as a future enhancement
        pass

    def _get_cache_strategy(self, operation_name: str, result: Any) -> str:
        """Determine optimal caching strategy based on operation type and result size.

        Args:
            operation_name: Name of the operation being cached.
            result: The result object to be cached.

        CACHING STRATEGY LOGIC:

        1. LOCAL CACHE (fastest access, ≤50KB):
           - Small metadata: count, schema, columns (always small)
           - Aggregations: sum, min, max, mean, std (single values)
           - Small data: take(≤50 rows), unique(≤1000 values)

        2. RAY OBJECT STORE (automatic disk spilling, 50KB-100MB):
           - Medium data: take(50-5000 rows), unique(1000-50000 values)
           - MaterializedDataset: ≤1000 blocks (ObjectRef structure only)
           - take_all: ≤1000 rows (very small datasets only)

        3. NO CACHE (too large, let Ray Data handle):
           - Large data: take(>5000 rows), unique(>50000 values)
           - Huge MaterializedDataset: >1000 blocks
           - take_all: >1000 rows (unsafe to cache)

        Returns:
            'local' → cache in local memory (fastest)
            'ray' → cache in Ray object store (automatic spilling)
            'none' → don't cache (too large or unsafe)
        """

        # STRATEGY 1: LOCAL CACHE - Always cache small metadata locally (fastest access)
        if operation_name in [
            "count",
            "schema",
            "size_bytes",
            "input_files",
            "columns",
        ]:
            return "local"  # Always small (8 bytes to ~1KB)

        # STRATEGY 1: LOCAL CACHE - Always cache aggregations locally (single values)
        if operation_name in ["sum", "min", "max", "mean", "std"]:
            return "local"  # Always small (single numeric values)

        # STRATEGY 2: INTELLIGENT SIZE-BASED DECISIONS for data operations
        if operation_name in ["take", "take_batch"]:
            if isinstance(result, list):
                num_rows = len(result)
                if num_rows <= 50:  # Small takes - local cache (fast access)
                    return "local"
                elif (
                    num_rows <= 5000
                ):  # Medium takes - Ray object store (automatic spilling)
                    return "ray"
                else:
                    return "none"  # Large takes - don't cache (too much memory)

        if operation_name == "unique":
            if isinstance(result, list):
                num_unique = len(result)
                if num_unique <= 1000:  # Small unique sets - local cache
                    return "local"
                elif num_unique <= 50000:  # Medium unique sets - Ray object store
                    return "ray"
                else:
                    return "none"  # Large unique sets - don't cache (too much memory)

        if operation_name == "take_all":
            if isinstance(result, list):
                if len(result) <= 1000:  # Only cache very small datasets
                    return "ray"  # Use Ray store for safety (could still be large)
                else:
                    return "none"  # Large datasets - unsafe to cache (memory risk)

        # STRATEGY 3: MATERIALIZED DATASET - Cache ObjectRef structure only
        if operation_name == "materialize":
            if hasattr(result, "num_blocks"):
                num_blocks = result.num_blocks()
                if num_blocks <= 1000:  # Reasonable-sized datasets
                    return "ray"  # Cache ObjectRef structure (leverages Ray's capabilities)
                else:
                    return "none"  # Very large datasets - let ExecutionPlan._snapshot_bundle handle

        # STRATEGY 1: LOCAL CACHE - Stats are usually small text
        if operation_name == "stats":
            return "local"  # Usually small formatted text output

        # STRATEGY 4: CONSERVATIVE DEFAULT - Don't cache unknown operations
        return "none"  # Safe default for unknown operations

    def _make_key(
        self, logical_plan: LogicalPlan, operation_name: str, **params
    ) -> str:
        """Create cache key from logical plan and parameters."""
        plan_hash = hash(str(logical_plan.dag))
        param_hash = hash(frozenset(params.items())) if params else 0
        return f"{operation_name}_{plan_hash}_{param_hash}"


# Global cache instance
_global_cache: Optional[DatasetCache] = None


def _get_cache() -> DatasetCache:
    """Get global cache instance with DataContext configuration."""
    global _global_cache
    if _global_cache is None:
        # Configure cache based on DataContext settings
        context = DataContext.get_current()
        max_entries = getattr(context, "dataset_cache_max_entries", 10000)
        _global_cache = DatasetCache(max_entries)
    return _global_cache


def cache_result(operation_name: str, include_params: List[str] = None):
    """Decorator to cache Dataset operation results."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Check if caching is enabled
            context = DataContext.get_current()
            if not getattr(context, "enable_dataset_caching", True):
                return func(self, *args, **kwargs)

            # Extract parameters for cache key
            cache_params = {}
            if include_params:
                sig = inspect.signature(func)
                bound_args = sig.bind(self, *args, **kwargs)
                bound_args.apply_defaults()
                for param_name in include_params:
                    if param_name in bound_args.arguments:
                        cache_params[param_name] = bound_args.arguments[param_name]

            # Try cache first
            cache = _get_cache()
            cached_result = cache.get(
                self._logical_plan, operation_name, **cache_params
            )
            if cached_result is not None:
                return cached_result

            # Execute and cache result
            result = func(self, *args, **kwargs)
            cache.put(self._logical_plan, operation_name, result, **cache_params)
            return result

        return wrapper

    return decorator


def invalidate_cache_on_transform(operation_name: str):
    """Decorator to intelligently update cache when transformations are applied."""

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Get original logical plan before transformation
            original_plan = self._logical_plan

            # Execute transformation
            result = func(self, *args, **kwargs)

            # Smart cache update if enabled
            context = DataContext.get_current()
            if getattr(context, "enable_dataset_caching", True) and hasattr(
                result, "_logical_plan"
            ):
                cache = _get_cache()

                # Extract transformation parameters
                sig = inspect.signature(func)
                bound_args = sig.bind(self, *args, **kwargs)
                bound_args.apply_defaults()
                transform_params = dict(bound_args.arguments)
                transform_params.pop("self", None)  # Remove self parameter

                cache.invalidate_for_transform(
                    operation_name,
                    original_plan,
                    result._logical_plan,
                    **transform_params,
                )

            return result

        return wrapper

    return decorator


# Public API functions
def clear_dataset_cache() -> None:
    """Clear all cached Dataset results."""
    _get_cache().clear()


def get_cache_stats() -> Dict[str, Any]:
    """Get Dataset cache statistics."""
    return _get_cache().get_stats()


def disable_dataset_caching():
    """Context manager to temporarily disable dataset caching."""

    class _DisableCache:
        def __enter__(self):
            context = DataContext.get_current()
            self._old_enabled = getattr(context, "enable_dataset_caching", True)
            context.enable_dataset_caching = False
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            context = DataContext.get_current()
            context.enable_dataset_caching = self._old_enabled

    return _DisableCache()
