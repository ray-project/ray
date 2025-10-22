"""
Smart cache updates for Ray Data transformations.

This module implements intelligent cache preservation and computation logic
for Dataset transformations. Instead of simply invalidating all cache entries
when a transformation is applied, it:

1. **Preserves** cached values that remain valid (e.g., count after sort)
2. **Computes** new values from existing ones (e.g., count after limit)
3. **Invalidates** only what's necessary (safe and efficient)

This is a large performance improvement. Examples:

    # Sort preserves EVERYTHING (count, sum, min, max, schema, size)
    ds = ray.data.range(100)
    ds.count()  # Executes, caches count=100
    ds.sum("id")  # Executes, caches sum=4950
    sorted_ds = ds.sort("id")
    sorted_ds.count()  # Returns 100 instantly (preserved!)
    sorted_ds.sum("id")  # Returns 4950 instantly (preserved!)

    # Limit smart-computes count
    ds = ray.data.range(100)
    ds.count()  # Executes, caches count=100
    limited_ds = ds.limit(10)
    limited_ds.count()  # Returns 10 instantly (computed: min(100, 10))

    # add_column smart-computes columns list
    ds = ray.data.from_items([{"a": 1}] * 100)
    ds.columns()  # Returns ["a"]
    added_ds = ds.add_column("b", lambda x: x["a"] * 2)
    added_ds.columns()  # Returns ["a", "b"] instantly (computed: ["a"] + ["b"])

The SmartCacheUpdater is the core of this system. It:
- Uses transformation type mappings from constants.py
- Applies preservation rules based on operation type
- Implements smart computation for specific operations
"""

from collections import OrderedDict
from typing import Any, Dict, List, Optional

from .constants import (
    CACHE_PRESERVATION_RULES,
    TRANSFORMATION_TYPES,
    TransformationType,
)

# =============================================================================
# SMART CACHE UPDATER
# =============================================================================


class SmartCacheUpdater:
    """Handles smart cache updates for Dataset transformations.

    This class implements the core logic for intelligently preserving and
    computing cache values after transformations. It has direct access to
    both cache dictionaries to efficiently move/copy entries.

    Architecture:
        - Receives both local and Ray cache references
        - Uses transformation type mappings to determine preservation rules
        - Implements operation-specific smart computation logic
        - Performs all updates atomically (caller holds lock)

    Thread Safety:
        This class does NOT manage locking. The caller (DatasetCache) must
        hold the lock while calling methods on this class.
    """

    def __init__(self, local_cache: OrderedDict, ray_cache: OrderedDict):
        """Initialize the smart cache updater.

        Args:
            local_cache: Reference to the local cache OrderedDict
            ray_cache: Reference to the Ray cache OrderedDict
        """
        self._local_cache = local_cache
        self._ray_cache = ray_cache

    # =========================================================================
    # MAIN ENTRY POINT
    # =========================================================================

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_key_prefix: str,
        target_key_prefix: str,
        **transform_params,
    ) -> None:
        """Update cache based on transformation type.

        This is the main entry point for cache updates. It determines the
        transformation type and applies the appropriate preservation rules.

        Args:
            operation_name: Name of the transformation (e.g., "map", "limit", "sort")
            source_key_prefix: Cache key prefix for source dataset
            target_key_prefix: Cache key prefix for target dataset
            **transform_params: Transformation parameters (args, kwargs)

        Algorithm:
            1. Look up transformation type from operation name
            2. Get preservation rules for that type
            3. Preserve cache entries per rules
            4. Smart-compute new values for specific operations
        """
        # ---------------------------------------------------------------------
        # Look up transformation type
        # ---------------------------------------------------------------------
        # Default to COMPLEX_DATA_TRANSFORM if unknown (safe - preserves nothing)
        transform_type = TRANSFORMATION_TYPES.get(
            operation_name, TransformationType.COMPLEX_DATA_TRANSFORM
        )

        # Get preservation rules for this transformation type
        preservation_rules = CACHE_PRESERVATION_RULES.get(transform_type)

        if preservation_rules is None:
            # Unknown transformation type, don't preserve anything (safe default)
            return

        # ---------------------------------------------------------------------
        # Apply preservation rules
        # ---------------------------------------------------------------------
        # Preserve count for 1:1 transformations
        if preservation_rules.preserves_count:
            self._preserve_count(source_key_prefix, target_key_prefix)

        # Preserve aggregations for reordering operations (HUGE performance win!)
        if preservation_rules.preserves_aggregations:
            self._preserve_aggregations(source_key_prefix, target_key_prefix)

        # Preserve schema and columns for structure-preserving operations
        if preservation_rules.preserves_schema or preservation_rules.preserves_columns:
            self._preserve_metadata(source_key_prefix, target_key_prefix)

        # Preserve size metadata for reordering operations
        if preservation_rules.preserves_size_metadata:
            self._preserve_size_metadata(source_key_prefix, target_key_prefix)

        # ---------------------------------------------------------------------
        # Handle special smart computation cases
        # ---------------------------------------------------------------------
        # Smart-compute count for operations like limit, filter
        if preservation_rules.can_compute_count:
            if operation_name == "limit":
                self._update_limit_count(
                    source_key_prefix, target_key_prefix, transform_params
                )
            elif operation_name == "random_sample":
                self._update_sample_count(
                    source_key_prefix, target_key_prefix, transform_params
                )

        # Smart-compute columns for add/drop/select operations
        if preservation_rules.can_compute_columns:
            if operation_name == "add_column":
                self._update_add_column(
                    source_key_prefix, target_key_prefix, transform_params
                )
            elif operation_name == "drop_columns":
                self._update_drop_columns(
                    source_key_prefix, target_key_prefix, transform_params
                )
            elif operation_name == "select_columns":
                self._update_select_columns(
                    source_key_prefix, target_key_prefix, transform_params
                )
            elif operation_name == "with_column":
                # Note: Currently the expression() decorator hardcodes "add_column" as the
                # operation name, so this branch is not reached. However, we keep it for
                # clarity and future-proofing in case with_column is decorated differently.
                self._update_add_column(
                    source_key_prefix, target_key_prefix, transform_params
                )

    # =========================================================================
    # PARAMETER EXTRACTION HELPER
    # =========================================================================

    @staticmethod
    def _extract_param(
        params: Dict, index: int = 0, param_name: Optional[str] = None
    ) -> Any:
        """Extract parameter from args or kwargs.

        This helper function extracts parameters from the transform_params
        dictionary, checking both positional args and keyword args.

        Args:
            params: Parameter dictionary with 'args' and/or 'kwargs' keys
            index: Index in args list to extract (for positional args)
            param_name: Name in kwargs to extract (for keyword args)

        Returns:
            Extracted parameter value or None if not found

        Examples:
            # Positional argument
            params = {"args": [10], "kwargs": {}}
            value = _extract_param(params, index=0)  # Returns 10

            # Keyword argument
            params = {"args": [], "kwargs": {"limit": 10}}
            value = _extract_param(params, param_name="limit")  # Returns 10

            # Keyword in top-level dict (backward compatibility)
            params = {"limit": 10}
            value = _extract_param(params, param_name="limit")  # Returns 10
        """
        # Try positional args first (by index)
        args = params.get("args", [])
        if args and index < len(args):
            return args[index]

        # Then try keyword args (by name)
        if param_name:
            # First check in kwargs dict
            kwargs = params.get("kwargs", {})
            if param_name in kwargs:
                return kwargs[param_name]

            # Fallback: check top-level params dict (backward compatibility)
            if param_name in params:
                return params[param_name]

        # Not found
        return None

    # =========================================================================
    # CACHE PRESERVATION HELPERS
    # =========================================================================

    def _preserve_cache_entries(
        self, source_prefix: str, target_prefix: str, operations: List[str]
    ) -> None:
        """Preserve cache entries for specified operations.

        This is a generic helper that copies cache entries from source to target.
        It's used by all the specific preservation methods below.

        Args:
            source_prefix: Source cache key prefix (format: "_hash" from make_cache_key)
            target_prefix: Target cache key prefix (format: "_hash" from make_cache_key)
            operations: List of operation names to preserve (e.g., ['count', 'schema'])

        Example:
            # Preserve count and schema
            self._preserve_cache_entries(
                "_abc123",  # source prefix from make_cache_key(plan, "")
                "_def456",  # target prefix from make_cache_key(plan, "")
                ["count", "schema"]
            )
            → Copies "count_abc123" to "count_def456"
            → Copies "schema_abc123" to "schema_def456"

        Note:
            This method checks BOTH _local_cache and _ray_cache to ensure
            all cached objects are preserved, regardless of their size.
        """
        for op in operations:
            # Build source and target cache keys
            # source_prefix is "_hash", so op + source_prefix = "op_hash"
            source_key = f"{op}{source_prefix}"
            target_key = f"{op}{target_prefix}"

            # Check LOCAL cache first
            if source_key in self._local_cache:
                # Copy value from source to target in local cache
                value = self._local_cache[source_key]
                self._local_cache[target_key] = value

            # Also check RAY cache for larger objects
            elif source_key in self._ray_cache:
                # Copy ObjectRef from source to target in Ray cache
                # The ObjectRef itself is small, so this is a cheap operation
                obj_ref = self._ray_cache[source_key]
                self._ray_cache[target_key] = obj_ref

    def _preserve_count(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve count for 1:1 transformations.

        Used for: map, add_column, drop_columns, select_columns, rename_columns

        These transformations produce exactly one output row per input row,
        so the count stays the same.
        """
        self._preserve_cache_entries(source_prefix, target_prefix, ["count"])

    def _preserve_aggregations(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve aggregations for reordering operations.

        Used for: sort, random_shuffle, repartition, randomize_block_order

        These operations just reorder rows without changing any values,
        so ALL aggregations stay valid. This is a HUGE performance win!

        Preserved: count, sum, min, max, mean, std
        """
        self._preserve_cache_entries(
            source_prefix, target_prefix, ["count", "sum", "min", "max", "mean", "std"]
        )

    def _preserve_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve schema and columns for structure-preserving operations.

        Used for: Operations that don't change column structure

        This preserves both schema (column names and types) and the columns
        list (just column names).
        """
        self._preserve_cache_entries(
            source_prefix, target_prefix, ["schema", "columns"]
        )

    def _preserve_size_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve size and block metadata for reordering operations.

        Used for: sort, random_shuffle, repartition

        These operations don't change the total data size or file sources,
        just how the data is organized.

        Preserved: size_bytes, num_blocks, input_files
        """
        self._preserve_cache_entries(
            source_prefix, target_prefix, ["size_bytes", "num_blocks", "input_files"]
        )

    # =========================================================================
    # SMART COUNT COMPUTATION
    # =========================================================================

    def _update_limit_count(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute count and size for limit operation.

        Algorithm:
            new_count = min(original_count, limit_value)
            new_size = (new_count / original_count) * original_size

        Args:
            source_prefix: Source cache key prefix
            target_prefix: Target cache key prefix
            params: Transformation parameters containing limit value

        Example:
            ds = ray.data.range(100)
            ds.count()  # Caches count=100
            limited = ds.limit(10)
            → Smart-computes: count = min(100, 10) = 10
        """
        # Extract limit value from parameters
        limit_value = self._extract_param(params, index=0, param_name="limit")

        # Validate limit value (allow 0 - it's valid and produces count=0)
        if not isinstance(limit_value, int) or limit_value < 0:
            return  # Invalid limit, skip smart update

        # ---------------------------------------------------------------------
        # Smart-compute new count
        # ---------------------------------------------------------------------
        source_count_key = f"count{source_prefix}"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]

            # Validate original count
            if isinstance(original_count, int) and original_count >= 0:
                # new_count = min(original_count, limit_value)
                new_count = min(original_count, limit_value)
                target_count_key = f"count{target_prefix}"
                self._local_cache[target_count_key] = new_count

                # -------------------------------------------------------------
                # Also smart-compute new size_bytes proportionally
                # -------------------------------------------------------------
                source_size_key = f"size_bytes{source_prefix}"
                if source_size_key in self._local_cache and original_count > 0:
                    original_size = self._local_cache[source_size_key]

                    # Validate original size
                    if isinstance(original_size, (int, float)) and original_size > 0:
                        # Estimate new size proportionally
                        # new_size ≈ (new_count / original_count) * original_size
                        new_size = int((new_count / original_count) * original_size)
                        target_size_key = f"size_bytes{target_prefix}"
                        self._local_cache[target_size_key] = new_size

    def _update_sample_count(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-estimate count for random_sample operation.

        Algorithm:
            estimated_count = int(original_count * fraction)

        Note: This is probabilistic - actual count may vary slightly.

        Args:
            source_prefix: Source cache key prefix
            target_prefix: Target cache key prefix
            params: Transformation parameters containing fraction

        Example:
            ds = ray.data.range(1000)
            ds.count()  # Caches count=1000
            sampled = ds.random_sample(0.1)
            → Estimates: count ≈ 1000 * 0.1 = 100
        """
        # Extract fraction from parameters
        fraction = self._extract_param(params, index=0, param_name="fraction")

        # Validate fraction (allow 0.0 - it's valid and produces count=0)
        if not isinstance(fraction, (int, float)) or fraction < 0:
            return  # Invalid fraction, skip smart update

        # ---------------------------------------------------------------------
        # Estimate new count
        # ---------------------------------------------------------------------
        source_count_key = f"count{source_prefix}"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]

            # Validate original count
            if isinstance(original_count, int) and original_count >= 0:
                # Estimate new count (sampling is probabilistic)
                estimated_count = int(original_count * fraction)
                target_count_key = f"count{target_prefix}"
                self._local_cache[target_count_key] = estimated_count

    # =========================================================================
    # SMART COLUMNS COMPUTATION
    # =========================================================================

    def _update_add_column(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute columns list for add_column operation.

        Algorithm:
            new_columns = original_columns + [new_column_name]

        Args:
            source_prefix: Source cache key prefix
            target_prefix: Target cache key prefix
            params: Transformation parameters containing column name

        Example:
            ds = ray.data.from_items([{"a": 1}] * 100)
            ds.columns()  # Returns ["a"]
            added = ds.add_column("b", lambda x: x["a"] * 2)
            → Computes: columns = ["a"] + ["b"] = ["a", "b"]
        """
        # Extract column name from parameters
        col_name = self._extract_param(params, index=0)

        # Validate column name
        if not isinstance(col_name, str):
            return  # Invalid column name, skip smart update

        # ---------------------------------------------------------------------
        # Smart-compute new columns list
        # ---------------------------------------------------------------------
        source_columns_key = f"columns{source_prefix}"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]

            # Validate original columns
            if isinstance(original_columns, list) and col_name not in original_columns:
                # new_columns = original_columns + [col_name]
                new_columns = original_columns + [col_name]
                target_columns_key = f"columns{target_prefix}"
                self._local_cache[target_columns_key] = new_columns

    def _update_drop_columns(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute columns list for drop_columns operation.

        Algorithm:
            new_columns = [col for col in original_columns if col not in dropped]

        Args:
            source_prefix: Source cache key prefix
            target_prefix: Target cache key prefix
            params: Transformation parameters containing columns to drop

        Example:
            ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)
            ds.columns()  # Returns ["a", "b", "c"]
            dropped = ds.drop_columns(["b"])
            → Computes: columns = ["a", "c"] (removed "b")
        """
        # Extract columns to drop from parameters
        cols_to_drop = self._extract_param(params, index=0)

        # Normalize to list
        if isinstance(cols_to_drop, str):
            cols_to_drop = [cols_to_drop]

        # Validate columns to drop
        if not isinstance(cols_to_drop, list):
            return  # Invalid, skip smart update

        # ---------------------------------------------------------------------
        # Smart-compute new columns list
        # ---------------------------------------------------------------------
        source_columns_key = f"columns{source_prefix}"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]

            # Validate original columns
            if isinstance(original_columns, list):
                # new_columns = original_columns - cols_to_drop
                new_columns = [c for c in original_columns if c not in cols_to_drop]

                # Only cache if we have columns remaining
                if new_columns:
                    target_columns_key = f"columns{target_prefix}"
                    self._local_cache[target_columns_key] = new_columns

    def _update_select_columns(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute columns list for select_columns operation.

        Algorithm:
            new_columns = selected_columns (as-is)

        Args:
            source_prefix: Source cache key prefix (unused - we just set columns)
            target_prefix: Target cache key prefix
            params: Transformation parameters containing columns to select

        Example:
            ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)
            selected = ds.select_columns(["a", "c"])
            → Computes: columns = ["a", "c"]
        """
        # Extract columns to select from parameters
        cols_to_select = self._extract_param(params, index=0)

        # Normalize to list
        if isinstance(cols_to_select, str):
            cols_to_select = [cols_to_select]

        # Validate columns to select
        if isinstance(cols_to_select, list):
            # new_columns = cols_to_select (directly)
            target_columns_key = f"columns{target_prefix}"
            self._local_cache[target_columns_key] = cols_to_select
