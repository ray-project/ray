"""
Simple smart cache updates for Ray Data caching.
"""

from collections import OrderedDict
from typing import Dict

from .constants import (
    CACHE_PRESERVATION_RULES,
    TRANSFORMATION_TYPES,
    TransformationType,
)


class SmartCacheUpdater:
    """Handles simple cache updates for transformations."""

    def __init__(self, local_cache: OrderedDict, ray_cache: OrderedDict):
        self._local_cache = local_cache
        self._ray_cache = ray_cache

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_key_prefix: str,
        target_key_prefix: str,
        **transform_params,
    ) -> None:
        """Update cache based on transformation type."""

        transform_type = TRANSFORMATION_TYPES.get(
            operation_name, TransformationType.COMPLEX_DATA_TRANSFORM
        )
        preservation_rules = CACHE_PRESERVATION_RULES.get(transform_type)

        if preservation_rules is None:
            # Unknown transformation type, don't preserve anything (safe default)
            return

        # Apply preservation rules based on transformation type
        if preservation_rules.preserves_count:
            self._preserve_count(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_aggregations:
            self._preserve_aggregations(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_schema or preservation_rules.preserves_columns:
            self._preserve_metadata(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_size_metadata:
            self._preserve_size_metadata(source_key_prefix, target_key_prefix)

        # Handle special computation cases
        if preservation_rules.can_compute_count:
            if operation_name == "limit":
                self._update_limit_count(
                    source_key_prefix, target_key_prefix, transform_params
                )
            elif operation_name == "random_sample":
                self._update_sample_count(
                    source_key_prefix, target_key_prefix, transform_params
                )

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
                self._update_add_column(
                    source_key_prefix, target_key_prefix, transform_params
                )

    def _update_limit_count(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Update count and size for limit operation."""
        # Extract limit value from args
        args = params.get("args", [])
        limit_value = (
            args[0] if args and isinstance(args[0], int) else params.get("limit")
        )
        if not isinstance(limit_value, int) or limit_value <= 0:
            return

        # Update count
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            if isinstance(original_count, int) and original_count >= 0:
                new_count = min(original_count, limit_value)
                target_count_key = f"count_{target_prefix}_"
                self._local_cache[target_count_key] = new_count

                # Also compute new size_bytes if we have the original
                source_size_key = f"size_bytes_{source_prefix}_"
                if source_size_key in self._local_cache and original_count > 0:
                    original_size = self._local_cache[source_size_key]
                    if isinstance(original_size, (int, float)) and original_size > 0:
                        # Estimate new size proportionally
                        new_size = int((new_count / original_count) * original_size)
                        target_size_key = f"size_bytes_{target_prefix}_"
                        self._local_cache[target_size_key] = new_size

    def _preserve_count(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve count for 1:1 transformations."""
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            count = self._local_cache[source_count_key]
            target_count_key = f"count_{target_prefix}_"
            self._local_cache[target_count_key] = count

    def _preserve_aggregations(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve aggregations for reordering operations."""
        preserve_ops = ["count", "sum", "min", "max", "mean", "std"]
        for op in preserve_ops:
            source_key = f"{op}_{source_prefix}_"
            if source_key in self._local_cache:
                value = self._local_cache[source_key]
                target_key = f"{op}_{target_prefix}_"
                self._local_cache[target_key] = value

    def _preserve_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve schema and columns for structure-preserving operations."""
        metadata_ops = ["schema", "columns"]
        for op in metadata_ops:
            source_key = f"{op}_{source_prefix}_"
            if source_key in self._local_cache:
                value = self._local_cache[source_key]
                target_key = f"{op}_{target_prefix}_"
                self._local_cache[target_key] = value

    def _preserve_size_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve size and block metadata for reordering operations."""
        size_ops = ["size_bytes", "num_blocks", "input_files"]
        for op in size_ops:
            source_key = f"{op}_{source_prefix}_"
            if source_key in self._local_cache:
                value = self._local_cache[source_key]
                target_key = f"{op}_{target_prefix}_"
                self._local_cache[target_key] = value

    def _update_sample_count(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Update count for random_sample operation."""
        # Extract fraction from args (random_sample typically takes a fraction)
        args = params.get("args", [])
        fraction = (
            args[0]
            if args and isinstance(args[0], (int, float))
            else params.get("fraction")
        )
        if not isinstance(fraction, (int, float)) or fraction <= 0:
            return

        # Update count
        source_count_key = f"count_{source_prefix}_"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            if isinstance(original_count, int) and original_count >= 0:
                # Estimate new count (sampling is probabilistic)
                estimated_count = int(original_count * fraction)
                target_count_key = f"count_{target_prefix}_"
                self._local_cache[target_count_key] = estimated_count

    def _update_add_column(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart update for add_column: compute new columns list."""
        # Get column name from params
        col_name = params.get("args", [None])[0] if params.get("args") else None
        if not isinstance(col_name, str):
            return

        # Update columns list
        source_columns_key = f"columns_{source_prefix}_"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]
            if isinstance(original_columns, list) and col_name not in original_columns:
                new_columns = original_columns + [col_name]
                target_columns_key = f"columns_{target_prefix}_"
                self._local_cache[target_columns_key] = new_columns

    def _update_drop_columns(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart update for drop_columns: compute new columns list."""
        # Get columns to drop from params
        cols_to_drop = params.get("args", [None])[0] if params.get("args") else None
        if isinstance(cols_to_drop, str):
            cols_to_drop = [cols_to_drop]
        if not isinstance(cols_to_drop, list):
            return

        # Update columns list
        source_columns_key = f"columns_{source_prefix}_"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]
            if isinstance(original_columns, list):
                new_columns = [c for c in original_columns if c not in cols_to_drop]
                if new_columns:  # Don't cache if all columns dropped
                    target_columns_key = f"columns_{target_prefix}_"
                    self._local_cache[target_columns_key] = new_columns

    def _update_select_columns(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart update for select_columns: set new columns list."""
        # Get columns to select from params
        cols_to_select = params.get("args", [None])[0] if params.get("args") else None
        if isinstance(cols_to_select, str):
            cols_to_select = [cols_to_select]
        if isinstance(cols_to_select, list):
            target_columns_key = f"columns_{target_prefix}_"
            self._local_cache[target_columns_key] = cols_to_select
