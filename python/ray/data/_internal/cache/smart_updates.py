"""Smart cache updates for Ray Data transformations.

Preserves cached values that remain valid and computes new values from existing
ones instead of invalidating everything.
"""

from collections import OrderedDict
from typing import Any, Dict, List, Optional

from .constants import (
    CACHE_PRESERVATION_RULES,
    TRANSFORMATION_TYPES,
    TransformationType,
)


class SmartCacheUpdater:
    """Handles smart cache updates for Dataset transformations."""

    def __init__(self, local_cache: OrderedDict, ray_cache: OrderedDict):
        """Initialize the smart cache updater."""
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
            return

        if preservation_rules.preserves_count:
            self._preserve_count(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_aggregations:
            self._preserve_aggregations(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_schema or preservation_rules.preserves_columns:
            self._preserve_metadata(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_size_metadata:
            self._preserve_size_metadata(source_key_prefix, target_key_prefix)

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

    @staticmethod
    def _extract_param(
        params: Dict, index: int = 0, param_name: Optional[str] = None
    ) -> Any:
        """Extract parameter from args or kwargs."""
        args = params.get("args", [])
        if args and index < len(args):
            return args[index]

        if param_name:
            kwargs = params.get("kwargs", {})
            if param_name in kwargs:
                return kwargs[param_name]

            if param_name in params:
                return params[param_name]

        return None

    def _preserve_cache_entries(
        self, source_prefix: str, target_prefix: str, operations: List[str]
    ) -> None:
        """Preserve cache entries for specified operations."""
        for op in operations:
            source_key = f"{op}{source_prefix}"
            target_key = f"{op}{target_prefix}"

            if source_key in self._local_cache:
                value = self._local_cache[source_key]
                self._local_cache[target_key] = value
            elif source_key in self._ray_cache:
                obj_ref = self._ray_cache[source_key]
                self._ray_cache[target_key] = obj_ref

    def _preserve_count(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve count for 1:1 transformations."""
        self._preserve_cache_entries(source_prefix, target_prefix, ["count"])

    def _preserve_aggregations(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve aggregations for reordering operations."""
        self._preserve_cache_entries(
            source_prefix, target_prefix, ["count", "sum", "min", "max", "mean", "std"]
        )

    def _preserve_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve schema and columns for structure-preserving operations."""
        self._preserve_cache_entries(
            source_prefix, target_prefix, ["schema", "columns"]
        )

    def _preserve_size_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve size and block metadata for reordering operations."""
        self._preserve_cache_entries(
            source_prefix, target_prefix, ["size_bytes", "num_blocks", "input_files"]
        )

    def _update_limit_count(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute count and size for limit operation."""
        limit_value = self._extract_param(params, index=0, param_name="limit")

        if not isinstance(limit_value, int) or limit_value < 0:
            return

        source_count_key = f"count{source_prefix}"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]

            if isinstance(original_count, int) and original_count >= 0:
                new_count = min(original_count, limit_value)
                target_count_key = f"count{target_prefix}"
                self._local_cache[target_count_key] = new_count

                source_size_key = f"size_bytes{source_prefix}"
                if source_size_key in self._local_cache and original_count > 0:
                    original_size = self._local_cache[source_size_key]

                    if isinstance(original_size, (int, float)) and original_size > 0:
                        new_size = int((new_count / original_count) * original_size)
                        target_size_key = f"size_bytes{target_prefix}"
                        self._local_cache[target_size_key] = new_size

    def _update_sample_count(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-estimate count for random_sample operation."""
        fraction = self._extract_param(params, index=0, param_name="fraction")

        if not isinstance(fraction, (int, float)) or fraction < 0:
            return

        source_count_key = f"count{source_prefix}"
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]

            if isinstance(original_count, int) and original_count >= 0:
                estimated_count = int(original_count * fraction)
                target_count_key = f"count{target_prefix}"
                self._local_cache[target_count_key] = estimated_count

    def _update_add_column(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute columns list for add_column operation."""
        col_name = self._extract_param(params, index=0)

        if not isinstance(col_name, str):
            return

        source_columns_key = f"columns{source_prefix}"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]

            if isinstance(original_columns, list) and col_name not in original_columns:
                new_columns = original_columns + [col_name]
                target_columns_key = f"columns{target_prefix}"
                self._local_cache[target_columns_key] = new_columns

    def _update_drop_columns(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute columns list for drop_columns operation."""
        cols_to_drop = self._extract_param(params, index=0)

        if isinstance(cols_to_drop, str):
            cols_to_drop = [cols_to_drop]

        if not isinstance(cols_to_drop, list):
            return

        source_columns_key = f"columns{source_prefix}"
        if source_columns_key in self._local_cache:
            original_columns = self._local_cache[source_columns_key]

            if isinstance(original_columns, list):
                new_columns = [c for c in original_columns if c not in cols_to_drop]

                if new_columns:
                    target_columns_key = f"columns{target_prefix}"
                    self._local_cache[target_columns_key] = new_columns

    def _update_select_columns(
        self, source_prefix: str, target_prefix: str, params: Dict
    ) -> None:
        """Smart-compute columns list for select_columns operation."""
        cols_to_select = self._extract_param(params, index=0)

        if isinstance(cols_to_select, str):
            cols_to_select = [cols_to_select]

        if isinstance(cols_to_select, list):
            target_columns_key = f"columns{target_prefix}"
            self._local_cache[target_columns_key] = cols_to_select
