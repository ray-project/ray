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
        """Smart-compute count for limit operation."""
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
