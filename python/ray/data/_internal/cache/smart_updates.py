"""Smart cache updates for Ray Data transformations.

Preserves cached values that remain valid and computes new values from existing
ones instead of invalidating everything.
"""

import sys
from collections import OrderedDict
from typing import Any, Dict, List, Optional

try:
    import ray
except ImportError:
    ray = None

from ray.data._internal.cache.constants import (
    CACHE_PRESERVATION_RULES,
    TRANSFORMATION_TYPES,
    TransformationType,
)

# Operation name constants
OP_LIMIT = "limit"
OP_COUNT = "count"
OP_SCHEMA = "schema"
OP_COLUMNS = "columns"
OP_SIZE_BYTES = "size_bytes"
OP_NUM_BLOCKS = "num_blocks"
OP_INPUT_FILES = "input_files"


class SmartCacheUpdater:
    """Handles smart cache updates for Dataset transformations."""

    def __init__(
        self,
        local_cache: OrderedDict[str, Any],
        ray_cache: OrderedDict[str, Any],
    ):
        """Initialize the smart cache updater.

        Args:
            local_cache: Local in-memory cache for small objects.
            ray_cache: Ray object store cache for medium objects.
        """
        self._local_cache = local_cache
        self._ray_cache = ray_cache

    def invalidate_for_transform(
        self,
        operation_name: str,
        source_key_prefix: str,
        target_key_prefix: str,
        **transform_params,
    ) -> None:
        """Update cache based on transformation type.

        Preserves cached values that remain valid after transformation and
        computes new values from existing ones (e.g., count after limit).

        Args:
            operation_name: Name of the transformation operation.
            source_key_prefix: Cache key prefix for source dataset.
            target_key_prefix: Cache key prefix for target dataset.
            **transform_params: Transformation parameters.

        Raises:
            ValueError: If operation_name or prefixes are invalid.
        """
        # Validate inputs
        if not operation_name or not isinstance(operation_name, str):
            return
        if not source_key_prefix or not isinstance(source_key_prefix, str):
            return
        if not target_key_prefix or not isinstance(target_key_prefix, str):
            return

        transform_type = TRANSFORMATION_TYPES.get(
            operation_name, TransformationType.COMPLEX_DATA_TRANSFORM
        )

        preservation_rules = CACHE_PRESERVATION_RULES.get(transform_type)

        if preservation_rules is None:
            return

        if preservation_rules.preserves_count:
            self._preserve_count(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_schema or preservation_rules.preserves_columns:
            self._preserve_metadata(source_key_prefix, target_key_prefix)

        if preservation_rules.preserves_size_metadata:
            self._preserve_size_metadata(source_key_prefix, target_key_prefix)

        if preservation_rules.can_compute_count:
            if operation_name == OP_LIMIT:
                self._update_limit_count(
                    source_key_prefix, target_key_prefix, transform_params
                )

    @staticmethod
    def _extract_param(
        params: Dict[str, Any], index: int = 0, param_name: Optional[str] = None
    ) -> Optional[Any]:
        """Extract parameter from args or kwargs.

        Args:
            params: Dictionary containing 'args' and 'kwargs' keys, or direct params.
            index: Positional index to extract from args.
            param_name: Name of keyword parameter to extract.

        Returns:
            Extracted parameter value, or None if not found.
        """
        if not params:
            return None

        args = params.get("args", [])
        if args and isinstance(args, list) and index < len(args):
            return args[index]

        if param_name:
            kwargs = params.get("kwargs", {})
            if isinstance(kwargs, dict) and param_name in kwargs:
                return kwargs[param_name]

            if isinstance(params, dict) and param_name in params:
                return params[param_name]

        return None

    def _preserve_cache_entries(
        self, source_prefix: str, target_prefix: str, operations: List[str]
    ) -> None:
        """Preserve cache entries for specified operations.

        Copies entries from source to target cache keys and updates LRU order.
        If a key exists in both caches, local cache takes precedence.

        Args:
            source_prefix: Cache key prefix for source dataset.
            target_prefix: Cache key prefix for target dataset.
            operations: List of operation names to preserve (e.g., ["count", "schema"]).
        """
        if not operations:
            return

        for op in operations:
            source_key = f"{op}{source_prefix}"
            target_key = f"{op}{target_prefix}"

            # Check local cache first (takes precedence).
            # Local cache is faster and preferred for small objects.
            if source_key in self._local_cache:
                value = self._local_cache[source_key]
                # Remove target from ray cache if it exists there to maintain
                # consistency.
                if target_key in self._ray_cache:
                    self._ray_cache.pop(target_key, None)
                # Copy entry to target key and update LRU order.
                # move_to_end() maintains Least Recently Used eviction order.
                self._local_cache[target_key] = value
                self._local_cache.move_to_end(target_key)
                # Move source to end as well (recently accessed for LRU).
                self._local_cache.move_to_end(source_key)
            elif source_key in self._ray_cache:
                # Object reference stored in Ray object store cache.
                obj_ref = self._ray_cache[source_key]
                # Remove target from local cache if it exists there to maintain
                # consistency.
                if target_key in self._local_cache:
                    self._local_cache.pop(target_key, None)
                # Copy object reference to target key and update LRU order.
                # move_to_end() maintains Least Recently Used eviction order.
                self._ray_cache[target_key] = obj_ref
                self._ray_cache.move_to_end(target_key)
                # Move source to end as well (recently accessed for LRU).
                self._ray_cache.move_to_end(source_key)

    def _preserve_count(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve count for 1:1 transformations.

        Args:
            source_prefix: Cache key prefix for source dataset.
            target_prefix: Cache key prefix for target dataset.
        """
        self._preserve_cache_entries(source_prefix, target_prefix, [OP_COUNT])

    def _preserve_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve schema and columns for structure-preserving operations.

        Args:
            source_prefix: Cache key prefix for source dataset.
            target_prefix: Cache key prefix for target dataset.
        """
        self._preserve_cache_entries(
            source_prefix, target_prefix, [OP_SCHEMA, OP_COLUMNS]
        )

    def _preserve_size_metadata(self, source_prefix: str, target_prefix: str) -> None:
        """Preserve size and block metadata for reordering operations.

        Args:
            source_prefix: Cache key prefix for source dataset.
            target_prefix: Cache key prefix for target dataset.
        """
        self._preserve_cache_entries(
            source_prefix,
            target_prefix,
            [OP_SIZE_BYTES, OP_NUM_BLOCKS, OP_INPUT_FILES],
        )

    def _update_limit_count(
        self, source_prefix: str, target_prefix: str, params: Dict[str, Any]
    ) -> None:
        """Smart-compute count for limit operation.

        If the source count is cached, compute the new count as
        min(original_count, limit_value) without re-executing the operation.

        Args:
            source_prefix: Cache key prefix for source dataset.
            target_prefix: Cache key prefix for target dataset.
            params: Transformation parameters containing limit value.
        """
        limit_value = self._extract_param(params, index=0, param_name="limit")

        # Validate limit_value
        if limit_value is None:
            return
        if not isinstance(limit_value, int):
            # Handle numpy types (e.g., numpy.int64) and other numeric types.
            # NumPy types need explicit conversion for type checking.
            try:
                limit_value = int(limit_value)
            except (ValueError, TypeError):
                return
        # Bounds check: limit must be non-negative and within system limits.
        # sys.maxsize is the maximum value for int on the platform.
        if limit_value < 0 or limit_value > sys.maxsize:
            return

        source_count_key = f"{OP_COUNT}{source_prefix}"
        target_count_key = f"{OP_COUNT}{target_prefix}"
        original_count = None

        # Check local cache first
        if source_count_key in self._local_cache:
            original_count = self._local_cache[source_count_key]
            # Move source to end for LRU
            self._local_cache.move_to_end(source_count_key)
        # Check Ray cache if not found in local cache
        elif source_count_key in self._ray_cache:
            if ray is None:
                return
            try:
                object_ref = self._ray_cache[source_count_key]
                original_count = ray.get(object_ref)
                # Move source to end for LRU
                self._ray_cache.move_to_end(source_count_key)
            except Exception as e:
                # Handle specific Ray exceptions if available.
                # Ray ObjectLostError occurs when an object is evicted from object
                # store.
                if ray is not None and hasattr(ray, "exceptions"):
                    if isinstance(e, ray.exceptions.ObjectLostError):
                        # Object was lost, remove from cache
                        self._ray_cache.pop(source_count_key, None)
                        return
                # Other failures (network, etc.) - cannot compute smart count
                return

        # Compute and store new count if we have the original count
        if original_count is not None:
            # Handle numpy types (e.g., numpy.int64) and other numeric types.
            # NumPy types need explicit conversion for type checking.
            try:
                if not isinstance(original_count, int):
                    original_count = int(original_count)
            except (ValueError, TypeError):
                return

            if isinstance(original_count, int) and original_count >= 0:
                # Compute new count: limit operation caps the count at limit_value.
                new_count = min(original_count, limit_value)
                # Remove target from ray cache if it exists there (since we're
                # storing locally). Local cache takes precedence for consistency.
                if target_count_key in self._ray_cache:
                    self._ray_cache.pop(target_count_key, None)
                # Store in local cache (counts are small integers < 50KB threshold)
                # and move to end for LRU eviction order.
                self._local_cache[target_count_key] = new_count
                self._local_cache.move_to_end(target_count_key)
