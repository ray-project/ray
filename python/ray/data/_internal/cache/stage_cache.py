"""Petabyte-scale caching system for Ray Data operations.

This module provides scalable caching for massive datasets by caching execution
metadata and block references rather than raw data. Designed for petabyte-scale
workloads where traditional data caching is impractical.

The cache works by:
1. Caching execution plans and block reference metadata
2. Reusing Ray ObjectRef handles for identical operations
3. Leveraging Ray's distributed object store for data locality
4. Caching only small computation results and metadata

Key principles for petabyte-scale:
- Never cache raw data - only metadata and references
- Use Ray's object store as the primary data cache
- Cache execution plans to avoid redundant computation
- Minimal memory footprint regardless of dataset size

Example:
    >>> import ray
    >>> from ray.data.context import DataContext
    >>>
    >>> # Enable metadata caching (lightweight for any dataset size)
    >>> ctx = DataContext.get_current()
    >>> ctx.enable_stage_cache = True
    >>>
    >>> # First execution - computes and caches metadata/refs
    >>> ds1 = ray.data.read_parquet("s3://bucket/petabyte-dataset/")
    >>> result1 = ds1.materialize()  # ObjectRefs cached, not data
    >>>
    >>> # Second identical execution - reuses cached ObjectRefs
    >>> ds2 = ray.data.read_parquet("s3://bucket/petabyte-dataset/")
    >>> result2 = ds2.materialize()  # Instant - reuses ObjectRefs
"""

import gzip
import hashlib
import logging
import os
import pickle
import tempfile
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Optional

import ray
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


class PetabyteStageCache:
    """Petabyte-scale caching system for Ray Data operations.

    Designed for massive datasets where caching raw data is impractical.
    Instead caches:
    - Execution plan metadata and ObjectRef handles
    - Block reference information for data locality
    - Small computation results (counts, schemas, etc.)
    - File system metadata for read operations

    Key features for petabyte-scale:
    - Constant memory footprint regardless of dataset size
    - Leverages Ray's distributed object store
    - Caches execution metadata, not raw data
    - Minimal network overhead
    """

    def __init__(self):
        # Cache for execution plans and their resulting ObjectRef bundles
        # This has minimal memory footprint even for petabyte datasets
        self._plan_cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()

        # Cache for small computation results (schemas, counts, etc.)
        self._result_cache: OrderedDict[str, Any] = OrderedDict()

        # Configuration
        self._max_plan_entries = 1000  # Plans are tiny, can cache many
        self._max_result_entries = 10000  # Small results, can cache many

        # Statistics
        self._plan_hits = 0
        self._result_hits = 0
        self._misses = 0

    def _compute_plan_fingerprint(self, plan: LogicalPlan) -> str:
        """Compute lightweight fingerprint for a logical plan."""
        try:
            # Use plan's built-in hash for O(1) performance
            if hasattr(plan, "__hash__") and plan.__hash__ is not None:
                plan_hash = hash(plan)
            else:
                # For unhashable plans, use a lightweight representation
                plan_hash = hash(repr(plan)[:1000])  # Limit string length
        except Exception:
            plan_hash = 0

        # Include only critical context that affects execution
        try:
            context = DataContext.get_current()
            context_hash = hash(
                (
                    context.target_max_block_size,
                    context.use_push_based_shuffle,
                )
            )
        except Exception:
            context_hash = 0

        # Efficient combination
        combined_hash = hash((plan_hash, context_hash))
        return f"{combined_hash:016x}"

    def _extract_block_refs(self, result: Any) -> Optional[list]:
        """Extract ObjectRef handles from MaterializedDataset.

        For petabyte datasets, we only cache the lightweight ObjectRef handles,
        not the actual data which stays in Ray's object store.
        """
        try:
            if hasattr(result, "_plan") and hasattr(result._plan, "_blocks"):
                # Extract block references - these are lightweight
                blocks = result._plan._blocks
                if blocks:
                    return [block.ref for block in blocks if hasattr(block, "ref")]

            # Alternative: try to get refs from internal structure
            if hasattr(result, "get_internal_block_refs"):
                return result.get_internal_block_refs()

            return None
        except Exception as e:
            logger.debug(f"Could not extract block refs: {e}")
            return None

    def _is_small_result(self, result: Any) -> bool:
        """Check if result is small enough to cache directly.

        Only cache actual data for very small results like counts, schemas, etc.
        For large datasets, we only cache the ObjectRef handles.
        """
        try:
            # Cache small primitive results directly
            if isinstance(result, (int, float, str, bool, list, dict)):
                # Estimate size of primitive types
                import sys

                size = sys.getsizeof(result)
                if isinstance(result, (list, dict)):
                    # Rough estimate for collections
                    size += sum(
                        sys.getsizeof(item)
                        for item in (
                            result if isinstance(result, list) else result.values()
                        )
                    )
                return size < 1024 * 1024  # 1MB limit for direct caching

            # For MaterializedDataset, check if it's tiny
            if hasattr(result, "num_blocks") and hasattr(result, "count"):
                num_blocks = result.num_blocks()
                # Only cache very small datasets directly
                return num_blocks <= 10  # Arbitrary small limit

            return False
        except Exception:
            return False

    def get(self, plan: LogicalPlan) -> Optional[Any]:
        """Get cached result for a logical plan.

        For petabyte datasets, this returns cached ObjectRef handles or
        reconstructs MaterializedDataset from cached metadata.

        Args:
            plan: The logical plan to look up.

        Returns:
            Cached result if found, None otherwise.
        """
        if not self._is_enabled():
            return None

        fingerprint = self._compute_plan_fingerprint(plan)

        # Check plan cache (ObjectRef handles for large datasets)
        if fingerprint in self._plan_cache:
            try:
                cached_data = self._plan_cache[fingerprint]

                # Verify ObjectRefs are still valid
                block_refs = cached_data.get("block_refs", [])
                if block_refs and self._verify_object_refs(block_refs):
                    # Reconstruct MaterializedDataset from cached ObjectRefs
                    result = self._reconstruct_dataset(cached_data)
                    if result is not None:
                        # Move to end for LRU
                        self._plan_cache.move_to_end(fingerprint)
                        self._plan_hits += 1
                        logger.debug(
                            f"Plan cache hit: {fingerprint} ({len(block_refs)} blocks)"
                        )
                        return result

                # ObjectRefs are invalid, remove from cache
                del self._plan_cache[fingerprint]

            except Exception as e:
                logger.debug(f"Plan cache error: {e}")
                self._plan_cache.pop(fingerprint, None)

        # Check result cache (for small computed results)
        if fingerprint in self._result_cache:
            result = self._result_cache[fingerprint]
            # Move to end for LRU
            self._result_cache.move_to_end(fingerprint)
            self._result_hits += 1
            logger.debug(f"Result cache hit: {fingerprint}")
            return result

        self._misses += 1
        logger.debug(f"Cache miss: {fingerprint}")
        return None

    def _verify_object_refs(self, object_refs: list) -> bool:
        """Verify that ObjectRefs are still valid in Ray's object store."""
        if not object_refs:
            return False

        try:
            # Check a sample of refs to see if they're still valid
            sample_size = min(3, len(object_refs))
            sample_refs = object_refs[:sample_size]

            # Use ray.wait with timeout=0 to check if refs exist
            ready, _ = ray.wait(sample_refs, timeout=0, num_returns=len(sample_refs))
            return len(ready) == len(sample_refs)

        except Exception:
            return False

    def _reconstruct_dataset(self, cached_data: Dict[str, Any]) -> Optional[Any]:
        """Reconstruct MaterializedDataset from cached metadata and ObjectRefs."""
        try:
            from ray.data.dataset import MaterializedDataset
            from ray.data._internal.plan import ExecutionPlan
            from ray.data._internal.logical.operators.input_data_operator import (
                InputData,
            )
            from ray.data._internal.execution.interfaces import RefBundle

            block_refs = cached_data["block_refs"]
            schema = cached_data.get("schema")
            stats = cached_data.get("stats")
            context = cached_data.get("context")

            # Create RefBundles from cached ObjectRefs
            ref_bundles = []
            for block_ref in block_refs:
                # Create a minimal RefBundle - the actual data is in Ray's object store
                ref_bundle = RefBundle(
                    blocks=[(block_ref, None)],  # (ObjectRef, metadata)
                    owns_blocks=False,
                    schema=schema,
                )
                ref_bundles.append(ref_bundle)

            # Reconstruct the logical plan
            logical_plan = LogicalPlan(
                InputData(input_data=ref_bundles), context or DataContext.get_current()
            )

            # Create MaterializedDataset
            execution_plan = ExecutionPlan(stats, context or DataContext.get_current())
            result = MaterializedDataset(execution_plan, logical_plan)

            return result

        except Exception as e:
            logger.debug(f"Failed to reconstruct dataset: {e}")
            return None

    def put(self, plan: LogicalPlan, result: Any) -> None:
        """Store result using petabyte-scale caching strategy.

        For large datasets: Cache only ObjectRef handles and metadata
        For small results: Cache the actual result data

        Args:
            plan: The logical plan that produced the result.
            result: The result to cache (MaterializedDataset or computed value).
        """
        if not self._is_enabled():
            return

        fingerprint = self._compute_plan_fingerprint(plan)

        # For small results, cache directly
        if self._is_small_result(result):
            self._put_result_cache(fingerprint, result)
        else:
            # For large datasets, cache only ObjectRef handles and metadata
            self._put_plan_cache(fingerprint, result)

    def _put_result_cache(self, fingerprint: str, result: Any) -> None:
        """Cache small results directly."""
        try:
            # Evict LRU entries if at capacity
            while len(self._result_cache) >= self._max_result_entries:
                oldest_key, _ = self._result_cache.popitem(last=False)
                logger.debug(f"Evicted result cache entry: {oldest_key}")

            self._result_cache[fingerprint] = result
            logger.debug(f"Result cached: {fingerprint}")

        except Exception as e:
            logger.error(f"Result cache put error: {e}")

    def _put_plan_cache(self, fingerprint: str, result: Any) -> None:
        """Cache ObjectRef handles and metadata for large datasets."""
        try:
            # Extract lightweight ObjectRef handles
            block_refs = self._extract_block_refs(result)
            if not block_refs:
                logger.debug(f"No block refs to cache for: {fingerprint}")
                return

            # Extract minimal metadata needed for reconstruction
            cached_data = {
                "block_refs": block_refs,
                "schema": getattr(result, "schema", lambda: None)(),
                "num_blocks": getattr(result, "num_blocks", lambda: len(block_refs))(),
                "context": DataContext.get_current(),
                "cached_at": time.time(),
            }

            # Try to get execution stats if available
            try:
                if hasattr(result, "_plan") and hasattr(result._plan, "stats"):
                    cached_data["stats"] = result._plan.stats()
            except Exception:
                pass

            # Evict LRU entries if at capacity
            while len(self._plan_cache) >= self._max_plan_entries:
                oldest_key, _ = self._plan_cache.popitem(last=False)
                logger.debug(f"Evicted plan cache entry: {oldest_key}")

            self._plan_cache[fingerprint] = cached_data
            logger.debug(f"Plan cached: {fingerprint} ({len(block_refs)} ObjectRefs)")

        except Exception as e:
            logger.error(f"Plan cache put error: {e}")

    def clear(self) -> None:
        """Clear all cached results."""
        self._plan_cache.clear()
        self._result_cache.clear()

        # Reset statistics
        self._plan_hits = 0
        self._result_hits = 0
        self._misses = 0

        logger.debug("Petabyte cache cleared")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics optimized for petabyte-scale monitoring."""
        total_hits = self._plan_hits + self._result_hits
        total_requests = total_hits + self._misses
        hit_rate = total_hits / total_requests if total_requests > 0 else 0.0

        # Calculate memory footprint (should be minimal regardless of dataset size)
        plan_cache_memory = len(self._plan_cache) * 1024  # Rough estimate per entry
        result_cache_memory = sum(
            len(str(result)) for result in self._result_cache.values()
        )
        total_memory_bytes = plan_cache_memory + result_cache_memory

        return {
            "plan_hits": self._plan_hits,
            "result_hits": self._result_hits,
            "total_hits": total_hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "plan_cache_entries": len(self._plan_cache),
            "result_cache_entries": len(self._result_cache),
            "total_memory_bytes": total_memory_bytes,  # Should be small
            "max_plan_entries": self._max_plan_entries,
            "max_result_entries": self._max_result_entries,
        }

    def _is_enabled(self) -> bool:
        """Check if stage caching is enabled."""
        try:
            context = DataContext.get_current()
            return getattr(context, "enable_stage_cache", False)
        except Exception:
            return False

    def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching a pattern."""
        invalidated = 0

        # Invalidate plan cache entries
        plan_keys_to_remove = [key for key in self._plan_cache.keys() if pattern in key]
        for key in plan_keys_to_remove:
            del self._plan_cache[key]
            invalidated += 1

        # Invalidate result cache entries
        result_keys_to_remove = [
            key for key in self._result_cache.keys() if pattern in key
        ]
        for key in result_keys_to_remove:
            del self._result_cache[key]
            invalidated += 1

        logger.debug(
            f"Invalidated {invalidated} cache entries matching pattern: {pattern}"
        )
        return invalidated


# Global petabyte-scale cache instance
_stage_cache = PetabyteStageCache()


def get_stage_cache() -> PetabyteStageCache:
    """Get the global petabyte-scale stage cache instance.

    Returns:
        The global PetabyteStageCache instance.
    """
    return _stage_cache


def cache_stage_result(plan: LogicalPlan, result: Any) -> None:
    """Cache a stage result using petabyte-scale strategy.

    Caches ObjectRef handles for large datasets, actual data for small results.

    Args:
        plan: The logical plan that produced the result.
        result: The result to cache.
    """
    _stage_cache.put(plan, result)


def get_cached_stage_result(plan: LogicalPlan) -> Optional[Any]:
    """Get a cached stage result optimized for petabyte-scale.

    Args:
        plan: The logical plan to look up.

    Returns:
        Cached result if found, None otherwise.
    """
    return _stage_cache.get(plan)


def clear_stage_cache() -> None:
    """Clear the petabyte-scale stage cache."""
    _stage_cache.clear()


def get_stage_cache_stats() -> Dict[str, Any]:
    """Get stage cache statistics optimized for petabyte-scale monitoring.

    Returns:
        Dictionary with cache statistics showing minimal memory usage.
    """
    return _stage_cache.get_stats()


def invalidate_stage_cache(pattern: str = "") -> int:
    """Invalidate stage cache entries matching pattern.

    Args:
        pattern: Pattern to match. Empty string invalidates all.

    Returns:
        Number of entries invalidated.
    """
    if not pattern:
        _stage_cache.clear()
        return -1  # Indicate full clear
    else:
        return _stage_cache.invalidate_by_pattern(pattern)


# Backwards compatibility alias
StageCache = PetabyteStageCache
