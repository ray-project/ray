"""
Type-safe constants and enums for Ray Data caching.

This module defines the core configuration and rules for the caching system:
- Cache size thresholds (when to use local vs Ray cache)
- Cacheable operations (what can be cached)
- Transformation types (how operations affect cache)
- Preservation rules (what cache values can be reused after transformations)

The hierarchical transformation type system enables intelligent cache updates:
- TIER 1: Best caching (map, add_column) - preserves count, schema, columns
- TIER 2: Great caching (sort, shuffle) - preserves EVERYTHING including aggregations!
- TIER 3: Good caching (limit, filter) - preserves schema, smart-computes count
- TIER 4+: Limited/no caching (map_batches, union) - invalidates everything
"""

from dataclasses import dataclass
from enum import Enum

# =============================================================================
# CACHE SIZE THRESHOLDS
# =============================================================================

# Cache placement thresholds (in bytes)
# These determine where objects are cached based on size
LOCAL_CACHE_THRESHOLD_BYTES = 50 * 1024  # 50KB - Small objects cached in local memory
RAY_CACHE_THRESHOLD_BYTES = (
    10 * 1024 * 1024
)  # 10MB - Medium objects in Ray object store

# Ray object size estimates (in bytes)
# Used to estimate sizes of Ray-specific objects
RAY_OBJECTREF_SIZE_BYTES = 64  # Size of an ObjectRef pointer (not the data!)
RAY_BLOCK_OVERHEAD_BYTES = 64  # Overhead for Block ObjectRef + metadata
RAY_DATASET_METADATA_SIZE_BYTES = 1024  # MaterializedDataset metadata overhead

# Cache size limits
# These prevent the cache from growing unbounded
MAX_CACHE_ENTRIES = 1000  # Maximum number of entries per cache (local or Ray)
DEFAULT_MAX_CACHE_SIZE_BYTES = 1024 * 1024 * 1024  # 1GB default total cache size


# =============================================================================
# CACHEABLE OPERATIONS
# =============================================================================


class CacheableOperation(Enum):
    """Operations that can be cached.

    These are the Dataset methods whose results are expensive to compute
    and benefit from caching. They're organized by category for clarity.
    """

    # Metadata operations (fast to cache, frequently reused)
    COUNT = "count"  # ds.count() → int
    SCHEMA = "schema"  # ds.schema() → Schema object
    COLUMNS = "columns"  # ds.columns() → List[str]
    SIZE_BYTES = "size_bytes"  # ds.size_bytes() → int
    INPUT_FILES = "input_files"  # ds.input_files() → List[str]
    NUM_BLOCKS = "num_blocks"  # ds.num_blocks() → int

    # Data operations (return actual data, more expensive)
    TAKE = "take"  # ds.take(n) → List[Dict]
    TAKE_BATCH = "take_batch"  # ds.take_batch(n) → DataBatch
    TAKE_ALL = "take_all"  # ds.take_all() → List[Dict]
    UNIQUE = "unique"  # ds.unique(column) → List[Any]

    # Aggregation operations (require full dataset scan)
    SUM = "sum"  # ds.sum(column) → numeric
    MIN = "min"  # ds.min(column) → numeric
    MAX = "max"  # ds.max(column) → numeric
    MEAN = "mean"  # ds.mean(column) → float
    STD = "std"  # ds.std(column) → float
    AGGREGATE = "aggregate"  # ds.aggregate(fn) → Any

    # Execution operations (very expensive, materialized data)
    MATERIALIZE = "materialize"  # ds.materialize() → MaterializedDataset
    STATS = "stats"  # ds.stats() → DatasetStats


# =============================================================================
# TRANSFORMATION TYPES
# =============================================================================


class TransformationType(Enum):
    """Hierarchical transformation types for intelligent cache invalidation.

    This is the heart of the smart caching system. Each transformation type
    defines what cache entries can be preserved, enabling major performance wins.

    The hierarchy from best to worst caching:
    """

    # TIER 1: Best caching - preserve almost everything
    # Examples: map, add_column, drop_columns, select_columns
    # These transformations don't change the number of rows or break relationships
    STRUCTURE_AND_COUNT_PRESERVING = "structure_and_count_preserving"

    # TIER 2: Great caching - preserve aggregations and metadata
    # Examples: sort, random_shuffle, repartition
    # These just reorder data, so all values stay the same (HUGE performance win!)
    REORDERING_ONLY = "reordering_only"

    # TIER 3: Good caching - preserve structure metadata
    # Examples: limit, filter, random_sample
    # These change the count but not the schema, and we can smart-compute new count
    STRUCTURE_PRESERVING_COUNT_CHANGING = "structure_preserving_count_changing"

    # TIER 4: Limited caching - complex data changes
    # Examples: map_batches, flat_map
    # These can change anything, so we must invalidate all cache
    COMPLEX_DATA_TRANSFORM = "complex_data_transform"

    # TIER 5: No caching - multi-dataset operations
    # Examples: union, join, zip
    # These combine datasets, making cache preservation impossible
    MULTI_DATASET_OPERATION = "multi_dataset_operation"

    # TIER 6: Special handling - splitting operations
    # Examples: split, train_test_split, streaming_split
    # These create multiple datasets, requiring special cache handling
    DATASET_SPLITTING = "dataset_splitting"


# =============================================================================
# CACHE PRESERVATION RULES
# =============================================================================


@dataclass(frozen=True)
class CachePreservationRules:
    """Defines what cache entries can be preserved for each transformation type.

    This dataclass specifies exactly what cached values are safe to reuse
    after a transformation. Each boolean flag represents a type of cache entry.

    Flags:
        preserves_count: Can we reuse the cached count? (1:1 transformations)
        preserves_aggregations: Can we reuse sum/min/max? (reordering operations)
        preserves_schema: Can we reuse schema? (structure-preserving operations)
        preserves_columns: Can we reuse columns list? (structure-preserving operations)
        preserves_size_metadata: Can we reuse size_bytes/num_blocks? (reordering)
        can_compute_count: Can we smart-compute new count? (limit: min(old, limit))
        can_compute_columns: Can we smart-compute new columns? (add/drop/select)

    Examples:
        - map(): preserves_count=True, preserves_schema=True, preserves_columns=True
        - sort(): preserves EVERYTHING (count, aggregations, schema, columns, size)
        - limit(10): preserves_schema=True, can_compute_count=True
        - map_batches(): preserves nothing (all False)
    """

    preserves_count: bool = False  # Count stays the same (1:1 row mapping)
    preserves_aggregations: bool = False  # Sum/min/max stay the same (reordering)
    preserves_schema: bool = False  # Column names and types stay the same
    preserves_columns: bool = False  # Column list stays the same
    preserves_size_metadata: bool = False  # Size/num_blocks stay the same
    can_compute_count: bool = False  # Can calculate new count (e.g., limit)
    can_compute_columns: bool = False  # Can calculate new column list (add/drop)

    @classmethod
    def structure_and_count_preserving(cls) -> "CachePreservationRules":
        """Rules for transformations that preserve structure and count.

        Used for: map, add_column, drop_columns, select_columns, rename_columns, with_column

        These are 1:1 transformations that produce exactly one output row per input row.
        The count stays the same, and we can track column changes.

        Example:
            ds.map(lambda x: {"doubled": x["id"] * 2})
            → count preserved, but schema changes (new columns)

        Returns:
            CachePreservationRules with count and schema preservation enabled.
        """
        return cls(
            preserves_count=True,  # Same number of rows
            preserves_schema=True,  # Column structure preserved
            preserves_columns=True,  # Column list preserved (or computable)
        )

    @classmethod
    def reordering_only(cls) -> "CachePreservationRules":
        """Rules for transformations that only reorder data.

        Used for: sort, random_shuffle, repartition, randomize_block_order

        These operations just reorder rows without changing any values.
        This is HUGE for performance - we can preserve ALL cached values!

        Example:
            ds.sort("id")
            → Everything preserved! count, sum, min, max, schema, size, etc.

        Returns:
            CachePreservationRules with all preservation flags enabled.
        """
        return cls(
            preserves_count=True,  # Same rows
            preserves_aggregations=True,  # Same values, just reordered
            preserves_schema=True,  # Same columns
            preserves_columns=True,  # Same column list
            preserves_size_metadata=True,  # Same size and blocks
        )

    @classmethod
    def structure_preserving_count_changing(cls) -> "CachePreservationRules":
        """Rules for transformations that preserve structure but change count.

        Used for: limit, filter, random_sample

        These change how many rows we have but keep the same columns.
        We can smart-compute the new count for some operations (limit).

        Example:
            ds.limit(100)
            → Schema preserved, count smart-computed: min(original_count, 100)

        Returns:
            CachePreservationRules with schema preservation and count computation enabled.
        """
        return cls(
            preserves_schema=True,  # Same columns
            preserves_columns=True,  # Same column list
            can_compute_count=True,  # Can calculate new count
        )

    @classmethod
    def no_preservation(cls) -> "CachePreservationRules":
        """Rules for complex transformations that preserve nothing.

        Used for: map_batches, flat_map, union, join, etc.

        These transformations can change anything, so it's unsafe to preserve
        any cache values. This is the safe default for unknown operations.

        Returns:
            CachePreservationRules with all preservation flags disabled.

        Example:
            ds.map_batches(lambda batch: transform(batch))
            → Can't preserve anything (schema, count, etc. all may change)
        """
        return cls()  # All flags False (safe default)


# =============================================================================
# MAPPING: TRANSFORMATION TYPE → PRESERVATION RULES
# =============================================================================

# This dictionary maps each transformation type to its preservation rules.
# Operations not in TRANSFORMATION_TYPES below get no_preservation() by default.
CACHE_PRESERVATION_RULES = {
    TransformationType.STRUCTURE_AND_COUNT_PRESERVING: (
        CachePreservationRules.structure_and_count_preserving()
    ),
    TransformationType.REORDERING_ONLY: CachePreservationRules.reordering_only(),
    TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING: (
        CachePreservationRules.structure_preserving_count_changing()
    ),
    # All other types (COMPLEX_DATA_TRANSFORM, MULTI_DATASET_OPERATION, DATASET_SPLITTING)
    # preserve nothing (safe default) - they're not in this dict, so they fall back to
    # no_preservation() in the smart update logic
}


# =============================================================================
# MAPPING: OPERATION NAME → TRANSFORMATION TYPE
# =============================================================================

# Comprehensive mapping of Dataset method names to their transformation types.
# This is used by the smart cache update system to determine what to preserve.
TRANSFORMATION_TYPES = {
    # -------------------------------------------------------------------------
    # TIER 1: Structure-preserving, count-preserving (1:1 transformations)
    # -------------------------------------------------------------------------
    # These operations produce exactly one output row per input row, so count
    # is preserved. Schema/columns may change but in trackable ways.
    "map": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "add_column": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "drop_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "select_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "rename_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "with_column": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,  # Expressions API
    # -------------------------------------------------------------------------
    # TIER 2: Value-preserving, reordering only (BEST for caching!)
    # -------------------------------------------------------------------------
    # These operations just reorder rows without changing any data values.
    # This means ALL cached values (count, sum, min, max, schema, etc.) stay valid!
    "sort": TransformationType.REORDERING_ONLY,
    "random_shuffle": TransformationType.REORDERING_ONLY,
    "repartition": TransformationType.REORDERING_ONLY,
    "randomize_block_order": TransformationType.REORDERING_ONLY,
    # -------------------------------------------------------------------------
    # TIER 3: Structure-preserving, count-changing
    # -------------------------------------------------------------------------
    # These operations change the number of rows but keep the same columns.
    # For some (like limit), we can smart-compute the new count.
    "limit": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    "filter": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    "random_sample": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    # -------------------------------------------------------------------------
    # TIER 4: Complex data transformations
    # -------------------------------------------------------------------------
    # These can change anything (count, schema, data), so we must invalidate cache.
    "map_batches": TransformationType.COMPLEX_DATA_TRANSFORM,
    "flat_map": TransformationType.COMPLEX_DATA_TRANSFORM,
    # -------------------------------------------------------------------------
    # TIER 5: Multi-dataset operations
    # -------------------------------------------------------------------------
    # These combine multiple datasets, making cache preservation impossible.
    "union": TransformationType.MULTI_DATASET_OPERATION,
    "join": TransformationType.MULTI_DATASET_OPERATION,
    "zip": TransformationType.MULTI_DATASET_OPERATION,
    # -------------------------------------------------------------------------
    # TIER 6: Dataset splitting operations
    # -------------------------------------------------------------------------
    # These split a dataset into multiple datasets, requiring special handling.
    "split": TransformationType.DATASET_SPLITTING,
    "train_test_split": TransformationType.DATASET_SPLITTING,
    "split_at_indices": TransformationType.DATASET_SPLITTING,
    "split_proportionately": TransformationType.DATASET_SPLITTING,
    "streaming_split": TransformationType.DATASET_SPLITTING,
}
