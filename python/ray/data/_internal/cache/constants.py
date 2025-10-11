"""
Type-safe constants and enums for Ray Data caching.
"""

from dataclasses import dataclass
from enum import Enum

# Cache placement thresholds (in bytes)
LOCAL_CACHE_THRESHOLD_BYTES = 50 * 1024  # 50KB - Small objects cached locally
RAY_CACHE_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10MB - Medium objects in Ray store

# Ray object size estimates (in bytes)
RAY_OBJECTREF_SIZE_BYTES = 64  # ObjectRef pointer size
RAY_BLOCK_OVERHEAD_BYTES = 64  # Block ObjectRef + metadata overhead
RAY_DATASET_METADATA_SIZE_BYTES = 1024  # MaterializedDataset metadata size

# Cache size limits
MAX_CACHE_ENTRIES = 1000  # Maximum number of entries per cache
DEFAULT_MAX_CACHE_SIZE_BYTES = 1024 * 1024 * 1024  # 1GB default max size


class CacheableOperation(Enum):
    """Operations that can be cached."""

    # Metadata operations
    COUNT = "count"
    SCHEMA = "schema"
    COLUMNS = "columns"
    SIZE_BYTES = "size_bytes"
    INPUT_FILES = "input_files"
    NUM_BLOCKS = "num_blocks"

    # Data operations
    TAKE = "take"
    TAKE_BATCH = "take_batch"
    TAKE_ALL = "take_all"
    UNIQUE = "unique"

    # Aggregation operations
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    STD = "std"
    AGGREGATE = "aggregate"

    # Execution operations
    MATERIALIZE = "materialize"
    STATS = "stats"


class TransformationType(Enum):
    """Hierarchical transformation types for intelligent cache invalidation."""

    # TIER 1: Best caching - preserve almost everything
    STRUCTURE_AND_COUNT_PRESERVING = "structure_and_count_preserving"

    # TIER 2: Great caching - preserve aggregations and metadata
    REORDERING_ONLY = "reordering_only"

    # TIER 3: Good caching - preserve structure metadata
    STRUCTURE_PRESERVING_COUNT_CHANGING = "structure_preserving_count_changing"

    # TIER 4: Limited caching - complex data changes
    COMPLEX_DATA_TRANSFORM = "complex_data_transform"

    # TIER 5: No caching - multi-dataset operations
    MULTI_DATASET_OPERATION = "multi_dataset_operation"

    # TIER 6: Special handling - splitting operations
    DATASET_SPLITTING = "dataset_splitting"


@dataclass(frozen=True)
class CachePreservationRules:
    """Defines what cache entries can be preserved for each transformation type."""

    preserves_count: bool = False
    preserves_aggregations: bool = False
    preserves_schema: bool = False
    preserves_columns: bool = False
    preserves_size_metadata: bool = False
    can_compute_count: bool = False
    can_compute_columns: bool = False

    @classmethod
    def structure_and_count_preserving(cls) -> "CachePreservationRules":
        """Rules for transformations that preserve structure and count (map, add_column, etc.)."""
        return cls(
            preserves_count=True,
            preserves_schema=True,
            preserves_columns=True,
        )

    @classmethod
    def reordering_only(cls) -> "CachePreservationRules":
        """Rules for transformations that only reorder data (sort, shuffle)."""
        return cls(
            preserves_count=True,
            preserves_aggregations=True,
            preserves_schema=True,
            preserves_columns=True,
            preserves_size_metadata=True,
        )

    @classmethod
    def structure_preserving_count_changing(cls) -> "CachePreservationRules":
        """Rules for transformations that preserve structure but change count (limit, filter)."""
        return cls(
            preserves_schema=True,
            preserves_columns=True,
            can_compute_count=True,
        )

    @classmethod
    def no_preservation(cls) -> "CachePreservationRules":
        """Rules for complex transformations that preserve nothing (safe default)."""
        return cls()


# What each transformation type preserves (using factory methods for clarity)
CACHE_PRESERVATION_RULES = {
    TransformationType.STRUCTURE_AND_COUNT_PRESERVING: (
        CachePreservationRules.structure_and_count_preserving()
    ),
    TransformationType.REORDERING_ONLY: CachePreservationRules.reordering_only(),
    TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING: (
        CachePreservationRules.structure_preserving_count_changing()
    ),
    # All other types preserve nothing (safe default)
}


# Comprehensive transformation type mapping
TRANSFORMATION_TYPES = {
    # TIER 1: Structure-preserving, count-preserving
    "map": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "add_column": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "drop_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "select_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "rename_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "with_column": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    # TIER 2: Value-preserving, reordering only
    "sort": TransformationType.REORDERING_ONLY,
    "random_shuffle": TransformationType.REORDERING_ONLY,
    "repartition": TransformationType.REORDERING_ONLY,
    "randomize_block_order": TransformationType.REORDERING_ONLY,
    # TIER 3: Structure-preserving, count-changing
    "limit": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    "filter": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    "random_sample": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    # TIER 4: Complex data transformations
    "map_batches": TransformationType.COMPLEX_DATA_TRANSFORM,
    "flat_map": TransformationType.COMPLEX_DATA_TRANSFORM,
    # TIER 5: Multi-dataset operations
    "union": TransformationType.MULTI_DATASET_OPERATION,
    "join": TransformationType.MULTI_DATASET_OPERATION,
    "zip": TransformationType.MULTI_DATASET_OPERATION,
    # TIER 6: Dataset splitting operations
    "split": TransformationType.DATASET_SPLITTING,
    "train_test_split": TransformationType.DATASET_SPLITTING,
    "split_at_indices": TransformationType.DATASET_SPLITTING,
    "split_proportionately": TransformationType.DATASET_SPLITTING,
    "streaming_split": TransformationType.DATASET_SPLITTING,
}
