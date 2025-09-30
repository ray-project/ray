"""
Type-safe constants and enums for Ray Data caching.
"""

from dataclasses import dataclass
from enum import Enum


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


@dataclass
class CachePreservationRules:
    """Defines what cache entries can be preserved for each transformation type."""

    preserves_count: bool = False
    preserves_aggregations: bool = False
    preserves_schema: bool = False
    preserves_columns: bool = False
    preserves_size_metadata: bool = False
    can_compute_count: bool = False
    can_compute_columns: bool = False


# What each transformation type preserves
CACHE_PRESERVATION_RULES = {
    TransformationType.STRUCTURE_AND_COUNT_PRESERVING: CachePreservationRules(
        preserves_count=True,
        preserves_aggregations=False,  # Data might change (except for map)
        preserves_schema=True,
        preserves_columns=True,
        preserves_size_metadata=False,
        can_compute_count=False,
        can_compute_columns=True,  # Can compute for add/drop/select columns
    ),
    TransformationType.REORDERING_ONLY: CachePreservationRules(
        preserves_count=True,
        preserves_aggregations=True,  # Order doesn't affect aggregations!
        preserves_schema=True,
        preserves_columns=True,
        preserves_size_metadata=True,  # Size and blocks unchanged
        can_compute_count=False,
        can_compute_columns=False,
    ),
    TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING: CachePreservationRules(
        preserves_count=False,
        preserves_aggregations=False,
        preserves_schema=True,
        preserves_columns=True,
        preserves_size_metadata=False,
        can_compute_count=True,  # Can compute for limit, estimate for sample
        can_compute_columns=False,
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
