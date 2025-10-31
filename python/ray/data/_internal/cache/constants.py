"""Constants and enums for Ray Data caching."""

from dataclasses import dataclass
from enum import Enum

LOCAL_CACHE_THRESHOLD_BYTES = 50 * 1024
RAY_CACHE_THRESHOLD_BYTES = 10 * 1024 * 1024
MAX_CACHE_ENTRIES = 1000
DEFAULT_MAX_CACHE_SIZE_BYTES = 1024 * 1024 * 1024


class CacheableOperation(Enum):
    """Operations that can be cached."""

    COUNT = "count"
    SCHEMA = "schema"
    COLUMNS = "columns"
    SIZE_BYTES = "size_bytes"
    INPUT_FILES = "input_files"
    NUM_BLOCKS = "num_blocks"
    TAKE = "take"
    TAKE_BATCH = "take_batch"
    TAKE_ALL = "take_all"
    UNIQUE = "unique"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    MEAN = "mean"
    STD = "std"
    AGGREGATE = "aggregate"
    MATERIALIZE = "materialize"
    STATS = "stats"


class TransformationType(Enum):
    """Transformation types for cache invalidation."""

    STRUCTURE_AND_COUNT_PRESERVING = "structure_and_count_preserving"
    REORDERING_ONLY = "reordering_only"
    STRUCTURE_PRESERVING_COUNT_CHANGING = "structure_preserving_count_changing"
    COMPLEX_DATA_TRANSFORM = "complex_data_transform"
    MULTI_DATASET_OPERATION = "multi_dataset_operation"
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
        """Rules for transformations that preserve structure and count."""
        return cls(
            preserves_count=True,
            preserves_schema=True,
            preserves_columns=True,
        )

    @classmethod
    def reordering_only(cls) -> "CachePreservationRules":
        """Rules for transformations that only reorder data."""
        return cls(
            preserves_count=True,
            preserves_aggregations=True,
            preserves_schema=True,
            preserves_columns=True,
            preserves_size_metadata=True,
        )

    @classmethod
    def structure_preserving_count_changing(cls) -> "CachePreservationRules":
        """Rules for transformations that preserve structure but change count."""
        return cls(
            preserves_schema=True,
            preserves_columns=True,
            can_compute_count=True,
        )

    @classmethod
    def no_preservation(cls) -> "CachePreservationRules":
        """Rules for complex transformations that preserve nothing."""
        return cls()


CACHE_PRESERVATION_RULES = {
    TransformationType.STRUCTURE_AND_COUNT_PRESERVING: (
        CachePreservationRules.structure_and_count_preserving()
    ),
    TransformationType.REORDERING_ONLY: CachePreservationRules.reordering_only(),
    TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING: (
        CachePreservationRules.structure_preserving_count_changing()
    ),
}

TRANSFORMATION_TYPES = {
    "map": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "add_column": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "drop_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "select_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "rename_columns": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "with_column": TransformationType.STRUCTURE_AND_COUNT_PRESERVING,
    "sort": TransformationType.REORDERING_ONLY,
    "random_shuffle": TransformationType.REORDERING_ONLY,
    "repartition": TransformationType.REORDERING_ONLY,
    "randomize_block_order": TransformationType.REORDERING_ONLY,
    "limit": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    "filter": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    "random_sample": TransformationType.STRUCTURE_PRESERVING_COUNT_CHANGING,
    "map_batches": TransformationType.COMPLEX_DATA_TRANSFORM,
    "flat_map": TransformationType.COMPLEX_DATA_TRANSFORM,
    "union": TransformationType.MULTI_DATASET_OPERATION,
    "join": TransformationType.MULTI_DATASET_OPERATION,
    "zip": TransformationType.MULTI_DATASET_OPERATION,
    "split": TransformationType.DATASET_SPLITTING,
    "train_test_split": TransformationType.DATASET_SPLITTING,
    "split_at_indices": TransformationType.DATASET_SPLITTING,
    "split_proportionately": TransformationType.DATASET_SPLITTING,
    "streaming_split": TransformationType.DATASET_SPLITTING,
}
