"""Hash-based distinct (deduplication) operators for Ray Data.

This module implements distributed deduplication using hash-based shuffling,
where rows with the same key are routed to the same partition for efficient
local deduplication.
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

# PyArrow is used for schema validation and type checking.
# See https://arrow.apache.org/docs/python/ for PyArrow documentation.
import pyarrow as pa

from ray.data._internal.arrow_block import ArrowBlockAccessor, ArrowBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
    StatefulShuffleAggregation,
)
from ray.data._internal.util import GiB, MiB
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


def _make_hashable_key(value: Any) -> Any:
    """Convert a value to a hashable type for use as a dictionary key.

    This handles common unhashable types by converting them to immutable equivalents:
    - Lists -> tuples
    - Dicts -> sorted tuple of items
    - None -> preserved as None
    - NaN -> special sentinel to ensure all NaNs are treated as equal

    Args:
        value: The value to convert to a hashable type.

    Returns:
        A hashable representation of the value.

    Raises:
        TypeError: If the value contains types that cannot be made hashable.
    """
    if value is None:
        return None

    # Handle NaN values - treat all NaNs as equal
    if isinstance(value, float) and math.isnan(value):
        return ("__NaN__",)

    # Handle lists by converting to tuples recursively
    if isinstance(value, list):
        return tuple(_make_hashable_key(item) for item in value)

    # Handle dicts by converting to sorted tuple of items
    if isinstance(value, dict):
        return tuple(sorted((k, _make_hashable_key(v)) for k, v in value.items()))

    # Handle PyArrow arrays and other unhashable types
    if isinstance(value, (pa.Array, pa.ChunkedArray)):
        raise TypeError(
            "Cannot use PyArrow array as distinct key. "
            "Distinct operation requires scalar values in key columns."
        )

    # Value is already hashable (int, str, tuple, etc.)
    return value


class DistinctAggregation(StatefulShuffleAggregation):
    """Stateful aggregation that deduplicates rows within partitions.

    This class implements efficient deduplication using a dictionary to track
    seen keys for O(1) average-case lookup and insertion. For each partition,
    it maintains a dictionary mapping key tuples to the first row encountered
    with that key.

    Args:
        aggregator_id: Unique identifier for this aggregator.
        target_partition_ids: List of partition IDs this aggregator handles.
        key_columns: Tuple of column names to use for identifying duplicates.
            Empty tuple means key columns will be determined from schema at runtime.
    """

    def __init__(
        self,
        aggregator_id: int,
        target_partition_ids: List[int],
        *,
        key_columns: Tuple[str, ...],
    ):
        super().__init__(aggregator_id)
        self._key_columns = key_columns
        # Dictionary per partition: {key_tuple -> first_row_dict}
        # Rows are stored as materialized dicts to ensure data integrity.
        self._partition_data: Dict[int, Dict[Tuple, Dict[str, Any]]] = {
            partition_id: {} for partition_id in target_partition_ids
        }
        # Cache determined key columns to ensure consistency across blocks
        self._determined_key_columns: Optional[Tuple[str, ...]] = None
        # Track statistics for logging
        self._rows_seen = 0
        self._unique_rows = 0

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        """Accept a block shard and deduplicate rows based on key columns.

        Args:
            input_seq_id: Input sequence index (must be 0 for distinct).
            partition_id: ID of the partition this shard belongs to.
            partition_shard: Block of data to process.

        Raises:
            ValueError: If input_seq_id is not 0, partition_id is invalid,
                or key columns cannot be determined.
            TypeError: If key column values contain unhashable types.
        """
        if input_seq_id != 0:
            raise ValueError(
                f"Distinct is a unary aggregation and only accepts input_seq_id=0, "
                f"got input_seq_id={input_seq_id}"
            )

        if partition_id not in self._partition_data:
            raise ValueError(
                f"Invalid partition_id={partition_id}. "
                f"Expected one of {list(self._partition_data.keys())}"
            )

        seen = self._partition_data[partition_id]
        accessor = BlockAccessor.for_block(partition_shard)

        # Get the number of rows in this block
        num_rows = accessor.num_rows()
        if num_rows == 0:
            return

        # Determine columns to use for distinct
        # Cache the determination to ensure consistency across all blocks
        if self._determined_key_columns is None:
            if not self._key_columns:
                # Key columns not specified - determine from schema
                block_schema = accessor.schema()
                if block_schema is None:
                    raise ValueError(
                        "Cannot perform distinct operation: dataset has no inferable schema. "
                        "Specify key columns explicitly using the 'keys' parameter."
                    )
                # Handle empty schema (zero columns) - use empty tuple for deduplication keys
                # This correctly keeps at most one row from datasets with no columns
                if not block_schema.names:
                    self._determined_key_columns = ()
                else:
                    self._determined_key_columns = tuple(block_schema.names)
            else:
                # Validate that specified key columns exist in the schema
                block_schema = accessor.schema()
                if block_schema is not None and block_schema.names:
                    schema_columns = set(block_schema.names)
                    missing_columns = set(self._key_columns) - schema_columns
                    if missing_columns:
                        raise ValueError(
                            f"Key columns {sorted(missing_columns)} not found in schema. "
                            f"Available columns: {sorted(schema_columns)}"
                        )
                self._determined_key_columns = self._key_columns

        key_columns = self._determined_key_columns

        # Deduplicate: keep only the first occurrence of each unique key
        for row in accessor.iter_rows(public_row_format=False):
            self._rows_seen += 1

            # Extract key values and make them hashable
            key_values = []
            for col in key_columns:
                value = row.get(col)
                # Convert to hashable type, handling lists, dicts, NaN, etc.
                hashable_value = _make_hashable_key(value)
                key_values.append(hashable_value)

            key = tuple(key_values)

            if key not in seen:
                self._unique_rows += 1
                # Materialize row data to ensure data integrity.
                # ArrowRow objects are lightweight views that may become invalid
                # after the block is garbage collected.
                seen[key] = row.as_pydict()

    def finalize(self, partition_id: int) -> Block:
        """Build and return the deduplicated block for a partition.

        Args:
            partition_id: ID of the partition to finalize.

        Returns:
            A Block containing all unique rows for this partition.

        Raises:
            ValueError: If partition_id is invalid.
        """
        if partition_id not in self._partition_data:
            raise ValueError(
                f"Cannot finalize invalid partition_id={partition_id}. "
                f"Expected one of {list(self._partition_data.keys())}"
            )

        partition_data = self._partition_data[partition_id]
        num_unique = len(partition_data)

        if num_unique == 0:
            return ArrowBlockAccessor._empty_table()

        # Build output block from unique rows
        builder = ArrowBlockBuilder()
        for row_dict in partition_data.values():
            builder.add(row_dict)

        output_block = builder.build()

        # Log deduplication statistics
        if self._rows_seen > 0:
            dedup_ratio = (1 - num_unique / self._rows_seen) * 100
            logger.debug(
                f"Distinct partition {partition_id}: "
                f"{self._rows_seen} rows -> {num_unique} unique rows "
                f"({dedup_ratio:.1f}% duplicates removed)"
            )

        return output_block

    def clear(self, partition_id: int):
        """Clear data for a partition to free memory.

        Args:
            partition_id: ID of the partition to clear.
        """
        partition_data = self._partition_data.pop(partition_id, None)
        if partition_data:
            num_cleared = len(partition_data)
            logger.debug(
                f"Cleared partition {partition_id}: freed {num_cleared} unique rows"
            )


class HashDistinctOperator(HashShufflingOperatorBase):
    """Physical operator for hash-based distinct (deduplication).

    This operator extends HashShufflingOperatorBase to perform distributed
    deduplication using hash partitioning. Rows with identical key values are
    routed to the same partition where local deduplication occurs using an
    efficient dictionary-based approach.

    The operator works in two phases:
    1. Shuffle phase: Hash partition rows by key columns to route duplicates together
    2. Reduce phase: Locally deduplicate rows within each partition

    Args:
        input_op: The input physical operator providing data to deduplicate.
        data_context: Data context configuration for execution.
        key_columns: Tuple of column names to use for identifying duplicates.
            Empty tuple means all columns will be used.
        num_partitions: Optional number of output partitions for the shuffle.
            If None, uses the data context's default parallelism.
            Must be positive if specified.
        aggregator_ray_remote_args_override: Optional override for Ray remote args
            used when creating aggregator actors.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        key_columns: Tuple[str, ...],
        num_partitions: Optional[int] = None,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
    ):
        if num_partitions is not None and num_partitions <= 0:
            raise ValueError(f"num_partitions must be positive, got {num_partitions}")

        super().__init__(
            name_factory=(
                lambda num_partitions: f"Distinct(keys={key_columns or 'all'}, "
                f"num_partitions={num_partitions})"
            ),
            input_ops=[input_op],
            data_context=data_context,
            key_columns=[key_columns],
            num_partitions=num_partitions,
            aggregator_ray_remote_args_override=aggregator_ray_remote_args_override,
            partition_aggregation_factory=(
                lambda aggregator_id, target_partition_ids: DistinctAggregation(
                    aggregator_id,
                    target_partition_ids,
                    key_columns=key_columns,
                )
            ),
            shuffle_progress_bar_name="Distinct Shuffle",
            finalize_progress_bar_name="Distinct Reduce",
        )

    def _get_operator_num_cpus_override(self) -> float:
        """Get the number of CPUs to use for the operator actor."""
        return self.data_context.hash_shuffle_operator_actor_num_cpus_override

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        estimated_dataset_bytes: int,
    ) -> int:
        """Estimate memory requirements for distinct aggregator actors.

        This estimates the memory needed for:
        1. Shuffle inputs stored in object store
        2. Single partition output stored in object store
        3. Dictionary overhead for tracking unique keys (estimated at 2x data size)

        Args:
            num_aggregators: Number of aggregator actors to create.
            num_partitions: Number of output partitions.
            estimated_dataset_bytes: Estimated total size of the dataset in bytes.

        Returns:
            Estimated total memory requirement in bytes for each aggregator.
        """
        if estimated_dataset_bytes <= 0:
            estimated_dataset_bytes = 1

        if num_partitions <= 0:
            num_partitions = 1

        partition_byte_size_estimate = max(
            1, math.ceil(estimated_dataset_bytes / num_partitions)
        )

        # Memory for shuffle inputs (object store)
        aggregator_shuffle_object_store_memory_required: int = max(
            1, math.ceil(estimated_dataset_bytes / num_aggregators)
        )

        # Memory for single partition output (object store)
        output_object_store_memory_required: int = partition_byte_size_estimate

        # Memory for dictionary overhead (Python dict + materialized rows)
        # Conservative estimate: 2x data size for dict overhead and row materialization
        dict_overhead_memory_required: int = partition_byte_size_estimate * 2

        aggregator_total_memory_required: int = (
            aggregator_shuffle_object_store_memory_required
            + output_object_store_memory_required
            + dict_overhead_memory_required
        )

        logger.info(
            f"Estimated memory requirement for distinct aggregator "
            f"(partitions={num_partitions}, "
            f"aggregators={num_aggregators}, "
            f"dataset (estimate)={estimated_dataset_bytes / GiB:.1f}GiB): "
            f"shuffle={aggregator_shuffle_object_store_memory_required / MiB:.1f}MiB, "
            f"output={output_object_store_memory_required / MiB:.1f}MiB, "
            f"dict_overhead={dict_overhead_memory_required / MiB:.1f}MiB, "
            f"total={aggregator_total_memory_required / MiB:.1f}MiB"
        )

        return aggregator_total_memory_required
