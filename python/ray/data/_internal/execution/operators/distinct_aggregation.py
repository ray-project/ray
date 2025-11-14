"""Hash-based distinct (deduplication) operators for Ray Data.

This module implements distributed deduplication using hash-based shuffling,
where rows with the same key are routed to the same partition for efficient
local deduplication.
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

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
        # Dictionary per partition: {key_tuple -> first_row_with_that_key}
        self._partition_data: Dict[int, Dict[Tuple, Dict]] = {
            partition_id: {} for partition_id in target_partition_ids
        }

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        """Accept a block shard and deduplicate rows based on key columns.

        Args:
            input_seq_id: Input sequence index (must be 0 for distinct).
            partition_id: ID of the partition this shard belongs to.
            partition_shard: Block of data to process.
        """
        assert input_seq_id == 0, (
            f"Distinct is unary aggregation, got sequence index {input_seq_id}"
        )
        assert partition_id in self._partition_data

        seen = self._partition_data[partition_id]
        accessor = BlockAccessor.for_block(partition_shard)

        # Determine columns to use for distinct.
        # If key_columns is empty (schema was None at planning time), use all columns.
        if not self._key_columns:
            block_schema = accessor.schema()
            if block_schema is None or not block_schema.names:
                raise ValueError(
                    "Cannot perform distinct: block has no inferable schema. "
                    "Provide explicit 'keys' parameter to distinct()."
                )
            key_columns = tuple(block_schema.names)
        else:
            key_columns = self._key_columns

        # Deduplicate: keep only the first occurrence of each unique key
        for row in accessor.iter_rows(public_row_format=False):
            key = tuple(row.get(col) for col in key_columns)
            if key not in seen:
                seen[key] = row

    def finalize(self, partition_id: int) -> Block:
        """Build and return the deduplicated block for a partition.

        Args:
            partition_id: ID of the partition to finalize.

        Returns:
            A Block containing all unique rows for this partition.
        """
        partition_data = self._partition_data[partition_id]
        if not partition_data:
            return ArrowBlockAccessor._empty_table()

        builder = ArrowBlockBuilder()
        for row in partition_data.values():
            builder.add(row)
        return builder.build()

    def clear(self, partition_id: int):
        """Clear data for a partition to free memory.

        Args:
            partition_id: ID of the partition to clear.
        """
        self._partition_data.pop(partition_id, None)


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
        num_partitions: Optional number of output partitions for the shuffle.
            If None, uses the data context's default parallelism.
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
        super().__init__(
            name_factory=(
                lambda num_partitions: f"Distinct(keys={key_columns}, "
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

        Args:
            num_aggregators: Number of aggregator actors to create.
            num_partitions: Number of output partitions.
            estimated_dataset_bytes: Estimated total size of the dataset in bytes.

        Returns:
            Estimated total memory requirement in bytes for each aggregator.
        """
        partition_byte_size_estimate = math.ceil(
            estimated_dataset_bytes / num_partitions
        )

        # Memory for shuffle inputs (object store)
        aggregator_shuffle_object_store_memory_required: int = math.ceil(
            estimated_dataset_bytes / num_aggregators
        )
        # Memory for single partition output (object store)
        output_object_store_memory_required: int = partition_byte_size_estimate

        aggregator_total_memory_required: int = (
            aggregator_shuffle_object_store_memory_required
            + output_object_store_memory_required
        )

        logger.info(
            f"Estimated memory requirement for distinct aggregator "
            f"(partitions={num_partitions}, "
            f"aggregators={num_aggregators}, "
            f"dataset (estimate)={estimated_dataset_bytes / GiB:.1f}GiB): "
            f"shuffle={aggregator_shuffle_object_store_memory_required / MiB:.1f}MiB, "
            f"output={output_object_store_memory_required / MiB:.1f}MiB, "
            f"total={aggregator_total_memory_required / MiB:.1f}MiB"
        )

        return aggregator_total_memory_required
