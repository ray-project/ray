"""Hash-based distinct (deduplication) operators for Ray Data.

This module implements distributed deduplication using hash-based shuffling,
where rows with the same key are routed to the same partition for efficient
local deduplication.
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

from ray.data._internal.arrow_block import ArrowBlockBuilder
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

    Uses a dictionary to track seen keys for O(1) average-case deduplication.
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
        # Dict per partition: {key_tuple -> first_row_with_that_key}
        self._partition_data: Dict[int, Dict[Tuple, Dict]] = {
            partition_id: {} for partition_id in target_partition_ids
        }

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        assert (
            input_seq_id == 0
        ), f"Distinct is unary aggregation, got sequence index {input_seq_id}"
        assert partition_id in self._partition_data

        seen = self._partition_data[partition_id]
        accessor = BlockAccessor.for_block(partition_shard)

        # Deduplicate: keep only first occurrence of each key
        for row in accessor.iter_rows(public_row_format=False):
            key = tuple(row.get(col) for col in self._key_columns)
            if key not in seen:
                seen[key] = row

    def finalize(self, partition_id: int) -> Block:
        builder = ArrowBlockBuilder()
        for row in self._partition_data[partition_id].values():
            builder.add(row)
        return builder.build()

    def clear(self, partition_id: int):
        self._partition_data.pop(partition_id, None)


class HashDistinctOperator(HashShufflingOperatorBase):
    """Physical operator for hash-based distinct (deduplication).

    Extends HashShufflingOperatorBase to perform distributed deduplication using
    hash partitioning, where rows with identical key values are routed to the same
    partition for local deduplication.
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
        return self.data_context.hash_shuffle_operator_actor_num_cpus_override

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        estimated_dataset_bytes: int,
    ) -> int:
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
