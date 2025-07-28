import logging
import math
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from ray.data import DataContext
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    BlockTransformer,
    HashShufflingOperatorBase,
    StatefulShuffleAggregation,
)
from ray.data._internal.util import GiB
from ray.data.aggregate import AggregateFn
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey


logger = logging.getLogger(__name__)


class ReducingShuffleAggregation(StatefulShuffleAggregation):
    """Aggregation performing reduction of the shuffled sequence using provided
    list of aggregating functions.

    NOTE: That reductions are performed incrementally in a streaming fashion upon
          accumulation of pre-configured buffer of rows to run aggregation on."""

    _DEFAULT_BLOCKS_BUFFER_LIMIT = 1000

    def __init__(
        self,
        aggregator_id: int,
        key_columns: Optional[Tuple[str]],
        aggregation_fns: Tuple[AggregateFn],
    ):

        super().__init__(aggregator_id)

        assert key_columns is not None, "Shuffle aggregation requires key columns"

        self._sort_key: "SortKey" = ReducingShuffleAggregation._get_sort_key(
            key_columns
        )
        self._aggregation_fns: Tuple[AggregateFn] = aggregation_fns

        self._aggregated_blocks: List[Block] = []

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        assert (
            input_seq_id == 0
        ), f"Single sequence is expected (got seq-id {input_seq_id})"

        # Received partition shard is already partially aggregated, hence
        # we simply add it to the list of aggregated blocks
        #
        # NOTE: We're not separating blocks by partition as it's ultimately not
        #       relevant for the aggregations performed
        self._aggregated_blocks.append(partition_shard)

        # Aggregation is performed incrementally, rather
        # than being deferred to the finalization stage
        if len(self._aggregated_blocks) > self._DEFAULT_BLOCKS_BUFFER_LIMIT:
            # NOTE: This method will reset partially aggregated blocks to hold
            #       the new combined one
            #
            # TODO make aggregation async
            self._combine_aggregated_blocks(should_finalize=False)

    def finalize(self, partition_id: int) -> Block:
        if len(self._aggregated_blocks) == 0:
            return ArrowBlockAccessor._empty_table()

        return self._combine_aggregated_blocks(should_finalize=True)

    def clear(self, partition_id: int):
        self._aggregated_blocks: List[Block] = []

    def _combine_aggregated_blocks(self, *, should_finalize: bool) -> Block:
        assert len(self._aggregated_blocks) > 0

        block_accessor = BlockAccessor.for_block(self._aggregated_blocks[0])
        combined_block, _ = block_accessor._combine_aggregated_blocks(
            self._aggregated_blocks,
            sort_key=self._sort_key,
            aggs=self._aggregation_fns,
            finalize=should_finalize,
        )

        # For combined block that's not yet finalized reset cached aggregated
        # blocks to only hold newly combined one
        if not should_finalize:
            self._aggregated_blocks = [combined_block]

        return combined_block

    @staticmethod
    def _get_sort_key(key_columns: Tuple[str]):
        from ray.data._internal.planner.exchange.sort_task_spec import SortKey

        return SortKey(key=list(key_columns), descending=False)


class HashAggregateOperator(HashShufflingOperatorBase):
    def __init__(
        self,
        data_context: DataContext,
        input_op: PhysicalOperator,
        key_columns: Tuple[str],
        aggregation_fns: Tuple[AggregateFn],
        *,
        num_partitions: int,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name=(
                f"HashAggregate("
                f"num_partitions={num_partitions}, "
                f"key_columns={key_columns}"
                f")"
            ),
            input_ops=[input_op],
            data_context=data_context,
            key_columns=[key_columns],
            num_partitions=(
                # NOTE: In case of global aggregations (ie with no key columns specified),
                #       we override number of partitions to 1, since the whole dataset
                #       will be reduced to just a single row
                num_partitions
                if len(key_columns) > 0
                else 1
            ),
            partition_aggregation_factory=(
                lambda aggregator_id, target_partition_ids: ReducingShuffleAggregation(
                    aggregator_id,
                    key_columns,
                    aggregation_fns,
                )
            ),
            input_block_transformer=_create_aggregating_transformer(
                key_columns, aggregation_fns
            ),
            aggregator_ray_remote_args_override=aggregator_ray_remote_args_override,
            shuffle_progress_bar_name="Shuffle",
            finalize_progress_bar_name="Aggregation",
        )

    def _get_default_num_cpus_per_partition(self) -> int:
        """
        CPU allocation for aggregating actors of Aggregate operator is calculated as:
        num_cpus (per partition) = CPU budget / # partitions

        Assuming:
        - Default number of partitions: 200
        - Total operator's CPU budget with default settings: 2 cores
        - Number of CPUs per partition: 2 / 200 = 0.01

        These CPU budgets are derived such that Ray Data pipeline could run on a
        single node (using the default settings).
        """
        return 0.01

    def _get_operator_num_cpus_per_partition_override(self) -> int:
        return (
            self.data_context.hash_aggregate_operator_actor_num_cpus_per_partition_override
        )

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        partition_byte_size_estimate: int,
    ) -> int:
        dataset_size = num_partitions * partition_byte_size_estimate
        # Estimate of object store memory required to accommodate all partitions
        # handled by a single aggregator
        aggregator_shuffle_object_store_memory_required: int = math.ceil(
            dataset_size / num_aggregators
        )
        # Estimate of memory required to accommodate single partition as an output
        # (inside Object Store)
        output_object_store_memory_required: int = partition_byte_size_estimate

        aggregator_total_memory_required: int = (
            # Inputs (object store)
            aggregator_shuffle_object_store_memory_required
            +
            # Output (object store)
            output_object_store_memory_required
        )

        logger.debug(
            f"Estimated memory requirement for aggregating operator "
            f"(partitions={num_partitions}, aggregators={num_aggregators}): "
            f"shuffle={aggregator_shuffle_object_store_memory_required / GiB:.2f}GiB, "
            f"output={output_object_store_memory_required / GiB:.2f}GiB, "
            f"total={aggregator_total_memory_required / GiB:.2f}GiB, "
        )

        return aggregator_total_memory_required


def _create_aggregating_transformer(
    key_columns: Tuple[str], aggregation_fns: Tuple[AggregateFn]
) -> BlockTransformer:
    """Method creates input block transformer performing partial aggregation of
    the block applied prior to block being shuffled (to reduce amount of bytes shuffled)"""

    sort_key = ReducingShuffleAggregation._get_sort_key(key_columns)

    def _aggregate(block: Block) -> Block:
        from ray.data._internal.planner.exchange.aggregate_task_spec import (
            SortAggregateTaskSpec,
        )

        # TODO unify blocks schemas, to avoid validating every block
        # Validate block's schema compatible with aggregations
        for agg_fn in aggregation_fns:
            agg_fn._validate(BlockAccessor.for_block(block).schema())

        # Project block to only carry columns used in aggregation
        pruned_block = SortAggregateTaskSpec._prune_unused_columns(
            block,
            sort_key,
            aggregation_fns,
        )

        # NOTE: If columns to aggregate on have been provided,
        #       sort the block on these before aggregation
        if sort_key.get_columns():
            target_block = BlockAccessor.for_block(pruned_block).sort(sort_key)
        else:
            target_block = pruned_block

        return BlockAccessor.for_block(target_block)._aggregate(
            sort_key, aggregation_fns
        )

    return _aggregate
