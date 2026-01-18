import logging
import math
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    BlockTransformer,
    HashShufflingOperatorBase,
    ShuffleAggregation,
)
from ray.data._internal.util import GiB, MiB
from ray.data.aggregate import AggregateFn
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext

if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey


logger = logging.getLogger(__name__)


class ReducingAggregation(ShuffleAggregation):
    """Stateless aggregation that reduces blocks using aggregation functions.

    This implementation performs incremental reduction during compaction,
    combining multiple partially-aggregated blocks into one. The final
    aggregation is performed during finalization.
    """

    def __init__(
        self,
        key_columns: Tuple[str, ...],
        aggregation_fns: Tuple[AggregateFn, ...],
    ):
        self._sort_key: "SortKey" = self._get_sort_key(key_columns)
        self._aggregation_fns: Tuple[AggregateFn, ...] = aggregation_fns

    @classmethod
    def is_compacting(cls):
        return True

    def compact(self, partition_shards: List[Block]) -> Block:
        assert len(partition_shards) > 0, "Provided sequence must be non-empty"

        return self._combine(partition_shards, finalize=False)

    def finalize(self, partition_shards_map: Dict[int, List[Block]]) -> Iterator[Block]:
        assert (
            len(partition_shards_map) == 1
        ), f"Single input-sequence is expected (got {len(partition_shards_map)})"

        blocks = partition_shards_map[0]

        if not blocks:
            return

        yield self._combine(blocks, finalize=True)

    def _combine(self, blocks: List[Block], *, finalize: bool) -> Block:
        """Internal method to combine blocks with optional finalization."""
        assert len(blocks) > 0

        block_accessor = BlockAccessor.for_block(blocks[0])
        combined_block, _ = block_accessor._combine_aggregated_blocks(
            blocks,
            sort_key=self._sort_key,
            aggs=self._aggregation_fns,
            finalize=finalize,
        )

        return combined_block

    @staticmethod
    def _get_sort_key(key_columns: Tuple[str, ...]) -> "SortKey":
        from ray.data._internal.planner.exchange.sort_task_spec import SortKey

        return SortKey(key=list(key_columns), descending=False)


class HashAggregateOperator(HashShufflingOperatorBase):

    _DEFAULT_MIN_NUM_SHARDS_COMPACTION_THRESHOLD = 100
    _DEFAULT_MAX_NUM_SHARDS_COMPACTION_THRESHOLD = 2000

    def __init__(
        self,
        data_context: DataContext,
        input_op: PhysicalOperator,
        key_columns: Tuple[str],
        aggregation_fns: Tuple[AggregateFn],
        *,
        num_partitions: Optional[int] = None,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
    ):
        # Use new stateless ReducingAggregation factory
        def _create_reducing_aggregation() -> ReducingAggregation:
            return ReducingAggregation(
                key_columns=key_columns,
                aggregation_fns=aggregation_fns,
            )

        super().__init__(
            name_factory=(
                lambda num_partitions: f"HashAggregate(key_columns={key_columns}, "
                f"num_partitions={num_partitions})"
            ),
            input_ops=[input_op],
            data_context=data_context,
            key_columns=[key_columns],
            num_input_seqs=1,
            num_partitions=(
                # NOTE: In case of global aggregations (ie with no key columns specified),
                #       we override number of partitions to 1, since the whole dataset
                #       will be reduced to just a single row
                num_partitions
                if len(key_columns) > 0
                else 1
            ),
            partition_aggregation_factory=_create_reducing_aggregation,
            input_block_transformer=_create_aggregating_transformer(
                key_columns, aggregation_fns
            ),
            aggregator_ray_remote_args_override=aggregator_ray_remote_args_override,
            shuffle_progress_bar_name="Shuffle",
            finalize_progress_bar_name="Aggregation",
        )

    def _get_operator_num_cpus_override(self) -> float:
        return self.data_context.hash_aggregate_operator_actor_num_cpus_override

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

        # Estimate of object store memory required to accommodate all partitions
        # handled by a single aggregator
        aggregator_shuffle_object_store_memory_required: int = math.ceil(
            estimated_dataset_bytes / num_aggregators
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

        logger.info(
            f"Estimated memory requirement for aggregating aggregator "
            f"(partitions={num_partitions}, "
            f"aggregators={num_aggregators}, "
            f"dataset (estimate)={estimated_dataset_bytes / GiB:.1f}GiB): "
            f"shuffle={aggregator_shuffle_object_store_memory_required / MiB:.1f}MiB, "
            f"output={output_object_store_memory_required / MiB:.1f}MiB, "
            f"total={aggregator_total_memory_required / MiB:.1f}MiB, "
        )

        return aggregator_total_memory_required

    @classmethod
    def _get_min_max_partition_shards_compaction_thresholds(
        cls,
    ) -> Optional[Tuple[int, int]]:
        return (
            cls._DEFAULT_MIN_NUM_SHARDS_COMPACTION_THRESHOLD,
            cls._DEFAULT_MAX_NUM_SHARDS_COMPACTION_THRESHOLD,
        )


def _create_aggregating_transformer(
    key_columns: Tuple[str], aggregation_fns: Tuple[AggregateFn]
) -> BlockTransformer:
    """Method creates input block transformer performing partial aggregation of
    the block applied prior to block being shuffled (to reduce amount of bytes shuffled)"""

    sort_key = ReducingAggregation._get_sort_key(key_columns)

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
