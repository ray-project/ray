from typing import List, Tuple, Union

from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.aggregate import AggregateFn, AggregateFnV2, Count
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    KeyType,
)


class SortAggregateTaskSpec(ExchangeTaskSpec):
    """
    The implementation for sort-based aggregate tasks.

    Aggregate is done in 2 steps: partial aggregate of individual blocks, and
    final aggregate of sorted blocks.

    Partial aggregate (`map`): each block is sorted locally, then partitioned into
    smaller blocks according to the boundaries. Each partitioned block is aggregated
    separately, then passed to a final aggregate task.

    Final aggregate (`reduce`): each task would receive a block from every worker that
    consists of items in a certain range. It then merges the sorted blocks and
    aggregates on-the-fly.
    """

    def __init__(
        self,
        boundaries: List[KeyType],
        key: SortKey,
        aggs: List[AggregateFn],
        batch_format: str,
    ):
        super().__init__(
            map_args=[boundaries, key, aggs],
            reduce_args=[key, aggs, batch_format],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        boundaries: List[KeyType],
        sort_key: SortKey,
        aggs: List[AggregateFn],
    ) -> List[Union[Block, "BlockMetadataWithSchema"]]:
        stats = BlockExecStats.builder()

        block = SortAggregateTaskSpec._prune_unused_columns(block, sort_key, aggs)
        if sort_key.get_columns():
            partitions = BlockAccessor.for_block(block).sort_and_partition(
                boundaries,
                sort_key,
            )
        else:
            partitions = [block]
        parts = [
            BlockAccessor.for_block(p)._aggregate(sort_key, aggs) for p in partitions
        ]
        from ray.data.block import BlockMetadataWithSchema

        meta_with_schema = BlockMetadataWithSchema.from_block(
            block, stats=stats.build()
        )
        return parts + [meta_with_schema]

    @staticmethod
    def reduce(
        key: SortKey,
        aggs: List[AggregateFn],
        batch_format: str,
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> Tuple[Block, "BlockMetadataWithSchema"]:
        normalized_blocks = TableBlockAccessor.normalize_block_types(
            mapper_outputs,
            target_block_type=ExchangeTaskSpec._derive_target_block_type(batch_format),
        )
        blocks, meta_with_schema = BlockAccessor.for_block(
            normalized_blocks[0]
        )._combine_aggregated_blocks(
            list(normalized_blocks), key, aggs, finalize=not partial_reduce
        )
        return blocks, meta_with_schema

    @staticmethod
    def _prune_unused_columns(
        block: Block,
        sort_key: SortKey,
        aggs: Tuple[AggregateFn],
    ) -> Block:
        """Prune unused columns from block before aggregate."""
        prune_columns = True
        columns = set()
        key = sort_key.get_columns()

        if isinstance(key, str):
            columns.add(key)
        elif isinstance(key, list):
            columns.update(key)
        elif callable(key):
            prune_columns = False

        for agg in aggs:
            if isinstance(agg, AggregateFnV2) and agg.get_target_column():
                columns.add(agg.get_target_column())
            elif not isinstance(agg, Count):
                # Don't prune columns if any aggregate key is not string.
                prune_columns = False

        block_accessor = BlockAccessor.for_block(block)
        if (
            prune_columns
            and isinstance(block_accessor, TableBlockAccessor)
            and block_accessor.num_rows() > 0
        ):
            return block_accessor.select(list(columns))
        else:
            return block
