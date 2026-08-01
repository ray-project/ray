from typing import TYPE_CHECKING, Iterable, List, Tuple

import pyarrow as pa

from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    BlockTransformer,
    ReduceFn,
)
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data.aggregate import AggregateFn


def _make_aggregating_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> BlockTransformer:
    """Creates input block transformer performing partial aggregation of
    the block applied prior to block being shuffled (to reduce amount of bytes shuffled)
    Copy of `_create_aggregating_transformer` in `hash_aggregate.py`.
    """
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    sort_key = SortKey(key=list(key_columns), descending=False)

    def _transform(block: Block) -> Block:
        from ray.data._internal.planner.exchange.aggregate_task_spec import (
            SortAggregateTaskSpec,
        )

        # TODO unify block schemas to avoid validating every block.
        block_schema = BlockAccessor.for_block(block).schema()
        for agg_fn in aggregation_fns:
            agg_fn._validate(block_schema)  # pyrefly: ignore[bad-argument-type]

        # Project down to only the key + aggregation-input columns.
        pruned_block = SortAggregateTaskSpec._prune_unused_columns(
            block, sort_key, aggregation_fns  # pyrefly: ignore[bad-argument-type]
        )

        # `_aggregate` assumes the block is sorted by key; skip when global.
        if sort_key.get_columns():
            target_block = BlockAccessor.for_block(pruned_block).sort(sort_key)
        else:
            target_block = pruned_block

        return BlockAccessor.for_block(target_block)._aggregate(
            sort_key, aggregation_fns  # pyrefly: ignore[bad-argument-type]
        )

    return _transform


def _make_aggregating_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple["AggregateFn", ...],
) -> ReduceFn:
    """Reduce-side merge + finalize of the map-side partial aggregates."""
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    sort_key = SortKey(key=list(key_columns), descending=False)

    def _reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[Block]:
        # Aggregation is single-input, so there is exactly one shard list.
        tables = tables_by_input[0]
        if not tables:
            return
        combined_block, _ = BlockAccessor.for_block(
            tables[0]
        )._combine_aggregated_blocks(
            list(tables),
            sort_key=sort_key,
            aggs=aggregation_fns,  # pyrefly: ignore[bad-argument-type]
            finalize=True,
        )
        yield combined_block

    return _reduce
