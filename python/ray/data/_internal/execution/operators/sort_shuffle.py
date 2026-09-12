from typing import Dict, Iterable, List

import pyarrow as pa

from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    PartitionFn,
    ReduceFn,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.block import BlockAccessor
from ray.data.context import DataContext


def make_range_partition_fn(
    boundaries: List,
    sort_key: SortKey,
    data_context: DataContext,
) -> PartitionFn:
    """Return a function that locally sorts and range-partitions an Arrow table."""

    def _partition(block: pa.Table) -> Dict[int, pa.Table]:
        with DataContext.current(data_context):
            partitions = BlockAccessor.for_block(block).sort_and_partition(
                boundaries, sort_key
            )
        return dict(enumerate(partitions))

    return _partition


def make_sort_reduce_fn(
    sort_key: SortKey,
    data_context: DataContext,
) -> ReduceFn:
    """Return a reducer that produces one sorted table for a range partition."""

    def _reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[pa.Table]:
        tables = tables_by_input[0]
        if not tables:
            return
        with DataContext.current(data_context):
            block, _ = BlockAccessor.for_block(tables[0]).merge_sorted_blocks(
                tables, sort_key
            )
        yield block

    return _reduce
