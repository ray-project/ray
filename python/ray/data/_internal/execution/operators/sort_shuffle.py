from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pac

from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    PartitionFn,
    ReduceFn,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.block import BlockAccessor
from ray.data.context import DataContext

_SAMPLE_ROWS_COLUMN = "__sort_sample_rows__"
_SAMPLE_BYTES_COLUMN = "__sort_sample_bytes__"


def user_boundaries(sort_key: SortKey) -> Optional[List[Tuple]]:
    """The user's ``sort(boundaries=...)`` as the partitioner's key tuples, in
    the sort's direction; None when the user gave none."""
    if not sort_key.boundaries:
        return None
    boundaries = [(boundary,) for boundary in sort_key.boundaries]
    if sort_key.get_descending()[0]:
        boundaries.reverse()
    return boundaries


def plan_range_partitions(
    samples: List[pa.Table],
    rows_per_sample: List[float],
    bytes_per_sample: List[float],
    sort_key: SortKey,
    num_partitions: int,
    boundaries: Optional[List[Tuple]] = None,
) -> Tuple[List[Tuple], np.ndarray, np.ndarray]:
    """Range boundaries and the estimated rows and bytes of each partition.

    Each sample table holds sort-key rows drawn evenly from one sorted run;
    ``rows_per_sample[i]`` and ``bytes_per_sample[i]`` are the rows and
    decoded bytes of that run each of its sampled keys stands for. Unless
    ``boundaries`` is given, the boundaries are the weighted quantiles of the
    pooled samples, so partitions are balanced by rows rather than by sample
    count. The estimates are the weights of the pooled samples that fall in
    each partition.

    Returns ``(boundaries, estimated_rows, estimated_bytes)`` with
    ``num_partitions - 1`` boundaries and ``num_partitions`` estimates.
    """
    weighted = []
    for sample, rows, nbytes in zip(samples, rows_per_sample, bytes_per_sample):
        if sample.num_rows == 0:
            continue
        weighted.append(
            sample.append_column(
                _SAMPLE_ROWS_COLUMN, pa.array(np.full(sample.num_rows, float(rows)))
            ).append_column(
                _SAMPLE_BYTES_COLUMN, pa.array(np.full(sample.num_rows, float(nbytes)))
            )
        )
    estimated_rows = np.zeros(num_partitions, dtype=np.int64)
    estimated_bytes = np.zeros(num_partitions, dtype=np.int64)
    if not weighted:
        if boundaries is None:
            empty = tuple(None for _ in sort_key.get_columns())
            boundaries = [empty] * (num_partitions - 1)
        return boundaries, estimated_rows, estimated_bytes

    pooled = (
        transform_pyarrow.concat(weighted, promote_types=True)
        if len(weighted) > 1
        else weighted[0]
    )
    pooled = BlockAccessor.for_block(pooled).sort(sort_key)
    if boundaries is None:
        row_weights = pooled.column(_SAMPLE_ROWS_COLUMN).to_numpy()
        cumulative_rows = np.cumsum(row_weights)
        # Boundary k is the first sample at or past k/P of the total rows. A
        # row equal to a boundary falls above it, or below it when the
        # primary key is descending (see ``find_insertion_index``), so count
        # the boundary sample on that side.
        if sort_key.get_descending()[0]:
            rows_through = cumulative_rows
        else:
            rows_through = cumulative_rows - row_weights
        targets = cumulative_rows[-1] * np.arange(1, num_partitions) / num_partitions
        indices = np.minimum(
            np.searchsorted(rows_through, targets, side="left"), pooled.num_rows - 1
        )
        keys = BlockAccessor.for_block(pooled).to_numpy(columns=sort_key.get_columns())
        boundaries = [
            tuple(column[i] for column in keys.values()) for i in indices.tolist()
        ]
    if boundaries:
        pieces = BlockAccessor.for_block(pooled)._find_partitions_sorted(
            boundaries, sort_key
        )
    else:
        pieces = [pooled]
    for partition_id, piece in enumerate(pieces):
        if piece.num_rows:
            estimated_rows[partition_id] = int(
                pac.sum(piece.column(_SAMPLE_ROWS_COLUMN)).as_py()
            )
            estimated_bytes[partition_id] = int(
                pac.sum(piece.column(_SAMPLE_BYTES_COLUMN)).as_py()
            )
    return boundaries, estimated_rows, estimated_bytes


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
        if not any(table.num_rows for table in tables):
            yield tables[0]
            return
        with DataContext.current(data_context):
            block, _ = BlockAccessor.for_block(tables[0]).merge_sorted_blocks(
                tables, sort_key
            )
        yield block

    return _reduce
