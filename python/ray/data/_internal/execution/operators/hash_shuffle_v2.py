from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.base_shuffle_operator import (
    BaseShuffleOperator,
    PartitionFn,
    ReduceFn,
)
from ray.data.context import DataContext

# Isolate shuffle map workers into a dedicated worker pool so that
# ReadParquet/Project tasks don't run on the same workers.  Without this,
# shared memory pages from object store accesses (mmap'd during
# combine_chunks) accumulate across task types and inflate worker RSS.
_SHUFFLE_MAP_RUNTIME_ENV = {"env_vars": {"RAY_DATA_SHUFFLE_MAP_WORKER": "1"}}


# ---------------------------------------------------------------------------
# Hash-specific partition function
# ---------------------------------------------------------------------------


def _make_hash_partition_fn(key_columns: List[str], num_partitions: int) -> PartitionFn:
    """Return a partition function that hash-partitions by key_columns."""

    def _partition(block: pa.Table) -> Dict[int, pa.Table]:
        return hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )

    return _partition


# ---------------------------------------------------------------------------
# Reduce functions
# ---------------------------------------------------------------------------


def _concat_reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
    """Concatenate all shards of a partition into a single block.

    Used under the partition = block contract: called once per partition
    with the full shard list so the output is exactly one block.
    """
    if not tables:
        return
    yield pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _sort_reduce(key_columns: List[str]) -> ReduceFn:
    """Return a reduce function that concatenates then sorts by key_columns.

    Requires blocking mode (streaming_reduce=False) because sorting needs
    all shards before it can emit a correctly ordered output.
    """

    def _reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
        if not tables:
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        yield combined.sort_by([(k, "ascending") for k in key_columns])

    return _reduce


# ---------------------------------------------------------------------------
# Operator
# ---------------------------------------------------------------------------


class HashShuffleOperatorV2(BaseShuffleOperator):
    """Actorless hash shuffle built on :class:`BaseShuffleOperator`.

    Args:
        input_op: Upstream physical operator.
        data_context: Runtime configuration.
        key_columns: Columns used for hash-partitioning.
        num_partitions: Target number of output partitions.  If None,
            falls back to the upstream operator's estimated output count, then
            to data_context.default_hash_shuffle_parallelism.
        sort: If True, V2 substitutes a sort by key_columns reduce
            function so each output partition is sorted.

    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        key_columns: Tuple[str, ...],
        num_partitions: Optional[int] = None,
        sort: bool = False,
    ):
        input_logical_op = input_op._logical_operators[0]
        estimated_input_blocks = input_logical_op.estimated_num_outputs()

        target_num_partitions: int = (
            num_partitions
            or estimated_input_blocks
            or data_context.default_hash_shuffle_parallelism
        )

        key_list = list(key_columns)
        partition_fn = _make_hash_partition_fn(key_list, target_num_partitions)
        reduce_fn: ReduceFn = _sort_reduce(key_list) if sort else _concat_reduce

        super().__init__(
            input_op,
            data_context,
            num_partitions=target_num_partitions,
            partition_fn=partition_fn,
            reduce_fn=reduce_fn,
            # Partition = block contract: each partition must be reduced to
            # a single output block.  That requires waiting for all shards
            # (blocking reduce) and forbidding the output buffer from
            # splitting on target_max_block_size.  Users must size
            # num_partitions so each partition fits in node memory.
            streaming_reduce=False,
            disallow_block_splitting=True,
            map_runtime_env=_SHUFFLE_MAP_RUNTIME_ENV,
            name=(
                f"HashShuffleV2(keys={key_columns}, "
                f"partitions={target_num_partitions})"
            ),
        )
