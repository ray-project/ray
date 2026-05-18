"""Task-based hash shuffle operator (V2).

Hash-shuffle implementation built on top of :class:`BaseShuffleOperator`.

The operator hash-partitions input blocks by ``key_columns`` into
``num_partitions`` output partitions using stateless Ray tasks (in contrast to
the legacy actor-based ``HashShuffleOperator``).  Most constructor arguments
match the legacy operator; ``should_sort`` has been renamed to
``blocking_reduce`` to describe the underlying mechanism (wait for all shards
of a partition before reducing) rather than its first use case (sort).

Partition function
------------------
Uses :func:`~ray.data._internal.arrow_ops.transform_pyarrow.hash_partition`.

Reduce functions
----------------
- ``_concat_reduce``: simple concatenation, compatible with streaming mode.
  Used when ``blocking_reduce=False`` (plain repartition).
- ``_make_sort_reduce``: concatenates then sorts by ``key_columns``.  Used
  when ``blocking_reduce=True``; needs the full partition before emitting.

Worker isolation
----------------
``_SHUFFLE_MAP_RUNTIME_ENV`` pins map workers into a dedicated worker pool so
that memory pages mmap'd by ReadParquet tasks do not accumulate in the shared
worker pool and inflate shuffle-worker RSS.
"""

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
    """Return a partition function that hash-partitions by ``key_columns``."""

    def _partition(block: pa.Table) -> Dict[int, pa.Table]:
        return hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )

    return _partition


# ---------------------------------------------------------------------------
# Reduce functions
# ---------------------------------------------------------------------------


def _concat_reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
    """Simple concatenation reduce — streaming-compatible.

    Produces a valid partial result from any subset of shards, so it works
    correctly when called multiple times per partition in streaming mode.
    """
    if not tables:
        return
    yield pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _make_sort_reduce(key_columns: List[str]) -> ReduceFn:
    """Return a reduce function that concatenates then sorts by ``key_columns``.

    Requires blocking mode (``streaming_reduce=False``) because sorting needs
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

    Provides the same constructor interface as the legacy actor-based shuffle
    so no call-site changes are required.

    Args:
        input_op: Upstream physical operator.
        data_context: Runtime configuration.
        key_columns: Columns used for hash-partitioning.
        num_partitions: Target number of output partitions.  If ``None``,
            falls back to the upstream operator's estimated output count, then
            to ``data_context.default_hash_shuffle_parallelism``.
        blocking_reduce: If ``True``, the reducer waits for *all* shards of a
            partition before calling the reduce function (blocking mode).
            Required for reduce functions that need the full partition — e.g.,
            sort, or aggregations that can't combine partial results.  When
            set, V2 substitutes a sort-by-``key_columns`` reduce function so
            the output is sorted within each partition.  Defaults to ``False``
            (streaming concat).
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        key_columns: Tuple[str, ...],
        num_partitions: Optional[int] = None,
        blocking_reduce: bool = False,
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
        reduce_fn: ReduceFn = (
            _make_sort_reduce(key_list) if blocking_reduce else _concat_reduce
        )

        super().__init__(
            input_op,
            data_context,
            num_partitions=target_num_partitions,
            partition_fn=partition_fn,
            reduce_fn=reduce_fn,
            streaming_reduce=not blocking_reduce,
            map_runtime_env=_SHUFFLE_MAP_RUNTIME_ENV,
            name=(
                f"HashShuffleV2(keys={key_columns}, "
                f"partitions={target_num_partitions})"
            ),
        )
