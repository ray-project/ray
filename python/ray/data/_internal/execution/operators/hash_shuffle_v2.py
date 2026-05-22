from typing import Dict, Iterable, List

import pyarrow as pa

from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
from ray.data._internal.execution.operators.shuffle_operators._shuffle_tasks import (
    PartitionFn,
    ReduceFn,
)

# Isolate shuffle map workers into a dedicated worker pool so that
# ReadParquet/Project tasks don't run on the same workers.  Without this,
# shared memory pages from object store accesses (mmap'd during
# combine_chunks) accumulate across task types and inflate worker RSS.
_SHUFFLE_MAP_RUNTIME_ENV = {"env_vars": {"RAY_DATA_SHUFFLE_MAP_WORKER": "1"}}


def _make_hash_partition_fn(key_columns: List[str], num_partitions: int) -> PartitionFn:
    """Return a partition function that hash-partitions by key_columns."""

    def _partition(block: pa.Table) -> Dict[int, pa.Table]:
        return hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )

    return _partition


def _concat_reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
    """Concatenate all shards of a partition into a single block.

    Used under the partition = block contract: called once per partition with
    the full shard list so the output is exactly one block.
    """
    if not tables:
        return
    yield pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _sort_reduce(key_columns: List[str]) -> ReduceFn:
    """Return a reduce function that concatenates then sorts by key_columns.

    Requires blocking mode because sorting needs all shards before emitting.
    """

    def _reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
        if not tables:
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        yield combined.sort_by([(k, "ascending") for k in key_columns])

    return _reduce
