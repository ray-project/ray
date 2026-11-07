from typing import Dict, Iterable, List, Optional

import pyarrow as pa

from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    PartitionFn,
    ReduceFn,
)

_SHUFFLE_MAP_RUNTIME_ENV = {"env_vars": {"POLARS_MAX_THREADS": "1"}}


def _validate_repartition_schema(
    schema: pa.Schema,
    key_columns: List[str],
    expected: Optional[pa.Schema],
) -> pa.Schema:
    if expected is not None:
        if any(
            schema.field(key).type != expected.field(key).type for key in key_columns
        ):
            raise ValueError(
                "Hash repartition requires identical key types across blocks; "
                "cast key columns to a common type before repartitioning."
            )
        if {f.name: f.type for f in schema} != {f.name: f.type for f in expected}:
            raise ValueError(
                "Repartition requires matching column names and types across blocks; "
                "normalize schemas before repartitioning."
            )
    return schema


def _make_hash_partition_fn(
    key_columns: List[str], num_partitions: int, *, validate_schema: bool = False
) -> PartitionFn:
    """Return a partition function that hash-partitions by key_columns."""
    input_schema: Optional[pa.Schema] = None

    def _partition(block: pa.Table) -> Dict[int, pa.Table]:
        nonlocal input_schema
        if validate_schema and block.num_rows:
            input_schema = _validate_repartition_schema(
                block.schema, key_columns, input_schema
            )
        return hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )

    return _partition


def _make_slice_partition_fn(num_partitions: int) -> PartitionFn:
    """Return a partitioner that assigns balanced contiguous slices in order."""

    def _partition(block: pa.Table) -> Dict[int, pa.Table]:
        partitions: Dict[int, pa.Table] = {}
        num_rows = block.num_rows
        if num_rows == 0:
            return partitions

        num_slices = min(num_partitions, num_rows)
        slice_size, remainder = divmod(num_rows, num_slices)
        row_start = 0
        for partition_id in range(num_slices):
            current_slice_size = slice_size + (1 if partition_id < remainder else 0)
            partitions[partition_id] = block.slice(row_start, current_slice_size)
            row_start += current_slice_size
        return partitions

    return _partition


def _concat_reduce(
    partition_id: int, tables_by_input: List[List[pa.Table]]
) -> Iterable[pa.Table]:
    """Concatenate all shards of a (single-input) partition into one block."""
    tables = tables_by_input[0]
    if not tables:
        return
    yield transform_pyarrow.concat(tables, preserve_order=True)


def _sort_reduce(key_columns: List[str]) -> ReduceFn:
    """Return a reduce function that concatenates then sorts by key_columns.

    Requires blocking mode because sorting needs all shards before emitting.
    """

    def _reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[pa.Table]:
        tables = tables_by_input[0]
        if not tables:
            return
        combined = transform_pyarrow.concat(tables, preserve_order=True)
        if combined.num_rows == 0:
            yield combined
        else:
            yield combined.sort_by([(k, "ascending") for k in key_columns])

    return _reduce
