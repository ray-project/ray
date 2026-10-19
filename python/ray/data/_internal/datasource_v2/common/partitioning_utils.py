"""Partition discovery and typed path predicates shared by file formats."""

import copy
from dataclasses import replace
from typing import Any, Optional, Sequence

import pyarrow as pa

from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _cast_value,
)
from ray.data.expressions import Expr


def _parse_partition_values(path: str, partitioning: Partitioning) -> dict[str, Any]:
    """Apply declared conversions only to keys actually present in the path."""
    parser = PathPartitionParser(replace(partitioning, field_types={}))
    values: dict[str, Any] = parser(path)
    for name, data_type in (partitioning.field_types or {}).items():
        if name in values:
            values[name] = _cast_value(values[name], data_type)
    return values


def resolve_partitioning(
    partitioning: Optional[Partitioning], paths: Sequence[str]
) -> Optional[Partitioning]:
    """Discover keys from a nonempty path and validate the sampled layout."""
    if partitioning is None or partitioning.field_names:
        return copy.deepcopy(partitioning)
    partition_kv = next(
        (
            values
            for path in paths
            if (values := _parse_partition_values(path, partitioning))
        ),
        {},
    )
    if not partition_kv:
        return copy.deepcopy(partitioning)
    resolved = replace(partitioning, field_names=list(partition_kv))
    for path in paths:
        _parse_partition_values(path, resolved)
    return resolved


def _partition_table(values: dict[str, Any], schema: pa.Schema | None) -> pa.Table:
    """Match the reader's string-to-logical-type partition conversion."""
    arrays = []
    for name, value in values.items():
        array = pa.array([None if value is None else str(value)], type=pa.string())
        if schema is not None and schema.get_field_index(name) != -1:
            array = array.cast(schema.field(name).type)
        arrays.append(array)
    if not arrays:
        # A constant predicate still needs one row on which to be evaluated.
        return pa.table({"__partition_row__": [None]})
    return pa.Table.from_arrays(arrays, names=list(values))


def _evaluate_path_predicate(
    predicate: Expr, values: dict[str, Any], schema: pa.Schema | None
) -> bool | None:
    """Return unknown when a predicate needs values absent from the path."""
    referenced_columns = set(get_column_references(predicate))
    if not referenced_columns.issubset(values):
        return None
    predicate_values = {
        name: value for name, value in values.items() if name in referenced_columns
    }
    partition_table = _partition_table(predicate_values, schema)
    return partition_table.filter(predicate.to_pyarrow()).num_rows == 1
