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
    _broadcast_partition_value,
    _cast_value,
)
from ray.data.expressions import Expr


class _PartitionValueParser(PathPartitionParser):
    """Reuse path normalization and convert only fields present in each path."""

    def __init__(self, partitioning: Partitioning):
        super().__init__(replace(partitioning, field_types={}))
        self._field_types = partitioning.field_types or {}
        # Resolve once before callers share the parser with footer/fragment threads.
        _ = self.scheme.normalized_base_dir

    def __call__(self, path: str) -> dict[str, Any]:
        values: dict[str, Any] = super().__call__(path)
        for name, data_type in self._field_types.items():
            if name in values:
                try:
                    values[name] = _cast_value(values[name], data_type)
                except ValueError as exc:
                    raise ValueError(
                        f"Failed to parse partition path {path!r}: {exc}"
                    ) from exc
        return values


def resolve_partitioning(
    partitioning: Optional[Partitioning], paths: Sequence[str]
) -> Optional[Partitioning]:
    """Discover keys and validate layout within the supplied schema sample.

    Default reads supply at most 16 files; full-schema reads supply all eligible
    files. A key absent from both the sample paths and its physical schemas is
    not added to the inferred schema, even if a later fragment supplies it.
    Explicit field names bypass discovery. No additional files are listed here.
    """
    if partitioning is None or partitioning.field_names or not paths:
        return copy.deepcopy(partitioning)
    parser = _PartitionValueParser(partitioning)
    partition_kv = next(
        (values for path in paths if (values := parser(path))),
        {},
    )
    if not partition_kv:
        return copy.deepcopy(partitioning)
    resolved = replace(partitioning, field_names=list(partition_kv))
    parser = _PartitionValueParser(resolved)
    for path in paths:
        parser(path)
    return resolved


def _partition_table(values: dict[str, Any], schema: pa.Schema | None) -> pa.Table:
    """Match the reader's string-to-logical-type partition conversion."""
    arrays = [
        _broadcast_partition_value(name, value, 1, schema)
        for name, value in values.items()
    ]
    if not arrays:
        # A constant predicate still needs one row on which to be evaluated.
        return pa.table({"__partition_row__": [None]})
    return pa.Table.from_arrays(arrays, names=list(values))


def _evaluate_path_predicate(
    predicate: Expr,
    values: dict[str, Any],
    schema: pa.Schema | None,
    *,
    path: str,
) -> bool | None:
    """Return unknown when a predicate needs values absent from the path."""
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        NativeExpressionEvaluator,
    )

    referenced_columns = set(get_column_references(predicate))
    if not referenced_columns.issubset(values):
        return None
    predicate_values = {
        name: value for name, value in values.items() if name in referenced_columns
    }
    try:
        partition_table = _partition_table(predicate_values, schema)
        result = NativeExpressionEvaluator(partition_table).visit(predicate)
        if isinstance(result, (pa.Array, pa.ChunkedArray)):
            result = result[0]
        if isinstance(result, pa.Scalar):
            result = result.as_py()
        return bool(result)
    except pa.ArrowInvalid as exc:
        raise pa.ArrowInvalid(
            f"Failed to evaluate predicate for partition path {path!r}: {exc}"
        ) from exc
