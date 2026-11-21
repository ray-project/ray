"""Decides at listing time whether every row of a data file passes the filter.

When it does, the file's ``record_count`` is its exact row count after the
filter, so the listing marks the file's chunk ``fully_matched`` and
``count()`` takes the count without decoding the file.

``RowMatchCheck`` is built once per listing from the scan's filter and asked
once per planned ``FileScanTask``. A file qualifies in one of two ways:

- its residual is ``AlwaysTrue``: the partition values alone satisfy the
  filter;
- PyIceberg's ``_StrictMetricsEvaluator`` proves it from the file's column
  bounds and null and NaN counts.

Both fail closed. Neither applies to a file with delete files, or when part
of the filter runs in Ray after the read. Both need the filter to treat null
the same way in Iceberg and PyArrow (``_nulls_match_arrow``): Iceberg plans
``null != 3`` as true, but PyArrow drops the row.

The strict evaluator treats a missing null or NaN count as zero, and PyIceberg
writes no NaN counts, so it is trusted only for a file that has a null count
for every column the filter names and a NaN count for every float column. It
is a private PyIceberg class, so any error from it means "not proven".
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, FrozenSet, Optional

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.expressions.visitors import _StrictMetricsEvaluator
    from pyiceberg.schema import Schema
    from pyiceberg.table import FileScanTask

logger = logging.getLogger(__name__)


class RowMatchCheck:
    """Whether every row of a planned file passes the scan's filter."""

    def __init__(
        self,
        *,
        schema: "Schema",
        row_filter: "BooleanExpression",
        case_sensitive: bool,
        filters_after_read: bool,
    ):
        self._enabled = not filters_after_read and _nulls_match_arrow(row_filter)
        self._strict: Optional["_StrictMetricsEvaluator"] = None
        self._field_ids: FrozenSet[int] = frozenset()
        self._float_field_ids: FrozenSet[int] = frozenset()
        if self._enabled:
            self._build_strict_evaluator(schema, row_filter, case_sensitive)

    def every_row_matches(self, task: "FileScanTask") -> bool:
        """Whether ``task.file.record_count`` is the file's count after the filter."""
        from pyiceberg.expressions import AlwaysTrue

        if not self._enabled or task.delete_files or task.file.record_count < 0:
            return False
        if isinstance(task.residual, AlwaysTrue):
            return True
        if self._strict is None or not self._has_counts(task):
            return False
        try:
            return self._strict.eval(task.file)
        except Exception:
            logger.debug("Strict metrics check failed on %s", task.file.file_path)
            return False

    def _build_strict_evaluator(
        self, schema: "Schema", row_filter: "BooleanExpression", case_sensitive: bool
    ) -> None:
        from pyiceberg.expressions.visitors import (
            _StrictMetricsEvaluator,
            extract_field_ids,
        )
        from pyiceberg.types import DoubleType, FloatType

        try:
            strict = _StrictMetricsEvaluator(schema, row_filter, case_sensitive)
            field_ids = frozenset(extract_field_ids(strict.expr))
            float_field_ids = frozenset(
                field_id
                for field_id in field_ids
                if isinstance(schema.find_type(field_id), (FloatType, DoubleType))
            )
        except Exception:
            logger.debug("No strict metrics check for filter %s", row_filter)
            return
        self._strict = strict
        self._field_ids = field_ids
        self._float_field_ids = float_field_ids

    def _has_counts(self, task: "FileScanTask") -> bool:
        null_counts = task.file.null_value_counts or {}
        nan_counts = task.file.nan_value_counts or {}
        return all(field_id in null_counts for field_id in self._field_ids) and all(
            field_id in nan_counts for field_id in self._float_field_ids
        )


def _nulls_match_arrow(row_filter: "BooleanExpression") -> bool:
    """Whether every operator in ``row_filter`` treats null the same in PyArrow.

    Iceberg plans residuals from partition values with ``null != 3`` true, and
    its strict evaluator gets ``!=`` and ``not in`` wrong on a column with
    nulls, but PyArrow evaluates ``null != 3`` as null and drops the row. So
    only filters built from operators that agree in both qualify.
    """
    from pyiceberg.expressions import (
        AlwaysFalse,
        AlwaysTrue,
        And,
        EqualTo,
        GreaterThan,
        GreaterThanOrEqual,
        In,
        IsNaN,
        IsNull,
        LessThan,
        LessThanOrEqual,
        NotNull,
        Or,
    )
    from pyiceberg.expressions.visitors import rewrite_not

    def agrees(expr: "BooleanExpression") -> bool:
        if isinstance(expr, (And, Or)):
            return agrees(expr.left) and agrees(expr.right)
        return isinstance(
            expr,
            (
                AlwaysTrue,
                AlwaysFalse,
                EqualTo,
                GreaterThan,
                GreaterThanOrEqual,
                In,
                IsNaN,
                IsNull,
                LessThan,
                LessThanOrEqual,
                NotNull,
            ),
        )

    return agrees(rewrite_not(row_filter))
