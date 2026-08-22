"""File pruning driven by the Delta Lake transaction log.

``DeltaTable.get_add_actions(flatten=True)`` describes every data file in a
table snapshot::

    path | size_bytes | modification_time | num_records
    partition.<col> ...   min.<col> | max.<col> | null_count.<col> ...

That is enough to answer, without opening a single data file, "could this
file hold a row matching the query?". Two independent mechanisms do so:

* **Partition pruning** is *exact*. ``partition.<col>`` holds the single
  value every row in the file has for that column, so the predicate is
  evaluated directly -- with Ray's own expression evaluator, giving
  semantics identical to the ``Filter`` operator it replaces.

* **Statistics-based skipping** is *conservative*. ``min``/``max`` bound the
  values in a file, so a file can only be dropped when the interval proves
  no row can match. Anything not provable keeps the file.

The asymmetry matters: keeping a file that holds no matching row costs a
wasted read, while dropping a file that holds one silently loses data. Every
"don't know" in this module therefore resolves to *keep*.
"""

import logging
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc

from ray.data.expressions import BinaryExpr, ColumnExpr, Expr, LiteralExpr, Operation

logger = logging.getLogger(__name__)

MIN_STAT_PREFIX = "min."
MAX_STAT_PREFIX = "max."
PARTITION_VALUE_PREFIX = "partition."

# Comparison rewritten so the column sits on the left: ``lit(5) < col("x")``
# is the same file-level question as ``col("x") > lit(5)``.
_FLIPPED_OPERATION = {
    Operation.GT: Operation.LT,
    Operation.LT: Operation.GT,
    Operation.GE: Operation.LE,
    Operation.LE: Operation.GE,
    Operation.EQ: Operation.EQ,
}


def prune_add_actions(
    add_actions: "pa.Table",
    *,
    partition_predicate: Optional[Expr] = None,
    data_predicate: Optional[Expr] = None,
    table_schema: Optional["pa.Schema"] = None,
) -> "pa.Table":
    """Drop add actions whose files cannot contain a matching row.

    Args:
        add_actions: Flattened add actions for one table snapshot.
        partition_predicate: Predicate over partition columns only. Evaluated
            exactly against ``partition.<col>``.
        data_predicate: Predicate over data columns. Evaluated conservatively
            against ``min.<col>`` / ``max.<col>``.
        table_schema: Arrow schema of the Delta table, used to cast partition
            values (always strings in the log) to their real types before
            comparison. Without it, partition pruning is skipped for any
            non-string column rather than risk a lexicographic comparison.

    Returns:
        A subset of ``add_actions``, in the original order. Returns the input
        unchanged when nothing can be proven.
    """
    if add_actions.num_rows == 0:
        return add_actions

    keep = None
    if partition_predicate is not None:
        keep = _intersect(
            keep, _partition_keep_mask(add_actions, partition_predicate, table_schema)
        )
    if data_predicate is not None:
        keep = _intersect(keep, _statistics_keep_mask(add_actions, data_predicate))

    if keep is None:
        return add_actions
    return add_actions.filter(keep)


def _intersect(left, right):
    """AND two optional masks, where ``None`` means "keeps everything"."""
    if left is None:
        return right
    if right is None:
        return left
    return pc.and_(left, right)


# ---------------------------------------------------------------------------
# Partition pruning (exact)
# ---------------------------------------------------------------------------


def _partition_keep_mask(
    add_actions: "pa.Table",
    predicate: Expr,
    table_schema: Optional["pa.Schema"],
):
    """Evaluate ``predicate`` against the per-file partition values.

    Returns ``None`` when the predicate can't be evaluated -- an unknown
    column, a value that won't cast to its declared type, or an expression
    the evaluator rejects.
    """
    # Imported lazily: the expression evaluator pulls in the logical rules
    # package, which imports the Delta pruning rule, which reaches back here.
    # Deferring to call time breaks that cycle at no meaningful cost --
    # pruning runs once per listing, not per row.
    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_expr,
    )
    from ray.data._internal.planner.plan_expression.expression_visitors import (
        _ColumnReferenceCollector,
    )

    columns = {}
    for name in add_actions.schema.names:
        if not name.startswith(PARTITION_VALUE_PREFIX):
            continue
        field_name = name[len(PARTITION_VALUE_PREFIX) :]
        values = add_actions.column(name)
        if table_schema is not None:
            index = table_schema.get_field_index(field_name)
            if index >= 0:
                values = _cast_partition_values(
                    values, table_schema.field(index).type, field_name
                )
                if values is None:
                    return None
        columns[field_name] = values

    if not columns:
        return None

    collector = _ColumnReferenceCollector()
    collector.visit(predicate)
    referenced = set(collector.get_column_refs() or [])
    if not referenced.issubset(columns):
        # The predicate touches something that isn't a partition column, so
        # the partition values alone can't decide it.
        return None

    try:
        result = eval_expr(predicate, pa.table(columns))
    except Exception as e:  # noqa: BLE001 - pruning must never fail a read
        logger.debug("Delta partition pruning skipped: %s", e)
        return None

    if not isinstance(result, (pa.Array, pa.ChunkedArray)) or not pa.types.is_boolean(
        result.type
    ):
        return None
    # A null comparison result decides nothing -- keep the file.
    return pc.fill_null(result, True)


def _cast_partition_values(values, target_type: "pa.DataType", field_name: str):
    """Cast log-encoded partition strings to their declared type.

    Delta stores every partition value as a string. Comparing ``"9"`` against
    ``"100"`` lexicographically gives the wrong answer for an integer column,
    so a failed cast returns ``None`` (prune nothing) rather than a wrong one.
    """
    if values.type == target_type:
        return values
    try:
        return pc.cast(values, target_type)
    except Exception as e:  # noqa: BLE001
        logger.debug(
            "Delta partition pruning skipped: cannot cast partition column "
            "%r to %s: %s",
            field_name,
            target_type,
            e,
        )
        return None


# ---------------------------------------------------------------------------
# Statistics-based skipping (conservative)
# ---------------------------------------------------------------------------


def _statistics_keep_mask(add_actions: "pa.Table", predicate: Expr):
    """Build a keep-mask from per-file ``min``/``max`` statistics.

    Returns ``None`` when the predicate proves nothing about any file.
    """
    if isinstance(predicate, BinaryExpr):
        if predicate.op == Operation.AND:
            # Either side proving "no match" is enough to drop the file.
            return _intersect(
                _statistics_keep_mask(add_actions, predicate.left),
                _statistics_keep_mask(add_actions, predicate.right),
            )
        if predicate.op == Operation.OR:
            left = _statistics_keep_mask(add_actions, predicate.left)
            right = _statistics_keep_mask(add_actions, predicate.right)
            if left is None or right is None:
                # An unprovable branch could match anywhere, so the whole
                # disjunction is unprovable.
                return None
            return pc.or_(left, right)
        return _comparison_keep_mask(add_actions, predicate)

    # NOT, IS_NULL, UDFs, literals: nothing an interval can rule out.
    return None


def _comparison_keep_mask(add_actions: "pa.Table", predicate: BinaryExpr):
    """Bound a single ``column <op> literal`` comparison by its interval."""
    op, column, literal = _normalize_comparison(predicate)
    if op is None:
        return None

    minimum = _stat_column(add_actions, MIN_STAT_PREFIX, column.name)
    maximum = _stat_column(add_actions, MAX_STAT_PREFIX, column.name)

    # A file survives unless its interval makes the comparison impossible.
    # A null bound reads as unbounded in that direction, so ``fill_null(True)``
    # is what makes partial statistics safe.
    try:
        if op == Operation.GT:
            return _keep(pc.greater, maximum, literal.value)
        if op == Operation.GE:
            return _keep(pc.greater_equal, maximum, literal.value)
        if op == Operation.LT:
            return _keep(pc.less, minimum, literal.value)
        if op == Operation.LE:
            return _keep(pc.less_equal, minimum, literal.value)
        if op == Operation.EQ:
            return _intersect(
                _keep(pc.less_equal, minimum, literal.value),
                _keep(pc.greater_equal, maximum, literal.value),
            )
    except Exception as e:  # noqa: BLE001 - e.g. incomparable types
        logger.debug("Delta statistics pruning skipped for %r: %s", column.name, e)
        return None

    # NE and everything else: an interval can't rule out a single value.
    return None


def _keep(compare, stat_column, value):
    """``compare(stat, value)`` with a missing statistic meaning "keep"."""
    if stat_column is None:
        return None
    return pc.fill_null(compare(stat_column, value), True)


def _normalize_comparison(predicate: BinaryExpr):
    """Rewrite a comparison to ``(op, ColumnExpr, LiteralExpr)``.

    Returns ``(None, None, None)`` when the comparison isn't between a plain
    column and a literal -- column-to-column comparisons, arithmetic, and
    anything else prove nothing about a file.
    """
    left, right = predicate.left, predicate.right
    if isinstance(left, ColumnExpr) and isinstance(right, LiteralExpr):
        return predicate.op, left, right
    if isinstance(left, LiteralExpr) and isinstance(right, ColumnExpr):
        flipped = _FLIPPED_OPERATION.get(predicate.op)
        if flipped is None:
            return None, None, None
        return flipped, right, left
    return None, None, None


def _stat_column(add_actions: "pa.Table", prefix: str, column_name: str):
    """Return the ``min.``/``max.`` column for ``column_name``, if present."""
    index = add_actions.schema.get_field_index(f"{prefix}{column_name}")
    if index < 0:
        return None
    return add_actions.column(index)
