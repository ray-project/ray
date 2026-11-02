"""Skipping Delta data files with the statistics in the transaction log.

Each ``add`` action can carry the minimum and maximum of every column of its
file (``min.<col>`` / ``max.<col>`` in ``DeltaTable.get_add_actions(flatten=
True)``). A file whose range proves no row can satisfy the read's predicate is
left out of the listing, before its footer is read.

Skipping is conservative: a file is kept unless its statistics prove it holds
no matching row. Every "don't know" -- a missing or null statistic, an
expression this module doesn't understand, a comparison Arrow can't evaluate
-- keeps the file, because the reader applies the predicate to every row
anyway. Keeping a file costs a read; dropping a matching one would lose rows.

Not every statistic is a true bound. Writers truncate long strings, which can
leave ``max`` below the real maximum (a truncated ``min`` is still a lower
bound), and Delta stores timestamps in milliseconds, so either bound can be
off. Floating-point bounds may be NaN when the file holds NaN. Only the bounds
that are exact for their type are used.
"""

import logging
from typing import Any, Callable, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
)

logger = logging.getLogger(__name__)

MIN_STAT_PREFIX = "min."
MAX_STAT_PREFIX = "max."

# ``lit <op> col`` asks the same question of a file as ``col <flipped> lit``.
_FLIPPED_OPERATION = {
    Operation.GT: Operation.LT,
    Operation.GE: Operation.LE,
    Operation.LT: Operation.GT,
    Operation.LE: Operation.GE,
    Operation.EQ: Operation.EQ,
}

# A keep-mask over the add actions; ``None`` keeps every file.
Mask = Optional[pa.ChunkedArray]


def statistics_keep_mask(add_actions: pa.Table, predicate: Expr) -> Mask:
    """Which files may hold a row matching ``predicate``.

    Args:
        add_actions: ``DeltaTable.get_add_actions(flatten=True)``, as a
            PyArrow table.
        predicate: The read's predicate over data columns.

    Returns:
        A boolean mask, ``True`` for each file to keep, or ``None`` if the
        statistics rule out no file.
    """
    if add_actions.num_rows == 0:
        return None
    return _keep_mask(add_actions, predicate)


def _keep_mask(add_actions: pa.Table, predicate: Expr) -> Mask:
    if not isinstance(predicate, BinaryExpr):
        # NOT, IS NULL, UDFs and the like: a range can't rule them out.
        return None
    if predicate.op == Operation.AND:
        # Either side ruling a file out rules out the conjunction.
        return _and(
            _keep_mask(add_actions, predicate.left),
            _keep_mask(add_actions, predicate.right),
        )
    if predicate.op == Operation.OR:
        left = _keep_mask(add_actions, predicate.left)
        right = _keep_mask(add_actions, predicate.right)
        if left is None or right is None:
            # A side that rules nothing out could match in any file.
            return None
        return pc.or_(left, right)
    if predicate.op == Operation.IN:
        return _in_keep_mask(add_actions, predicate)
    return _comparison_keep_mask(add_actions, predicate)


def _and(left: Mask, right: Mask) -> Mask:
    if left is None:
        return right
    if right is None:
        return left
    return pc.and_(left, right)


def _comparison_keep_mask(add_actions: pa.Table, predicate: BinaryExpr) -> Mask:
    """Keep-mask of ``column <op> literal``."""
    comparison = _as_column_comparison(predicate)
    if comparison is None:
        return None
    op, column, value = comparison
    minimum = _bound(add_actions, MIN_STAT_PREFIX, column)
    maximum = _bound(add_actions, MAX_STAT_PREFIX, column)

    if op == Operation.GT:
        return _keep_where(pc.greater, maximum, value)
    if op == Operation.GE:
        return _keep_where(pc.greater_equal, maximum, value)
    if op == Operation.LT:
        return _keep_where(pc.less, minimum, value)
    if op == Operation.LE:
        return _keep_where(pc.less_equal, minimum, value)
    if op == Operation.EQ:
        return _value_in_range(minimum, maximum, value)
    # NE: a range can't rule out a single value.
    return None


def _in_keep_mask(add_actions: pa.Table, predicate: BinaryExpr) -> Mask:
    """Keep-mask of ``column IN (values)``: some value in the file's range."""
    if not (
        isinstance(predicate.left, ColumnExpr)
        and isinstance(predicate.right, LiteralExpr)
        and isinstance(predicate.right.value, (list, tuple))
    ):
        return None
    values = predicate.right.value
    if not values:
        return None
    column = predicate.left.name
    minimum = _bound(add_actions, MIN_STAT_PREFIX, column)
    maximum = _bound(add_actions, MAX_STAT_PREFIX, column)
    keep: Mask = None
    for value in values:
        in_range = _value_in_range(minimum, maximum, value)
        if in_range is None:
            return None
        keep = in_range if keep is None else pc.or_(keep, in_range)
    return keep


def _value_in_range(minimum: Mask, maximum: Mask, value: Any) -> Mask:
    return _and(
        _keep_where(pc.less_equal, minimum, value),
        _keep_where(pc.greater_equal, maximum, value),
    )


def _as_column_comparison(
    predicate: BinaryExpr,
) -> Optional[Tuple[Operation, str, Any]]:
    """``predicate`` as ``(op, column, value)`` with the column on the left, or
    ``None`` unless it compares a column with a non-null literal."""
    left, right = predicate.left, predicate.right
    op: Optional[Operation] = predicate.op
    if isinstance(left, LiteralExpr) and isinstance(right, ColumnExpr):
        left, right, op = right, left, _FLIPPED_OPERATION.get(predicate.op)
    if (
        op is None
        or not isinstance(left, ColumnExpr)
        or not isinstance(right, LiteralExpr)
        or right.value is None
    ):
        return None
    return op, left.name, right.value


def _bound(add_actions: pa.Table, prefix: str, column: str) -> Mask:
    """The ``min``/``max`` statistic of ``column``, when it is a true bound.

    ``None`` when the log has no such statistic or its values can't be
    trusted for that column's type. NaN bounds become null: unknown.
    """
    index = add_actions.schema.get_field_index(f"{prefix}{column}")
    if index < 0:
        return None
    values = add_actions.column(index)
    value_type = values.type
    if (
        pa.types.is_integer(value_type)
        or pa.types.is_decimal(value_type)
        or pa.types.is_date(value_type)
        or pa.types.is_boolean(value_type)
    ):
        return values
    if pa.types.is_floating(value_type):
        return pc.if_else(pc.is_nan(values), pa.scalar(None, value_type), values)
    if prefix == MIN_STAT_PREFIX and (
        pa.types.is_string(value_type) or pa.types.is_large_string(value_type)
    ):
        # A truncated string is a prefix of the real minimum, so still a
        # lower bound. A truncated maximum isn't an upper bound.
        return values
    return None


def _keep_where(compare: Callable[..., Any], bound: Mask, value: Any) -> Mask:
    """Keep the files where ``compare(bound, value)`` holds or is unknown."""
    if bound is None:
        return None
    try:
        result = compare(bound, value)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as e:
        # E.g. a literal of a type the statistic doesn't compare with.
        logger.debug("Delta statistics can't evaluate %r: %s", value, e)
        return None
    # A null bound -- no statistic for this file, or an all-null column --
    # rules nothing out.
    return pc.fill_null(result, True)
