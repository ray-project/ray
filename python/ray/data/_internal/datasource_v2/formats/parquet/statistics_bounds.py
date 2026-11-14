"""Statistics bounds for the filter leaves PyArrow cannot prune with."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa

if TYPE_CHECKING:
    from ray.data.expressions import Expr


def _add_statistics_bounds(expr: Expr, schema: pa.Schema) -> Expr:
    """Rewrite the leaves PyArrow cannot prune with into statistics bounds.

    PyArrow cannot evaluate these leaf forms against Parquet statistics, so
    each is converted into comparisons it can prune with today:

    - ``(col + c) >= t`` / ``(col + c) > t``: the equivalent column range.
      A signed column also keeps its original leaf, because the kernel that
      runs the filter can compute at a wider width than the file column,
      where sums past the column's maximum do not wrap.
    - ``str.starts_with(col, p)``: the string range the prefix spans.

    Every other form is returned untouched: PyArrow already prunes plain
    comparisons, null checks and membership by itself.
    """
    from ray.data.expressions import BinaryExpr, Operation, UnaryExpr

    if isinstance(expr, BinaryExpr) and expr.op in (Operation.AND, Operation.OR):
        left = _add_statistics_bounds(expr.left, schema)
        right = _add_statistics_bounds(expr.right, schema)
        if left is expr.left and right is expr.right:
            return expr
        return left & right if expr.op == Operation.AND else left | right

    # NOT recurses: a bound prunes only through comparisons the leaf itself
    # implies, so the rewritten negation never prunes a group ~leaf could
    # keep, while staying evaluable where ~leaf does not.
    if isinstance(expr, UnaryExpr) and expr.op == Operation.NOT:
        operand = _add_statistics_bounds(expr.operand, schema)
        if operand is expr.operand:
            return expr
        return ~operand

    for bound in (_bound_addition_comparison, _bound_starts_with):
        rewritten = bound(expr, schema)
        if rewritten is not None:
            return rewritten
    return expr


def _bound_addition_comparison(expr: Expr, schema: pa.Schema) -> Expr | None:
    """``(col + c) >=/> t`` as the column range the statistics evaluate.

    Returns None for any other shape, and for additions whose bound would
    not hold at the column's width (an out-of-range or bool/float literal).
    """
    from ray.data.expressions import BinaryExpr, ColumnExpr, LiteralExpr, Operation

    if not (
        isinstance(expr, BinaryExpr)
        and expr.op in (Operation.GE, Operation.GT)
        and isinstance(expr.left, BinaryExpr)
        and expr.left.op == Operation.ADD
        and isinstance(expr.right, LiteralExpr)
    ):
        return None

    addition = expr.left
    column: ColumnExpr | None = None
    literal: LiteralExpr | None = None
    if isinstance(addition.left, ColumnExpr) and isinstance(
        addition.right, LiteralExpr
    ):
        column, literal = addition.left, addition.right
    elif isinstance(addition.left, LiteralExpr) and isinstance(
        addition.right, ColumnExpr
    ):
        column, literal = addition.right, addition.left
    if column is None or literal is None:
        return None

    offset = literal.value
    target = expr.right.value
    idx = schema.get_field_index(column.name)
    value_type = schema.field(idx).type if idx >= 0 else None

    # Mixed signed/unsigned kernels can narrow uint64 to int64.
    # Keep uint64 conservative until its cast semantics are modeled.
    if value_type is None or not (
        pa.types.is_signed_integer(value_type)
        or (pa.types.is_unsigned_integer(value_type) and value_type.bit_width < 64)
    ):
        return None
    if not (
        isinstance(offset, (int, np.integer))
        and not isinstance(offset, (bool, np.bool_))
        and isinstance(target, (int, np.integer))
        and not isinstance(target, (bool, np.bool_))
    ):
        return None

    bit_width = value_type.bit_width
    if pa.types.is_signed_integer(value_type):
        min_value = -(1 << (bit_width - 1))
        max_value = (1 << (bit_width - 1)) - 1
    else:
        min_value = 0
        max_value = (1 << bit_width) - 1

    # Python ints avoid overflow in NumPy scalar subtraction.
    offset_val = int(offset)
    # Fold the strict comparison into the threshold: x + c > t is
    # x + c >= t + 1, so GE and GT share one formula.
    threshold = int(target) + (1 if expr.op == Operation.GT else 0)
    if not (
        0 < offset_val <= max_value
        and min_value <= threshold <= max_value
        and threshold - offset_val >= min_value
    ):
        return None

    lower = column >= threshold - offset_val
    if pa.types.is_int64(pa.scalar(offset).type):
        if pa.types.is_signed_integer(value_type):
            # A file column can be narrower than the schema the filter runs
            # at (an int8 file next to int64 files), where sums past
            # max_value do not wrap -- the range alone would drop rows, so
            # the leaf stays and the evaluation decides.
            return (expr & lower) | (lower & (column <= max_value - offset_val))
        # Unsigned kernels promote to int64, so the lower side alone is exact.
        return lower
    # Other numpy scalar types take order-dependent kernel paths; only the
    # necessary lower bound is appended.
    return expr & lower


def _bound_starts_with(expr: Expr, schema: pa.Schema) -> Expr | None:
    """``str.starts_with(col, p)`` as the string range the prefix spans."""
    import pyarrow.compute as pc

    from ray.data.expressions import ColumnExpr, PyArrowComputeUDFExpr

    if not (
        isinstance(expr, PyArrowComputeUDFExpr)
        and expr.pc_func is pc.starts_with
        and len(expr.args) == 1
        and isinstance(expr.args[0], ColumnExpr)
        and len(expr.pc_positional) == 1
        and not expr.pc_kwargs
    ):
        return None

    column = expr.args[0]
    prefix = expr.pc_positional[0]
    idx = schema.get_field_index(column.name)

    if not (
        idx >= 0
        and (
            pa.types.is_string(schema.field(idx).type)
            or pa.types.is_large_string(schema.field(idx).type)
        )
        and isinstance(prefix, str)
        and not any(0xD800 <= ord(ch) <= 0xDFFF for ch in prefix)
    ):
        return None

    if not prefix:
        # Every non-null string starts with the empty prefix.
        return column >= prefix

    lower = column >= prefix
    last = ord(prefix[-1])

    if last == 0x10FFFF:
        # No next codepoint exists, so no exact upper end can be given. Keep
        # the leaf and append the lower side, which is still equivalent to
        # the leaf; the negation just stays unevaluable, like the leaf itself.
        return expr & lower

    # U+D7FF has no successor inside a single UTF-8 unit step (U+D800 is a
    # surrogate), so skip the whole surrogate block.
    next_char = "\ue000" if last == 0xD7FF else chr(last + 1)
    return lower & (column < prefix[:-1] + next_char)
