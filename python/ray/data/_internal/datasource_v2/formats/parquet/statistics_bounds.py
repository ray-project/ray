"""Statistics bounds for the filter leaves PyArrow cannot prune with."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    LiteralExpr,
    Operation,
    PyArrowComputeUDFExpr,
    UnaryExpr,
)

if TYPE_CHECKING:
    from ray.data.expressions import Expr

# U+D800..U+DFFF are surrogates, which valid strings never contain, so
# lexicographic order jumps from U+D7FF straight to U+E000.
_SURROGATE_RANGE = range(0xD800, 0xDFFF + 1)


def _add_statistics_bounds(expr: Expr, schema: pa.Schema) -> Expr:
    """Rewrite the leaves PyArrow cannot prune with into statistics bounds.

    PyArrow cannot evaluate these leaf forms against Parquet statistics, so
    each is converted into comparisons it can prune with today:

    - ``(col + c) >= t`` / ``(col + c) > t``: the equivalent column range.
      A signed column also keeps its original leaf, and so does an unsigned
      column unless its literal is an int64, because the kernel that runs
      the filter can compute at a wider width than the file column, where
      sums past the column's maximum do not wrap.
    - ``str.starts_with(col, p)``: the string range the prefix spans.

    Every other form is returned untouched: PyArrow already prunes plain
    comparisons, null checks and membership by itself.
    """
    if isinstance(expr, BinaryExpr) and expr.op in (Operation.AND, Operation.OR):
        left = _add_statistics_bounds(expr=expr.left, schema=schema)
        right = _add_statistics_bounds(expr=expr.right, schema=schema)
        if left is expr.left and right is expr.right:
            return expr
        return left & right if expr.op == Operation.AND else left | right

    # NOT recurses: each rewrite matches exactly the rows its leaf matches,
    # so ~rewritten is ~leaf -- the negation never prunes a group the leaf's
    # negation keeps, while staying evaluable where ~leaf does not.
    if isinstance(expr, UnaryExpr) and expr.op == Operation.NOT:
        operand = _add_statistics_bounds(expr=expr.operand, schema=schema)
        if operand is expr.operand:
            return expr
        return ~operand

    for bound in (_bound_addition_comparison, _bound_starts_with):
        rewritten = bound(expr=expr, schema=schema)
        if rewritten is not None:
            return rewritten
    return expr


def _bound_addition_comparison(expr: Expr, schema: pa.Schema) -> Expr | None:
    """``(col + c) >=/> t`` as the column range the statistics evaluate.

    Returns None for any other shape, and for additions whose bound would
    not hold at the column's width (an out-of-range or bool/float literal).
    """
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

    max_value = np.iinfo(value_type.to_pandas_dtype()).max

    # Python ints avoid overflow in NumPy scalar subtraction.
    offset_val = int(offset)
    # Fold the strict comparison into the threshold: x + c > t is
    # x + c >= t + 1, so GE and GT share one formula.
    threshold = int(target) + (1 if expr.op == Operation.GT else 0)
    if not 0 < offset_val <= max_value:
        return None

    lower = column >= threshold - offset_val
    if pa.types.is_unsigned_integer(value_type) and pa.types.is_int64(
        pa.scalar(offset).type
    ):
        # Unsigned kernels promote to int64, so the lower side alone is exact.
        return lower
    # Rows where x + c overflows the file's type are left to the leaf.
    return lower & (expr | (column <= max_value - offset_val))


def _bound_starts_with(expr: Expr, schema: pa.Schema) -> Expr | None:
    """``str.starts_with(col, p)`` as the string range the prefix spans."""
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
        and not any(ord(ch) in _SURROGATE_RANGE for ch in prefix)
    ):
        return None

    if not prefix:
        # Every non-null string starts with the empty prefix.
        return column >= prefix

    lower = column >= prefix
    last = ord(prefix[-1])

    if last == sys.maxunicode:
        # No next codepoint exists, so no exact upper end can be given. Keep
        # the leaf and append the lower side, which is still equivalent to
        # the leaf; the negation just stays unevaluable, like the leaf itself.
        return expr & lower

    next_char = (
        chr(_SURROGATE_RANGE.stop)
        if last == _SURROGATE_RANGE.start - 1
        else chr(last + 1)
    )
    return lower & (column < prefix[:-1] + next_char)


def _has_modelable_leaf(expr: Expr | None) -> bool:
    """Whether the predicate holds a leaf shape the rewrite models.

    A cheap walk over the filter, resolved once per reader: predicates
    PyArrow already prunes by itself skip the rewrite, and with it the
    per-file schema conversion the rewrite reads its column types from.
    """
    if expr is None:
        return False
    if isinstance(expr, BinaryExpr):
        if expr.op == Operation.ADD:
            # The addition sits inside a comparison the rewrite may model;
            # whether that comparison has a safe bound is its call.
            return True
        return _has_modelable_leaf(expr=expr.left) or _has_modelable_leaf(
            expr=expr.right
        )
    if isinstance(expr, UnaryExpr):
        return _has_modelable_leaf(expr=expr.operand)
    return isinstance(expr, PyArrowComputeUDFExpr) and expr.pc_func is pc.starts_with
