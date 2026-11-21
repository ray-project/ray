"""Which parts of a Ray filter an Iceberg read can take, and in what form.

A pushed predicate is split at its top-level ``&`` and each part goes to one of
three places (``split_predicate``):

- ``scan``: PyIceberg plans files with it and ``ArrowScan`` filters rows with it.
- ``after_read``: PyIceberg plans files with it, and the reader applies it with
  Ray's own evaluator after ``ArrowScan``. These are the float parts that
  ``is_statistics_safe`` rejects, like ``~(col("f") < 1.0)``: Iceberg's file
  bounds and PyArrow's row-group statistics both leave NaN out, so either can
  drop a NaN row Ray keeps. For planning, ``to_iceberg_filter`` adds
  ``| is_nan(f)``, so Iceberg's NaN counts still prune files exactly (a count
  of 0 skips the file; 1 or a missing count reads it). PyArrow has no such
  count, so ``ArrowScan`` never sees these parts.
- ``residual``: parts Iceberg can't express (``col("x") * 2 > 10``) or bind
  (``col("id") >= 1.5`` on a ``long``). A ``Filter`` above the read keeps them.

Null rows: Ray's ``is_in`` never returns null, so ``~col("x").is_in([1])``
keeps rows where ``x`` is null. PyIceberg plans a one-value ``NotIn`` as
``NotEqualTo``, which drops them, so a negated ``is_in`` on a column is sent as
``not_in(...) | is_null(...)``. Any other part that negates an ``is_in`` stays
residual.

Splitting is deterministic, so the scanner (which builds the reader) and the
indexer (which plans files) each split the same accepted predicate and agree.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterator, List, Optional, Set, Tuple

from ray.data._internal.datasource_v2.common.pushdown_utils import (
    flatten_predicate_chain,
    is_statistics_safe,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_utils import (
    _IcebergExpressionVisitor,
)
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    UnaryExpr,
)

if TYPE_CHECKING:
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.schema import Schema


@dataclass(frozen=True)
class IcebergFilterParts:
    """A Ray predicate's ``&`` parts, grouped by where each one runs."""

    scan: List[Expr]
    after_read: List[Expr]
    residual: List[Expr]


def split_predicate(
    predicate: Expr, *, schema: "Schema", case_sensitive: bool
) -> IcebergFilterParts:
    """Split ``predicate`` at its top-level ``&``; see the module docstring.

    Args:
        predicate: The Ray predicate offered to the scan.
        schema: The table schema the filter binds to.
        case_sensitive: Whether column names match case-sensitively.

    Returns:
        The parts, grouped by where each one runs.
    """
    float_columns = _float_column_names(schema)
    parts = IcebergFilterParts(scan=[], after_read=[], residual=[])
    for part in flatten_predicate_chain(predicate, Operation.AND):
        if _translate(part, schema=schema, case_sensitive=case_sensitive) is None:
            parts.residual.append(part)
        elif is_statistics_safe(predicate=part, float_columns=float_columns):
            parts.scan.append(part)
        else:
            parts.after_read.append(part)
    return parts


def to_iceberg_filter(
    row_filter: "BooleanExpression",
    parts: List[Expr],
    *,
    schema: "Schema",
    case_sensitive: bool,
    nan_safe: bool = False,
) -> "BooleanExpression":
    """AND ``parts`` into ``row_filter``, in Iceberg form.

    Args:
        row_filter: ``read_iceberg``'s own filter.
        parts: Parts ``split_predicate`` put in ``scan`` or ``after_read``.
        schema: The table schema the filter binds to.
        case_sensitive: Whether column names match case-sensitively.
        nan_safe: Add ``| is_nan(f)`` for each float column a part names, so
            planning keeps the NaN rows Ray keeps.

    Returns:
        The combined Iceberg filter.
    """
    from pyiceberg.expressions import And, IsNaN, Or, Reference

    float_columns = _float_column_names(schema)
    combined = row_filter
    for part in parts:
        translated = _translate(part, schema=schema, case_sensitive=case_sensitive)
        assert translated is not None, f"{part} has no Iceberg form"
        if nan_safe:
            for name in sorted(float_columns & set(get_column_references(part))):
                translated = Or(translated, IsNaN(term=Reference(name)))
        combined = And(combined, translated)
    return combined


def _translate(
    part: Expr, *, schema: "Schema", case_sensitive: bool
) -> Optional["BooleanExpression"]:
    """``part`` in Iceberg form, or ``None`` if it has none or doesn't bind."""
    from pyiceberg.expressions import BooleanExpression, IsNull, NotIn, Or, Reference
    from pyiceberg.expressions.visitors import bind

    negated_in = _negated_column_in(part)
    if negated_in is not None:
        name, values = negated_in
        translated = Or(
            NotIn(term=Reference(name), values=set(values)),
            IsNull(term=Reference(name)),
        )
    elif _negates_in(part):
        return None
    else:
        try:
            translated = _IcebergExpressionVisitor().visit(part)
        except (ValueError, TypeError):
            return None
        if not isinstance(translated, BooleanExpression):
            # A bare column like ``col("is_valid")`` is a term, not a filter.
            return None
    try:
        # A filter ``plan_files()`` would reject stays with Ray.
        bind(schema, translated, case_sensitive=case_sensitive)
    except (ValueError, TypeError):
        return None
    return translated


def _negated_column_in(part: Expr) -> Optional[Tuple[str, List[Any]]]:
    """``(name, values)`` if ``part`` is ``col.not_in([...])`` or ``~col.is_in([...])``."""
    if isinstance(part, UnaryExpr) and part.op == Operation.NOT:
        inner = part.operand
        if not (isinstance(inner, BinaryExpr) and inner.op == Operation.IN):
            return None
    elif isinstance(part, BinaryExpr) and part.op == Operation.NOT_IN:
        inner = part
    else:
        return None
    if not (
        isinstance(inner.left, ColumnExpr)
        and isinstance(inner.right, LiteralExpr)
        and isinstance(inner.right.value, list)
    ):
        return None
    return inner.left.name, inner.right.value


def _negates_in(expr: Expr) -> bool:
    """Whether an ``is_in`` sits under a ``~``, or a ``not_in`` appears at all."""
    for node, under_not in _walk(expr, under_not=False):
        if isinstance(node, BinaryExpr) and (
            node.op == Operation.NOT_IN or (node.op == Operation.IN and under_not)
        ):
            return True
    return False


def _walk(expr: Expr, under_not: bool) -> Iterator[Tuple[Expr, bool]]:
    yield expr, under_not
    if isinstance(expr, UnaryExpr):
        yield from _walk(expr.operand, under_not=under_not or expr.op == Operation.NOT)
    elif isinstance(expr, BinaryExpr):
        yield from _walk(expr.left, under_not=under_not)
        yield from _walk(expr.right, under_not=under_not)


def _float_column_names(schema: "Schema") -> Set[str]:
    from pyiceberg.types import DoubleType, FloatType

    return {
        field.name
        for field in schema.columns
        if isinstance(field.field_type, (FloatType, DoubleType))
    }
