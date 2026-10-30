"""Selectivity estimation of Ray Data predicate expressions."""

import logging
from typing import Any, Optional, Tuple

from ray.data._internal.cbo_stats.operator_statistics import (
    DEFAULT_UNKNOWN_SELECTIVITY,
    ConfidenceLevel,
    OperatorStatistics,
    estimate_selectivity_from_column_stats,
)
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    UnaryExpr,
)

logger = logging.getLogger(__name__)

# Comparison operations understood by ``estimate_selectivity_from_column_stats``.
_COMPARISON_TO_SELECTIVITY_KEY = {
    Operation.EQ: "EQ",
    Operation.NE: "NE",
    Operation.GT: "GT",
    Operation.GE: "GE",
    Operation.LT: "LT",
    Operation.LE: "LE",
    Operation.IN: "IN",
    Operation.NOT_IN: "NOT_IN",
}
# Equivalent comparison when the literal is on the left (``5 < col``).
_MIRRORED_SELECTIVITY_KEY = {
    "EQ": "EQ",
    "NE": "NE",
    "GT": "LT",
    "GE": "LE",
    "LT": "GT",
    "LE": "GE",
}


def filter_statistics(
    stats: OperatorStatistics, predicate: Optional[Expr]
) -> OperatorStatistics:
    """Statistics of ``stats`` filtered by ``predicate``.

    A ``None`` predicate stands for an opaque (UDF) predicate. Heuristic
    selectivities cap the confidence at ``LOW``.
    """
    selectivity, from_stats = DEFAULT_UNKNOWN_SELECTIVITY, False
    if predicate is not None:
        try:
            selectivity, from_stats = estimate_predicate_selectivity(predicate, stats)
        except Exception as e:
            logger.debug(f"Failed to estimate predicate selectivity: {e}")
            selectivity, from_stats = DEFAULT_UNKNOWN_SELECTIVITY, False
    filtered = stats.scale(selectivity)
    if not from_stats:
        filtered = filtered.with_confidence_at_most(ConfidenceLevel.LOW.value)
    return filtered


def estimate_predicate_selectivity(
    expr: Expr, stats: OperatorStatistics
) -> Tuple[float, bool]:
    """Estimates the fraction of rows satisfying a predicate expression.

    Conjunctions and disjunctions assume independent predicates.

    Returns:
        A ``(selectivity, from_stats)`` tuple, where ``from_stats`` is True
        only if every leaf predicate was estimated from column statistics
        rather than from a default.
    """
    if isinstance(expr, BinaryExpr):
        if expr.op in (Operation.AND, Operation.OR):
            left, left_from_stats = estimate_predicate_selectivity(expr.left, stats)
            right, right_from_stats = estimate_predicate_selectivity(expr.right, stats)
            if expr.op == Operation.AND:
                selectivity = left * right
            else:
                selectivity = 1.0 - (1.0 - left) * (1.0 - right)
            return selectivity, left_from_stats and right_from_stats

        key = _COMPARISON_TO_SELECTIVITY_KEY.get(expr.op)
        if key is None:
            return DEFAULT_UNKNOWN_SELECTIVITY, False
        if isinstance(expr.left, ColumnExpr) and isinstance(expr.right, LiteralExpr):
            column, value = expr.left.name, expr.right.value
        elif (
            isinstance(expr.right, ColumnExpr)
            and isinstance(expr.left, LiteralExpr)
            and key in _MIRRORED_SELECTIVITY_KEY
        ):
            column, value = expr.right.name, expr.left.value
            key = _MIRRORED_SELECTIVITY_KEY[key]
        else:
            return DEFAULT_UNKNOWN_SELECTIVITY, False
        return _estimate_column_selectivity(column, key, value, stats)

    if isinstance(expr, UnaryExpr):
        if expr.op == Operation.NOT:
            selectivity, from_stats = estimate_predicate_selectivity(
                expr.operand, stats
            )
            return 1.0 - selectivity, from_stats
        if expr.op in (Operation.IS_NULL, Operation.IS_NOT_NULL) and isinstance(
            expr.operand, ColumnExpr
        ):
            key = "IS_NULL" if expr.op == Operation.IS_NULL else "IS_NOT_NULL"
            return _estimate_column_selectivity(expr.operand.name, key, None, stats)

    return DEFAULT_UNKNOWN_SELECTIVITY, False


def _estimate_column_selectivity(
    column: str, key: str, value: Any, stats: OperatorStatistics
) -> Tuple[float, bool]:
    column_stat = stats.get_column_stat(column)
    selectivity = estimate_selectivity_from_column_stats(
        column, key, value, column_stat, num_rows=stats.num_rows
    )
    if key in ("IS_NULL", "IS_NOT_NULL"):
        from_stats = (
            column_stat is not None
            and column_stat.null_count is not None
            and bool(stats.num_rows)
        )
    else:
        from_stats = column_stat is not None and column_stat.is_available()
    return selectivity, from_stats
