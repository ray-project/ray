"""The ``ds.filter`` pushdown: a predicate folded into the read's ``MCAPSelection``.

The summary gives each channel's topic and each chunk's log-time range, so a
conjunct on ``topic`` or ``log_time`` folds into the selection. The reader
applies it exactly, so the ``Filter`` above the read keeps only the other
conjuncts. The indexer folds the scanner's pushed predicate into the same base
selection with :func:`narrow_selection`, so listing prunes on exactly the
selection the reader applies.
"""

from dataclasses import dataclass, replace
from typing import FrozenSet, List, Optional, Tuple

from ray.data._internal.datasource_v2.common.pushdown_utils import combine_predicates
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    TimeRange,
)
from ray.data.expressions import BinaryExpr, ColumnExpr, Expr, LiteralExpr, Operation

TOPIC_COLUMN = "topic"
LOG_TIME_COLUMN = "log_time"

# The comparison with its operands swapped: ``5 < x`` is ``x > 5``.
_FLIPPED = {
    Operation.LT: Operation.GT,
    Operation.LE: Operation.GE,
    Operation.GT: Operation.LT,
    Operation.GE: Operation.LE,
    Operation.EQ: Operation.EQ,
}


@dataclass(frozen=True)
class NarrowedSelection:
    """What :func:`narrow_selection` folded into the selection and what it did not.

    Attributes:
        selection: The input selection narrowed by every folded conjunct.
        pushed: The folded conjuncts, ``AND``-ed, or ``None`` if none was folded.
        residual: The conjuncts that could not be folded, ``AND``-ed, or ``None``.
    """

    selection: MCAPSelection
    pushed: Optional[Expr]
    residual: Optional[Expr]


def _conjuncts(predicate: Expr) -> List[Expr]:
    """The top-level ``AND`` chain of ``predicate``, as separate expressions."""
    if isinstance(predicate, BinaryExpr) and predicate.op is Operation.AND:
        return _conjuncts(predicate.left) + _conjuncts(predicate.right)
    return [predicate]


def _column_and_literal(
    expr: BinaryExpr,
) -> Optional[Tuple[str, Operation, object]]:
    """``(column, op, value)`` for a column-literal comparison, either way round."""
    if isinstance(expr.left, ColumnExpr) and isinstance(expr.right, LiteralExpr):
        return expr.left.name, expr.op, expr.right.value
    if isinstance(expr.left, LiteralExpr) and isinstance(expr.right, ColumnExpr):
        flipped = _FLIPPED.get(expr.op)
        if flipped is None:
            return None
        return expr.right.name, flipped, expr.left.value
    return None


def _topics_kept(conjunct: Expr) -> Optional[FrozenSet[str]]:
    """The topics a conjunct keeps, or ``None`` if it is not a topic filter."""
    if not isinstance(conjunct, BinaryExpr):
        return None
    parts = _column_and_literal(conjunct)
    if parts is None or parts[0] != TOPIC_COLUMN:
        return None
    _, op, value = parts
    if op is Operation.EQ and isinstance(value, str):
        return frozenset({value})
    if (
        op is Operation.IN
        and isinstance(value, (list, tuple, set, frozenset))
        and all(isinstance(v, str) for v in value)
    ):
        return frozenset(value)
    return None


def _time_bounds(conjunct: Expr) -> Optional[Tuple[Optional[int], Optional[int]]]:
    """``(start, end)`` bounds a conjunct puts on ``log_time``, half-open.

    ``None`` if the conjunct is not a ``log_time`` comparison with an integer.
    """
    if not isinstance(conjunct, BinaryExpr):
        return None
    parts = _column_and_literal(conjunct)
    if parts is None or parts[0] != LOG_TIME_COLUMN:
        return None
    _, op, value = parts
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if op is Operation.GE:
        return value, None
    if op is Operation.GT:
        return value + 1, None
    if op is Operation.LT:
        return None, value
    if op is Operation.LE:
        return None, value + 1
    if op is Operation.EQ:
        return value, value + 1
    return None


@dataclass(frozen=True)
class _FoldedConjuncts:
    """A selection's topics and ``log_time`` bounds after folding a predicate in.

    The bounds are not clamped yet, so they may be negative or empty.

    Attributes:
        topics: The topics still selected, or ``None`` for every topic.
        start: Inclusive lower bound, or ``None`` for none.
        end: Exclusive upper bound, or ``None`` for none.
        pushed: The folded conjuncts, ``AND``-ed, or ``None`` if none was folded.
        residual: The conjuncts that could not be folded, ``AND``-ed, or ``None``.
    """

    topics: Optional[FrozenSet[str]]
    start: Optional[int]
    end: Optional[int]
    pushed: Optional[Expr]
    residual: Optional[Expr]


def _intersect_time_bounds(
    start: Optional[int],
    end: Optional[int],
    bounds: Tuple[Optional[int], Optional[int]],
) -> Tuple[Optional[int], Optional[int]]:
    """Intersect ``[start, end)`` with the half-open ``bounds``.

    ``None`` on either side means unbounded.
    """
    low, high = bounds
    if low is not None:
        start = low if start is None else max(start, low)
    if high is not None:
        end = high if end is None else min(end, high)
    return start, end


def _fold_conjuncts(selection: MCAPSelection, predicate: Expr) -> _FoldedConjuncts:
    """Intersect the selection's topics and time bounds with each foldable conjunct.

    A conjunct that is neither a topic filter nor an integer ``log_time``
    comparison goes to the residual.
    """
    topics = selection.topics
    start = selection.start_time
    end = selection.end_time
    pushed: Optional[Expr] = None
    residual: Optional[Expr] = None
    for conjunct in _conjuncts(predicate):
        kept = _topics_kept(conjunct)
        bounds = _time_bounds(conjunct) if kept is None else None
        if kept is not None:
            topics = kept if topics is None else topics & kept
        elif bounds is not None:
            start, end = _intersect_time_bounds(start, end, bounds)
        else:
            residual = combine_predicates(residual, conjunct)
            continue
        pushed = combine_predicates(pushed, conjunct)
    return _FoldedConjuncts(topics, start, end, pushed, residual)


def _restrict_selection(
    selection: MCAPSelection, folded: _FoldedConjuncts
) -> MCAPSelection:
    """Narrow ``selection`` to the folded topics and ``log_time`` bounds."""
    # Log times are non-negative: a negative lower bound is no bound, and an
    # upper bound at or below zero matches nothing.
    low = 0 if folded.start is None else max(folded.start, 0)
    high = 2**63 - 1 if folded.end is None else folded.end
    if low >= high:
        # The bounds are empty or inverted: select no channel at all.
        return replace(selection, topics=frozenset())
    # Keep the selection's own range unless a bound changed it, so a topic-only
    # filter leaves ``time_range`` unset and ``count()`` can use the statistics.
    time_range = selection.time_range
    if (folded.start, folded.end) != (selection.start_time, selection.end_time):
        time_range = TimeRange(start_time=low, end_time=high)
    return replace(selection, topics=folded.topics, time_range=time_range)


def narrow_selection(selection: MCAPSelection, predicate: Expr) -> NarrowedSelection:
    """Fold ``predicate``'s ``topic`` and ``log_time`` conjuncts into ``selection``.

    The selection already filters on topic and log time, so the reader applies
    a folded conjunct exactly and the ``Filter`` above the read can drop it. A
    selection that can match nothing, because its topics are disjoint or its
    time range is empty, gets an empty topic set, which selects no channel.
    """
    folded = _fold_conjuncts(selection, predicate)
    if folded.pushed is None:
        return NarrowedSelection(selection, None, predicate)
    narrowed = _restrict_selection(selection, folded)
    return NarrowedSelection(narrowed, folded.pushed, folded.residual)
