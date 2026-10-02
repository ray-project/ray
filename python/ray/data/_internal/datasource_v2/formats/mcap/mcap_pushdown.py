"""Turning a pushed-down ``ds.filter`` predicate into a narrower selection.

Two columns of a message row are known before any payload is read: ``topic``
(from the summary's channels) and ``log_time`` (bounded per chunk by its
index). A conjunct over either can therefore be folded into the read's
``MCAPSelection``: the listing prunes files and chunks with it, and the
reader applies it exactly, so the ``Filter`` above the read is dropped.
Everything else stays a residual the ``Filter`` keeps applying.

The scanner narrows its selection with :func:`narrow_selection` and reports
the folded conjuncts as its pushed predicate; the planner hands that
predicate to the indexer, which narrows the same base selection with the same
function. Both sides therefore prune on exactly the same selection.
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

# Comparison seen from the column's side when the literal is on the left.
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
        pushed: The folded conjuncts, ``AND``-ed, or ``None`` when none was.
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


def narrow_selection(selection: MCAPSelection, predicate: Expr) -> NarrowedSelection:
    """Fold the ``topic`` and ``log_time`` conjuncts of ``predicate`` into ``selection``.

    A folded conjunct is applied exactly by the reader (topic equality and
    log-time bounds are what the selection already evaluates), so it can be
    dropped above the read. A selection that can no longer match anything
    (disjoint topics, or an empty time range) is represented by an empty
    topic set, which selects no channel.
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
            low, high = bounds
            if low is not None:
                start = low if start is None else max(start, low)
            if high is not None:
                end = high if end is None else min(end, high)
        else:
            residual = combine_predicates(residual, conjunct)
            continue
        pushed = combine_predicates(pushed, conjunct)

    if pushed is None:
        return NarrowedSelection(selection, None, predicate)

    # Log times are non-negative, so a lower bound below zero is no bound and
    # an upper bound at or below zero, like an inverted pair, matches nothing.
    low = 0 if start is None else max(start, 0)
    high = 2**63 - 1 if end is None else end
    if low >= high:
        # Nothing can satisfy the bounds: select no channel at all.
        narrowed = replace(selection, topics=frozenset())
    else:
        time_range = selection.time_range
        if (start, end) != (selection.start_time, selection.end_time):
            time_range = TimeRange(start_time=low, end_time=high)
        narrowed = replace(selection, topics=topics, time_range=time_range)
    return NarrowedSelection(narrowed, pushed, residual)
