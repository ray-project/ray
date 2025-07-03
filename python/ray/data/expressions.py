from __future__ import annotations

import operator
from dataclasses import dataclass
from typing import Any, Callable, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc


# ──────────────────────────────────────
#  Basic expression node definitions
# ──────────────────────────────────────
class Expr:  # Base class – all expression nodes inherit from this
    # Binary/boolean operator overloads
    def _bin(self, other: Any, op: str) -> "Expr":
        other = other if isinstance(other, Expr) else LiteralExpr(other)
        return BinaryExpr(op, self, other)

    # arithmetic
    def __add__(self, other):
        return self._bin(other, "add")

    def __sub__(self, other):
        return self._bin(other, "sub")

    def __mul__(self, other):
        return self._bin(other, "mul")

    def __truediv__(self, other):
        return self._bin(other, "div")

    # comparison
    def __gt__(self, other):
        return self._bin(other, "gt")

    def __lt__(self, other):
        return self._bin(other, "lt")

    def __ge__(self, other):
        return self._bin(other, "ge")

    def __le__(self, other):
        return self._bin(other, "le")

    def __eq__(self, other):
        return self._bin(other, "eq")

    # boolean
    def __and__(self, other):
        return self._bin(other, "and")

    def __or__(self, other):
        return self._bin(other, "or")

    # Rename the output column
    def alias(self, name: str) -> "AliasExpr":
        return AliasExpr(self, name)


@dataclass(frozen=True, eq=False)
class ColumnExpr(Expr):
    name: str


@dataclass(frozen=True, eq=False)
class LiteralExpr(Expr):
    value: Any


@dataclass(frozen=True, eq=False)
class BinaryExpr(Expr):
    op: str
    left: Expr
    right: Expr


@dataclass(frozen=True, eq=False)
class AliasExpr(Expr):
    expr: Expr
    name: str


# ──────────────────────────────────────
#  User helpers
# ──────────────────────────────────────
def col(name: str) -> ColumnExpr:
    """Reference an existing column."""
    return ColumnExpr(name)


def lit(value: Any) -> LiteralExpr:
    """Create a scalar literal expression (e.g. lit(1))."""
    return LiteralExpr(value)


# ──────────────────────────────────────
#  Local evaluator (pandas batches)
# ──────────────────────────────────────
# This is used by Dataset.with_columns – kept here so it can be re-used by
# future optimised executors.
_PANDAS_OPS: Dict[str, Callable[[Any, Any], Any]] = {
    "add": operator.add,
    "sub": operator.sub,
    "mul": operator.mul,
    "div": operator.truediv,
    "gt": operator.gt,
    "lt": operator.lt,
    "ge": operator.ge,
    "le": operator.le,
    "eq": operator.eq,
    "and": operator.and_,
    "or": operator.or_,
}

_NUMPY_OPS: Dict[str, Callable[[Any, Any], Any]] = {
    "add": np.add,
    "sub": np.subtract,
    "mul": np.multiply,
    "div": np.divide,
    "gt": np.greater,
    "lt": np.less,
    "ge": np.greater_equal,
    "le": np.less_equal,
    "eq": np.equal,
    "and": np.logical_and,
    "or": np.logical_or,
}

_ARROW_OPS: Dict[str, Callable[[Any, Any], Any]] = {
    "add": pc.add,
    "sub": pc.subtract,
    "mul": pc.multiply,
    "div": pc.divide,
    "gt": pc.greater,
    "lt": pc.less,
    "ge": pc.greater_equal,
    "le": pc.less_equal,
    "eq": pc.equal,
    "and": pc.and_,
    "or": pc.or_,
}


def _eval_expr_recursive(expr: Expr, batch, ops: Dict[str, Callable]) -> Any:
    """Generic recursive expression evaluator."""
    if isinstance(expr, ColumnExpr):
        return batch[expr.name]
    if isinstance(expr, LiteralExpr):
        return expr.value
    if isinstance(expr, BinaryExpr):
        return ops[expr.op](
            _eval_expr_recursive(expr.left, batch, ops),
            _eval_expr_recursive(expr.right, batch, ops),
        )
    raise TypeError(f"Unsupported expression node: {type(expr).__name__}")


def eval_expr(expr: Expr, batch) -> Any:
    """Recursively evaluate *expr* against a batch of the appropriate type."""
    if isinstance(batch, pd.DataFrame):
        return _eval_expr_recursive(expr, batch, _PANDAS_OPS)
    elif isinstance(batch, (np.ndarray, dict)):
        return _eval_expr_recursive(expr, batch, _NUMPY_OPS)
    elif isinstance(batch, pa.Table):
        return _eval_expr_recursive(expr, batch, _ARROW_OPS)
    raise TypeError(f"Unsupported batch type: {type(batch).__name__}")
