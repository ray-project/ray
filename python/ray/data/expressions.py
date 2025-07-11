from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class Operation(Enum):
    """Enumeration of supported operations in expressions."""

    ADD = "add"
    SUB = "sub"
    MUL = "mul"
    DIV = "div"
    GT = "gt"
    LT = "lt"
    GE = "ge"
    LE = "le"
    EQ = "eq"
    AND = "and"
    OR = "or"


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class Expr(ABC):
    """Base class for all expression nodes."""

    @abstractmethod
    def structurally_equals(self, other: Any) -> bool:
        """Compare two expression ASTs for structural equality."""
        raise NotImplementedError

    def _bin(self, other: Any, op: Operation) -> "Expr":
        """Create a binary expression with the given operation."""
        if not isinstance(other, Expr):
            other = LiteralExpr(other)
        return BinaryExpr(op, self, other)

    # Arithmetic operators
    def __add__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.ADD)

    def __radd__(self, other: Any) -> "Expr":
        return LiteralExpr(other)._bin(self, Operation.ADD)

    def __sub__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.SUB)

    def __rsub__(self, other: Any) -> "Expr":
        return LiteralExpr(other)._bin(self, Operation.SUB)

    def __mul__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.MUL)

    def __rmul__(self, other: Any) -> "Expr":
        return LiteralExpr(other)._bin(self, Operation.MUL)

    def __truediv__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.DIV)

    def __rtruediv__(self, other: Any) -> "Expr":
        return LiteralExpr(other)._bin(self, Operation.DIV)

    # Comparison operators
    def __gt__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.GT)

    def __lt__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.LT)

    def __ge__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.GE)

    def __le__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.LE)

    def __eq__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.EQ)

    # Boolean operators
    def __and__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.AND)

    def __or__(self, other: Any) -> "Expr":
        return self._bin(other, Operation.OR)


@PublicAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class ColumnExpr(Expr):
    """Expression that references a column by name."""

    name: str

    def structurally_equals(self, other: Any) -> bool:
        return isinstance(other, ColumnExpr) and self.name == other.name


@PublicAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class LiteralExpr(Expr):
    """Expression that represents a constant scalar value."""

    value: Any

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, LiteralExpr)
            and self.value == other.value
            and type(self.value) is type(other.value)
        )


@PublicAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class BinaryExpr(Expr):
    """Expression that represents a binary operation between two expressions."""

    op: Operation
    left: Expr
    right: Expr

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, BinaryExpr)
            and self.op is other.op
            and self.left.structurally_equals(other.left)
            and self.right.structurally_equals(other.right)
        )


@PublicAPI(stability="beta")
def col(name: str) -> ColumnExpr:
    """Reference an existing column by name."""
    return ColumnExpr(name)


@PublicAPI(stability="beta")
def lit(value: Any) -> LiteralExpr:
    """Create a literal expression from a constant value."""
    return LiteralExpr(value)


__all__ = [
    "Operation",
    "Expr",
    "ColumnExpr",
    "LiteralExpr",
    "BinaryExpr",
    "col",
    "lit",
]
