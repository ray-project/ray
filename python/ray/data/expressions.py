from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional, Tuple

from ray.util.annotations import DeveloperAPI, PublicAPI


@DeveloperAPI(stability="alpha")
class Operation(Enum):
    """Enumeration of supported operations in expressions.

    This enum defines all the binary operations that can be performed
    between expressions, including arithmetic, comparison, and boolean operations.

    Attributes:
        ADD: Addition operation (+)
        SUB: Subtraction operation (-)
        MUL: Multiplication operation (*)
        DIV: Division operation (/)
        GT: Greater than comparison (>)
        LT: Less than comparison (<)
        GE: Greater than or equal comparison (>=)
        LE: Less than or equal comparison (<=)
        EQ: Equality comparison (==)
        AND: Logical AND operation (&)
        OR: Logical OR operation (|)
    """

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


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True)
class Expr(ABC):
    """Base class for all expression nodes.

    This is the abstract base class that all expression types inherit from.
    It provides operator overloads for building complex expressions using
    standard Python operators.

    Expressions form a tree structure where each node represents an operation
    or value. The tree can be evaluated against data batches to compute results.

    Example:
        >>> from ray.data.expressions import col, lit
        >>> # Create an expression tree: (col("x") + 5) * col("y")
        >>> expr = (col("x") + lit(5)) * col("y")
        >>> # This creates a BinaryExpr with operation=MUL
        >>> # left=BinaryExpr(op=ADD, left=ColumnExpr("x"), right=LiteralExpr(5))
        >>> # right=ColumnExpr("y")

    Note:
        This class should not be instantiated directly. Use the concrete
        subclasses like ColumnExpr, LiteralExpr, etc.
    """

    @abstractmethod
    def structurally_equals(self, other: Any) -> bool:
        """Compare two expression ASTs for structural equality."""
        raise NotImplementedError

    def _bin(self, other: Any, op: Operation) -> "Expr":
        """Create a binary expression with the given operation.

        Args:
            other: The right operand expression or literal value
            op: The operation to perform

        Returns:
            A new BinaryExpr representing the operation

        Note:
            If other is not an Expr, it will be automatically converted to a LiteralExpr.
        """
        if not isinstance(other, Expr):
            other = LiteralExpr(other)
        return BinaryExpr(op, self, other)

    # arithmetic
    def __add__(self, other: Any) -> "Expr":
        """Addition operator (+)."""
        return self._bin(other, Operation.ADD)

    def __radd__(self, other: Any) -> "Expr":
        """Reverse addition operator (for literal + expr)."""
        return LiteralExpr(other)._bin(self, Operation.ADD)

    def __sub__(self, other: Any) -> "Expr":
        """Subtraction operator (-)."""
        return self._bin(other, Operation.SUB)

    def __rsub__(self, other: Any) -> "Expr":
        """Reverse subtraction operator (for literal - expr)."""
        return LiteralExpr(other)._bin(self, Operation.SUB)

    def __mul__(self, other: Any) -> "Expr":
        """Multiplication operator (*)."""
        return self._bin(other, Operation.MUL)

    def __rmul__(self, other: Any) -> "Expr":
        """Reverse multiplication operator (for literal * expr)."""
        return LiteralExpr(other)._bin(self, Operation.MUL)

    def __truediv__(self, other: Any) -> "Expr":
        """Division operator (/)."""
        return self._bin(other, Operation.DIV)

    def __rtruediv__(self, other: Any) -> "Expr":
        """Reverse division operator (for literal / expr)."""
        return LiteralExpr(other)._bin(self, Operation.DIV)

    # comparison
    def __gt__(self, other: Any) -> "Expr":
        """Greater than operator (>)."""
        return self._bin(other, Operation.GT)

    def __lt__(self, other: Any) -> "Expr":
        """Less than operator (<)."""
        return self._bin(other, Operation.LT)

    def __ge__(self, other: Any) -> "Expr":
        """Greater than or equal operator (>=)."""
        return self._bin(other, Operation.GE)

    def __le__(self, other: Any) -> "Expr":
        """Less than or equal operator (<=)."""
        return self._bin(other, Operation.LE)

    def __eq__(self, other: Any) -> "Expr":
        """Equality operator (==)."""
        return self._bin(other, Operation.EQ)

    # boolean
    def __and__(self, other: Any) -> "Expr":
        """Logical AND operator (&)."""
        return self._bin(other, Operation.AND)

    def __or__(self, other: Any) -> "Expr":
        """Logical OR operator (|)."""
        return self._bin(other, Operation.OR)


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class ColumnExpr(Expr):
    """Expression that references a column by name.

    This expression type represents a reference to an existing column
    in the dataset. When evaluated, it returns the values from the
    specified column.

    Args:
        name: The name of the column to reference

    Example:
        >>> from ray.data.expressions import col
        >>> # Reference the "age" column
        >>> age_expr = col("age") # Creates ColumnExpr(name="age")
    """

    name: str

    def structurally_equals(self, other: Any) -> bool:
        return isinstance(other, ColumnExpr) and self.name == other.name


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class LiteralExpr(Expr):
    """Expression that represents a constant scalar value.

    This expression type represents a literal value that will be broadcast
    to all rows when evaluated. The value can be any Python object.

    Args:
        value: The constant value to represent

    Example:
        >>> from ray.data.expressions import lit
        >>> # Create a literal value
        >>> five = lit(5) # Creates LiteralExpr(value=5)
        >>> name = lit("John") # Creates LiteralExpr(value="John")
    """

    value: Any

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, LiteralExpr)
            and self.value == other.value
            and type(self.value) is type(other.value)
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class BinaryExpr(Expr):
    """Expression that represents a binary operation between two expressions.

    This expression type represents an operation with two operands (left and right).
    The operation is specified by the `op` field, which must be one of the
    supported operations from the Operation enum.

    Args:
        op: The operation to perform (from Operation enum)
        left: The left operand expression
        right: The right operand expression

    Example:
        >>> from ray.data.expressions import col, lit, Operation
        >>> # Manually create a binary expression (usually done via operators)
        >>> expr = BinaryExpr(Operation.ADD, col("x"), lit(5))
        >>> # This is equivalent to: col("x") + lit(5)
    """

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


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class CaseExpr(Expr):
    """Expression that represents a conditional case statement.

    This expression type represents a SQL-like CASE WHEN statement with multiple
    conditional branches and an optional default value. When evaluated, it tests
    each condition in order and returns the corresponding value for the first
    condition that evaluates to True.

    Args:
        when_clauses: List of (condition, value) tuples representing WHEN-THEN pairs
        default: Default value expression when no conditions match

    Example:
        >>> from ray.data.expressions import col, lit, case
        >>> # Create a case expression: CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END
        >>> expr = case([
        ...     (col("age") > 30, lit("Senior"))
        ... ], default=lit("Junior"))
    """

    when_clauses: List[Tuple[Expr, Expr]]
    default: Expr

    def structurally_equals(self, other: Any) -> bool:
        if not isinstance(other, CaseExpr):
            return False

        if len(self.when_clauses) != len(other.when_clauses):
            return False

        # Check each when clause
        for (self_cond, self_val), (other_cond, other_val) in zip(
            self.when_clauses, other.when_clauses
        ):
            if not (
                self_cond.structurally_equals(other_cond)
                and self_val.structurally_equals(other_val)
            ):
                return False

        # Check default value
        return self.default.structurally_equals(other.default)


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class WhenExpr(Expr):
    """Intermediate expression for building case statements with method chaining.

    This class represents an incomplete case statement that can be built
    incrementally using the .when() method. It maintains a chain of conditions
    and values, and can be completed with .otherwise() to create a final CaseExpr.

    The method chaining follows the PySpark API pattern:
    when(condition1, value1).when(condition2, value2).otherwise(default)

    Args:
        condition: The boolean condition expression for this WHEN clause
        value: The value expression when this condition is True
        next_when: Optional link to the next WHEN clause in the chain

    Example:
        >>> from ray.data.expressions import col, lit, when
        >>> # Build a case statement using method chaining
        >>> expr = when(col("age") > 50, lit("Elder")) \
        ...        .when(col("age") > 30, lit("Adult")) \
        ...        .otherwise(lit("Young"))
    """

    condition: Expr
    value: Expr
    next_when: Optional["WhenExpr"] = None

    def when(self, condition: Expr, value: Expr) -> "WhenExpr":
        """Add another WHEN clause to the case statement.

        Args:
            condition: The boolean condition expression
            value: The value expression when the condition is True

        Returns:
            A new WhenExpr with the additional WHEN clause

        Example:
            >>> expr = when(col("age") > 30, lit("Adult")) \
            ...        .when(col("age") > 18, lit("Young"))
        """
        return WhenExpr(condition, value, next_when=self)

    def otherwise(self, default: Expr) -> CaseExpr:
        """Complete the case statement with a default value.

        Args:
            default: The default value expression when no conditions match

        Returns:
            A complete CaseExpr representing the full case statement

        Example:
            >>> expr = when(col("age") > 30, lit("Senior")) \
            ...        .otherwise(lit("Junior"))
        """
        # Build the when_clauses list by traversing the chain
        when_clauses = []
        current = self
        while current is not None:
            when_clauses.append((current.condition, current.value))
            current = current.next_when

        # Reverse to get the correct order (first when should be evaluated first)
        when_clauses.reverse()

        return CaseExpr(when_clauses, default)

    def structurally_equals(self, other: Any) -> bool:
        if not isinstance(other, WhenExpr):
            return False

        return (
            self.condition.structurally_equals(other.condition)
            and self.value.structurally_equals(other.value)
            and (
                self.next_when is None
                and other.next_when is None
                or (
                    self.next_when is not None
                    and other.next_when is not None
                    and self.next_when.structurally_equals(other.next_when)
                )
            )
        )


@PublicAPI(stability="beta")
def col(name: str) -> ColumnExpr:
    """
    Reference an existing column by name.

    This is the primary way to reference columns in expressions.
    The returned expression will extract values from the specified
    column when evaluated.

    Args:
        name: The name of the column to reference

    Returns:
        A ColumnExpr that references the specified column

    Example:
        >>> from ray.data.expressions import col
        >>> # Reference columns in an expression
        >>> expr = col("price") * col("quantity")
        >>>
        >>> # Use with Dataset.with_column()
        >>> import ray
        >>> ds = ray.data.from_items([{"price": 10, "quantity": 2}])
        >>> ds = ds.with_column("total", col("price") * col("quantity"))
    """
    return ColumnExpr(name)


@PublicAPI(stability="beta")
def lit(value: Any) -> LiteralExpr:
    """
    Create a literal expression from a constant value.

    This creates an expression that represents a constant scalar value.
    The value will be broadcast to all rows when the expression is evaluated.

    Args:
        value: The constant value to represent. Can be any Python object
               (int, float, str, bool, etc.)

    Returns:
        A LiteralExpr containing the specified value

    Example:
        >>> from ray.data.expressions import col, lit
        >>> # Create literals of different types
        >>> five = lit(5)
        >>> pi = lit(3.14159)
        >>> name = lit("Alice")
        >>> flag = lit(True)
        >>>
        >>> # Use in expressions
        >>> expr = col("age") + lit(1) # Add 1 to age column
        >>>
        >>> # Use with Dataset.with_column()
        >>> import ray
        >>> ds = ray.data.from_items([{"age": 25}, {"age": 30}])
        >>> ds = ds.with_column("age_plus_one", col("age") + lit(1))
    """
    return LiteralExpr(value)


@PublicAPI(stability="beta")
def when(condition: Expr, value: Expr) -> WhenExpr:
    """Start building a case statement using method chaining.

    This function initiates the method chaining pattern for building case statements,
    similar to PySpark's when() function. It returns a WhenExpr that can be
    chained with more .when() calls and completed with .otherwise().

    Args:
        condition: The boolean condition expression for the first WHEN clause
        value: The value expression when the condition is True

    Returns:
        A WhenExpr that can be chained with more .when() calls or completed
        with .otherwise()

    Example:
        >>> from ray.data.expressions import col, lit, when
        >>> # Simple case statement
        >>> expr = when(col("age") > 30, lit("Senior")).otherwise(lit("Junior"))
        >>>
        >>> # Multiple conditions
        >>> expr = when(col("age") > 50, lit("Elder")) \
        ...        .when(col("age") > 30, lit("Adult")) \
        ...        .otherwise(lit("Young"))
        >>>
        >>> # Use with Dataset.with_column()
        >>> import ray
        >>> ds = ray.data.from_items([{"age": 25}, {"age": 35}, {"age": 55}])
        >>> ds = ds.with_column("age_group", when(col("age") > 50, lit("Elder")) \
        ...        .when(col("age") > 30, lit("Adult")) \
        ...        .otherwise(lit("Young")))
    """
    return WhenExpr(condition, value)


@PublicAPI(stability="beta")
def case(when_clauses: List[Tuple[Expr, Expr]], default: Expr) -> CaseExpr:
    """Create a conditional case statement expression (function-based approach).

    This function creates a SQL-like CASE WHEN statement that evaluates
    multiple conditional branches and returns the corresponding value
    for the first condition that evaluates to True.

    Note: This is the function-based approach. For better readability and
    consistency with PySpark, consider using the method chaining approach:
    when().when().otherwise()

    Args:
        when_clauses: List of (condition, value) tuples representing WHEN-THEN
            pairs. Each condition should be a boolean expression, and each value
            can be any expression type.
        default: Default value expression when no conditions match.

    Returns:
        A CaseExpr representing the conditional logic

    Example:
        >>> from ray.data.expressions import col, lit, case
        >>> # Simple case statement: CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END
        >>> expr = case([
        ...     (col("age") > 30, lit("Senior"))
        ... ], default=lit("Junior"))
        >>>
        >>> # Multiple conditions: CASE WHEN age > 50 THEN 'Elder' WHEN age > 30 THEN 'Adult' ELSE 'Young' END
        >>> expr = case([
        ...         (col("age") > 50, lit("Elder")),
        ...         (col("age") > 30, lit("Adult"))
        ...     ], default=lit("Young"))
        >>>
        >>> # Use with Dataset.with_column()
        >>> import ray
        >>> ds = ray.data.from_items([{"age": 25}, {"age": 35}, {"age": 55}])
        >>> ds = ds.with_column("age_group", case([
        ...         (col("age") > 50, lit("Elder")),
        ...         (col("age") > 30, lit("Adult"))
        ...     ], default=lit("Young")))
    """
    if not when_clauses:
        raise ValueError("case() must have at least one when clause")

    return CaseExpr(when_clauses, default)


# ──────────────────────────────────────
# Public API for evaluation
# ──────────────────────────────────────
# Note: Implementation details are in _expression_evaluator.py

# Re-export eval_expr for public use

__all__ = [
    "Operation",
    "Expr",
    "ColumnExpr",
    "LiteralExpr",
    "BinaryExpr",
    "CaseExpr",
    "WhenExpr",
    "col",
    "lit",
    "when",
    "case",
]
