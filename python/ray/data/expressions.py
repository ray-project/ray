from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Tuple, Union

from ray.data.block import BatchColumn
from ray.data.datatype import DataType
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pyarrow
    import pyarrow.compute


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
        FLOORDIV: Floor division operation (//)
        GT: Greater than comparison (>)
        LT: Less than comparison (<)
        GE: Greater than or equal comparison (>=)
        LE: Less than or equal comparison (<=)
        EQ: Equality comparison (==)
        NE: Not equal comparison (!=)
        AND: Logical AND operation (&)
        OR: Logical OR operation (|)
        NOT: Logical NOT operation (~)
        IS_NULL: Check if value is null
        IS_NOT_NULL: Check if value is not null
        IN: Check if value is in a list
        NOT_IN: Check if value is not in a list
    """

    ADD = "add"
    SUB = "sub"
    MUL = "mul"
    DIV = "div"
    FLOORDIV = "floordiv"
    GT = "gt"
    LT = "lt"
    GE = "ge"
    LE = "le"
    EQ = "eq"
    NE = "ne"
    AND = "and"
    OR = "or"
    NOT = "not"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IN = "in"
    NOT_IN = "not_in"


class _ExprVisitor(ABC):
    """Base visitor with generic dispatch for Ray Data expressions.

    This implements the Visitor pattern for traversing expression trees.
    Subclasses implement specific conversion logic for each expression type.
    """

    def visit(self, expr: "Expr") -> Any:
        """Dispatch to the appropriate visitor method based on expression type.

        Args:
            expr: Expression to visit

        Returns:
            Result of the specific visitor method

        Raises:
            TypeError: If expression type is not supported
        """
        # Dispatch to type-specific visitor methods
        if isinstance(expr, ColumnExpr):
            return self.visit_column(expr)
        elif isinstance(expr, LiteralExpr):
            return self.visit_literal(expr)
        elif isinstance(expr, BinaryExpr):
            return self.visit_binary(expr)
        elif isinstance(expr, UnaryExpr):
            return self.visit_unary(expr)
        elif isinstance(expr, AliasExpr):
            return self.visit_alias(expr)
        elif isinstance(expr, CaseExpr):
            return self.visit_case(expr)
        elif isinstance(expr, WhenExpr):
            return self.visit_when(expr)
        elif isinstance(expr, UDFExpr):
            return self.visit_udf(expr)
        elif isinstance(expr, DownloadExpr):
            return self.visit_download(expr)
        else:
            raise TypeError(f"Unsupported expression type for conversion: {type(expr)}")

    @abstractmethod
    def visit_column(self, expr: "ColumnExpr") -> Any:
        pass

    @abstractmethod
    def visit_literal(self, expr: "LiteralExpr") -> Any:
        pass

    @abstractmethod
    def visit_binary(self, expr: "BinaryExpr") -> Any:
        pass

    @abstractmethod
    def visit_unary(self, expr: "UnaryExpr") -> Any:
        pass

    @abstractmethod
    def visit_alias(self, expr: "AliasExpr") -> Any:
        pass

    @abstractmethod
    def visit_case(self, expr: "CaseExpr") -> Any:
        pass

    @abstractmethod
    def visit_when(self, expr: "WhenExpr") -> Any:
        pass

    @abstractmethod
    def visit_udf(self, expr: "UDFExpr") -> Any:
        pass

    @abstractmethod
    def visit_download(self, expr: "DownloadExpr") -> Any:
        pass


class _PyArrowExpressionVisitor(_ExprVisitor):
    """Visitor that converts Ray Data expressions to PyArrow compute expressions.

    This visitor enables filter pushdown and other optimizations by converting
    Ray Data expression trees into equivalent PyArrow compute expressions.
    """

    def visit_column(self, expr: "ColumnExpr") -> "pyarrow.compute.Expression":
        """Convert column reference to PyArrow field reference."""
        import pyarrow.compute as pc

        return pc.field(expr.name)

    def visit_literal(self, expr: "LiteralExpr") -> "pyarrow.compute.Expression":
        """Convert literal value to PyArrow scalar."""
        import pyarrow.compute as pc

        return pc.scalar(expr.value)

    def visit_binary(self, expr: "BinaryExpr") -> "pyarrow.compute.Expression":
        """Convert binary operation to PyArrow compute expression.

        Special handling for IN/NOT_IN operations which require the right operand
        to be a literal list for PyArrow's is_in function.
        """
        import pyarrow as pa
        import pyarrow.compute as pc

        # Handle IN and NOT_IN operations specially
        if expr.op in (Operation.IN, Operation.NOT_IN):
            left = self.visit(expr.left)

            # Right operand must be a literal list for PyArrow
            if isinstance(expr.right, LiteralExpr):
                right_value = expr.right.value
                # Convert to PyArrow array
                right = (
                    pa.array(right_value)
                    if isinstance(right_value, list)
                    else pa.array([right_value])
                )
            else:
                raise ValueError(
                    f"is_in/not_in operations require the right operand to be a "
                    f"literal list, got {type(expr.right).__name__}."
                )

            # Apply is_in and optionally invert for NOT_IN
            result = pc.is_in(left, right)
            return pc.invert(result) if expr.op == Operation.NOT_IN else result

        # Standard binary operations
        left = self.visit(expr.left)
        right = self.visit(expr.right)
        from ray.data._expression_evaluator import _ARROW_EXPR_OPS_MAP

        if expr.op in _ARROW_EXPR_OPS_MAP:
            return _ARROW_EXPR_OPS_MAP[expr.op](left, right)
        raise ValueError(f"Unsupported binary operation for PyArrow: {expr.op}")

    def visit_unary(self, expr: "UnaryExpr") -> "pyarrow.compute.Expression":
        operand = self.visit(expr.operand)
        from ray.data._expression_evaluator import _ARROW_EXPR_OPS_MAP

        if expr.op in _ARROW_EXPR_OPS_MAP:
            return _ARROW_EXPR_OPS_MAP[expr.op](operand)
        raise ValueError(f"Unsupported unary operation for PyArrow: {expr.op}")

    def visit_alias(self, expr: "AliasExpr") -> "pyarrow.compute.Expression":
        return self.visit(expr.expr)

    def visit_case(self, expr: "CaseExpr") -> "pyarrow.compute.Expression":
        """Convert CaseExpr to PyArrow if_else chain.

        PyArrow doesn't support expression-based case_when directly, so we build
        a nested if_else structure:
        - if_else(cond1, val1, if_else(cond2, val2, default))

        Conditions are evaluated in order until one is True.
        """
        import pyarrow.compute as pc

        # Convert all components to PyArrow expressions
        conditions = [self.visit(cond) for cond, _ in expr.when_clauses]
        choices = [self.visit(val) for _, val in expr.when_clauses]
        default = self.visit(expr.default)

        # Build nested if_else structure in reverse order
        # Start from default and work backwards through conditions
        result = default
        for cond, choice in reversed(list(zip(conditions, choices))):
            result = pc.if_else(cond, choice, result)

        return result

    def visit_when(self, expr: "WhenExpr") -> "pyarrow.compute.Expression":
        raise TypeError(
            "WhenExpr cannot be converted to PyArrow expression directly. "
            "Use .otherwise() to complete the case statement first."
        )

    def visit_udf(self, expr: "UDFExpr") -> "pyarrow.compute.Expression":
        raise TypeError("UDF expressions cannot be converted to PyArrow expressions")

    def visit_download(self, expr: "DownloadExpr") -> "pyarrow.compute.Expression":
        raise TypeError(
            "Download expressions cannot be converted to PyArrow expressions"
        )


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

    data_type: DataType

    @property
    def name(self) -> str | None:
        """Get the name associated with this expression.

        Returns:
            The name for expressions that have one (ColumnExpr, AliasExpr),
            None otherwise.
        """
        return None

    @abstractmethod
    def structurally_equals(self, other: Any) -> bool:
        """Compare two expression ASTs for structural equality."""
        raise NotImplementedError

    def to_pyarrow(self) -> "pyarrow.compute.Expression":
        """Convert this expression to a PyArrow compute expression.

        Returns:
            A PyArrow compute expression equivalent to this Ray Data expression

        Raises:
            TypeError: If the expression cannot be converted to PyArrow
            ValueError: If the expression uses unsupported operations

        Example:
            >>> from ray.data.expressions import col
            >>> expr = col("age") > 30
            >>> pa_expr = expr.to_pyarrow()
        """
        return _PyArrowExpressionVisitor().visit(self)

    def _bin(self, other: Any, op: Operation) -> "Expr":
        """Create a binary expression with the given operation.

        Args:
            other: The right operand expression or literal value
            op: The operation to perform

        Returns:
            A new BinaryExpr representing the operation

        Note:
            If other is not an Expr, it will be automatically converted to a
            LiteralExpr. This enables natural Python syntax like col("x") + 5.
        """
        # Auto-wrap non-expression values as literals
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

    def __floordiv__(self, other: Any) -> "Expr":
        """Floor division operator (//)."""
        return self._bin(other, Operation.FLOORDIV)

    def __rfloordiv__(self, other: Any) -> "Expr":
        """Reverse floor division operator (for literal // expr)."""
        return LiteralExpr(other)._bin(self, Operation.FLOORDIV)

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

    def __ne__(self, other: Any) -> "Expr":
        """Not equal operator (!=)."""
        return self._bin(other, Operation.NE)

    # boolean
    def __and__(self, other: Any) -> "Expr":
        """Logical AND operator (&)."""
        return self._bin(other, Operation.AND)

    def __or__(self, other: Any) -> "Expr":
        """Logical OR operator (|)."""
        return self._bin(other, Operation.OR)

    def __invert__(self) -> "Expr":
        """Logical NOT operator (~)."""
        return UnaryExpr(Operation.NOT, self)

    # predicate methods
    def is_null(self) -> "Expr":
        """Check if the expression value is null.

        Returns:
            A boolean expression that is True where values are null

        Example:
            >>> from ray.data.expressions import col
            >>> # Filter rows where age column is null
            >>> expr = col("age").is_null()
        """
        return UnaryExpr(Operation.IS_NULL, self)

    def is_not_null(self) -> "Expr":
        """Check if the expression value is not null.

        Returns:
            A boolean expression that is True where values are not null

        Example:
            >>> from ray.data.expressions import col
            >>> # Filter rows where age column is not null
            >>> expr = col("age").is_not_null()
        """
        return UnaryExpr(Operation.IS_NOT_NULL, self)

    def is_in(self, values: Union[List[Any], "Expr"]) -> "Expr":
        """Check if the expression value is in a list of values.

        Args:
            values: List of values to check membership against

        Returns:
            A boolean expression that is True where values are in the list

        Example:
            >>> from ray.data.expressions import col
            >>> # Check if status is in a list
            >>> expr = col("status").is_in(["active", "pending", "approved"])
        """
        if not isinstance(values, Expr):
            values = LiteralExpr(values)
        return self._bin(values, Operation.IN)

    def not_in(self, values: Union[List[Any], "Expr"]) -> "Expr":
        """Check if the expression value is not in a list of values.

        Args:
            values: List of values to check membership against

        Returns:
            A boolean expression that is True where values are not in the list

        Example:
            >>> from ray.data.expressions import col
            >>> # Check if status is not in a list
            >>> expr = col("status").not_in(["rejected", "failed"])
        """
        if not isinstance(values, Expr):
            values = LiteralExpr(values)
        return self._bin(values, Operation.NOT_IN)

    def alias(self, name: str) -> "Expr":
        """Rename the expression.

        This method allows you to assign a new name to an expression result.
        This is particularly useful when you want to specify the output column name
        directly within the expression rather than as a separate parameter.

        Args:
            name: The new name for the expression

        Returns:
            An AliasExpr that wraps this expression with the specified name

        Example:
            >>> from ray.data.expressions import col
            >>> # Create an expression with an alias
            >>> expr = (col("price") * col("quantity")).alias("total_cost")
        """
        return AliasExpr(data_type=self.data_type, expr=self, _name=name)


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

    _name: str
    data_type: DataType = field(default_factory=lambda: DataType(object), init=False)

    @property
    def name(self) -> str:
        """Get the column name."""
        return self._name

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
        >>> import numpy as np
        >>> # Create a literal value
        >>> five = lit(5) # Creates LiteralExpr(value=5)
        >>> name = lit("John") # Creates LiteralExpr(value="John")
        >>> numpy_val = lit(np.int32(42)) # Creates LiteralExpr with numpy type
    """

    value: Any
    data_type: DataType = field(init=False)

    def __post_init__(self):
        """Initialize data_type by inferring from the literal value.

        Since the dataclass is frozen, we use object.__setattr__ to set the
        data_type field after construction.
        """
        # Infer the type from the value using DataType.infer_dtype
        inferred_dtype = DataType.infer_dtype(self.value)

        # Use object.__setattr__ since the dataclass is frozen
        object.__setattr__(self, "data_type", inferred_dtype)

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

    data_type: DataType = field(default_factory=lambda: DataType(object), init=False)

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, BinaryExpr)
            and self.op is other.op
            and self.left.structurally_equals(other.left)
            and self.right.structurally_equals(other.right)
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class UnaryExpr(Expr):
    """Expression that represents a unary operation on a single expression.

    This expression type represents an operation with one operand.
    Currently used primarily for the NOT operation (~).

    Args:
        op: The operation to perform (from Operation enum)
        operand: The operand expression

    Example:
        >>> from ray.data.expressions import col, Operation
        >>> # Manually create a unary expression (usually done via operators)
        >>> expr = UnaryExpr(Operation.NOT, col("is_active"))
        >>> # This is equivalent to: ~col("is_active")
    """

    op: Operation
    operand: Expr

    data_type: DataType = field(default_factory=lambda: DataType(object), init=False)

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, UnaryExpr)
            and self.op is other.op
            and self.operand.structurally_equals(other.operand)
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class AliasExpr(Expr):
    """Expression that represents an alias for an expression.

    This expression type allows you to assign a new name to the result
    of an expression. It's particularly useful when you want to specify
    the output column name directly in the expression.

    Args:
        expr: The expression to alias
        _name: The alias name for the expression

    Example:
        >>> from ray.data.expressions import col
        >>> # Create an expression with an alias
        >>> expr = (col("price") * col("quantity")).alias("total_cost")
    """

    expr: Expr
    _name: str

    data_type: DataType = field(default_factory=lambda: DataType(object))

    @property
    def name(self) -> str:
        """Get the alias name."""
        return self._name

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, AliasExpr)
            and self.expr.structurally_equals(other.expr)
            and self.name == other.name
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
        >>> from ray.data.expressions import col, lit, when
        >>> # Create a case expression: CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END
        >>> expr = when(col("age") > 30, lit("Senior")).otherwise(lit("Junior"))
    """

    when_clauses: List[Tuple[Expr, Expr]]
    default: Expr

    data_type: DataType = field(default_factory=lambda: DataType(object), init=False)

    def __post_init__(self):
        """Validate CaseExpr construction."""
        if not self.when_clauses:
            raise ValueError(
                "CaseExpr requires at least one when clause. "
                "Use when(condition, value).otherwise(default) to create case expressions."
            )

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
        >>> expr = when(col("age") > 50, lit("Elder"))
        >>> expr = expr.when(col("age") > 30, lit("Adult"))
        >>> expr = expr.otherwise(lit("Young"))
    """

    condition: Expr
    value: Expr
    next_when: "WhenExpr | None" = None

    data_type: DataType = field(default_factory=lambda: DataType(object), init=False)

    def when(self, condition: "Expr", value: "Expr") -> "WhenExpr":
        """Add another WHEN clause to the case statement.

        Args:
            condition: The boolean condition expression
            value: The value expression when the condition is True

        Returns:
            A new WhenExpr with the additional WHEN clause

        Example:
            >>> expr = when(col("age") > 30, lit("Adult"))
            >>> expr = expr.when(col("age") > 18, lit("Young"))
        """
        return WhenExpr(condition, value, next_when=self)

    def otherwise(self, default: "Expr") -> "CaseExpr":
        """Complete the case statement with a default value.

        Args:
            default: The default value expression when no conditions match

        Returns:
            A complete CaseExpr representing the full case statement

        Example:
            >>> expr = when(col("age") > 30, lit("Senior"))
            >>> expr = expr.otherwise(lit("Junior"))
        """
        # Build the when_clauses list by traversing the linked list chain
        when_clauses = []
        current = self
        while current is not None:
            when_clauses.append((current.condition, current.value))
            current = current.next_when

        # Reverse to get the correct evaluation order
        # (first when() call should be evaluated first)
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


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class UDFExpr(Expr):
    """Expression that represents a user-defined function call.

    This expression type wraps a UDF with schema inference capabilities,
    allowing UDFs to be used seamlessly within the expression system.

    UDFs operate on batches of data, where each column argument is passed
    as a PyArrow Array containing multiple values from that column across the batch.

    Args:
        fn: The user-defined function to call
        args: List of argument expressions (positional arguments)
        kwargs: Dictionary of keyword argument expressions

    Example:
        >>> from ray.data.expressions import col, udf
        >>> import pyarrow as pa
        >>> import pyarrow.compute as pc
        >>>
        >>> @udf(return_dtype=DataType.int32())
        ... def add_one(x: pa.Array) -> pa.Array:
        ...     return pc.add(x, 1)
        >>>
        >>> # Use in expressions
        >>> expr = add_one(col("value"))
    """

    fn: Callable[..., BatchColumn]
    args: List[Expr]
    kwargs: Dict[str, Expr]

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, UDFExpr)
            and self.fn == other.fn
            and len(self.args) == len(other.args)
            and all(a.structurally_equals(b) for a, b in zip(self.args, other.args))
            and self.kwargs.keys() == other.kwargs.keys()
            and all(
                self.kwargs[k].structurally_equals(other.kwargs[k])
                for k in self.kwargs.keys()
            )
        )


def _create_udf_callable(
    fn: Callable[..., BatchColumn], return_dtype: DataType
) -> Callable[..., UDFExpr]:
    """Create a callable that generates UDFExpr when called with expressions.

    This wrapper enables UDFs to be called with a mix of expressions and literal
    values, automatically converting literals to LiteralExpr nodes.

    Args:
        fn: The original UDF function
        return_dtype: The return data type of the UDF

    Returns:
        A callable that creates UDFExpr instances
    """

    def udf_callable(*args, **kwargs) -> UDFExpr:
        # Convert all arguments to expressions
        # This allows calls like: my_udf(col("x"), 5)
        # where 5 gets wrapped as LiteralExpr(5)
        expr_args = []
        for arg in args:
            if isinstance(arg, Expr):
                expr_args.append(arg)
            else:
                expr_args.append(LiteralExpr(arg))

        expr_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, Expr):
                expr_kwargs[k] = v
            else:
                expr_kwargs[k] = LiteralExpr(v)

        # Create the UDF expression node
        return UDFExpr(
            fn=fn,
            args=expr_args,
            kwargs=expr_kwargs,
            data_type=return_dtype,
        )

    # Preserve original function metadata (name, docstring, etc.)
    functools.update_wrapper(udf_callable, fn)

    # Store the original function for access if needed
    udf_callable._original_fn = fn

    return udf_callable


@PublicAPI(stability="alpha")
def udf(return_dtype: DataType) -> Callable[..., UDFExpr]:
    """
    Decorator to convert a UDF into an expression-compatible function.

    This decorator allows UDFs to be used seamlessly within the expression system,
    enabling schema inference and integration with other expressions.

    IMPORTANT: UDFs operate on batches of data, not individual rows. When your UDF
    is called, each column argument will be passed as a PyArrow Array containing
    multiple values from that column across the batch. Under the hood, when working
    with multiple columns, they get translated to PyArrow arrays (one array per column).

    Args:
        return_dtype: The data type of the return value of the UDF

    Returns:
        A callable that creates UDFExpr instances when called with expressions

    Example:
        >>> from ray.data.expressions import col, udf
        >>> import pyarrow as pa
        >>> import pyarrow.compute as pc
        >>> import ray
        >>>
        >>> # UDF that operates on a batch of values (PyArrow Array)
        >>> @udf(return_dtype=DataType.int32())
        ... def add_one(x: pa.Array) -> pa.Array:
        ...     return pc.add(x, 1)  # Vectorized operation on the entire Array
        >>>
        >>> # UDF that combines multiple columns (each as a PyArrow Array)
        >>> @udf(return_dtype=DataType.string())
        ... def format_name(first: pa.Array, last: pa.Array) -> pa.Array:
        ...     return pc.binary_join_element_wise(first, last, " ")  # Vectorized string concatenation
        >>>
        >>> # Use in dataset operations
        >>> ds = ray.data.from_items([
        ...     {"value": 5, "first": "John", "last": "Doe"},
        ...     {"value": 10, "first": "Jane", "last": "Smith"}
        ... ])
        >>>
        >>> # Single column transformation (operates on batches)
        >>> ds_incremented = ds.with_column("value_plus_one", add_one(col("value")))
        >>>
        >>> # Multi-column transformation (each column becomes a PyArrow Array)
        >>> ds_formatted = ds.with_column("full_name", format_name(col("first"), col("last")))
        >>>
        >>> # Can also be used in complex expressions
        >>> ds_complex = ds.with_column("doubled_plus_one", add_one(col("value")) * 2)
    """

    def decorator(func: Callable[..., BatchColumn]) -> Callable[..., UDFExpr]:
        return _create_udf_callable(func, return_dtype)

    return decorator


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False)
class DownloadExpr(Expr):
    """Expression that represents a download operation."""

    uri_column_name: str
    data_type: DataType = field(default_factory=lambda: DataType.binary(), init=False)

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, DownloadExpr)
            and self.uri_column_name == other.uri_column_name
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
    return ColumnExpr(_name=name)


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
def when(condition: "Expr", value: "Expr") -> "WhenExpr":
    """Create conditional case statements using method chaining.

    This function creates case statements similar to SQL CASE WHEN, PySpark, and Polars,
    using an intuitive method chaining pattern. Chain multiple conditions with .when()
    and complete with .otherwise() for the default case.

    Case expressions are evaluated in order - the first condition that evaluates to True
    determines the result. This is similar to if-elif-else statements in Python.

    Args:
        condition: The boolean condition expression for the first WHEN clause
        value: The value expression when the condition is True

    Returns:
        A WhenExpr that can be chained with more .when() calls or completed
        with .otherwise()

    Examples:
        Basic usage with simple conditions:

        >>> from ray.data.expressions import col, lit, when
        >>> import ray
        >>> ds = ray.data.from_items([{"age": 25}, {"age": 35}, {"age": 55}])
        >>>
        >>> # Simple case statement
        >>> ds = ds.with_column(
        ...     "age_group",
        ...     when(col("age") > 50, lit("Elder")).otherwise(lit("Young"))
        ... )

        Multiple conditions evaluated in order:

        >>> ds = ds.with_column(
        ...     "category",
        ...     when(col("age") > 50, lit("Elder"))
        ...     .when(col("age") > 30, lit("Adult"))
        ...     .when(col("age") > 18, lit("Young"))
        ...     .otherwise(lit("Minor"))
        ... )

        Complex conditions with boolean operators:

        >>> ds = ray.data.from_items([
        ...     {"age": 25, "income": 50000, "credit_score": 750}
        ... ])
        >>> ds = ds.with_column(
        ...     "loan_category",
        ...     when(
        ...         (col("age") >= 30) & (col("income") >= 100000) & (col("credit_score") >= 700),
        ...         lit("Premium")
        ...     )
        ...     .when((col("age") >= 25) | (col("credit_score") >= 750), lit("Standard"))
        ...     .otherwise(lit("Basic"))
        ... )

        Handling null values:

        >>> ds = ray.data.from_items([
        ...     {"score": 95, "age": None},
        ...     {"score": 82, "age": 25}
        ... ])
        >>> ds = ds.with_column(
        ...     "age_status",
        ...     when(col("age").is_null(), lit("Unknown"))
        ...     .when(col("age") < 30, lit("Young"))
        ...     .otherwise(lit("Experienced"))
        ... )

        Using new operators (!=, //, is_in):

        >>> ds = ray.data.from_items([
        ...     {"status": "active", "score": 95},
        ...     {"status": "pending", "score": 82}
        ... ])
        >>> ds = ds.with_column(
        ...     "priority",
        ...     when(col("score") != 100, lit("Standard")).otherwise(lit("Perfect"))
        ... )
        >>> ds = ds.with_column(
        ...     "state",
        ...     when(col("status").is_in(["active", "approved"]), lit("Active"))
        ...     .otherwise(lit("Inactive"))
        ... )

        Nested case expressions for complex logic:

        >>> ds = ray.data.from_items([
        ...     {"score": 95, "extra_credit": 5},
        ...     {"score": 82, "extra_credit": 0}
        ... ])
        >>> ds = ds.with_column(
        ...     "final_grade",
        ...     when(
        ...         col("score") >= 90,
        ...         when(col("extra_credit") > 0, lit("A+")).otherwise(lit("A"))
        ...     )
        ...     .when(col("score") >= 80, lit("B"))
        ...     .otherwise(lit("C"))
        ... )

    Note:
        Case expressions are evaluated lazily and vectorized for efficiency. They work
        with both pandas and PyArrow backends transparently.

    See Also:
        - :class:`~ray.data.expressions.CaseExpr`: The result of a completed case statement
        - :class:`~ray.data.expressions.WhenExpr`: The intermediate chained expression
    """
    return WhenExpr(condition, value)


@DeveloperAPI(stability="alpha")
def download(uri_column_name: str) -> DownloadExpr:
    """
    Create a download expression that downloads content from URIs.

    This creates an expression that will download bytes from URIs stored in
    a specified column. When evaluated, it will fetch the content from each URI
    and return the downloaded bytes.

    Args:
        uri_column_name: The name of the column containing URIs to download from
    Returns:
        A DownloadExpr that will download content from the specified URI column

    Example:
        >>> from ray.data.expressions import download
        >>> import ray
        >>> # Create dataset with URIs
        >>> ds = ray.data.from_items([
        ...     {"uri": "s3://bucket/file1.jpg", "id": "1"},
        ...     {"uri": "s3://bucket/file2.jpg", "id": "2"}
        ... ])
        >>> # Add downloaded bytes column
        >>> ds_with_bytes = ds.with_column("bytes", download("uri"))
    """
    return DownloadExpr(uri_column_name=uri_column_name)


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
    "UnaryExpr",
    "AliasExpr",
    "CaseExpr",
    "WhenExpr",
    "col",
    "lit",
    "when",
    "UDFExpr",
    "udf",
    "DownloadExpr",
    "download",
]
