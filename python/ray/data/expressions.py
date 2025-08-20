from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from ray.data.datatype import DataType
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
        return_dtype: Return data type for schema inference
        function_name: Optional name for the function (for debugging)

    Example:
        >>> from ray.data.expressions import col, udf
        >>> from ray.data.types import DataType
        >>> import pyarrow as pa
        >>> import pyarrow.compute as pc
        >>>
        >>> @udf(return_dtype=DataType.int64())
        >>> def add_one(x: pa.Array) -> pa.Array:
        ...     return pc.add(x, 1)
        >>>
        >>> # Use in expressions
        >>> expr = add_one(col("value"))
    """

    fn: Callable
    args: List[Expr]
    kwargs: Dict[str, Expr]
    return_dtype: DataType
    function_name: Optional[str] = None

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
            and self.return_dtype == other.return_dtype
            and self.function_name == other.function_name
        )


def _create_udf_callable(fn: Callable, return_dtype: DataType):
    """Create a callable that generates UDFExpr when called with expressions."""

    def udf_callable(*args, **kwargs):
        # Convert arguments to expressions if they aren't already
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

        return UDFExpr(
            fn=fn,
            args=expr_args,
            kwargs=expr_kwargs,
            return_dtype=return_dtype,
            function_name=getattr(fn, "__name__", None),
        )

    # Preserve original function metadata
    functools.update_wrapper(udf_callable, fn)

    # Store the original function for access if needed
    udf_callable._original_fn = fn
    udf_callable._return_dtype = return_dtype

    return udf_callable


@PublicAPI(stability="alpha")
def udf(fn: Optional[Callable] = None, *, return_dtype: DataType):
    """
    Decorator to convert a UDF into an expression-compatible function.

    This decorator allows UDFs to be used seamlessly within the expression system,
    enabling schema inference and integration with other expressions.

    IMPORTANT: UDFs operate on batches of data, not individual rows. When your UDF
    is called, each column argument will be passed as a PyArrow Array containing
    multiple values from that column across the batch. Under the hood, when working
    with multiple columns, they get translated to PyArrow arrays (one array per column).

    Args:
        fn: The function to decorate (when used as @udf)
        return_dtype: Required return data type for schema inference.
    Returns:
        A callable that creates UDFExpr instances when called with expressions

    Example:
        >>> from ray.data.expressions import col, udf
        >>> from ray.data.types import DataType
        >>> import pyarrow as pa
        >>> import pyarrow.compute as pc
        >>> import ray
        >>>
        >>> # UDF that operates on a batch of values (PyArrow Array)
        >>> @udf(return_dtype=DataType.int64())
        >>> def add_one(x: pa.Array) -> pa.Array:
        ...     return pc.add(x, 1)  # Vectorized operation on the entire Array
        >>>
        >>> # UDF that combines multiple columns (each as a PyArrow Array)
        >>> @udf(return_dtype=DataType.string())
        >>> def format_name(first: pa.Array, last: pa.Array) -> pa.Array:
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

    def decorator(func: Callable):
        return _create_udf_callable(func, return_dtype)

    # Handle both @udf and @udf(...) syntax
    if fn is not None:
        # This is the @udf syntax without parentheses, which is no longer supported
        # since return_dtype is mandatory
        raise ValueError(
            "return_dtype is required for UDF expressions. "
            f"Please use @udf(return_dtype=DataType...) instead of @udf for function '{fn.__name__}'"
        )
    else:
        return decorator


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
    "UDFExpr",
    "udf",
    "col",
    "lit",
]
