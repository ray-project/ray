from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import inspect
import functools

from ray.util.annotations import DeveloperAPI, PublicAPI


# Import the DataType class
from pydantic import BaseModel, field_validator
import pyarrow as pa
import numpy as np


class DataType(BaseModel):
    """A simplified Ray Data DataType supporting Arrow, NumPy, and Python types."""

    _internal_type: Union[pa.DataType, np.dtype, type]

    class Config:
        arbitrary_types_allowed = True

    @field_validator("_internal_type")
    def validate_type(cls, v):
        if not isinstance(v, (pa.DataType, np.dtype, type)):
            raise TypeError(
                "DataType supports only PyArrow DataType, NumPy dtype, or Python type."
            )
        return v

    # Type checking methods
    def is_arrow_type(self) -> bool:
        return isinstance(self._internal_type, pa.DataType)

    def is_numpy_type(self) -> bool:
        return isinstance(self._internal_type, np.dtype)

    def is_python_type(self) -> bool:
        return isinstance(self._internal_type, type)

    # Conversion methods
    def to_arrow_dtype(self) -> pa.DataType:
        if self.is_arrow_type():
            return self._internal_type
        elif self.is_numpy_type():
            return pa.from_numpy_dtype(self._internal_type)
        else:
            return pa.string()

    def to_numpy_dtype(self) -> np.dtype:
        if self.is_numpy_type():
            return self._internal_type
        elif self.is_arrow_type():
            return self._internal_type.to_pandas_dtype()
        else:
            return np.dtype("object")

    def to_python_type(self) -> type:
        if self.is_python_type():
            return self._internal_type
        else:
            raise ValueError(f"DataType {self} is not a Python type")

    # Factory methods for Arrow types
    @classmethod
    def int8(cls) -> "DataType":
        return cls(_internal_type=pa.int8())

    @classmethod
    def int16(cls) -> "DataType":
        return cls(_internal_type=pa.int16())

    @classmethod
    def int32(cls) -> "DataType":
        return cls(_internal_type=pa.int32())

    @classmethod
    def int64(cls) -> "DataType":
        return cls(_internal_type=pa.int64())

    @classmethod
    def uint8(cls) -> "DataType":
        return cls(_internal_type=pa.uint8())

    @classmethod
    def uint16(cls) -> "DataType":
        return cls(_internal_type=pa.uint16())

    @classmethod
    def uint32(cls) -> "DataType":
        return cls(_internal_type=pa.uint32())

    @classmethod
    def uint64(cls) -> "DataType":
        return cls(_internal_type=pa.uint64())

    @classmethod
    def float32(cls) -> "DataType":
        return cls(_internal_type=pa.float32())

    @classmethod
    def float64(cls) -> "DataType":
        return cls(_internal_type=pa.float64())

    @classmethod
    def string(cls) -> "DataType":
        return cls(_internal_type=pa.string())

    @classmethod
    def bool(cls) -> "DataType":
        return cls(_internal_type=pa.bool_())

    @classmethod
    def binary(cls) -> "DataType":
        return cls(_internal_type=pa.binary())

    # Factory methods from external systems
    @classmethod
    def from_arrow(cls, arrow_type: pa.DataType) -> "DataType":
        return cls(_internal_type=arrow_type)

    @classmethod
    def from_numpy(cls, numpy_dtype: Union[np.dtype, str]) -> "DataType":
        if isinstance(numpy_dtype, str):
            numpy_dtype = np.dtype(numpy_dtype)
        return cls(_internal_type=numpy_dtype)

    @classmethod
    def from_python(cls, python_type: type) -> "DataType":
        return cls(_internal_type=python_type)

    def __repr__(self) -> str:
        if self.is_arrow_type():
            return f"DataType(arrow:{self._internal_type})"
        elif self.is_numpy_type():
            return f"DataType(numpy:{self._internal_type})"
        else:
            return f"DataType(python:{self._internal_type.__name__})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, DataType):
            return False
        return self._internal_type == other._internal_type

    def __hash__(self) -> int:
        return hash(str(self._internal_type))


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

    Args:
        fn: The user-defined function to call
        args: List of argument expressions (positional arguments)
        kwargs: Dictionary of keyword argument expressions
        return_dtype: Optional return data type for schema inference
        function_name: Optional name for the function (for debugging)

    Example:
        >>> from ray.data.expressions import col, udf
        >>>
        >>> @udf
        >>> def add_one(x: int) -> int:
        ...     return x + 1
        >>>
        >>> # Use in expressions
        >>> expr = add_one(col("value"))
    """

    fn: Callable
    args: List[Expr]
    kwargs: Dict[str, Expr]
    return_dtype: Optional[DataType] = None
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
        >>> # Use with Dataset.with_columns()
        >>> import ray
        >>> ds = ray.data.from_items([{"price": 10, "quantity": 2}])
        >>> ds = ds.with_columns({"total": col("price") * col("quantity")})
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
        >>> # Use with Dataset.with_columns()
        >>> import ray
        >>> ds = ray.data.from_items([{"age": 25}, {"age": 30}])
        >>> ds = ds.with_columns({"age_plus_one": col("age") + lit(1)})
    """
    return LiteralExpr(value)


def _infer_return_dtype_from_annotation(fn: Callable) -> Optional[DataType]:
    """Infer the return DataType from function type annotations."""
    sig = inspect.signature(fn)
    if sig.return_annotation != inspect.Signature.empty:
        return_annotation = sig.return_annotation

        # Convert common Python types to DataType
        if isinstance(return_annotation, int):
            return DataType.from_python(int)
        elif isinstance(return_annotation, float):
            return DataType.from_python(float)
        elif isinstance(return_annotation, str):
            return DataType.from_python(str)
        elif isinstance(return_annotation, bool):
            return DataType.from_python(bool)
        elif isinstance(return_annotation, type):
            return DataType.from_python(return_annotation)

    return None


def _create_udf_callable(fn: Callable, return_dtype: Optional[DataType] = None):
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
def udf(fn: Optional[Callable] = None, *, return_dtype: Optional[DataType] = None):
    """
    Decorator to convert a UDF into an expression-compatible function.

    This decorator allows UDFs to be used seamlessly within the expression system,
    enabling schema inference and integration with other expressions.

    Args:
        fn: The function to decorate (when used as @udf)
        return_dtype: Optional explicit return data type for schema inference.
                     If not provided, will attempt to infer from type annotations.

    Returns:
        A callable that creates UDFExpr instances when called with expressions

    Example:
        >>> from ray.data.expressions import col, udf, DataType
        >>> import ray
        >>>
        >>> @udf
        >>> def add_one(x: int) -> int:
        ...     return x + 1
        >>>
        >>> @udf(return_dtype=DataType.string())
        >>> def format_name(first: str, last: str) -> str:
        ...     return f"{first} {last}"
        >>>
        >>> # Use in dataset operations
        >>> ds = ray.data.from_items([{"value": 5, "first": "John", "last": "Doe"}])
        >>>
        >>> # Single column transformation
        >>> ds_incremented = ds.with_column("value_plus_one", add_one(col("value")))
        >>>
        >>> # Multi-column transformation
        >>> ds_formatted = ds.with_column("full_name", format_name(col("first"), col("last")))
        >>>
        >>> # Can also be used in complex expressions
        >>> ds_complex = ds.with_column("doubled_plus_one", add_one(col("value")) * 2)
    """

    def decorator(func: Callable):
        # Try to infer return dtype from annotations if not explicitly provided
        inferred_return_dtype = return_dtype or _infer_return_dtype_from_annotation(
            func
        )
        return _create_udf_callable(func, inferred_return_dtype)

    # Handle both @udf and @udf(...) syntax
    if fn is not None:
        return decorator(fn)
    else:
        return decorator


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
    "DataType",
    "col",
    "lit",
    "udf",
]
