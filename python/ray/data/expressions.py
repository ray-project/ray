from __future__ import annotations

import functools
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    TypeVar,
    Union,
)

import pyarrow
import pyarrow.compute as pc
from typing_extensions import override

from ray.data.block import BatchColumn, Schema
from ray.data.datatype import DataType
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data.namespace_expressions.dt_namespace import _DatetimeNamespace
    from ray.data.namespace_expressions.list_namespace import _ListNamespace
    from ray.data.namespace_expressions.string_namespace import _StringNamespace
    from ray.data.namespace_expressions.struct_namespace import _StructNamespace

T = TypeVar("T")


# TODO(Justin): I'm going to assume this is annoying for users to use if they want
# to find all their options. So I'm going to add registry for all operations later.
# This TODO is a reminder.
@DeveloperAPI(stability="alpha")
class Operation:
    """Mixin for operations that provide dtype inference."""

    def infer_dtype(self, *dtype: DataType) -> DataType:
        raise NotImplementedError(
            f"{self.__class__.__name__}.infer_dtype() must be implemented"
        )


@DeveloperAPI(stability="alpha")
class UnaryOperation(Operation, Enum):
    """Enumeration of supported operations in expressions.

    This enum defines all the binary operations that can be performed
    between expressions, including arithmetic, comparison, and boolean operations.

    Attributes:
        NOT: Logical NOT operation (~)
        IS_NULL: Check if value is null
        IS_NOT_NULL: Check if value is not null
    """

    NOT = "not"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"

    @override
    def infer_dtype(self, dtype: DataType) -> DataType:
        match self:
            case self.NOT | self.IS_NULL | self.IS_NOT_NULL:
                return DataType.bool()
        raise RuntimeError(f"Unreachable {self}")


@DeveloperAPI(stability="alpha")
class BinaryOperation(Operation, Enum):
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
    IN = "in"
    NOT_IN = "not_in"

    @override
    def infer_dtype(self, l_dtype: DataType, r_dtype: DataType) -> DataType:
        import pyarrow as pa

        match self:
            case self.ADD:
                # ADD: numbers + numbers
                if l_dtype.is_numerical_type() and r_dtype.is_numerical_type():
                    left = pa.scalar(1, l_dtype.to_arrow_dtype())
                    right = pa.scalar(1, r_dtype.to_arrow_dtype())
                    return DataType.from_arrow((left + right).type)
                # ADD: string + string
                elif l_dtype.is_string_type() and r_dtype.is_string_type():
                    # String concatenation returns the same string type
                    return l_dtype
                else:
                    raise TypeError(
                        f"ADD operation not supported for types {l_dtype} and {r_dtype}"
                    )

            case self.MUL:
                # MUL: numbers * numbers
                if l_dtype.is_numerical_type() and r_dtype.is_numerical_type():
                    left = pa.scalar(1, l_dtype.to_arrow_dtype())
                    right = pa.scalar(1, r_dtype.to_arrow_dtype())
                    return DataType.from_arrow((left * right).type)
                # MUL: string * integer (repetition)
                elif l_dtype.is_string_type() and pa.types.is_integer(
                    r_dtype.to_arrow_dtype()
                ):
                    return l_dtype
                # MUL: integer * string (repetition)
                elif (
                    pa.types.is_integer(l_dtype.to_arrow_dtype())
                    and r_dtype.is_string_type()
                ):
                    return r_dtype
                else:
                    raise TypeError(
                        f"MUL operation not supported for types {l_dtype} and {r_dtype}"
                    )

            case self.SUB | self.DIV | self.FLOORDIV:
                # SUB, DIV, FLOORDIV: only numbers
                if l_dtype.is_numerical_type() and r_dtype.is_numerical_type():
                    left = pa.scalar(1, l_dtype.to_arrow_dtype())
                    right = pa.scalar(1, r_dtype.to_arrow_dtype())

                    if self == self.SUB:
                        result = left - right
                    elif self == self.DIV:
                        result = left / right
                    else:  # FLOORDIV
                        result = left // right

                    return DataType.from_arrow(result.type)
                else:
                    raise TypeError(
                        f"{self.name} operation not supported for types {l_dtype} and {r_dtype}"
                    )

            case (
                self.GT
                | self.LT
                | self.GE
                | self.LE
                | self.EQ
                | self.NE
                | self.AND
                | self.OR
                | self.IN
                | self.NOT_IN
            ):
                return DataType.bool()

        raise RuntimeError(f"Unreachable {self}")


class _ExprVisitor(ABC, Generic[T]):
    """Base visitor with generic dispatch for Ray Data expressions."""

    def visit(self, expr: "Expr") -> T:
        # Check for specific column types before checking parent classes
        # to avoid incorrect routing (AliasExpr and StarExpr inherit from NamedExpr)
        if isinstance(expr, AliasExpr):
            return self.visit_alias(expr)
        elif isinstance(expr, StarExpr):
            return self.visit_star(expr)
        elif isinstance(expr, ResolvedColumnExpr):
            return self.visit_resolved_column(expr)
        elif isinstance(expr, UnresolvedColumnExpr):
            return self.visit_unresolved_column(expr)
        elif isinstance(expr, LiteralExpr):
            return self.visit_literal(expr)
        elif isinstance(expr, BinaryExpr):
            return self.visit_binary(expr)
        elif isinstance(expr, UnaryExpr):
            return self.visit_unary(expr)
        elif isinstance(expr, UDFExpr):
            return self.visit_udf(expr)
        elif isinstance(expr, DownloadExpr):
            return self.visit_download(expr)
        else:
            raise TypeError(f"Unsupported expression type for conversion: {type(expr)}")

    @abstractmethod
    def visit_resolved_column(self, expr: "ResolvedColumnExpr") -> T:
        pass

    @abstractmethod
    def visit_unresolved_column(self, expr: "UnresolvedColumnExpr") -> T:
        pass

    @abstractmethod
    def visit_literal(self, expr: "LiteralExpr") -> T:
        pass

    @abstractmethod
    def visit_binary(self, expr: "BinaryExpr") -> T:
        pass

    @abstractmethod
    def visit_unary(self, expr: "UnaryExpr") -> T:
        pass

    @abstractmethod
    def visit_alias(self, expr: "AliasExpr") -> T:
        pass

    @abstractmethod
    def visit_udf(self, expr: "UDFExpr") -> T:
        pass

    @abstractmethod
    def visit_star(self, expr: "StarExpr") -> T:
        pass

    @abstractmethod
    def visit_download(self, expr: "DownloadExpr") -> T:
        pass


class _PyArrowExpressionVisitor(_ExprVisitor["pyarrow.compute.Expression"]):
    """Visitor that converts Ray Data expressions to PyArrow compute expressions."""

    def visit_resolved_column(
        self, expr: "ResolvedColumnExpr"
    ) -> "pyarrow.compute.Expression":
        return pc.field(expr.name)

    def visit_unresolved_column(
        self, expr: "UnresolvedColumnExpr"
    ) -> "pyarrow.compute.Expression":
        return pc.field(expr.name)

    def visit_literal(self, expr: "LiteralExpr") -> "pyarrow.compute.Expression":

        return pc.scalar(expr.value)

    def visit_binary(self, expr: "BinaryExpr") -> "pyarrow.compute.Expression":
        import pyarrow as pa

        if expr.op in (BinaryOperation.IN, BinaryOperation.NOT_IN):
            left = self.visit(expr.left)
            if isinstance(expr.right, LiteralExpr):
                right_value = expr.right.value
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
            result = pc.is_in(left, right)
            return pc.invert(result) if expr.op == BinaryOperation.NOT_IN else result

        left = self.visit(expr.left)
        right = self.visit(expr.right)
        from ray.data._internal.planner.plan_expression.expression_evaluator import (
            _ARROW_EXPR_OPS_MAP,
        )

        if expr.op in _ARROW_EXPR_OPS_MAP:
            return _ARROW_EXPR_OPS_MAP[expr.op](left, right)
        raise ValueError(f"Unsupported binary operation for PyArrow: {expr.op}")

    def visit_unary(self, expr: "UnaryExpr") -> "pyarrow.compute.Expression":
        operand = self.visit(expr.operand)
        from ray.data._internal.planner.plan_expression.expression_evaluator import (
            _ARROW_EXPR_OPS_MAP,
        )

        if expr.op in _ARROW_EXPR_OPS_MAP:
            return _ARROW_EXPR_OPS_MAP[expr.op](operand)
        raise ValueError(f"Unsupported unary operation for PyArrow: {expr.op}")

    def visit_alias(self, expr: "AliasExpr") -> "pyarrow.compute.Expression":
        return self.visit(expr.expr)

    def visit_udf(self, expr: "UDFExpr") -> "pyarrow.compute.Expression":
        raise TypeError("UDF expressions cannot be converted to PyArrow expressions")

    def visit_download(self, expr: "DownloadExpr") -> "pyarrow.compute.Expression":
        raise TypeError(
            "Download expressions cannot be converted to PyArrow expressions"
        )

    def visit_star(self, expr: "StarExpr") -> "pyarrow.compute.Expression":
        raise TypeError("Star expressions cannot be converted to PyArrow expressions")


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
        >>> # left=BinaryExpr(op=ADD, left=UnresolvedColumnExpr("x"), right=LiteralExpr(5))
        >>> # right=UnresolvedColumnExpr("y")

    Note:
        This class should not be instantiated directly. Use the concrete
        subclasses like ResolvedColumnExpr, UnresolvedColumnExpr, LiteralExpr, etc.
    """

    # data_type: DataType

    # TODO(Justin): This will probably break things
    # @property
    # def name(self) -> str | None:
    #     """Get the name associated with this expression.
    #
    #     Returns:
    #         The name for expressions that have one (ResolvedColumnExpr, UnresolvedColumnExpr, AliasExpr),
    #         None otherwise.
    #     """
    #     return None

    @property
    @abstractmethod
    def data_type(self) -> DataType:
        """Return data type.
        - If resolved=False: MAY throw an exception
        - If resolved=True: MUST return a valid DataType
        """
        ...

    @abstractmethod
    def _is_resolved(self) -> bool:
        ...

    @abstractmethod
    def structurally_equals(self, other: Any) -> bool:
        """Compare two expression ASTs for structural equality."""
        raise NotImplementedError

    def to_pyarrow(self) -> "pyarrow.compute.Expression":
        """Convert this Ray Data expression to a PyArrow compute expression.

        Returns:
            A PyArrow compute expression equivalent to this Ray Data expression.

        Raises:
            ValueError: If the expression contains operations not supported by PyArrow.
            TypeError: If the expression type cannot be converted to PyArrow.
        """
        return _PyArrowExpressionVisitor().visit(self)

    def __repr__(self) -> str:
        """Return a tree-structured string representation of the expression.

        Returns:
            A multi-line string showing the expression tree structure using
            box-drawing characters for visual clarity.

        Example:
            >>> from ray.data.expressions import col, lit
            >>> expr = (col("x") + lit(5)) * col("y")
            >>> print(expr)
            MUL
                ├── left: ADD
                │   ├── left: UNRESOLVED_COL('x')
                │   └── right: LIT(5)
                └── right: UNRESOLVED_COL('y')
        """
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            _TreeReprVisitor,
        )

        return _TreeReprVisitor().visit(self)

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
        return self._bin(other, BinaryOperation.ADD)

    def __radd__(self, other: Any) -> "Expr":
        """Reverse addition operator (for literal + expr)."""
        return LiteralExpr(other)._bin(self, BinaryOperation.ADD)

    def __sub__(self, other: Any) -> "Expr":
        """Subtraction operator (-)."""
        return self._bin(other, BinaryOperation.SUB)

    def __rsub__(self, other: Any) -> "Expr":
        """Reverse subtraction operator (for literal - expr)."""
        return LiteralExpr(other)._bin(self, BinaryOperation.SUB)

    def __mul__(self, other: Any) -> "Expr":
        """Multiplication operator (*)."""
        return self._bin(other, BinaryOperation.MUL)

    def __rmul__(self, other: Any) -> "Expr":
        """Reverse multiplication operator (for literal * expr)."""
        return LiteralExpr(other)._bin(self, BinaryOperation.MUL)

    def __truediv__(self, other: Any) -> "Expr":
        """Division operator (/)."""
        return self._bin(other, BinaryOperation.DIV)

    def __rtruediv__(self, other: Any) -> "Expr":
        """Reverse division operator (for literal / expr)."""
        return LiteralExpr(other)._bin(self, BinaryOperation.DIV)

    def __floordiv__(self, other: Any) -> "Expr":
        """Floor division operator (//)."""
        return self._bin(other, BinaryOperation.FLOORDIV)

    def __rfloordiv__(self, other: Any) -> "Expr":
        """Reverse floor division operator (for literal // expr)."""
        return LiteralExpr(other)._bin(self, BinaryOperation.FLOORDIV)

    # comparison
    def __gt__(self, other: Any) -> "Expr":
        """Greater than operator (>)."""
        return self._bin(other, BinaryOperation.GT)

    def __lt__(self, other: Any) -> "Expr":
        """Less than operator (<)."""
        return self._bin(other, BinaryOperation.LT)

    def __ge__(self, other: Any) -> "Expr":
        """Greater than or equal operator (>=)."""
        return self._bin(other, BinaryOperation.GE)

    def __le__(self, other: Any) -> "Expr":
        """Less than or equal operator (<=)."""
        return self._bin(other, BinaryOperation.LE)

    def __eq__(self, other: Any) -> "Expr":
        """Equality operator (==)."""
        return self._bin(other, BinaryOperation.EQ)

    def __ne__(self, other: Any) -> "Expr":
        """Not equal operator (!=)."""
        return self._bin(other, BinaryOperation.NE)

    # boolean
    def __and__(self, other: Any) -> "Expr":
        """Logical AND operator (&)."""
        return self._bin(other, BinaryOperation.AND)

    def __or__(self, other: Any) -> "Expr":
        """Logical OR operator (|)."""
        return self._bin(other, BinaryOperation.OR)

    def __invert__(self) -> "Expr":
        """Logical NOT operator (~)."""
        return UnaryExpr(UnaryOperation.NOT, self)

    # predicate methods
    def is_null(self) -> "Expr":
        """Check if the expression value is null."""
        return UnaryExpr(UnaryOperation.IS_NULL, self)

    def is_not_null(self) -> "Expr":
        """Check if the expression value is not null."""
        return UnaryExpr(UnaryOperation.IS_NOT_NULL, self)

    def is_in(self, values: Union[List[Any], "Expr"]) -> "Expr":
        """Check if the expression value is in a list of values."""
        if not isinstance(values, Expr):
            values = LiteralExpr(values)
        return self._bin(values, BinaryOperation.IN)

    def not_in(self, values: Union[List[Any], "Expr"]) -> "Expr":
        """Check if the expression value is not in a list of values."""
        if not isinstance(values, Expr):
            values = LiteralExpr(values)
        return self._bin(values, BinaryOperation.NOT_IN)

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
            >>> from ray.data.expressions import col, lit
            >>> # Create an expression with a new aliased name
            >>> expr = (col("price") * col("quantity")).alias("total")
            >>> # Can be used with Dataset operations that support named expressions
        """
        return AliasExpr(expr=self, _name=name, _is_rename=False)

    @property
    def list(self) -> "_ListNamespace":
        """Access list operations for this expression.

        Returns:
            A _ListNamespace that provides list-specific operations.

        Example:
            >>> from ray.data.expressions import col
            >>> import ray
            >>> ds = ray.data.from_items([
            ...     {"items": [1, 2, 3]},
            ...     {"items": [4, 5]}
            ... ])
            >>> ds = ds.with_column("num_items", col("items").list.len())
            >>> ds = ds.with_column("first_item", col("items").list[0])
            >>> ds = ds.with_column("slice", col("items").list[1:3])
        """
        from ray.data.namespace_expressions.list_namespace import _ListNamespace

        return _ListNamespace(self)

    @property
    def str(self) -> "_StringNamespace":
        """Access string operations for this expression.

        Returns:
            A _StringNamespace that provides string-specific operations.

        Example:
            >>> from ray.data.expressions import col
            >>> import ray
            >>> ds = ray.data.from_items([
            ...     {"name": "Alice"},
            ...     {"name": "Bob"}
            ... ])
            >>> ds = ds.with_column("upper_name", col("name").str.upper())
            >>> ds = ds.with_column("name_len", col("name").str.len())
            >>> ds = ds.with_column("starts_a", col("name").str.starts_with("A"))
        """
        from ray.data.namespace_expressions.string_namespace import _StringNamespace

        return _StringNamespace(self)

    @property
    def struct(self) -> "_StructNamespace":
        """Access struct operations for this expression.

        Returns:
            A _StructNamespace that provides struct-specific operations.

        Example:
            >>> from ray.data.expressions import col
            >>> import ray
            >>> import pyarrow as pa
            >>> ds = ray.data.from_arrow(pa.table({
            ...     "user": pa.array([
            ...         {"name": "Alice", "age": 30}
            ...     ], type=pa.struct([
            ...         pa.field("name", pa.string()),
            ...         pa.field("age", pa.int32())
            ...     ]))
            ... }))
            >>> ds = ds.with_column("age", col("user").struct["age"])  # doctest: +SKIP
        """
        from ray.data.namespace_expressions.struct_namespace import _StructNamespace

        return _StructNamespace(self)

    @property
    def dt(self) -> "_DatetimeNamespace":
        """Access datetime operations for this expression."""
        from ray.data.namespace_expressions.dt_namespace import _DatetimeNamespace

        return _DatetimeNamespace(self)

    def _unalias(self) -> "Expr":
        return self


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
class NamedExpr(Expr, ABC):

    _name: str

    @property
    def name(self) -> str:
        return self._name

    def get_root_name(self) -> str:
        """The root name of a NamedExpr is the original source name after
        aliases or renames. For example
            >>> from ray.data.expressions import col
            >>> col("a").get_root_name() == "a"
            >>> col("a").alias("b").get_root_name() == "a"
            >>> col("a").alias("b").alias("c").get_root_name() == "a"
        """
        return self._name


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
class UnresolvedColumnExpr(NamedExpr):
    """Expression that references a column by name.

    This expression type represents an unresolved reference to a possibly
    existing column in the dataset. After resolution, it becomes a
    `ResolvedColumnExpr`

    Args:
        _name: The name of the column to reference

    Example:
        >>> from ray.data.expressions import col
        >>> # Reference the "age" column
        >>> age_expr = col("age") # Creates UnresolvedColumnExpr(name="age")
    """

    @override
    def _is_resolved(self) -> bool:
        return False

    @override
    @property
    def data_type(self) -> DataType:
        raise ValueError("Cannot infer data type for unresolved column")

    def _rename(self, name: str):
        return AliasExpr(_name=name, expr=self, _is_rename=True)

    @override
    def structurally_equals(self, other: Any) -> bool:
        return isinstance(other, UnresolvedColumnExpr) and self.name == other.name


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
class ResolvedColumnExpr(NamedExpr):
    """Expression that references a column by name.

    This expression type represents a reference to an existing column
    in the dataset. When evaluated, it returns the values from the
    specified column.

    Args:
        _name: The name of the column to reference
        _data_type: The data type of the column to refernce.

    Example:
        >>> from ray.data.expressions import col
        >>> # Reference the "age" column
        >>> age_expr = col("age") # Creates ResolvedColumnExpr(name="age")
    """

    _data_type: DataType = field(default_factory=lambda: DataType(object))

    @property
    def _is_resolved(self) -> bool:
        return True

    @override
    @property
    def data_type(self) -> DataType:
        return self._data_type

    def _rename(self, name: str):
        return AliasExpr(_name=name, expr=self, _is_rename=True)

    @override
    def structurally_equals(self, other: Any) -> bool:
        return isinstance(other, ResolvedColumnExpr) and self.name == other.name


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
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
    _data_type: DataType = field(init=False)

    def __post_init__(self):
        # Infer the type from the value using DataType.infer_dtype
        inferred_dtype = DataType.infer_dtype(self.value)

        # Use object.__setattr__ since the dataclass is frozen
        object.__setattr__(self, "_data_type", inferred_dtype)

    @override
    def _is_resolved(self) -> bool:
        return True

    @override
    @property
    def data_type(self) -> DataType:
        return self._data_type

    @override
    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, LiteralExpr)
            and self.value == other.value
            and type(self.value) is type(other.value)
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
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
        >>> from ray.data.expressions import col, lit, BinaryOperation
        >>> # Manually create a binary expression (usually done via operators)
        >>> expr = BinaryExpr(BinaryOperation.ADD, col("x"), lit(5))
        >>> # This is equivalent to: col("x") + lit(5)
    """

    op: BinaryOperation
    left: Expr
    right: Expr

    @override
    def _is_resolved(self) -> bool:
        return self.left._is_resolved() and self.right._is_resolved()

    @override
    @property
    def data_type(self) -> DataType:
        if not self._is_resolved():
            raise ValueError("Can't infer data type for unresolved binary expression")
        return self.op.infer_dtype(self.left, self.right)

    @override
    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, BinaryExpr)
            and self.op is other.op
            and self.left.structurally_equals(other.left)
            and self.right.structurally_equals(other.right)
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
class UnaryExpr(Expr):
    """Expression that represents a unary operation on a single expression.

    This expression type represents an operation with one operand.
    Common unary operations include logical NOT, IS NULL, IS NOT NULL, etc.

    Args:
        op: The operation to perform (from Operation enum)
        operand: The operand expression

    Example:
        >>> from ray.data.expressions import col
        >>> # Check if a column is null
        >>> expr = col("age").is_null()  # Creates UnaryExpr(IS_NULL, col("age"))
        >>> # Logical not
        >>> expr = ~(col("active"))  # Creates UnaryExpr(NOT, col("active"))
    """

    op: Operation
    operand: Expr

    # Default to bool return dtype for unary operations like is_null() and NOT.
    # This enables chaining operations such as col("x").is_not_null().alias("valid"),
    # where downstream expressions (like AliasExpr) need the data type.
    # data_type: DataType = field(default_factory=lambda: DataType.bool(), init=False)

    @override
    def _is_resolved(self) -> bool:
        return self.operand._is_resolved()

    @override
    @property
    def data_type(self) -> DataType:
        if not self._is_resolved():
            raise ValueError("Can't infer data type for unresolved unary expression")
        return self.op.infer_dtype(self.operand.data_type)

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, UnaryExpr)
            and self.op is other.op
            and self.operand.structurally_equals(other.operand)
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
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
    # TODO(Justin): Is this stricly required?
    _data_type: DataType

    @override
    def _is_resolved(self) -> bool:
        return all(arg._is_resolved() for arg in self.args) and all(
            kwarg._is_resolved() for kwarg in self.kwargs.values()
        )

    @override
    @property
    def data_type(self) -> DataType:
        # TODO(Justin): This behavior is a one off because we don't need to know
        # the arguments before knowing the data type.
        return self._data_type

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
    """Create a callable that generates UDFExpr when called with expressions."""

    def udf_callable(*args, **kwargs) -> UDFExpr:
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
            _data_type=return_dtype,
        )

    # Preserve original function metadata
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


def _create_pyarrow_wrapper(
    fn: Callable[..., BatchColumn]
) -> Callable[..., BatchColumn]:
    """Wrap a PyArrow compute function to auto-convert inputs to PyArrow format.

    This wrapper ensures that pandas Series and numpy arrays are converted to
    PyArrow Arrays before being passed to the function, enabling PyArrow compute
    functions to work seamlessly with any block format.

    Args:
        fn: The PyArrow compute function to wrap

    Returns:
        A wrapped function that handles format conversion
    """

    @functools.wraps(fn)
    def arrow_wrapper(*args, **kwargs):
        import numpy as np
        import pandas as pd
        import pyarrow as pa

        def to_arrow(val):
            """Convert a value to PyArrow Array if needed."""
            if isinstance(val, (pa.Array, pa.ChunkedArray)):
                return val, False
            elif isinstance(val, pd.Series):
                return pa.Array.from_pandas(val), True
            elif isinstance(val, np.ndarray):
                return pa.array(val), False
            else:
                return val, False

        # Convert inputs to PyArrow and track pandas flags
        args_results = [to_arrow(arg) for arg in args]
        kwargs_results = {k: to_arrow(v) for k, v in kwargs.items()}

        converted_args = [v[0] for v in args_results]
        converted_kwargs = {k: v[0] for k, v in kwargs_results.items()}
        input_was_pandas = any(v[1] for v in args_results) or any(
            v[1] for v in kwargs_results.values()
        )

        # Call function with converted inputs
        result = fn(*converted_args, **converted_kwargs)

        # Convert result back to pandas if input was pandas
        if input_was_pandas and isinstance(result, (pa.Array, pa.ChunkedArray)):
            result = result.to_pandas()

        return result

    return arrow_wrapper


@PublicAPI(stability="alpha")
def pyarrow_udf(return_dtype: DataType) -> Callable[..., UDFExpr]:
    """Decorator for PyArrow compute functions with automatic format conversion.

    This decorator wraps PyArrow compute functions to automatically convert pandas
    Series and numpy arrays to PyArrow Arrays, ensuring the function works seamlessly
    regardless of the underlying block format (pandas, arrow, or items).

    Used internally by namespace methods (list, str, struct) that wrap PyArrow
    compute functions.

    Args:
        return_dtype: The data type of the return value

    Returns:
        A callable that creates UDFExpr instances with automatic conversion
    """

    def decorator(func: Callable[..., BatchColumn]) -> Callable[..., UDFExpr]:
        # Wrap the function with PyArrow conversion logic
        wrapped_fn = _create_pyarrow_wrapper(func)
        # Create UDFExpr callable using the wrapped function
        return _create_udf_callable(wrapped_fn, return_dtype)

    return decorator


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
class DownloadExpr(Expr):
    """Expression that represents a download operation."""

    uri_column: NamedExpr

    @classmethod
    def from_uri(cls, uri_column_name: str) -> "DownloadExpr":
        return cls(uri_column=UnresolvedColumnExpr(_name=uri_column_name))

    @property
    def uri_column_name(self) -> str:
        """Get the name of the URI column for backward compatibility."""
        return self.uri_column.get_root_name()

    @override
    def _is_resolved(self) -> bool:
        return self.uri_column._is_resolved()

    @override
    @property
    def data_type(self) -> DataType:
        # TODO(Justin): See comment for UDFExpr, same idea, can infer datatype
        # before resolution.
        return DataType.binary()

    def structurally_equals(self, other: Any) -> bool:
        return isinstance(other, DownloadExpr) and self.uri_column.structurally_equals(
            other.uri_column
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
class AliasExpr(NamedExpr):
    """Expression that represents an alias for an expression."""

    expr: Expr
    _is_rename: bool

    def alias(self, name: str) -> "Expr":
        # Always unalias before creating new one
        return AliasExpr(_name=name, expr=self.expr, _is_rename=self._is_rename)

    @override
    def _is_resolved(self) -> bool:
        return self.expr._is_resolved()

    @override
    def get_root_name(self) -> str:
        if isinstance(self.expr, NamedExpr):
            return self.expr.get_root_name()
        return self._name

    @override
    @property
    def data_type(self) -> DataType:
        if not self._is_resolved():
            raise ValueError("Cannot infer data type of unresolved alias expression")
        return self.expr.data_type

    def _unalias(self) -> "Expr":
        return self.expr

    def structurally_equals(self, other: Any) -> bool:
        return (
            isinstance(other, AliasExpr)
            and self.expr.structurally_equals(other.expr)
            and self.name == other.name
            and self._is_rename == other._is_rename
        )


@DeveloperAPI(stability="alpha")
@dataclass(frozen=True, eq=False, repr=False)
class StarExpr(NamedExpr):
    """Expression that represents all columns from the input.

    This is a special expression used in projections to indicate that
    all existing columns should be preserved at this position in the output.
    It's typically used internally by operations like with_column() and
    rename_columns() to maintain existing columns.

    Example:
        When with_column("new_col", expr) is called, it creates:
        Project(exprs=[star(), expr.alias("new_col")])

        This means: keep all existing columns, then add/overwrite "new_col"
    """

    # TODO: Add UnresolvedExpr. Both StarExpr and UnresolvedExpr won't have a defined data_type.
    _name: str = "*"
    # data_type: DataType = field(default_factory=lambda: DataType(object), init=False)

    @override
    def _is_resolved(self) -> bool:
        return False

    @override
    @property
    def data_type(self) -> DataType:
        raise ValueError("Star expressions have no data type")

    def structurally_equals(self, other: Any) -> bool:
        return isinstance(other, StarExpr)

    def expand(self, schema: Schema) -> List[ResolvedColumnExpr]:
        cols = []
        for name, dtype in zip(schema.names, schema.types):
            cols.append(ResolvedColumnExpr(_name=name, _data_type=dtype))
        return cols


@PublicAPI(stability="beta")
def col(name: str) -> UnresolvedColumnExpr:
    """
    Reference an existing column by name.

    This is the primary way to reference columns in expressions.
    The returned expression will extract values from the specified
    column when evaluated.

    Args:
        name: The name of the column to reference

    Returns:
        An UnresolvedColumnExpr that references the specified column

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
    return UnresolvedColumnExpr(_name=name)


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


# TODO remove
@DeveloperAPI(stability="alpha")
def star() -> StarExpr:
    """
    References all input columns from the input.

    This is a special expression used in projections to preserve all
    existing columns. It's typically used with operations that want to
    add or modify columns while keeping the rest.

    Returns:
        A StarExpr that represents all input columns.
    """
    return StarExpr()


@PublicAPI(stability="alpha")
def download(uri_column_name: str | NamedExpr) -> DownloadExpr:
    """
    Create a download expression that downloads content from URIs.

    This creates an expression that will download bytes from URIs stored in
    a specified column. When evaluated, it will fetch the content from each URI
    and return the downloaded bytes.

    Args:
        uri_column_name: The name of the column containing URIs to download from,
            or a named expressions. A named expression is one of col() or alias().
    Returns:
        A DownloadExpr that will download content from the specified URI column

    Example:
        >>> from ray.data.expressions import download, col
        >>> import ray
        >>> # Create dataset with URIs
        >>> ds = ray.data.from_items([
        ...     {"uri": "s3://bucket/file1.jpg", "id": "1"},
        ...     {"uri": "s3://bucket/file2.jpg", "id": "2"}
        ... ])
        >>> # Add downloaded bytes column
        >>> ds_with_bytes = ds.with_column("bytes", download("uri")) # OR
        >>> ds_with_bytes = ds.with_column("bytes", download(col("uri")))
    """
    if isinstance(uri_column_name, str):
        return DownloadExpr.from_uri(uri_column_name=uri_column_name)
    return DownloadExpr(uri_column=uri_column_name)


# ──────────────────────────────────────
# Public API for evaluation
# ──────────────────────────────────────
# Note: Implementation details are in _expression_evaluator.py

# Re-export eval_expr for public use

__all__ = [
    "Operation",
    "Expr",
    "NamedExpr",
    "ResolvedColumnExpr",
    "UnresolvedColumnExpr",
    "LiteralExpr",
    "BinaryExpr",
    "UnaryExpr",
    "UDFExpr",
    "DownloadExpr",
    "AliasExpr",
    "StarExpr",
    "pyarrow_udf",
    "udf",
    "col",
    "lit",
    "download",
    "star",
    "_ListNamespace",
    "_StringNamespace",
    "_StructNamespace",
    "_DatetimeNamespace",
]


def __getattr__(name: str):
    """Lazy import of namespace classes to avoid circular imports."""
    if name == "_ListNamespace":
        from ray.data.namespace_expressions.list_namespace import _ListNamespace

        return _ListNamespace
    elif name == "_StringNamespace":
        from ray.data.namespace_expressions.string_namespace import _StringNamespace

        return _StringNamespace
    elif name == "_StructNamespace":
        from ray.data.namespace_expressions.struct_namespace import _StructNamespace

        return _StructNamespace
    elif name == "_DatetimeNamespace":
        from ray.data.namespace_expressions.dt_namespace import _DatetimeNamespace

        return _DatetimeNamespace
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
