"""Expression compilation for Ray Data SQL API.

This module compiles SQLGlot expressions into executable Python functions
that can be used with Ray Dataset operations.
"""

import math
import operator
from typing import Any, Callable, Dict, Mapping

from sqlglot import exp

from ray.data.sql.exceptions import SchemaError, UnsupportedOperationError


class ExpressionCompiler:
    """Compiles SQLGlot expressions into executable Python functions.

    This class converts SQL expressions into Python callables that can be
    used with Ray Dataset operations like map(), filter(), and aggregate().
    """

    # Comparison operators mapping
    COMPARISON_OPS = {
        exp.EQ: operator.eq,
        exp.NEQ: operator.ne,
        exp.GT: operator.gt,
        exp.LT: operator.lt,
        exp.GTE: operator.ge,
        exp.LTE: operator.le,
        exp.Is: lambda a, b: a is b,
        exp.Null: lambda a, b: a is None,
    }

    # Arithmetic operators mapping
    ARITHMETIC_OPS = {
        exp.Add: operator.add,
        exp.Sub: operator.sub,
        exp.Mul: operator.mul,
        exp.Div: lambda a, b: float(a) / float(b) if b != 0 else None,
        exp.Mod: operator.mod,
    }

    # Logical operators mapping
    LOGICAL_OPS = {
        exp.And: lambda a, b: bool(a and b),
        exp.Or: lambda a, b: bool(a or b),
        exp.Not: operator.not_,
    }

    # String functions mapping
    STRING_FUNCTIONS = {
        exp.Upper: str.upper,
        exp.Lower: str.lower,
        exp.Length: len,
        exp.Substring: lambda s, start, length=None: (
            s[start - 1 : start - 1 + length] if length else s[start - 1 :]
        ),
        exp.Concat: lambda *args: "".join(str(arg) for arg in args),
        exp.Trim: str.strip,
    }

    # Mathematical functions mapping
    MATH_FUNCTIONS = {
        exp.Abs: abs,
        exp.Round: lambda x, digits=0: round(x, digits),
        exp.Ceil: math.ceil,
        exp.Floor: math.floor,
        exp.Sqrt: math.sqrt,
        exp.Log: math.log,
        exp.Exp: math.exp,
    }

    # Date/time functions mapping
    DATETIME_FUNCTIONS = {
        exp.Year: lambda d: d.year if hasattr(d, "year") else None,
        exp.Month: lambda d: d.month if hasattr(d, "month") else None,
        exp.Day: lambda d: d.day if hasattr(d, "day") else None,
    }

    @classmethod
    def compile(cls, expr: exp.Expression) -> Callable[[Mapping[str, Any]], Any]:
        """Compile with caching for performance."""
        # Create cache key from expression
        expr_key = cls._get_expression_cache_key(expr)

        # Check cache first
        if expr_key in cls._compilation_cache:
            return cls._compilation_cache[expr_key]

        # Compile and cache
        compiled_func = cls._compile_expression(expr)

        # Cache with size limit
        if len(cls._compilation_cache) >= 1000:  # Prevent memory bloat
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(cls._compilation_cache))
            del cls._compilation_cache[oldest_key]

        cls._compilation_cache[expr_key] = compiled_func
        return compiled_func

    @classmethod
    def _get_expression_cache_key(cls, expr: exp.Expression) -> str:
        """Generate a cache key for an expression."""
        return f"{type(expr).__name__}:{str(expr)}"

    @classmethod
    def _compile_expression(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile a SQLGlot expression into a Python function.

        Args:
            expr: SQLGlot expression to compile.

        Returns:
            Callable function that takes a row dict and returns the computed value.

        Raises:
            UnsupportedOperationError: If the expression type is not supported.
        """
        expr_type = type(expr)

        # Column references
        if isinstance(expr, exp.Column):
            col_name = str(expr.name)
            if "." in col_name:
                col_name = col_name.split(".")[-1]
            return lambda row: row.get(col_name)

        # Literal values
        if isinstance(expr, exp.Literal):
            value = cls._parse_literal(expr)
            return lambda _: value

        # Comparison operators
        if expr_type in cls.COMPARISON_OPS:
            return cls._compile_comparison(expr)

        # Arithmetic operators
        if expr_type in cls.ARITHMETIC_OPS:
            return cls._compile_arithmetic(expr)

        # Logical operators
        if expr_type in cls.LOGICAL_OPS:
            return cls._compile_logical(expr)

        # String functions
        if expr_type in cls.STRING_FUNCTIONS:
            return cls._compile_string_function(expr)

        # Mathematical functions
        if expr_type in cls.MATH_FUNCTIONS:
            return cls._compile_math_function(expr)

        # Date/time functions
        if expr_type in cls.DATETIME_FUNCTIONS:
            return cls._compile_datetime_function(expr)

        # Aggregate functions
        if expr_type in {exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max}:
            return cls._compile_aggregate(expr)

        # Special expressions
        if isinstance(expr, exp.Between):
            return cls._compile_between(expr)

        if isinstance(expr, exp.In):
            return cls._compile_in(expr)

        if isinstance(expr, exp.Like):
            return cls._compile_like(expr)

        if isinstance(expr, exp.Cast):
            return cls._compile_cast(expr)

        # COALESCE function
        if isinstance(expr, exp.Coalesce):
            return cls._compile_coalesce(expr)

        # NULLIF function
        if isinstance(expr, exp.Nullif):
            return cls._compile_nullif(expr)

        # CASE expressions
        if isinstance(expr, exp.Case):
            return cls._compile_case(expr)

        # Scalar subqueries (limited support)
        if isinstance(expr, exp.Subquery):
            return cls._compile_scalar_subquery(expr)

        # Window functions (basic support)
        if isinstance(expr, exp.Window):
            return cls._compile_window_function(expr)

        # Parenthesized expressions
        if isinstance(expr, exp.Paren):
            return cls.compile(expr.this)

        # Unsupported expression
        raise UnsupportedOperationError(
            f"Expression type {expr_type.__name__}",
            suggestion="Check the documentation for supported SQL constructs",
        )

    @classmethod
    def _compile_comparison(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], bool]:
        """Compile comparison expressions.

        Args:
            expr: Comparison expression.

        Returns:
            Function that performs the comparison.
        """
        left_func = cls.compile(expr.left)
        right_func = cls.compile(expr.right)
        op = cls.COMPARISON_OPS[type(expr)]

        def compare_func(row: Mapping[str, Any]) -> bool:
            left_val = left_func(row)
            right_val = right_func(row)

            # Handle NULL comparisons
            if left_val is None or right_val is None:
                return False

            try:
                return op(left_val, right_val)
            except TypeError:
                raise SchemaError(
                    f"Cannot compare {type(left_val).__name__} and {type(right_val).__name__}"
                )

        return compare_func

    @classmethod
    def _compile_arithmetic(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile arithmetic expressions.

        Args:
            expr: Arithmetic expression.

        Returns:
            Function that performs the arithmetic operation.
        """
        left_func = cls.compile(expr.left)
        right_func = cls.compile(expr.right)
        op = cls.ARITHMETIC_OPS[type(expr)]

        def arithmetic_func(row: Mapping[str, Any]) -> Any:
            left_val = left_func(row)
            right_val = right_func(row)

            # Handle NULL values
            if left_val is None or right_val is None:
                return None

            try:
                return op(left_val, right_val)
            except (TypeError, ValueError, ZeroDivisionError) as e:
                if isinstance(expr, exp.Div) and right_val == 0:
                    return None  # Division by zero returns NULL
                raise SchemaError(f"Arithmetic error: {str(e)}")

        return arithmetic_func

    @classmethod
    def _compile_logical(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], bool]:
        """Compile logical expressions.

        Args:
            expr: Logical expression.

        Returns:
            Function that performs the logical operation.
        """
        if isinstance(expr, exp.Not):
            func = cls.compile(expr.this)
            op = cls.LOGICAL_OPS[type(expr)]

            def not_func(row: Mapping[str, Any]) -> bool:
                val = func(row)
                return op(val)

            return not_func
        else:
            left_func = cls.compile(expr.left)
            right_func = cls.compile(expr.right)
            op = cls.LOGICAL_OPS[type(expr)]

            def logical_func(row: Mapping[str, Any]) -> bool:
                left_val = left_func(row)
                right_val = right_func(row)
                return op(left_val, right_val)

            return logical_func

    @classmethod
    def _compile_string_function(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile string function expressions.

        Args:
            expr: String function expression.

        Returns:
            Function that performs the string operation.
        """
        func_type = type(expr)
        func = cls.STRING_FUNCTIONS[func_type]

        if hasattr(expr, "this"):
            arg_func = cls.compile(expr.this)

            def string_func(row: Mapping[str, Any]) -> Any:
                arg = arg_func(row)
                if arg is None:
                    return None
                return func(arg)

            return string_func
        else:
            return lambda _: None

    @classmethod
    def _compile_math_function(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile mathematical function expressions.

        Args:
            expr: Mathematical function expression.

        Returns:
            Function that performs the mathematical operation.
        """
        func_type = type(expr)
        func = cls.MATH_FUNCTIONS[func_type]

        if hasattr(expr, "this"):
            arg_func = cls.compile(expr.this)

            # Handle functions with optional second argument (like ROUND)
            if hasattr(expr, "expressions") and expr.expressions:
                second_arg_func = cls.compile(expr.expressions[0])

                def math_func_two_args(row: Mapping[str, Any]) -> Any:
                    arg1 = arg_func(row)
                    arg2 = second_arg_func(row)
                    if arg1 is None:
                        return None
                    try:
                        return func(arg1, arg2)
                    except (TypeError, ValueError, ZeroDivisionError):
                        return None

                return math_func_two_args
            else:

                def math_func_one_arg(row: Mapping[str, Any]) -> Any:
                    arg = arg_func(row)
                    if arg is None:
                        return None
                    try:
                        return func(arg)
                    except (TypeError, ValueError, ZeroDivisionError):
                        return None

                return math_func_one_arg
        else:
            return lambda _: None

    @classmethod
    def _compile_datetime_function(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile date/time function expressions.

        Args:
            expr: Date/time function expression.

        Returns:
            Function that performs the date/time operation.
        """
        func_type = type(expr)
        func = cls.DATETIME_FUNCTIONS[func_type]

        if hasattr(expr, "this"):
            arg_func = cls.compile(expr.this)

            def datetime_func(row: Mapping[str, Any]) -> Any:
                arg = arg_func(row)
                if arg is None:
                    return None
                return func(arg)

            return datetime_func
        else:
            return lambda _: None

    @classmethod
    def _compile_aggregate(
        cls, expr: exp.Expression
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile aggregate function expressions.

        Args:
            expr: Aggregate function expression.

        Returns:
            Function that returns the aggregate function name for later processing.
        """
        func_name = type(expr).__name__.lower()

        if hasattr(expr, "this"):
            arg_func = cls.compile(expr.this)

            def aggregate_func(row: Mapping[str, Any]) -> Dict[str, Any]:
                arg = arg_func(row)
                return {"function": func_name, "argument": arg, "type": "aggregate"}

            return aggregate_func
        else:
            return lambda _: {
                "function": func_name,
                "argument": None,
                "type": "aggregate",
            }

    @classmethod
    def _compile_between(cls, expr: exp.Between) -> Callable[[Mapping[str, Any]], bool]:
        """Compile BETWEEN expressions.

        Args:
            expr: BETWEEN expression.

        Returns:
            Function that checks if a value is between two bounds.
        """
        value_func = cls.compile(expr.this)
        low_func = cls.compile(expr.args.get("low"))
        high_func = cls.compile(expr.args.get("high"))

        def between_func(row: Mapping[str, Any]) -> bool:
            value = value_func(row)
            low = low_func(row)
            high = high_func(row)

            if any(v is None for v in [value, low, high]):
                return False

            try:
                return low <= value <= high
            except TypeError:
                raise SchemaError("Cannot compare values in BETWEEN clause")

        return between_func

    @classmethod
    def _compile_in(cls, expr: exp.In) -> Callable[[Mapping[str, Any]], bool]:
        """Compile IN expressions.

        Args:
            expr: IN expression.

        Returns:
            Function that checks if a value is in a list.
        """
        value_func = cls.compile(expr.this)

        # Extract the list of values from the IN clause
        if hasattr(expr, "expressions"):
            values = [cls.compile(e) for e in expr.expressions]
        else:
            values = []

        def in_func(row: Mapping[str, Any]) -> bool:
            value = value_func(row)
            if value is None:
                return False

            try:
                return any(val_func(row) == value for val_func in values)
            except TypeError:
                return False

        return in_func

    @classmethod
    def _compile_like(cls, expr: exp.Like) -> Callable[[Mapping[str, Any]], bool]:
        """Compile LIKE expressions.

        Args:
            expr: LIKE expression.

        Returns:
            Function that performs pattern matching.
        """
        value_func = cls.compile(expr.this)
        pattern_func = cls.compile(expr.args.get("pattern"))

        def like_func(row: Mapping[str, Any]) -> bool:
            value = value_func(row)
            pattern = pattern_func(row)

            if value is None or pattern is None:
                return False

            try:
                # Convert SQL LIKE pattern to regex
                import re

                regex_pattern = pattern.replace("%", ".*").replace("_", ".")
                return bool(re.match(regex_pattern, str(value)))
            except (TypeError, re.error):
                return False

        return like_func

    @classmethod
    def _compile_cast(cls, expr: exp.Cast) -> Callable[[Mapping[str, Any]], Any]:
        """Compile CAST expressions.

        Args:
            expr: CAST expression.

        Returns:
            Function that performs type casting.
        """
        value_func = cls.compile(expr.this)
        target_type = str(expr.args.get("to")).upper()

        def cast_func(row: Mapping[str, Any]) -> Any:
            value = value_func(row)
            if value is None:
                return None

            try:
                if target_type == "INTEGER" or target_type == "INT":
                    return int(value)
                elif target_type == "FLOAT" or target_type == "DOUBLE":
                    return float(value)
                elif target_type == "STRING" or target_type == "VARCHAR":
                    return str(value)
                elif target_type == "BOOLEAN" or target_type == "BOOL":
                    return bool(value)
                else:
                    # For unsupported types, return the original value
                    return value
            except (ValueError, TypeError):
                return None

        return cast_func

    @classmethod
    def _compile_coalesce(
        cls, expr: exp.Coalesce
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile COALESCE expressions.

        Args:
            expr: COALESCE expression.

        Returns:
            Function that returns the first non-null value.
        """
        arg_funcs = [cls.compile(arg) for arg in expr.expressions]

        def coalesce_func(row: Mapping[str, Any]) -> Any:
            for arg_func in arg_funcs:
                value = arg_func(row)
                if value is not None:
                    return value
            return None

        return coalesce_func

    @classmethod
    def _compile_nullif(cls, expr: exp.Nullif) -> Callable[[Mapping[str, Any]], Any]:
        """Compile NULLIF expressions.

        Args:
            expr: NULLIF expression.

        Returns:
            Function that returns NULL if two values are equal, otherwise the first value.
        """
        first_func = cls.compile(expr.this)
        second_func = cls.compile(expr.expressions[0])

        def nullif_func(row: Mapping[str, Any]) -> Any:
            first_val = first_func(row)
            second_val = second_func(row)

            if first_val == second_val:
                return None
            return first_val

        return nullif_func

    @classmethod
    def _compile_case(cls, expr: exp.Case) -> Callable[[Mapping[str, Any]], Any]:
        """Compile CASE expressions.

        Args:
            expr: CASE expression.

        Returns:
            Function that evaluates conditional logic.
        """
        # Compile all WHEN conditions and values
        when_conditions = []
        when_values = []

        for when_expr in expr.find_all(exp.When):
            condition_func = cls.compile(when_expr.this)
            value_func = cls.compile(when_expr.args.get("then"))
            when_conditions.append(condition_func)
            when_values.append(value_func)

        # Compile ELSE clause if present
        else_func = None
        if expr.args.get("default"):
            else_func = cls.compile(expr.args["default"])

        def case_func(row: Mapping[str, Any]) -> Any:
            # Evaluate WHEN conditions in order
            for condition_func, value_func in zip(when_conditions, when_values):
                if condition_func(row):
                    return value_func(row)

            # If no WHEN condition matched, return ELSE value or NULL
            if else_func:
                return else_func(row)
            return None

        return case_func

    @classmethod
    def _compile_scalar_subquery(
        cls, expr: exp.Subquery
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile scalar subqueries.

        Args:
            expr: Subquery expression.

        Returns:
            Function that returns the scalar result of the subquery.
        """
        # For now, we'll implement a simplified version that pre-computes the result
        # This requires access to the registry and execution engine

        # Note: This is a simplified implementation. Full subquery support would require
        # more complex execution planning and context passing.
        def scalar_subquery_func(row: Mapping[str, Any]) -> Any:
            # For now, return None as placeholder
            # Full implementation would need to execute the subquery and return scalar result
            return None

        return scalar_subquery_func

    @classmethod
    def _compile_window_function(
        cls, expr: exp.Window
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Compile window function expressions.

        Args:
            expr: Window function expression.

        Returns:
            Function that handles window operations.
        """
        # Basic window function support - ROW_NUMBER only for now
        # Full window function support would require complex execution planning

        def window_func(row: Mapping[str, Any]) -> Any:
            # For basic ROW_NUMBER, we'll need to handle this at the execution level
            # This is a placeholder that would need execution context
            return 1

        return window_func

    @classmethod
    def _parse_literal(cls, literal_expr: exp.Literal) -> Any:
        """Parse a SQLGlot literal into a Python value.

        Args:
            literal_expr: SQLGlot literal expression.

        Returns:
            Parsed Python value with appropriate type.
        """
        value = literal_expr.this

        # Handle different literal types
        if literal_expr.is_string:
            return str(value)
        elif literal_expr.is_number:
            # Try to parse as int first, then float
            try:
                if "." in str(value):
                    return float(value)
                else:
                    return int(value)
            except (ValueError, TypeError):
                return str(value)
        elif str(value).lower() in ("true", "false"):
            return str(value).lower() == "true"
        elif str(value).lower() in ("null", "none"):
            return None
        else:
            # Default to string representation
            return str(value)

    @classmethod
    def _create_column_accessor(
        cls, col_name: str
    ) -> Callable[[Mapping[str, Any]], Any]:
        """Create an optimized column accessor function.

        This creates a closure that avoids string operations on each call
        for better performance in distributed operations.
        """

        def column_accessor(row: Mapping[str, Any]) -> Any:
            return row.get(col_name)

        # Add column name as attribute for debugging
        column_accessor.__name__ = f"get_{col_name}"
        return column_accessor

    @classmethod
    def clear_compilation_cache(cls) -> None:
        """Clear the expression compilation cache."""
        cls._compilation_cache.clear()

    @classmethod
    def get_cache_stats(cls) -> Dict[str, int]:
        """Get compilation cache statistics."""
        return {"cache_size": len(cls._compilation_cache), "cache_limit": 1000}
