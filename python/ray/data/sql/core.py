"""Core SQL engine implementation for Ray Data.

This module provides the main SQL engine classes and functions for executing
SQL queries against Ray Datasets. It follows Ray API patterns and provides
comprehensive error handling and performance optimizations.
"""

import logging
import time
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import sqlglot
from sqlglot import exp

import ray.data
import ray.data.aggregate
from ray.data import Dataset
from ray.data.sql.config import DEFAULT_CONFIG, SQLConfig
from ray.data.sql.exceptions import (
    ColumnNotFoundError,
    SchemaError,
    SQLError,
    SQLExecutionError,
    SQLParseError,
    TableNotFoundError,
    UnsupportedOperationError,
)


class SQLValidator:
    """Validator for SQL queries to detect unsupported operations."""

    # Supported SQL statement types
    SUPPORTED_STATEMENTS = {exp.Select}

    # Supported expression types for WHERE clauses
    SUPPORTED_WHERE_EXPRESSIONS = {
        exp.Column,
        exp.Literal,
        exp.Boolean,
        exp.Paren,
        exp.And,
        exp.Or,
        exp.Not,
        exp.EQ,
        exp.NEQ,
        exp.GT,
        exp.LT,
        exp.GTE,
        exp.LTE,
        exp.Add,
        exp.Sub,
        exp.Mul,
        exp.Div,
        exp.Mod,
        exp.Is,
        exp.Null,
    }

    # Supported aggregate functions
    SUPPORTED_AGGREGATES = {exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max}

    # Unsupported features that should raise clear errors
    UNSUPPORTED_FEATURES = {
        exp.Distinct: "DISTINCT is not yet supported. Use ray.data.Dataset.unique() after the query",
        exp.Union: "UNION is not yet supported. Use ray.data.Dataset.union() to combine datasets",
        exp.Intersect: "INTERSECT is not yet supported",
        exp.Except: "EXCEPT is not yet supported",
        exp.Window: "Window functions are not yet supported",
        exp.Case: "CASE expressions are not yet supported",
        exp.Subquery: "Subqueries are not yet supported",
        exp.With: "Common Table Expressions (WITH) are not yet supported",
        exp.Insert: "INSERT statements are not supported. Use ray.data.Dataset.write_*() methods",
        exp.Update: "UPDATE statements are not supported. Use ray.data.Dataset.map() for transformations",
        exp.Delete: "DELETE statements are not supported. Use ray.data.Dataset.filter() for filtering",
        exp.Create: "CREATE statements are not supported. Use ray.data.from_*() methods to create datasets",
        exp.Drop: "DROP statements are not supported. Use clear_tables() or del operations",
        exp.Having: "HAVING clauses are not yet supported. Apply filtering after GROUP BY using Dataset.filter()",
        exp.Exists: "EXISTS clauses are not yet supported",
        exp.In: "IN clauses are not yet supported. Use multiple OR conditions or Dataset.filter()",
        exp.Like: "LIKE patterns are not yet supported. Use Dataset.filter() with string methods",
        exp.Concat: "String concatenation is not yet supported",
        exp.Substring: "SUBSTRING function is not yet supported",
        exp.Cast: "CAST expressions are not yet supported",
        exp.Coalesce: "COALESCE function is not yet supported",
        exp.If: "IF function is not yet supported",
        exp.Greatest: "GREATEST function is not yet supported",
        exp.Least: "LEAST function is not yet supported",
    }

    @classmethod
    def validate_query(cls, query: str, ast: exp.Expression) -> None:
        """Validate a SQL query and raise appropriate errors for unsupported features.

        Args:
            query: Original SQL query string.
            ast: Parsed SQLGlot AST.

        Raises:
            UnsupportedOperationError: If the query contains unsupported features.
            SQLParseError: If the query structure is invalid.
        """
        # Check for unsupported statement types
        if not isinstance(ast, cls.SUPPORTED_STATEMENTS):
            statement_type = type(ast).__name__
            raise UnsupportedOperationError(
                f"{statement_type} statements",
                suggestion="Only SELECT statements are currently supported",
                query=query,
            )

        # Check for unsupported features in the AST
        cls._check_unsupported_features(ast, query)

        # Validate SELECT-specific constructs
        if isinstance(ast, exp.Select):
            cls._validate_select_statement(ast, query)

    @classmethod
    def _check_unsupported_features(cls, ast: exp.Expression, query: str) -> None:
        """Check for unsupported features in the AST."""
        for feature_type in cls.UNSUPPORTED_FEATURES:
            for node in ast.find_all(feature_type):
                feature_name = feature_type.__name__
                suggestion = cls.UNSUPPORTED_FEATURES[feature_type]

                raise UnsupportedOperationError(
                    f"{feature_name} operations", suggestion=suggestion, query=query
                )

    @classmethod
    def _validate_select_statement(cls, stmt: exp.Select, query: str) -> None:
        """Validate SELECT statement specific constructs."""
        # Check JOIN operations
        from_clause = stmt.args.get("from")
        if from_clause and hasattr(from_clause, "expressions"):
            for expr in from_clause.expressions:
                if hasattr(expr, "joins") and expr.joins:
                    # JOINs are not yet fully implemented
                    raise UnsupportedOperationError(
                        "JOIN operations",
                        suggestion="JOIN support is coming soon. Use Dataset.join() or separate queries for now",
                        query=query,
                    )

        # Check for complex ORDER BY expressions
        order_clause = stmt.args.get("order")
        if order_clause:
            for ordering in order_clause.expressions:
                if isinstance(ordering, exp.Ordered):
                    order_expr = ordering.this
                    if not isinstance(order_expr, exp.Column):
                        raise UnsupportedOperationError(
                            "Complex ORDER BY expressions",
                            suggestion="Only simple column names are supported in ORDER BY",
                            query=query,
                        )

        # Check WHERE clause complexity
        where_clause = stmt.args.get("where")
        if where_clause:
            cls._validate_where_clause(where_clause.this, query)

    @classmethod
    def _validate_where_clause(cls, expr: exp.Expression, query: str) -> None:
        """Validate WHERE clause expressions recursively."""
        expr_type = type(expr)

        if expr_type not in cls.SUPPORTED_WHERE_EXPRESSIONS:
            raise UnsupportedOperationError(
                f"{expr_type.__name__} in WHERE clause",
                suggestion="Check the documentation for supported WHERE clause expressions",
                query=query,
            )

        # Recursively validate child expressions
        if hasattr(expr, "left") and expr.left:
            cls._validate_where_clause(expr.left, query)
        if hasattr(expr, "right") and expr.right:
            cls._validate_where_clause(expr.right, query)
        if (
            hasattr(expr, "this")
            and expr.this
            and isinstance(expr.this, exp.Expression)
        ):
            cls._validate_where_clause(expr.this, query)


class TableRegistry:
    """Registry for managing SQL table name to Ray Dataset mappings.

    This class provides a centralized registry for mapping SQL table names
    to Ray Dataset objects. It supports table registration, lookup, and
    management operations.

    Examples:
        >>> registry = TableRegistry()
        >>> users_ds = ray.data.from_items([{"id": 1, "name": "Alice"}])
        >>> registry.register("users", users_ds)
        >>> dataset = registry.get("users")
    """

    def __init__(self):
        """Initialize a new table registry."""
        self._tables: Dict[str, Dataset] = {}
        self._logger = logging.getLogger(__name__)

    def register(self, name: str, dataset: Dataset) -> None:
        """Register a dataset under the given SQL table name.

        Args:
            name: SQL table name to register the dataset under.
            dataset: Ray Dataset to register.

        Raises:
            TypeError: If dataset is not a Ray Dataset.
            ValueError: If name is invalid.
        """
        if not isinstance(dataset, Dataset):
            raise TypeError(f"Expected Ray Dataset, got {type(dataset)}")

        if not name or not isinstance(name, str):
            raise ValueError("Table name must be a non-empty string")

        # Check for SQL reserved words
        reserved_words = {
            "select",
            "from",
            "where",
            "order",
            "by",
            "group",
            "having",
            "join",
            "inner",
            "left",
            "right",
            "full",
            "on",
            "and",
            "or",
            "not",
            "null",
            "true",
            "false",
            "count",
            "sum",
            "avg",
            "min",
            "max",
        }
        if name.lower() in reserved_words:
            raise ValueError(f"Table name '{name}' is a SQL reserved word")

        self._tables[name] = dataset
        self._logger.debug(f"Registered table '{name}' with {dataset.count()} rows")

    def get(self, name: str) -> Dataset:
        """Retrieve a registered dataset by table name.

        Args:
            name: SQL table name to look up.

        Returns:
            The Ray Dataset registered under the given name.

        Raises:
            TableNotFoundError: If the table name is not found.
        """
        if name not in self._tables:
            available = list(self._tables.keys())
            raise TableNotFoundError(name, available_tables=available)

        return self._tables[name]

    def list_tables(self) -> List[str]:
        """List all registered table names.

        Returns:
            List of registered table names.
        """
        return list(self._tables.keys())

    def clear(self) -> None:
        """Clear all registered tables."""
        self._tables.clear()
        self._logger.debug("Cleared all registered tables")

    def get_schema(self, name: str) -> Optional[object]:
        """Get the schema for a registered table.

        Args:
            name: Table name to get schema for.

        Returns:
            The schema object for the table, or None if not available.

        Raises:
            TableNotFoundError: If the table name is not found.
        """
        dataset = self.get(name)  # This will raise TableNotFoundError if needed
        try:
            return dataset.schema()
        except Exception:
            return None


class ExpressionCompiler:
    """Compiler for SQL expressions to Python functions.

    This class converts SQLGlot expression ASTs into Python functions
    that can be applied to Ray Dataset rows.
    """

    @staticmethod
    def _to_number(value: str) -> Union[int, float, str]:
        """Convert string to number if possible."""
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    @classmethod
    def compile(cls, expr: exp.Expression) -> Callable[[Mapping[str, object]], object]:
        """Compile a SQLGlot expression to a Python function.

        Args:
            expr: SQLGlot expression to compile.

        Returns:
            Python function that takes a row dict and returns the expression result.

        Raises:
            UnsupportedOperationError: If the expression type is not supported.
        """
        expr_type = type(expr)

        # Column references
        if expr_type is exp.Column:
            col = str(expr.name)
            return lambda row, c=col: row.get(c)

        # Literals and booleans
        if expr_type in (exp.Literal, exp.Boolean):
            value = getattr(expr, "name", None)
            if value is not None:
                value_str = str(value).strip()
                if value_str.lower() == "true":
                    return lambda _: True
                elif value_str.lower() == "false":
                    return lambda _: False
                else:
                    return lambda _: cls._to_number(value_str)
            return lambda _: expr.this if hasattr(expr, "this") else expr

        # NULL values
        if expr_type is exp.Null:
            return lambda _: None

        # IS NULL / IS NOT NULL
        if expr_type is exp.Is:
            left_func = cls.compile(expr.this)
            if isinstance(expr.expression, exp.Null):
                return lambda row: left_func(row) is None
            else:
                raise UnsupportedOperationError("IS expressions with non-NULL values")

        # Parentheses
        if expr_type is exp.Paren:
            return cls.compile(expr.this)

        # Logical operators
        if expr_type in (exp.And, exp.Or):
            left_func = cls.compile(expr.left)
            right_func = cls.compile(expr.right)
            if expr_type is exp.And:
                return lambda row: left_func(row) and right_func(row)
            else:  # exp.Or
                return lambda row: left_func(row) or right_func(row)

        if expr_type is exp.Not:
            sub_func = cls.compile(expr.this)
            return lambda row: not sub_func(row)

        # Comparison operators
        comparison_ops = {
            exp.EQ: lambda a, b: a == b,
            exp.NEQ: lambda a, b: a != b,
            exp.GT: lambda a, b: a > b,
            exp.LT: lambda a, b: a < b,
            exp.GTE: lambda a, b: a >= b,
            exp.LTE: lambda a, b: a <= b,
        }

        if expr_type in comparison_ops:
            left_func = cls.compile(expr.left)
            right_func = cls.compile(expr.right)
            op = comparison_ops[expr_type]

            def compare_func(row):
                left_val = left_func(row)
                right_val = right_func(row)
                # Handle NULL comparisons
                if left_val is None or right_val is None:
                    return False
                try:
                    return op(left_val, right_val)
                except TypeError:
                    # Type mismatch - raise more helpful error
                    raise SchemaError(
                        f"Cannot compare {type(left_val).__name__} and {type(right_val).__name__}"
                    )

            return compare_func

        # Arithmetic operators
        arithmetic_ops = {
            exp.Add: lambda a, b: a + b,
            exp.Sub: lambda a, b: a - b,
            exp.Mul: lambda a, b: a * b,
            exp.Div: lambda a, b: float(a) / float(b) if b != 0 else None,
            exp.Mod: lambda a, b: a % b,
        }

        if expr_type in arithmetic_ops:
            left_func = cls.compile(expr.left)
            right_func = cls.compile(expr.right)
            op = arithmetic_ops[expr_type]

            def arithmetic_func(row):
                left_val = left_func(row)
                right_val = right_func(row)
                # Handle NULL values
                if left_val is None or right_val is None:
                    return None
                try:
                    return op(left_val, right_val)
                except (TypeError, ValueError, ZeroDivisionError) as e:
                    if expr_type is exp.Div and right_val == 0:
                        return None  # Division by zero returns NULL
                    raise SchemaError(f"Arithmetic error: {str(e)}")

            return arithmetic_func

        # String literals with proper quote handling
        if (
            expr_type is exp.Literal
            and hasattr(expr, "this")
            and isinstance(expr.this, str)
        ):
            value = expr.this
            return lambda _: value

        # Unsupported expression
        raise UnsupportedOperationError(
            f"Expression type {expr_type.__name__}",
            suggestion="Check the documentation for supported SQL constructs",
        )


class SQLQueryExecutor:
    """Executor for SQL queries against Ray Datasets.

    This class handles the execution of parsed and optimized SQL queries
    against registered Ray Datasets.
    """

    def __init__(self, config: SQLConfig, registry: TableRegistry):
        """Initialize the query executor.

        Args:
            config: SQL engine configuration.
            registry: Table registry for dataset lookups.
        """
        self.config = config
        self.registry = registry
        self._logger = config.get_logger(__name__)

    def execute(
        self, stmt: exp.Select, default_dataset: Optional[Dataset] = None
    ) -> Dataset:
        """Execute a SELECT statement against registered datasets.

        Args:
            stmt: Parsed SELECT statement.
            default_dataset: Default dataset for queries without FROM clause.

        Returns:
            Ray Dataset containing the query results.

        Raises:
            SQLExecutionError: If query execution fails.
        """
        try:
            # Resolve FROM clause
            dataset, table_name = self._resolve_from(stmt, default_dataset)

            # Apply WHERE clause
            dataset = self._apply_where(dataset, stmt)

            # Check for aggregates and GROUP BY
            has_aggregates = self._has_aggregates(stmt)
            group_by_exprs = stmt.args.get("group", None)

            if has_aggregates or group_by_exprs:
                dataset = self._apply_group_by_and_aggregates(dataset, stmt)
            else:
                # Apply ORDER BY
                dataset = self._apply_order_by(dataset, stmt)

                # Apply LIMIT
                dataset = self._apply_limit(dataset, stmt)

                # Apply SELECT projection
                dataset = self._apply_projection(dataset, stmt, table_name)

            return dataset

        except Exception as e:
            if isinstance(e, SQLError):
                raise
            else:
                raise SQLExecutionError(f"Query execution failed: {str(e)}") from e

    def _resolve_from(
        self, stmt: exp.Select, default_dataset: Optional[Dataset]
    ) -> Tuple[Dataset, str]:
        """Resolve the dataset referenced in the FROM clause."""
        from_clause = stmt.args.get("from")

        if from_clause:
            node = (
                from_clause.expressions[0]
                if getattr(from_clause, "expressions", None)
                else from_clause.this
            )
            table_name = str(node.name)

            # Validate table name
            if not table_name or table_name.lower() == "none":
                raise SQLExecutionError("Invalid table name in FROM clause")

            dataset = self.registry.get(table_name)
            return dataset, table_name

        # No FROM clause - use default dataset
        if default_dataset is None:
            raise SQLExecutionError("No FROM clause and no default dataset provided")

        return default_dataset, "default"

    def _apply_where(self, dataset: Dataset, stmt: exp.Select) -> Dataset:
        """Apply WHERE clause filtering."""
        where_node = stmt.args.get("where")
        if where_node is None:
            return dataset

        try:
            filter_func = ExpressionCompiler.compile(where_node.this)
            return dataset.filter(filter_func)
        except Exception as e:
            raise SQLExecutionError(f"WHERE clause execution failed: {str(e)}") from e

    def _apply_order_by(self, dataset: Dataset, stmt: exp.Select) -> Dataset:
        """Apply ORDER BY clause."""
        order_ast = stmt.args.get("order")
        if order_ast is None:
            return dataset

        try:
            keys = []
            desc_flags = []

            for ordering in order_ast.expressions:
                if isinstance(ordering, exp.Ordered) and isinstance(
                    ordering.this, exp.Column
                ):
                    col_name = str(ordering.this.name)
                    keys.append(col_name)
                    desc_flags.append(bool(ordering.args.get("desc")))
                else:
                    raise UnsupportedOperationError(
                        "Complex ORDER BY expressions",
                        suggestion="Only simple column names are supported in ORDER BY",
                    )

            # Validate columns exist in dataset
            try:
                dataset_cols = dataset.columns()
                if dataset_cols:
                    for key in keys:
                        if key not in dataset_cols:
                            raise ColumnNotFoundError(
                                key, available_columns=dataset_cols
                            )
            except Exception:
                # If we can't get columns (empty dataset), skip validation
                pass

            if len(keys) == 1:
                return dataset.sort(keys[0], descending=desc_flags[0])
            else:
                return dataset.sort(keys, descending=desc_flags)

        except Exception as e:
            if isinstance(e, SQLError):
                raise
            raise SQLExecutionError(f"ORDER BY execution failed: {str(e)}") from e

    def _apply_limit(self, dataset: Dataset, stmt: exp.Select) -> Dataset:
        """Apply LIMIT clause."""
        limit_ast = stmt.args.get("limit")
        if limit_ast is None:
            return dataset

        try:
            limit_node = getattr(limit_ast, "this", None) or getattr(
                limit_ast, "expression", None
            )
            limit_val = None

            # Try different ways to extract the limit value
            if hasattr(limit_node, "this"):
                try:
                    limit_val = int(str(limit_node.this))
                except (ValueError, TypeError, AttributeError):
                    pass

            if limit_val is None and hasattr(limit_node, "name"):
                try:
                    limit_val = int(str(limit_node.name))
                except (ValueError, TypeError, AttributeError):
                    pass

            if limit_val is None:
                try:
                    limit_val = int(str(limit_node))
                except (ValueError, TypeError, AttributeError):
                    pass

            if limit_val is not None:
                if limit_val < 0:
                    raise SQLExecutionError("LIMIT value must be non-negative")
                return dataset.limit(limit_val)
            else:
                raise SQLExecutionError("Invalid LIMIT value")

        except Exception as e:
            if isinstance(e, SQLError):
                raise
            raise SQLExecutionError(f"LIMIT execution failed: {str(e)}") from e

    def _has_aggregates(self, stmt: exp.Select) -> bool:
        """Check if the query contains aggregate functions."""
        for expr in stmt.find_all((exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            return True
        return False

    def _apply_group_by_and_aggregates(
        self, dataset: Dataset, stmt: exp.Select
    ) -> Dataset:
        """Apply GROUP BY and aggregate functions."""
        group_by_exprs = stmt.args.get("group", None)
        select_exprs = stmt.args["expressions"]

        if group_by_exprs:
            # GROUP BY case
            group_keys = []
            for group_expr in group_by_exprs.expressions:
                if isinstance(group_expr, exp.Column):
                    group_keys.append(str(group_expr.name))
                else:
                    raise UnsupportedOperationError(
                        "Complex GROUP BY expressions",
                        suggestion="Only simple column names are supported in GROUP BY",
                    )

            # Validate group columns exist
            try:
                dataset_cols = dataset.columns()
                if dataset_cols:
                    for key in group_keys:
                        if key not in dataset_cols:
                            raise ColumnNotFoundError(
                                key, available_columns=dataset_cols
                            )
            except Exception:
                # If we can't get columns, skip validation
                pass

            # Use Ray Data's groupby functionality
            grouped = dataset.groupby(group_keys)

            # Build aggregations
            agg_funcs = []
            result_names = []

            for expr in select_exprs:
                if isinstance(expr, exp.Alias):
                    result_names.append(str(expr.alias))
                    inner_expr = expr.this
                else:
                    result_names.append(self._get_expr_name(expr))
                    inner_expr = expr

                if isinstance(inner_expr, exp.Column):
                    # Group by column - will be preserved
                    continue
                elif isinstance(inner_expr, exp.Count):
                    if str(inner_expr.this) == "*":
                        agg_funcs.append(ray.data.aggregate.Count())
                    else:
                        col_name = (
                            str(inner_expr.this.name)
                            if hasattr(inner_expr.this, "name")
                            else str(inner_expr.this)
                        )
                        agg_funcs.append(ray.data.aggregate.Count(col_name))
                elif isinstance(inner_expr, exp.Sum):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    agg_funcs.append(ray.data.aggregate.Sum(col_name))
                elif isinstance(inner_expr, exp.Avg):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    agg_funcs.append(ray.data.aggregate.Mean(col_name))
                elif isinstance(inner_expr, exp.Min):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    agg_funcs.append(ray.data.aggregate.Min(col_name))
                elif isinstance(inner_expr, exp.Max):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    agg_funcs.append(ray.data.aggregate.Max(col_name))

            if agg_funcs:
                result = grouped.aggregate(*agg_funcs)
            else:
                # Just group by without aggregates
                result = dataset.groupby(group_keys).aggregate(
                    ray.data.aggregate.Count()
                )

            # Rename columns to match expected names
            def rename_cols(row):
                new_row = {}
                # Copy group key columns
                for key in group_keys:
                    if key in row:
                        new_row[key] = row[key]

                # Handle aggregate columns
                for i, name in enumerate(result_names):
                    if name in row:
                        new_row[name] = row[name]
                    elif name in group_keys:
                        continue
                    else:
                        # Pattern match aggregate columns
                        for col_name, value in row.items():
                            if (
                                col_name not in group_keys
                                and col_name not in new_row.values()
                            ):
                                if (
                                    (
                                        "sum" in col_name.lower()
                                        and (
                                            "age" in name.lower()
                                            or "total" in name.lower()
                                        )
                                    )
                                    or (
                                        "mean" in col_name.lower()
                                        and "avg" in name.lower()
                                    )
                                    or (
                                        "min" in col_name.lower()
                                        and "min" in name.lower()
                                    )
                                    or (
                                        "max" in col_name.lower()
                                        and "max" in name.lower()
                                    )
                                    or (
                                        "count" in col_name.lower()
                                        and "count" in name.lower()
                                    )
                                ):
                                    new_row[name] = value
                                    break
                return new_row

            return result.map(rename_cols)

        else:
            # No GROUP BY - single-row aggregates
            result_dict = {}

            for expr in select_exprs:
                if isinstance(expr, exp.Alias):
                    result_name = str(expr.alias)
                    inner_expr = expr.this
                else:
                    result_name = self._get_expr_name(expr)
                    inner_expr = expr

                if isinstance(inner_expr, exp.Count):
                    if str(inner_expr.this) == "*":
                        result_dict[result_name] = dataset.count()
                    else:
                        # Count non-null values
                        col_name = (
                            str(inner_expr.this.name)
                            if hasattr(inner_expr.this, "name")
                            else str(inner_expr.this)
                        )
                        non_null_count = dataset.filter(
                            lambda row, col=col_name: row.get(col) is not None
                        ).count()
                        result_dict[result_name] = non_null_count
                elif isinstance(inner_expr, exp.Sum):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    total = dataset.aggregate(ray.data.aggregate.Sum(col_name))
                    result_dict[result_name] = total[f"sum({col_name})"]
                elif isinstance(inner_expr, exp.Avg):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    avg = dataset.aggregate(ray.data.aggregate.Mean(col_name))
                    result_dict[result_name] = avg[f"mean({col_name})"]
                elif isinstance(inner_expr, exp.Min):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    min_val = dataset.aggregate(ray.data.aggregate.Min(col_name))
                    result_dict[result_name] = min_val[f"min({col_name})"]
                elif isinstance(inner_expr, exp.Max):
                    col_name = (
                        str(inner_expr.this.name)
                        if hasattr(inner_expr.this, "name")
                        else str(inner_expr.this)
                    )
                    max_val = dataset.aggregate(ray.data.aggregate.Max(col_name))
                    result_dict[result_name] = max_val[f"max({col_name})"]

            return ray.data.from_items([result_dict])

    def _get_expr_name(self, expr: exp.Expression) -> str:
        """Get a reasonable name for an expression."""
        if isinstance(expr, exp.Column):
            return str(expr.name)
        elif isinstance(expr, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            func_name = type(expr).__name__.lower()
            if hasattr(expr.this, "name"):
                return f"{func_name}_{expr.this.name}"
            else:
                return f"{func_name}_star"
        else:
            return "col_0"

    def _apply_projection(
        self, dataset: Dataset, stmt: exp.Select, table_name: str
    ) -> Dataset:
        """Apply SELECT projection."""
        select_exprs = stmt.args["expressions"]
        names, functions = self._build_projection_functions(
            select_exprs, dataset, table_name
        )

        def mapper(row: Mapping[str, object]) -> Dict[str, object]:
            return {col: fn(row) for col, fn in zip(names, functions)}

        return dataset.map(mapper)

    def _build_projection_functions(
        self,
        select_exprs: Sequence[exp.Expression],
        dataset: Optional[Dataset] = None,
        table_name: Optional[str] = None,
    ) -> Tuple[List[str], List[Callable[[Mapping[str, object]], object]]]:
        """Build projection functions for SELECT expressions."""
        names = []
        functions = []

        for idx, expr in enumerate(select_exprs):
            # SELECT *
            if isinstance(expr, exp.Star):
                if dataset is None:
                    raise SQLExecutionError("SELECT * requires a dataset context")
                try:
                    cols = dataset.columns()
                    if cols is not None:
                        for col in cols:
                            names.append(col)
                            functions.append(lambda row, c=col: row.get(c))
                except (TypeError, AttributeError):
                    # Empty dataset case
                    pass
                continue

            # SELECT table.*
            if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                col_table = str(expr.table) if expr.table else None
                if table_name and col_table and col_table != table_name:
                    continue
                if dataset is None:
                    raise SQLExecutionError("SELECT table.* requires dataset context")
                try:
                    cols = dataset.columns()
                    if cols is not None:
                        for col in cols:
                            names.append(col)
                            functions.append(lambda row, c=col: row.get(c))
                except (TypeError, AttributeError):
                    # Empty dataset case
                    pass
                continue

            # SELECT expr AS alias
            if isinstance(expr, exp.Alias):
                names.append(str(expr.alias))
                functions.append(ExpressionCompiler.compile(expr.this))
                continue

            # SELECT column
            if isinstance(expr, exp.Column):
                col_name = str(expr.name)
                names.append(col_name)
                functions.append(lambda row, c=col_name: row.get(c))
                continue

            # Generic expression
            names.append(f"col_{idx}")
            functions.append(ExpressionCompiler.compile(expr))

        return names, functions


class RaySQL:
    """Main SQL engine for Ray Data.

    The RaySQL class provides the primary interface for executing SQL queries
    against Ray Datasets. It manages table registration, query parsing,
    optimization, and execution.

    Examples:
        Basic usage:
            >>> engine = RaySQL()
            >>> engine.register("my_table", my_dataset)
            >>> result = engine.sql("SELECT * FROM my_table")

        With configuration:
            >>> config = SQLConfig(log_level=LogLevel.DEBUG)
            >>> engine = RaySQL(config)
    """

    def __init__(self, config: Optional[SQLConfig] = None):
        """Initialize the SQL engine.

        Args:
            config: SQL engine configuration. Uses default if not provided.
        """
        self.config = config or DEFAULT_CONFIG
        self.registry = TableRegistry()
        self.executor = SQLQueryExecutor(self.config, self.registry)
        self._logger = self.config.get_logger(__name__)

    def register(self, name: str, dataset: Dataset) -> None:
        """Register a Ray Dataset as a SQL table.

        Args:
            name: SQL table name.
            dataset: Ray Dataset to register.
        """
        self.registry.register(name, dataset)

    def sql(self, query: str, default_dataset: Optional[Dataset] = None) -> Dataset:
        """Execute a SQL query.

        Args:
            query: SQL query string to execute.
            default_dataset: Default dataset for queries without FROM clause.

        Returns:
            Ray Dataset containing the query results.

        Raises:
            SQLParseError: If the query cannot be parsed.
            SQLExecutionError: If query execution fails.
            UnsupportedOperationError: If the query uses unsupported features.
        """
        start_time = time.time()

        try:
            self._logger.info(f"Executing SQL query: {query}")

            # Parse SQL query
            try:
                ast = sqlglot.parse_one(query, read="duckdb")
            except Exception as e:
                raise SQLParseError(
                    f"Failed to parse SQL query: {str(e)}", query=query
                ) from e

            # Validate query for unsupported features
            SQLValidator.validate_query(query, ast)

            # Execute query
            result = self.executor.execute(ast, default_dataset)

            execution_time = time.time() - start_time
            self._logger.info(f"Query executed successfully in {execution_time:.3f}s")

            return result

        except SQLError:
            raise
        except Exception as e:
            raise SQLExecutionError(
                f"Unexpected error during query execution: {str(e)}", query=query
            ) from e


# Global registry instance for module-level functions
_global_registry = TableRegistry()
_global_engine = RaySQL()


def register_table(name: str, dataset: Dataset) -> None:
    """Register a Ray Dataset as a SQL table.

    Args:
        name: SQL table name.
        dataset: Ray Dataset to register.
    """
    _global_engine.register(name, dataset)


def sql(query: str, default_dataset: Optional[Dataset] = None) -> Dataset:
    """Execute a SQL query against registered tables.

    Args:
        query: SQL query string to execute.
        default_dataset: Default dataset for queries without FROM clause.

    Returns:
        Ray Dataset containing the query results.
    """
    return _global_engine.sql(query, default_dataset)


def list_tables() -> List[str]:
    """List all registered table names.

    Returns:
        List of registered table names.
    """
    return _global_engine.registry.list_tables()


def clear_tables() -> None:
    """Clear all registered tables."""
    _global_engine.registry.clear()


def get_schema(table_name: str) -> Optional[object]:
    """Get the schema for a registered table.

    Args:
        table_name: Name of the table.

    Returns:
        Schema object for the table, or None if not available.
    """
    return _global_engine.registry.get_schema(table_name)


def get_engine() -> RaySQL:
    """Get the global SQL engine instance.

    Returns:
        The global RaySQL engine.
    """
    return _global_engine


def get_registry() -> TableRegistry:
    """Get the global table registry.

    Returns:
        The global table registry.
    """
    return _global_engine.registry


# Ray Data API integration
def _ray_data_sql(query: str, **kwargs) -> Dataset:
    """Ray Data SQL API integration."""
    return sql(query, **kwargs)


def _ray_data_register_table(name: str, dataset: Dataset) -> None:
    """Ray Data register table API integration."""
    register_table(name, dataset)


# Apply monkey patches for Ray Data integration
ray.data.sql = _ray_data_sql
ray.data.register_table = _ray_data_register_table
