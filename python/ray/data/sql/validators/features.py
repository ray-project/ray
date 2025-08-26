"""Feature validator for SQL queries.

This module validates SQL queries against supported and unsupported features.
"""

from typing import Set

from sqlglot import exp

from .base import SQLValidator


class FeatureValidator(SQLValidator):
    """Validates SQL query features against supported capabilities.

    This validator checks if the SQL query uses features that are supported
    by the Ray Data SQL engine.
    """

    # Supported SQL statement types
    SUPPORTED_STATEMENTS = {exp.Select}

    # Supported expression types for WHERE clauses
    SUPPORTED_WHERE_EXPRESSIONS = {
        exp.Column,
        exp.Identifier,  # Allow column identifiers in WHERE clauses
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
        exp.In,  # Now supported with subqueries
        exp.Exists,  # Now supported with subqueries
        exp.Like,  # String pattern matching
        exp.Between,  # Range comparisons
        exp.Cast,  # Type casting
        exp.Case,  # CASE expressions
        exp.Subquery,  # Now supported in FROM and WHERE clauses
    }

    # Supported aggregate functions
    SUPPORTED_AGGREGATES = {
        exp.Count,
        exp.Sum,
        exp.Avg,
        exp.Min,
        exp.Max,
        exp.Stddev,  # Standard deviation
        exp.Variance,  # Variance
    }

    # Supported string functions
    SUPPORTED_STRING_FUNCTIONS = {
        exp.Upper,
        exp.Lower,
    }

    # Supported date/time functions
    SUPPORTED_DATETIME_FUNCTIONS = {
        exp.Year,
        exp.Month,
        exp.Day,
        exp.Date,
        exp.Timestamp,
    }

    # Unsupported features that should raise clear errors
    UNSUPPORTED_FEATURES = {
        exp.Distinct: "DISTINCT is not yet supported. Use ray.data.Dataset.unique() after the query",
        exp.Union: "UNION is not yet supported. Use ray.data.Dataset.union() to combine datasets",
        exp.Intersect: "INTERSECT is not yet supported",
        exp.Except: "EXCEPT is not yet supported",
        exp.Window: "Window functions are not yet supported",
        exp.With: "Common Table Expressions (WITH) are not yet supported",
        exp.Insert: "INSERT statements are not supported. Use ray.data.Dataset.write_*() methods",
        exp.Update: "UPDATE statements are not supported. Use ray.data.Dataset.map() for transformations",
        exp.Delete: "DELETE statements are not supported. Use ray.data.Dataset.filter() for filtering",
        exp.Create: "CREATE statements are not supported. Use ray.data.from_*() methods to create datasets",
        exp.Drop: "DROP statements are not supported. Use clear_tables() or del operations",
        exp.Having: "HAVING clauses are not yet supported. Apply filtering after GROUP BY using Dataset.filter()",
    }

    def validate(self, query: str, ast: exp.Expression) -> None:
        """Validate a SQL query for supported features.

        Args:
            query: Original SQL query string.
            ast: Parsed SQLGlot AST.

        Raises:
            ValueError: If the query contains unsupported features.
        """
        # Check for unsupported statement types
        if not any(
            isinstance(ast, stmt_type) for stmt_type in self.SUPPORTED_STATEMENTS
        ):
            statement_type = type(ast).__name__
            raise ValueError(
                f"{statement_type} statements are not supported. Only SELECT statements are currently supported"
            )

        # Check for unsupported features in the AST
        self._check_unsupported_features(ast, query)

        # Validate SELECT-specific constructs
        if isinstance(ast, exp.Select):
            self._validate_select_statement(ast, query)

    def _check_unsupported_features(self, ast: exp.Expression, query: str) -> None:
        """Check for unsupported features in the AST."""
        for feature_type in self.UNSUPPORTED_FEATURES:
            for node in ast.find_all(feature_type):
                feature_name = feature_type.__name__
                suggestion = self.UNSUPPORTED_FEATURES[feature_type]

                raise ValueError(
                    f"{feature_name} operations are not supported: {suggestion}"
                )

    def _validate_select_statement(self, stmt: exp.Select, query: str) -> None:
        """Validate SELECT statement specific constructs."""
        # Check WHERE clause complexity
        where_clause = stmt.args.get("where")
        if where_clause:
            self._validate_where_clause(where_clause.this, query)

        # Check GROUP BY - now supported
        group_by = stmt.args.get("group")
        if group_by:
            self._validate_group_by(group_by, query)

        # Check ORDER BY - now supported
        order_by = stmt.args.get("order")
        if order_by:
            self._validate_order_by(order_by, query)

        # Check LIMIT - now supported
        limit = stmt.args.get("limit")
        if limit:
            self._validate_limit(limit, query)

    def _validate_where_clause(self, expr: exp.Expression, query: str) -> None:
        """Validate WHERE clause expressions recursively."""
        expr_type = type(expr)

        if expr_type not in self.SUPPORTED_WHERE_EXPRESSIONS:
            raise ValueError(f"{expr_type.__name__} in WHERE clause is not supported")

        # Recursively validate child expressions
        if hasattr(expr, "left") and expr.left:
            self._validate_where_clause(expr.left, query)
        if hasattr(expr, "right") and expr.right:
            self._validate_where_clause(expr.right, query)
        if (
            hasattr(expr, "this")
            and expr.this
            and isinstance(expr.this, exp.Expression)
        ):
            self._validate_where_clause(expr.this, query)

        # Special handling for subqueries
        if isinstance(expr, exp.Subquery):
            self._validate_subquery(expr.this, query)
        elif isinstance(expr, exp.In) and expr.expressions:
            # Check if IN clause contains subquery
            for sub_expr in expr.expressions:
                if isinstance(sub_expr, exp.Subquery):
                    self._validate_subquery(sub_expr.this, query)

    def _validate_group_by(self, group_by: exp.Group, query: str) -> None:
        """Validate GROUP BY clause."""
        for expr in group_by.expressions:
            if isinstance(expr, exp.Column):
                # Simple column reference - supported
                continue
            elif isinstance(expr, exp.Identifier):
                # Column identifier - supported
                continue
            else:
                # Complex expressions in GROUP BY not yet supported
                raise ValueError(
                    f"Complex expressions in GROUP BY are not yet supported: {type(expr).__name__}"
                )

    def _validate_order_by(self, order_by: exp.Order, query: str) -> None:
        """Validate ORDER BY clause."""
        for ordering in order_by.expressions:
            if isinstance(ordering, exp.Ordered):
                sort_expr = ordering.this
                if isinstance(sort_expr, (exp.Column, exp.Identifier)):
                    # Simple column reference - supported
                    continue
                elif isinstance(sort_expr, exp.Literal):
                    # Position-based ordering (ORDER BY 1, 2) - supported
                    continue
                else:
                    # Complex expressions in ORDER BY not yet supported
                    raise ValueError(
                        f"Complex expressions in ORDER BY are not yet supported: {type(sort_expr).__name__}"
                    )

    def _validate_limit(self, limit: exp.Limit, query: str) -> None:
        """Validate LIMIT clause."""
        # LIMIT is fully supported
        pass

    def _validate_subquery(self, subquery_ast: exp.Expression, query: str) -> None:
        """Validate subquery expressions."""
        if isinstance(subquery_ast, exp.Select):
            # Recursively validate the subquery
            self._validate_select_statement(subquery_ast, query)
        else:
            raise ValueError(
                f"Unsupported subquery type: {type(subquery_ast).__name__}"
            )

    def get_supported_features(self) -> Set[str]:
        """Get the set of supported SQL features.

        Returns:
            Set of supported feature names.
        """
        features = set()

        # Add supported statement types
        for stmt_type in self.SUPPORTED_STATEMENTS:
            features.add(f"{stmt_type.__name__} statements")

        # Add supported WHERE expressions
        for expr_type in self.SUPPORTED_WHERE_EXPRESSIONS:
            features.add(f"{expr_type.__name__} in WHERE clauses")

        # Add supported aggregates
        for agg_type in self.SUPPORTED_AGGREGATES:
            features.add(f"{agg_type.__name__} aggregate functions")

        # Add supported string functions
        for func_type in self.SUPPORTED_STRING_FUNCTIONS:
            features.add(f"{func_type.__name__} string functions")

        # Add supported date/time functions
        for func_type in self.SUPPORTED_DATETIME_FUNCTIONS:
            features.add(f"{func_type.__name__} date/time functions")

        # Add other supported features
        features.update(
            [
                "JOIN operations (INNER, LEFT, RIGHT, FULL)",
                "GROUP BY with aggregation",
                "ORDER BY with multiple columns",
                "LIMIT clause",
                "Subqueries in FROM and WHERE clauses",
                "IN subqueries",
                "EXISTS subqueries",
                "Comparison with subqueries",
                "Table aliases",
                "Column aliases",
                "Arithmetic expressions",
                "Logical expressions",
                "NULL handling",
                "String pattern matching (LIKE)",
                "Range comparisons (BETWEEN)",
                "Type casting (CAST)",
                "Conditional expressions (CASE)",
            ]
        )

        return features

    def get_unsupported_features(self) -> Set[str]:
        """Get the set of unsupported SQL features.

        Returns:
            Set of unsupported feature names.
        """
        features = set()

        for feature_type in self.UNSUPPORTED_FEATURES:
            features.add(f"{feature_type.__name__} operations")

        # Add other unsupported features
        features.update(
            [
                "Window functions",
                "Common Table Expressions (WITH)",
                "HAVING clauses",
                "Complex expressions in GROUP BY",
                "Complex expressions in ORDER BY",
                "Correlated subqueries",
                "Recursive queries",
                "Materialized views",
                "Stored procedures",
                "User-defined functions",
                "Triggers",
                "Transactions",
                "Locking",
                "Partitioning",
            ]
        )

        return features
