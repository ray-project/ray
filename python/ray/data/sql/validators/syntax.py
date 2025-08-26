"""Syntax validation for Ray Data SQL API.

This module validates SQL query syntax and provides helpful error messages
for malformed queries.
"""

from typing import List, Set

import sqlglot
from sqlglot import exp

from ray.data.sql.exceptions import SQLParseError
from ray.data.sql.validators.base import SQLValidator


class SyntaxValidator(SQLValidator):
    """Validates SQL query syntax and structure.

    This validator ensures that SQL queries have proper syntax and structure,
    checking for common issues like missing clauses, invalid expressions,
    and malformed statements.
    """

    def validate(self, query: str, ast: exp.Expression) -> None:
        """Validate SQL query syntax.

        Args:
            query: Original SQL query string.
            ast: Parsed SQLGlot AST.

        Raises:
            SQLParseError: If the query has syntax errors.
        """
        if not ast:
            raise SQLParseError("Query could not be parsed", query=query)

        # Validate SELECT statements
        if isinstance(ast, exp.Select):
            self._validate_select_syntax(ast, query)

    def get_supported_features(self) -> Set[str]:
        """Get set of supported syntax features.

        Returns:
            Set of supported syntax feature names.
        """
        return {"SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "JOIN"}

    def get_unsupported_features(self) -> Set[str]:
        """Get set of unsupported syntax features.

        Returns:
            Set of unsupported syntax feature names.
        """
        return set()  # Syntax validator doesn't block features, just validates syntax

    def _validate_select_syntax(self, stmt: exp.Select, query: str) -> None:
        """Validate SELECT statement syntax.

        Args:
            stmt: SELECT statement AST.
            query: Original SQL query string.

        Raises:
            SQLParseError: If syntax errors are found.
        """
        # Check for required SELECT expressions
        if not stmt.expressions:
            raise SQLParseError(
                "SELECT statement must have at least one expression",
                query=query,
            )

        # Check for valid FROM clause
        from_clause = stmt.args.get("from")
        if not from_clause:
            raise SQLParseError(
                "SELECT statement must have a FROM clause",
                query=query,
            )

        # Validate JOIN syntax if present
        if from_clause and hasattr(from_clause, "expressions"):
            for expr in from_clause.expressions:
                if hasattr(expr, "joins") and expr.joins:
                    self._validate_join_syntax(expr.joins, query)

        # Validate WHERE clause if present
        where_clause = stmt.args.get("where")
        if where_clause:
            self._validate_where_syntax(where_clause.this, query)

        # Validate GROUP BY if present
        group_clause = stmt.args.get("group")
        if group_clause:
            self._validate_group_by_syntax(group_clause, query)

        # Validate ORDER BY if present
        order_clause = stmt.args.get("order")
        if order_clause:
            self._validate_order_by_syntax(order_clause, query)

        # Validate LIMIT if present
        limit_clause = stmt.args.get("limit")
        if limit_clause:
            self._validate_limit_syntax(limit_clause, query)

    def _validate_join_syntax(self, joins: List[exp.Join], query: str) -> None:
        """Validate JOIN clause syntax.

        Args:
            joins: List of JOIN expressions.
            query: Original SQL query string.

        Raises:
            SQLParseError: If JOIN syntax is invalid.
        """
        for join in joins:
            # Check for ON clause
            if not join.on:
                raise SQLParseError(
                    "JOIN clause must have an ON condition",
                    query=query,
                )

            # Check for valid join type
            if not join.join_type:
                raise SQLParseError(
                    "JOIN clause must specify join type (INNER, LEFT, RIGHT, FULL)",
                    query=query,
                )

    def _validate_where_syntax(self, expr: exp.Expression, query: str) -> None:
        """Validate WHERE clause syntax.

        Args:
            expr: WHERE clause expression.
            query: Original SQL query string.

        Raises:
            SQLParseError: If WHERE syntax is invalid.
        """
        if not expr:
            raise SQLParseError(
                "WHERE clause cannot be empty",
                query=query,
            )

        # Check for valid comparison operators
        if isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)):
            if not expr.left or not expr.right:
                raise SQLParseError(
                    "Comparison operator must have both left and right operands",
                    query=query,
                )

        # Check for valid logical operators
        if isinstance(expr, (exp.And, exp.Or)):
            if not expr.left or not expr.right:
                raise SQLParseError(
                    "Logical operator must have both left and right operands",
                    query=query,
                )

    def _validate_group_by_syntax(self, group_clause: exp.Group, query: str) -> None:
        """Validate GROUP BY clause syntax.

        Args:
            group_clause: GROUP BY clause.
            query: Original SQL query string.

        Raises:
            SQLParseError: If GROUP BY syntax is invalid.
        """
        if not group_clause.expressions:
            raise SQLParseError(
                "GROUP BY clause must specify at least one column",
                query=query,
            )

    def _validate_order_by_syntax(self, order_clause: exp.Order, query: str) -> None:
        """Validate ORDER BY clause syntax.

        Args:
            order_clause: ORDER BY clause.
            query: Original SQL query string.

        Raises:
            SQLParseError: If ORDER BY syntax is invalid.
        """
        if not order_clause.expressions:
            raise SQLParseError(
                "ORDER BY clause must specify at least one column",
                query=query,
            )

    def _validate_limit_syntax(self, limit_clause: exp.Limit, query: str) -> None:
        """Validate LIMIT clause syntax.

        Args:
            limit_clause: LIMIT clause.
            query: Original SQL query string.

        Raises:
            SQLParseError: If LIMIT syntax is invalid.
        """
        if not limit_clause.expression:
            raise SQLParseError(
                "LIMIT clause must specify a number",
                query=query,
            )

        # Check if limit is a positive integer
        try:
            limit_value = int(limit_clause.expression.this)
            if limit_value <= 0:
                raise SQLParseError(
                    "LIMIT value must be positive",
                    query=query,
                )
        except (ValueError, AttributeError):
            raise SQLParseError(
                "LIMIT value must be a positive integer",
                query=query,
            )
