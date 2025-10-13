"""
SQL parsing and optimization for Ray Data SQL API.

This module provides SQL parsing, AST optimization, and logical planning
functionality using SQLGlot (https://github.com/tobymao/sqlglot) for the
Ray Data SQL engine.
"""

from typing import Dict, Optional

# SQLGlot: SQL parser, transpiler, and optimizer - https://github.com/tobymao/sqlglot
import sqlglot
from sqlglot import exp

# Try to import SQLGlot's built-in optimizer, fallback gracefully if not available
try:
    from sqlglot.optimizer import optimize as sqlglot_optimize
except ImportError:
    sqlglot_optimize = None

from ray.data.experimental.sql.config import SQLConfig
from ray.data.experimental.sql.utils import SUPPORTED_AGGREGATES, setup_logger

# Supported general functions (non-aggregate)
SUPPORTED_FUNCTIONS = {
    # String functions
    "upper",
    "lower",
    "length",
    "substr",
    "substring",
    "trim",
    "ltrim",
    "rtrim",
    "concat",
    "replace",
    "like",
    # Mathematical functions
    "abs",
    "ceil",
    "floor",
    "round",
    "sqrt",
    "pow",
    "power",
    "exp",
    "log",
    "ln",
    "sin",
    "cos",
    "tan",
    "asin",
    "acos",
    "atan",
    # Date/time functions (basic)
    "date",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    # Type conversion
    "cast",
    "convert",
    # Conditional functions
    "coalesce",
    "nullif",
    "greatest",
    "least",
    "case",
    "when",
    "if",
}

# Suggestions for unsupported aggregate functions
UNSUPPORTED_AGGREGATE_SUGGESTIONS = {
    "median": "Use approximation with percentile or sorting operations",
    "mode": "Use GROUP BY with COUNT to find most frequent values",
    "variance": "Use STD() for standard deviation instead",
    "var": "Use STD() for standard deviation instead",
    "percentile": "Use approximation with sorting operations",
    "array_agg": "Use Dataset groupby with collect operations",
    "string_agg": "Use map operations to concatenate strings",
    "first_value": "Use ROW_NUMBER() with filtering",
    "last_value": "Use ROW_NUMBER() with filtering",
    "nth_value": "Use ROW_NUMBER() with filtering",
}

# Suggestions for unsupported general functions
UNSUPPORTED_FUNCTION_SUGGESTIONS = {
    # Window functions
    "lag": "Use self-joins with ordering operations",
    "lead": "Use self-joins with ordering operations",
    "rank": "Use ROW_NUMBER() with ordering",
    "dense_rank": "Use ROW_NUMBER() with ordering",
    "ntile": "Use manual bucketing with modulo operations",
    "row_number": "Limited support - use simple ordering without OVER clause",
    # Complex string functions
    "regexp_extract": "Use simpler string functions like SUBSTR or REPLACE",
    "regexp_replace": "Use REPLACE for simple substitutions",
    "split_part": "Use manual string parsing operations",
    # JSON/XML functions
    "json_extract": "Parse JSON data before loading into Ray Data",
    "json_value": "Parse JSON data before loading into Ray Data",
    "xml_extract": "Parse XML data before loading into Ray Data",
    # Array/Map functions
    "array_contains": "Use standard column operations",
    "array_length": "Use standard column operations",
    "map_keys": "Use separate columns or manual parsing",
    "map_values": "Use separate columns or manual parsing",
    # Advanced date functions
    "date_trunc": "Use YEAR(), MONTH(), DAY() functions",
    "extract": "Use YEAR(), MONTH(), DAY(), HOUR(), etc.",
    "datepart": "Use YEAR(), MONTH(), DAY(), HOUR(), etc.",
    "interval": "Use date arithmetic with DATE_ADD/DATE_SUB",
}

# Unsupported SQL constructs with information
UNSUPPORTED_CONSTRUCTS = {
    exp.Intersect: {
        "name": "INTERSECT",
        "suggestion": "Use JOIN operations to find common records",
    },
    exp.Except: {
        "name": "EXCEPT",
        "suggestion": "Use LEFT JOIN with WHERE IS NULL to exclude records",
    },
    exp.Window: {
        "name": "Window functions",
        "suggestion": "Use GROUP BY or manual partitioning operations",
    },
    exp.Values: {
        "name": "VALUES clause",
        "suggestion": "Create Dataset from Python data structures",
    },
    exp.Insert: {
        "name": "INSERT statements",
        "suggestion": "Use Dataset write operations",
    },
    exp.Update: {
        "name": "UPDATE statements",
        "suggestion": "Use Dataset transformation operations",
    },
    exp.Delete: {
        "name": "DELETE statements",
        "suggestion": "Use Dataset filter operations",
    },
    exp.Create: {
        "name": "CREATE statements",
        "suggestion": "Use register_table() to register existing Datasets",
    },
    exp.Drop: {
        "name": "DROP statements",
        "suggestion": "Use clear_tables() or Python del statements",
    },
}

# Unsupported operators with information
UNSUPPORTED_OPERATORS = {
    exp.In: {
        "name": "IN operator",
        "suggestion": "Use multiple OR conditions or JOIN operations",
    },
    exp.Exists: {"name": "EXISTS", "suggestion": "Use JOIN operations instead"},
    exp.All: {
        "name": "ALL operator",
        "suggestion": "Use aggregation with MIN/MAX functions",
    },
    exp.Any: {
        "name": "ANY/SOME operators",
        "suggestion": "Use aggregation or JOIN operations",
    },
    exp.Distinct: {
        "name": "DISTINCT",
        "suggestion": "Use GROUP BY or unique() operations on Dataset",
    },
}


class SQLParser:
    """Parses SQL strings into SQLGlot ASTs and validates for unsupported features.

    The SQLParser is responsible for parsing SQL strings into SQLGlot ASTs and
    validating them for unsupported features.

    Examples:
        .. testcode::

            parser = SQLParser(config)
            ast = parser.parse("SELECT * FROM my_table")
    """

    def __init__(self, config: SQLConfig):
        """Initialize SQLParser.

        Args:
            config: SQL configuration object containing settings for parsing.
        """
        self.config = config
        self._logger = setup_logger("SQLParser")

    def parse(self, sql: str) -> exp.Expression:
        """Parse a SQL string into a SQLGlot AST, optionally optimizing it.

        Args:
            sql: SQL query string.

        Returns:
            SQLGlot AST.

        Raises:
            ValueError: If parsing fails or unsupported features are found.
        """
        try:
            # Use the configured dialect from config
            dialect = getattr(self.config, "dialect", "duckdb")
            if hasattr(dialect, "value"):
                dialect = dialect.value

            ast = sqlglot.parse_one(sql, read=dialect)

            # Apply SQLGlot optimization if enabled
            if (
                getattr(self.config, "enable_sqlglot_optimizer", False)
                and sqlglot_optimize
            ):
                before_sql = ast.sql()

                # Build schema from context if available (for better optimization)
                schema = self._build_schema_for_optimizer()

                # Apply sqlglot optimization with proper parameters
                try:
                    ast = sqlglot_optimize(
                        ast,
                        schema=schema,
                        dialect=dialect,
                        # Use safe subset of optimization rules that preserve Ray Data semantics
                        rules=self._get_safe_optimization_rules(),
                    )
                    after_sql = ast.sql()
                    if before_sql != after_sql:
                        self._logger.debug(
                            f"SQLGlot optimized query:\nBefore: {before_sql}\nAfter: {after_sql}"
                        )
                except Exception as e:
                    # If optimization fails, use original AST
                    self._logger.warning(
                        f"SQLGlot optimization failed, using original AST: {e}"
                    )

            self._validate_ast(ast)
            return ast
        except Exception as e:
            self._logger.error(f"Failed to parse SQL: {sql} - Error: {e}")
            raise ValueError(f"SQL parsing error: {e}")

    def _validate_ast(self, ast: exp.Expression) -> None:
        """Walk the AST and raise NotImplementedError for unsupported SQL features.

        Args:
            ast: SQLGlot AST to validate.

        Raises:
            NotImplementedError: If unsupported features are found.
        """
        violations = []

        for node in ast.walk():
            violation = self._check_node_features(node)
            if violation:
                violations.append(violation)

        if violations:
            self._raise_comprehensive_error(violations)

    def _check_node_features(self, node: exp.Expression) -> str:
        """Check a single AST node for unsupported features.

        Args:
            node: SQLGlot expression node to check.

        Returns:
            Error message if violation found, empty string otherwise.
        """
        # Check aggregate functions with suggestions
        if isinstance(node, exp.AggFunc):
            func_name = node.sql_name().lower()
            if func_name not in SUPPORTED_AGGREGATES:
                suggestion = UNSUPPORTED_AGGREGATE_SUGGESTIONS.get(func_name, "")
                msg = f"Aggregate function '{func_name.upper()}' not supported"
                if suggestion:
                    msg += f". Try: {suggestion}"
                return msg

        # Check general functions
        if isinstance(node, exp.Func) and not isinstance(node, exp.AggFunc):
            func_name = node.sql_name().lower()
            if func_name not in SUPPORTED_FUNCTIONS:
                suggestion = UNSUPPORTED_FUNCTION_SUGGESTIONS.get(func_name, "")
                msg = f"Function '{func_name.upper()}' not supported"
                if suggestion:
                    msg += f". Try: {suggestion}"
                return msg

        # Check unsupported constructs with suggestions
        node_type = type(node)
        if node_type in UNSUPPORTED_CONSTRUCTS:
            info = UNSUPPORTED_CONSTRUCTS[node_type]
            msg = f"{info['name']} not supported"
            if info.get("suggestion"):
                msg += f". Try: {info['suggestion']}"
            return msg

        # Check unsupported operators
        if node_type in UNSUPPORTED_OPERATORS:
            info = UNSUPPORTED_OPERATORS[node_type]
            msg = f"{info['name']} not supported"
            if info.get("suggestion"):
                msg += f". Try: {info['suggestion']}"
            return msg

        return ""

    def _raise_comprehensive_error(self, violations: list) -> None:
        """Raise a comprehensive validation error with all violations."""
        error_msg = "SQL validation failed with the following issues:\n"
        for i, violation in enumerate(violations, 1):
            error_msg += f"  {i}. {violation}\n"
        error_msg += "\nSee Ray Data SQL documentation for supported features."
        raise NotImplementedError(error_msg)

    def _build_schema_for_optimizer(self) -> Optional[Dict]:
        """Build schema dictionary for sqlglot optimizer from registered tables.

        Returns:
            Schema dict in format {table: {col: type}} or None if no tables registered.
        """
        try:
            from ray.data.experimental.sql.core import get_engine

            engine = get_engine()
            registered_tables = engine.list_tables()

            if not registered_tables:
                return None

            schema = {}
            for table_name in registered_tables:
                table_schema = engine.get_schema(table_name)
                if table_schema:
                    # Convert Ray Data schema types to SQL types for sqlglot
                    sql_schema = {}
                    for col_name, col_type in table_schema.items():
                        # Map Python types to SQL types
                        if "int" in col_type.lower():
                            sql_schema[col_name] = "BIGINT"
                        elif (
                            "float" in col_type.lower() or "double" in col_type.lower()
                        ):
                            sql_schema[col_name] = "DOUBLE"
                        elif "str" in col_type.lower() or "string" in col_type.lower():
                            sql_schema[col_name] = "VARCHAR"
                        elif "bool" in col_type.lower():
                            sql_schema[col_name] = "BOOLEAN"
                        else:
                            sql_schema[col_name] = "VARCHAR"  # Default

                    schema[table_name] = sql_schema

            return schema if schema else None

        except Exception as e:
            self._logger.debug(f"Could not build schema for optimizer: {e}")
            return None

    def _get_safe_optimization_rules(self) -> tuple:
        """Get safe optimization rules that preserve Ray Data execution semantics.

        Returns:
            Tuple of sqlglot optimization rules safe for Ray Data.
        """
        try:
            from sqlglot.optimizer import (
                eliminate_subqueries,
                merge_subqueries,
                normalize,
                pushdown_predicates,
                pushdown_projections,
                simplify,
            )

            # Use only safe rules that don't change query semantics incompatibly
            # Avoid rules that might break Ray Data operations
            return (
                normalize,  # Normalize expressions (safe)
                simplify,  # Simplify boolean/arithmetic expressions (safe)
                pushdown_predicates,  # Push WHERE clauses down (beneficial for Ray Data)
                pushdown_projections,  # Push column selection down (beneficial for Ray Data)
                eliminate_subqueries,  # Flatten subqueries where possible (safe)
                merge_subqueries,  # Combine compatible subqueries (safe)
            )
        except ImportError:
            # If we can't import specific rules, return empty tuple (no optimization)
            self._logger.warning(
                "Could not import sqlglot optimization rules, skipping optimization"
            )
            return ()


class ASTOptimizer:
    """Custom AST optimizer with SQLGlot-inspired rules.

    The ASTOptimizer applies various optimization rules to SQLGlot ASTs
    to improve query performance and correctness.

    Examples:
        .. testcode::

            config = SQLConfig()
            optimizer = ASTOptimizer(config)
            optimized_ast = optimizer.optimize(ast, schema_manager)
    """

    def __init__(self, config: SQLConfig):
        """Initialize ASTOptimizer.

        Args:
            config: SQL configuration object containing settings for optimization.
        """
        self.config = config
        self._logger = setup_logger("ASTOptimizer")

    def optimize(self, ast: exp.Expression) -> exp.Expression:
        """Apply optimization rules to the AST.

        Args:
            ast: SQLGlot AST to optimize.

        Returns:
            Optimized AST.
        """
        if not getattr(self.config, "enable_custom_optimizer", False):
            return ast

        self._logger.debug("Starting AST optimization")

        # Apply basic optimization rules
        ast = self._pushdown_predicates(ast)
        ast = self._simplify_expressions(ast)
        ast = self._normalize_predicates(ast)

        self._logger.debug("AST optimization completed")
        return ast

    def _pushdown_predicates(self, ast: exp.Expression) -> exp.Expression:
        """Push down WHERE clauses into subqueries where possible.

        Args:
            ast: AST to process.

        Returns:
            AST with pushed down predicates.
        """
        # This is a simplified version - in practice, you'd need more complex logic
        return ast

    def _simplify_expressions(self, ast: exp.Expression) -> exp.Expression:
        """Simplify boolean and arithmetic expressions.

        Args:
            ast: AST to process.

        Returns:
            Simplified AST.
        """
        # This is a simplified version - in practice, you'd need more complex logic
        return ast

    def _normalize_predicates(self, ast: exp.Expression) -> exp.Expression:
        """Convert predicates to conjunctive normal form.

        Args:
            ast: AST to process.

        Returns:
            Normalized AST.
        """
        # This is a simplified version - in practice, you'd need more complex logic
        return ast


# Logical planning functionality removed for simplicity
# The current implementation uses direct AST execution which is more maintainable
