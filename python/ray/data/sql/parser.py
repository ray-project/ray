"""
SQL parsing and optimization for Ray Data SQL API.

This module provides SQL parsing, AST optimization, and logical planning
functionality using SQLGlot for the Ray Data SQL engine.
"""

from typing import Optional

import sqlglot
from sqlglot import exp

# Try to import optimize, fallback gracefully if not available
try:
    from sqlglot.optimizer import optimize as sqlglot_optimize
except ImportError:
    sqlglot_optimize = None

from ray.data.sql.config import LogicalPlan, SQLConfig
from ray.data.sql.schema import SchemaManager
from ray.data.sql.utils import SUPPORTED_AGGREGATES, setup_logger

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
    exp.CTE: {
        "name": "Common Table Expressions (WITH)",
        "suggestion": "Break into multiple steps using intermediate tables",
    },
    exp.Union: {"name": "UNION", "suggestion": "Use Dataset.union() method instead"},
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
            ast = sqlglot.parse_one(sql, read="duckdb")
            if getattr(self.config, "enable_sqlglot_optimizer", False):
                before = ast
                ast = sqlglot_optimize(ast) if sqlglot_optimize else ast
                self._logger.debug(
                    f"SQLGlot optimized AST:\nBefore: {before}\nAfter: {ast}"
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

    def optimize(
        self, ast: exp.Expression, schema_manager: SchemaManager
    ) -> exp.Expression:
        """Apply optimization rules to the AST.

        Args:
            ast: SQLGlot AST to optimize.
            schema_manager: Schema manager for column information.

        Returns:
            Optimized AST.
        """
        if not self.config.enable_custom_optimizer:
            return ast

        self._logger.debug("Starting AST optimization")

        # Apply optimization rules
        ast = self._qualify_columns(ast, schema_manager)
        ast = self._pushdown_predicates(ast)
        ast = self._simplify_expressions(ast)
        ast = self._normalize_predicates(ast)

        self._logger.debug("AST optimization completed")
        return ast

    def _qualify_columns(
        self, ast: exp.Expression, schema_manager: SchemaManager
    ) -> exp.Expression:
        """Add table qualifiers to ambiguous columns.

        Args:
            ast: AST to process.
            schema_manager: Schema manager for column information.

        Returns:
            AST with qualified columns.
        """
        # This is a simplified version - in practice, you'd need more complex logic
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


class LogicalPlanner:
    """Converts optimized AST into logical plan.

    The LogicalPlanner takes an optimized SQLGlot AST and converts it into
    a logical execution plan that can be executed by the query engine.

    Examples:
        .. testcode::

            config = SQLConfig()
            planner = LogicalPlanner(config)
            plan = planner.plan(ast)
    """

    def __init__(self, config: SQLConfig):
        """Initialize LogicalPlanner.

        Args:
            config: SQL configuration object containing settings for planning.
        """
        self.config = config
        self._logger = setup_logger("LogicalPlanner")

    def plan(self, ast: exp.Expression) -> Optional[LogicalPlan]:
        """Convert AST to logical plan.

        Args:
            ast: SQLGlot AST to convert.

        Returns:
            Logical execution plan.

        Raises:
            NotImplementedError: If planning is not implemented for the AST type.
        """
        if not self.config.enable_logical_planning:
            return None

        self._logger.debug("Starting logical planning")

        if isinstance(ast, exp.Select):
            return self._plan_select(ast)
        elif isinstance(ast, exp.Create):
            # CREATE TABLE AS SELECT doesn't need logical planning
            # It will be handled directly in the executor
            self._logger.debug("CREATE statement detected - skipping logical planning")
            return None
        else:
            raise NotImplementedError(
                f"Planning not implemented for {type(ast).__name__}"
            )

    def _plan_select(self, ast: exp.Select) -> LogicalPlan:
        """Plan a SELECT statement.

        Args:
            ast: SELECT AST to plan.

        Returns:
            Logical plan for the SELECT statement.
        """
        # This is a simplified version - in practice, you'd need more complex logic
        plan = LogicalPlan(LogicalPlan.Operation.SELECT, ast=ast)
        self._logger.debug(f"Created logical plan: {plan}")
        return plan
