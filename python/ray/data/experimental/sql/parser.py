"""SQL parsing and optimization for Ray Data SQL API.

This module provides SQL parsing, validation, and optional optimization
using SQLGlot.

SQLGlot: SQL parser, transpiler, and optimizer
https://github.com/tobymao/sqlglot
"""

from typing import Dict, Optional

import sqlglot
from sqlglot import exp

try:
    from sqlglot.optimizer import optimize as sqlglot_optimize
except ImportError:
    sqlglot_optimize = None

from ray.data.experimental.sql.config import SQLConfig
from ray.data.experimental.sql.utils import SUPPORTED_AGGREGATES, setup_logger

SUPPORTED_FUNCTIONS = {
    "upper", "lower", "length", "substr", "substring", "trim", "ltrim", "rtrim", "concat", "replace", "like",
    "abs", "ceil", "floor", "round", "sqrt", "pow", "power", "exp", "log", "ln", "sin", "cos", "tan", "asin", "acos", "atan",
    "date", "year", "month", "day", "hour", "minute", "second",
    "cast", "convert", "coalesce", "nullif", "greatest", "least", "case", "when", "if",
}

UNSUPPORTED_AGGREGATE_SUGGESTIONS = {
    "median": "Use approximation with percentile or sorting operations",
    "mode": "Use GROUP BY with COUNT to find most frequent values",
    "variance": "Use STD() for standard deviation instead", "var": "Use STD() for standard deviation instead",
    "percentile": "Use approximation with sorting operations",
    "array_agg": "Use Dataset groupby with collect operations", "string_agg": "Use map operations to concatenate strings",
    "first_value": "Use ROW_NUMBER() with filtering", "last_value": "Use ROW_NUMBER() with filtering", "nth_value": "Use ROW_NUMBER() with filtering",
}

UNSUPPORTED_FUNCTION_SUGGESTIONS = {
    "lag": "Use self-joins with ordering operations", "lead": "Use self-joins with ordering operations",
    "rank": "Use ROW_NUMBER() with ordering", "dense_rank": "Use ROW_NUMBER() with ordering",
    "ntile": "Use manual bucketing with modulo operations", "row_number": "Limited support - use simple ordering without OVER clause",
    "regexp_extract": "Use simpler string functions like SUBSTR or REPLACE", "regexp_replace": "Use REPLACE for simple substitutions", "split_part": "Use manual string parsing operations",
    "json_extract": "Parse JSON data before loading into Ray Data", "json_value": "Parse JSON data before loading into Ray Data", "xml_extract": "Parse XML data before loading into Ray Data",
    "array_contains": "Use standard column operations", "array_length": "Use standard column operations",
    "map_keys": "Use separate columns or manual parsing", "map_values": "Use separate columns or manual parsing",
    "date_trunc": "Use YEAR(), MONTH(), DAY() functions", "extract": "Use YEAR(), MONTH(), DAY(), HOUR(), etc.", "datepart": "Use YEAR(), MONTH(), DAY(), HOUR(), etc.", "interval": "Use date arithmetic with DATE_ADD/DATE_SUB",
}

UNSUPPORTED_CONSTRUCTS = {
    exp.Intersect: {"name": "INTERSECT", "suggestion": "Use JOIN operations to find common records"},
    exp.Except: {"name": "EXCEPT", "suggestion": "Use LEFT JOIN with WHERE IS NULL to exclude records"},
    exp.Window: {"name": "Window functions", "suggestion": "Use GROUP BY or manual partitioning operations"},
    exp.Values: {"name": "VALUES clause", "suggestion": "Create Dataset from Python data structures"},
    exp.Insert: {"name": "INSERT statements", "suggestion": "Use Dataset write operations"},
    exp.Update: {"name": "UPDATE statements", "suggestion": "Use Dataset transformation operations"},
    exp.Delete: {"name": "DELETE statements", "suggestion": "Use Dataset filter operations"},
    exp.Create: {"name": "CREATE statements", "suggestion": "Use register_table() to register existing Datasets"},
    exp.Drop: {"name": "DROP statements", "suggestion": "Use clear_tables() or Python del statements"},
}

UNSUPPORTED_OPERATORS = {
    exp.In: {"name": "IN operator", "suggestion": "Use multiple OR conditions or JOIN operations"},
    exp.Exists: {"name": "EXISTS", "suggestion": "Use JOIN operations instead"},
    exp.All: {"name": "ALL operator", "suggestion": "Use aggregation with MIN/MAX functions"},
    exp.Any: {"name": "ANY/SOME operators", "suggestion": "Use aggregation or JOIN operations"},
    exp.Distinct: {"name": "DISTINCT", "suggestion": "Use GROUP BY or unique() operations on Dataset"},
}


class SQLParser:
    """Parses SQL strings into SQLGlot ASTs and validates for unsupported features."""

    def __init__(self, config: SQLConfig):
        """Initialize SQLParser."""
        self.config = config
        self._logger = setup_logger("SQLParser")

    def parse(self, sql: str) -> exp.Expression:
        """Parse a SQL string into a SQLGlot AST, optionally optimizing it."""
        try:
            dialect = getattr(self.config, "dialect", "duckdb")
            dialect = dialect.value if hasattr(dialect, "value") else dialect
            ast = sqlglot.parse_one(sql, read=dialect)

            if getattr(self.config, "enable_sqlglot_optimizer", False) and sqlglot_optimize:
                before_sql = ast.sql()
                try:
                    ast = sqlglot_optimize(
                        ast,
                        schema=self._build_schema_for_optimizer(),
                        dialect=dialect,
                        rules=self._get_safe_optimization_rules(),
                    )
                    if (after_sql := ast.sql()) != before_sql:
                        self._logger.debug(f"SQLGlot optimized query:\nBefore: {before_sql}\nAfter: {after_sql}")
                except Exception as e:
                    self._logger.warning(f"SQLGlot optimization failed, using original AST: {e}")

            self._validate_ast(ast, sql)
            return ast
        except Exception as e:
            self._logger.error(f"Failed to parse SQL: {sql} - Error: {e}")
            raise ValueError(f"SQL parsing error: {e}")

    def _validate_ast(self, ast: exp.Expression, query: str = "") -> None:
        """Walk the AST and validate syntax and features."""
        if not ast:
            from ray.data.experimental.sql.exceptions import SQLParseError
            raise SQLParseError("Query could not be parsed", query=query)

        if isinstance(ast, exp.Select):
            self._validate_select_syntax(ast, query)

        violations = [v for node in ast.walk() if (v := self._check_node_features(node))]
        if violations:
            self._raise_comprehensive_error(violations)

    def _validate_select_syntax(self, stmt: exp.Select, query: str) -> None:
        """Validate SELECT statement syntax."""
        from ray.data.experimental.sql.exceptions import SQLParseError

        if not stmt.expressions:
            raise SQLParseError(
                "SELECT statement must have at least one expression",
                query=query,
            )

        from_clause = stmt.args.get("from")
        if not from_clause:
            raise SQLParseError(
                "SELECT statement must have a FROM clause",
                query=query,
            )

    def _check_node_features(self, node: exp.Expression) -> str:
        """Check a single AST node for unsupported features."""
        if isinstance(node, exp.AggFunc):
            func_name = node.sql_name().lower()
            if func_name not in SUPPORTED_AGGREGATES:
                suggestion = UNSUPPORTED_AGGREGATE_SUGGESTIONS.get(func_name, "")
                suggestion_text = f". Try: {suggestion}" if suggestion else ""
                return f"Aggregate function '{func_name.upper()}' not supported{suggestion_text}"
        if isinstance(node, exp.Func) and not isinstance(node, exp.AggFunc):
            func_name = node.sql_name().lower()
            if func_name not in SUPPORTED_FUNCTIONS:
                suggestion = UNSUPPORTED_FUNCTION_SUGGESTIONS.get(func_name, "")
                suggestion_text = f". Try: {suggestion}" if suggestion else ""
                return f"Function '{func_name.upper()}' not supported{suggestion_text}"
        node_type = type(node)
        if node_type in UNSUPPORTED_CONSTRUCTS:
            info = UNSUPPORTED_CONSTRUCTS[node_type]
            suggestion_text = f". Try: {info['suggestion']}" if info.get("suggestion") else ""
            return f"{info['name']} not supported{suggestion_text}"
        if node_type in UNSUPPORTED_OPERATORS:
            info = UNSUPPORTED_OPERATORS[node_type]
            suggestion_text = f". Try: {info['suggestion']}" if info.get("suggestion") else ""
            return f"{info['name']} not supported{suggestion_text}"
        return ""

    def _raise_comprehensive_error(self, violations: list) -> None:
        """Raise a comprehensive validation error with all violations."""
        error_msg = "SQL validation failed with the following issues:\n" + "\n".join(f"  {i}. {v}" for i, v in enumerate(violations, 1)) + "\n\nSee Ray Data SQL documentation for supported features."
        raise NotImplementedError(error_msg)

    def _build_schema_for_optimizer(self) -> Optional[Dict]:
        """Build schema dictionary for sqlglot optimizer from registered tables."""
        try:
            from ray.data.experimental.sql.core import get_engine
            engine = get_engine()
            registered_tables = engine.list_tables()
            if not registered_tables:
                return None
            schema = {}
            for table_name in registered_tables:
                if table_schema := engine.get_schema(table_name):
                    sql_schema = {}
                    for col_name, col_type in table_schema.items():
                        col_type_lower = col_type.lower()
                        sql_schema[col_name] = ("BIGINT" if "int" in col_type_lower else "DOUBLE" if "float" in col_type_lower or "double" in col_type_lower else "VARCHAR" if "str" in col_type_lower or "string" in col_type_lower else "BOOLEAN" if "bool" in col_type_lower else "VARCHAR")
                    schema[table_name] = sql_schema
            return schema if schema else None
        except Exception as e:
            self._logger.debug(f"Could not build schema for optimizer: {e}")
            return None

    def _get_safe_optimization_rules(self) -> tuple:
        """Get safe optimization rules that preserve Ray Data execution semantics."""
        try:
            from sqlglot.optimizer import (
                eliminate_subqueries,
                merge_subqueries,
                normalize,
                pushdown_predicates,
                pushdown_projections,
                simplify,
            )

            return (
                normalize,
                simplify,
                pushdown_predicates,
                pushdown_projections,
                eliminate_subqueries,
                merge_subqueries,
            )
        except ImportError:
            self._logger.warning(
                "Could not import sqlglot optimization rules, skipping optimization"
            )
            return ()


