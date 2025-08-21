"""
Utility functions for Ray Data SQL API.

This module contains utility functions used throughout the SQL engine,
including identifier normalization, logging setup, column mappings,
and other helper functions.
"""

import logging
import re
from typing import Any, Dict, List, Optional

from sqlglot import exp


def setup_logger(name: str = "RaySQL") -> logging.Logger:
    """Set up a logger with a standard format for the SQL engine.

    Args:
        name: Logger name.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.WARNING)
    return logger


def normalize_identifier(name: str, case_sensitive: bool = False) -> str:
    """Normalize SQL identifiers for case-insensitive matching.

    Args:
        name: Identifier to normalize.
        case_sensitive: Whether to preserve case.

    Returns:
        Normalized identifier.
    """
    if name is None:
        return None
    name = str(name).strip()
    return name if case_sensitive else name.lower()


def safe_get_column(
    row: Dict[str, Any], column_name: str, case_sensitive: bool = False
) -> Any:
    """Safely retrieve a column value from a row dictionary.

    Args:
        row: Row dictionary.
        column_name: Name of the column to retrieve.
        case_sensitive: Whether column name matching is case sensitive.

    Returns:
        Column value.

    Raises:
        KeyError: If column is not found.
    """
    if column_name in row:
        return row[column_name]
    if not case_sensitive:
        normalized_name = normalize_identifier(column_name, case_sensitive=False)
        for key, value in row.items():
            if normalize_identifier(key, case_sensitive=False) == normalized_name:
                return value
    raise KeyError(
        f"Column '{column_name}' not found in row. Available columns: {list(row.keys())}"
    )


def create_column_mapping(
    columns: List[str], case_sensitive: bool = False
) -> Dict[str, str]:
    """Create a mapping from normalized column names to their actual names.

    Args:
        columns: List of column names.
        case_sensitive: Whether to preserve case in normalization.

    Returns:
        Dictionary mapping normalized names to actual names.
    """
    mapping = {}
    if columns:
        for col in columns:
            normalized = normalize_identifier(col, case_sensitive)
            if normalized not in mapping:
                mapping[normalized] = col
    return mapping


def _is_create_table_as(ast: exp.Expression) -> bool:
    """Check if the AST represents a CREATE TABLE ... AS SELECT ... statement.

    Args:
        ast: SQLGlot AST to check.

    Returns:
        True if it's a CREATE TABLE AS statement.
    """
    return (
        hasattr(ast, "args")
        and isinstance(ast, exp.Create)
        and "expression" in ast.args
        and isinstance(ast.args["expression"], exp.Select)
    )


def extract_table_names_from_query(query: str) -> set:
    """Extract table names from a SQL query using sqlglot parsing.

    Uses sqlglot's AST parsing to robustly extract table names, handling
    complex queries with subqueries, CTEs, and comments better than regex.
    This is crucial for validating that all referenced tables are registered
    before query execution.

    Args:
        query: SQL query string to analyze (e.g., "SELECT * FROM users JOIN orders").

    Returns:
        Set of table names found in the query (e.g., {"users", "orders"}).
    """
    try:
        import sqlglot

        # Parse the query into an AST using DuckDB SQL dialect (most permissive)
        parsed = sqlglot.parse_one(query, read="duckdb")

        # Find all table references in the AST (FROM, JOIN, subqueries, etc.)
        return {table.name for table in parsed.find_all(sqlglot.exp.Table)}

    except Exception:
        # Fallback to regex-based extraction for invalid/malformed queries
        # This is less accurate but provides basic functionality when parsing fails
        table_names = set()

        # Find tables in FROM clauses using regex pattern
        # Pattern matches: FROM table_name (case insensitive)
        from_matches = re.findall(
            r"from\s+([a-zA-Z_][a-zA-Z0-9_]*)", query, re.IGNORECASE
        )
        table_names.update(from_matches)

        # Find tables in JOIN clauses using regex pattern
        # Pattern matches: JOIN table_name (case insensitive)
        join_matches = re.findall(
            r"join\s+([a-zA-Z_][a-zA-Z0-9_]*)", query, re.IGNORECASE
        )
        table_names.update(join_matches)

        return table_names


def validate_table_name(name: str) -> None:
    """Validate a table name according to SQL standards.

    Args:
        name: Table name to validate.

    Raises:
        ValueError: If table name is invalid.
    """
    if not name or not isinstance(name, str):
        raise ValueError("Table name must be a non-empty string")

    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(
            f"Invalid table name '{name}'. Must start with letter or underscore and contain only alphanumeric characters and underscores"
        )


def safe_divide(a: Any, b: Any) -> float:
    """Safely divide two values, handling division by zero and type errors.

    This function is used in SQL arithmetic expressions to prevent crashes
    when dividing by zero or when operands are not numeric. It follows SQL
    standards for division behavior with special values.

    Args:
        a: Numerator (dividend) - any value that can be converted to float.
        b: Denominator (divisor) - any value that can be converted to float.

    Returns:
        Result of division as float, with special handling:
        - Normal division: a / b
        - Positive / 0: +infinity
        - Negative / 0: -infinity
        - 0 / 0: NaN (Not a Number)
        - Invalid types: None
    """
    try:
        # Check for division by zero before converting to float
        if b == 0:
            # Handle division by zero according to IEEE 754 standards
            return float("inf") if a > 0 else float("-inf") if a < 0 else float("nan")

        # Perform normal division after converting both operands to float
        return float(a) / float(b)

    except (TypeError, ValueError):
        # Return None for non-numeric inputs (e.g., strings, None values)
        # This allows the caller to handle the error appropriately
        return None


def parse_literal_value(literal: exp.Literal) -> Any:
    """Parse a SQLGlot literal into a Python value (int, float, or str).

    Args:
        literal: SQLGlot literal expression.

    Returns:
        Python value (int, float, or str).
    """
    value_str = str(literal.name)
    if getattr(literal, "is_string", False):
        return value_str
    try:
        return int(value_str)
    except ValueError:
        pass
    try:
        return float(value_str)
    except ValueError:
        pass
    return value_str


def get_function_name_from_expression(expr: exp.Expression) -> str:
    """Extract function name from various SQLGlot expression types.

    Args:
        expr: SQLGlot expression.

    Returns:
        Function name as string.
    """
    if hasattr(expr, "sql_name") and expr.sql_name().lower() != "anonymous":
        return expr.sql_name()
    elif isinstance(expr, exp.Anonymous):
        return expr.name
    else:
        return type(expr).__name__


def normalize_join_type(join_kind: str) -> str:
    """Normalize SQL join type to Ray Data join type.

    Args:
        join_kind: SQL join type string.

    Returns:
        Ray Data join type string.
    """
    join_type_mapping = {
        "inner": "inner",
        "left": "left_outer",
        "right": "right_outer",
        "full": "full_outer",
        "outer": "full_outer",
    }
    return join_type_mapping.get(join_kind.lower(), "inner")


def extract_column_from_expression(expr: exp.Expression) -> Optional[str]:
    """Extract column name from an expression, handling table qualifiers.

    Args:
        expr: Expression to extract column name from.

    Returns:
        Column name if found, None otherwise.
    """
    if isinstance(expr, exp.Column):
        col_name = str(expr.name)
        # Handle qualified column names (table.column) by extracting just the column part
        if "." in col_name:
            col_name = col_name.split(".")[-1]
        return col_name
    elif hasattr(expr, "name"):
        return str(expr.name)
    else:
        return str(expr)


def is_aggregate_function(expr: exp.Expression) -> bool:
    """Check if an expression is an aggregate function.

    Args:
        expr: Expression to check.

    Returns:
        True if it's an aggregate function.
    """
    return isinstance(expr, (exp.AggFunc, exp.Anonymous))


def get_config_from_context():
    """Get SQL configuration from Ray DataContext.

    Returns:
        SQLConfig object with settings from DataContext or defaults.
    """
    from ray.data.sql.config import LogLevel, SQLConfig

    try:
        from ray.data import DataContext

        ctx = DataContext.get_current()
        if hasattr(ctx, "sql_config") and ctx.sql_config:
            config_dict = ctx.sql_config
            return SQLConfig(
                log_level=LogLevel[config_dict.get("log_level", "ERROR").upper()],
                case_sensitive=config_dict.get("case_sensitive", False),
                strict_mode=config_dict.get("strict_mode", True),
                max_join_partitions=config_dict.get("max_join_partitions", 10),
                enable_pushdown_optimization=config_dict.get(
                    "enable_optimization", True
                ),
                enable_sqlglot_optimizer=config_dict.get("enable_optimization", True),
                enable_custom_optimizer=config_dict.get("enable_optimization", True),
                enable_logical_planning=config_dict.get("enable_optimization", True),
            )
    except Exception:
        pass

    return SQLConfig()


# Constants for aggregate function mapping
SUPPORTED_AGGREGATES = {"sum", "min", "max", "count", "avg", "mean", "std", "stddev"}

AGGREGATE_MAPPING = {
    "sum": "Sum",
    "min": "Min",
    "max": "Max",
    "count": "Count",
    "avg": "Mean",
    "mean": "Mean",
    "std": "Std",
    "stddev": "Std",
}

# JOIN type mapping from SQL to Arrow
JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP = {
    "inner": "inner",
    "left_outer": "left outer",
    "right_outer": "right outer",
    "full_outer": "full outer",
}


def get_supported_sql_features():
    """Get a comprehensive list of supported SQL features.

    Returns:
        Dict containing categorized lists of supported SQL features.

    Examples:
        .. testcode::

            from ray.data.sql.utils import get_supported_sql_features
            features = get_supported_sql_features()
            print("Supported aggregates:", features["aggregates"])
    """
    from ray.data.sql.parser import SUPPORTED_FUNCTIONS

    return {
        "aggregates": sorted(SUPPORTED_AGGREGATES),
        "functions": sorted(SUPPORTED_FUNCTIONS),
        "operators": [
            "Arithmetic: +, -, *, /, %",
            "Comparison: =, !=, <>, <, <=, >, >=",
            "Logical: AND, OR, NOT",
            "Pattern: LIKE, ILIKE",
            "Null: IS NULL, IS NOT NULL",
            "Range: BETWEEN",
        ],
        "constructs": [
            "SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT",
            "JOINs (INNER, LEFT, RIGHT, FULL OUTER)",
            "CASE expressions",
            "Subqueries (limited)",
            "Column aliases",
        ],
        "data_types": [
            "Numeric: INT, BIGINT, FLOAT, DOUBLE, DECIMAL",
            "String: STRING, VARCHAR, CHAR, TEXT",
            "Boolean: BOOLEAN, BOOL",
            "Date/Time: DATE, TIME, TIMESTAMP, DATETIME",
            "Binary: BINARY, VARBINARY",
        ],
    }


def get_feature_suggestion(feature_name: str):
    """Get a helpful suggestion for an unsupported SQL feature.

    This function provides user-friendly suggestions for unsupported SQL features,
    helping users understand alternatives or workarounds available in Ray Data.

    Args:
        feature_name: Name of the unsupported SQL feature.

    Returns:
        A helpful suggestion string, or None if no suggestion is available.
    """
    from ray.data.sql.parser import (
        UNSUPPORTED_AGGREGATE_SUGGESTIONS,
        UNSUPPORTED_CONSTRUCTS,
        UNSUPPORTED_FUNCTION_SUGGESTIONS,
        UNSUPPORTED_OPERATORS,
    )

    feature_lower = feature_name.lower()

    # Check aggregate functions
    if feature_lower in UNSUPPORTED_AGGREGATE_SUGGESTIONS:
        return UNSUPPORTED_AGGREGATE_SUGGESTIONS[feature_lower]

    # Check general functions
    if feature_lower in UNSUPPORTED_FUNCTION_SUGGESTIONS:
        return UNSUPPORTED_FUNCTION_SUGGESTIONS[feature_lower]

    # Check constructs and operators by name
    for info_dict in [UNSUPPORTED_CONSTRUCTS, UNSUPPORTED_OPERATORS]:
        for info in info_dict.values():
            if info["name"].lower() == feature_lower:
                return info.get("suggestion")

    return None


def validate_sql_feature_support(sql_string: str, strict_mode: bool = True):
    """Validate SQL feature support without executing the query.

    Args:
        sql_string: SQL query string to validate.
        strict_mode: If True, raises error for unsupported features.
                    If False, returns list of warnings.

    Returns:
        List of validation warnings if strict_mode=False, empty list if valid.

    Raises:
        NotImplementedError: If unsupported features found in strict_mode=True.

    Examples:
        .. testcode::

            from ray.data.sql.utils import validate_sql_feature_support

            # Check without raising errors
            warnings = validate_sql_feature_support(
                "SELECT MEDIAN(price) FROM sales",
                strict_mode=False
            )
            print(warnings)

            # Validate with errors
            validate_sql_feature_support("SELECT * FROM sales")  # OK
    """
    from ray.data.sql.config import SQLConfig
    from ray.data.sql.parser import SQLParser

    config = SQLConfig()
    parser = SQLParser(config)

    # Temporarily modify validation behavior for non-strict mode
    if not strict_mode:
        try:
            parser.parse(sql_string)
            return []
        except NotImplementedError as e:
            # Extract individual violations from the error message
            error_str = str(e)
            if "SQL validation failed with the following issues:" in error_str:
                lines = error_str.split("\n")[1:-2]  # Skip header and footer
                return [
                    line.strip().lstrip("0123456789. ")
                    for line in lines
                    if line.strip()
                ]
            else:
                return [error_str]
    else:
        parser.parse(sql_string)
        return []
