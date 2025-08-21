"""
SQL feature validation for Ray Data SQL API.

This module provides comprehensive whitelist and blacklist validation for SQL expressions,
helping catch unsupported features during parsing with helpful error messages.
"""

from typing import Dict, List, Optional, Set, Type
from sqlglot import exp
from ray.data.sql.utils import setup_logger


class SQLFeatureValidator:
    """Validates SQL ASTs against supported and unsupported feature lists.

    This validator provides comprehensive checking of SQL features with detailed
    error messages and suggestions for alternatives when available.
    """

    def __init__(self, strict_mode: bool = True):
        """Initialize the validator.

        Args:
            strict_mode: If True, raises errors for unsupported features.
                        If False, logs warnings but allows processing.
        """
        self.strict_mode = strict_mode
        self._logger = setup_logger("SQLFeatureValidator")

    def validate_ast(self, ast: exp.Expression) -> None:
        """Validate an AST against supported features.

        Args:
            ast: SQLGlot AST to validate.

        Raises:
            NotImplementedError: If unsupported features are found in strict mode.
        """
        violations = []

        for node in ast.walk():
            violation = self._check_node(node)
            if violation:
                violations.append(violation)

        if violations and self.strict_mode:
            self._raise_validation_error(violations)
        elif violations:
            for violation in violations:
                self._logger.warning(violation)

    def _check_node(self, node: exp.Expression) -> Optional[str]:
        """Check a single AST node for violations.

        Args:
            node: SQLGlot expression node to check.

        Returns:
            Error message if violation found, None otherwise.
        """
        # Check aggregate functions
        if isinstance(node, exp.AggFunc):
            return self._validate_aggregate_function(node)

        # Check string functions
        if isinstance(node, exp.Func):
            return self._validate_function(node)

        # Check operators
        operator_violation = self._validate_operator(node)
        if operator_violation:
            return operator_violation

        # Check SQL constructs
        construct_violation = self._validate_sql_construct(node)
        if construct_violation:
            return construct_violation

        # Check data types
        if isinstance(node, exp.DataType):
            return self._validate_data_type(node)

        return None

    def _validate_aggregate_function(self, node: exp.AggFunc) -> Optional[str]:
        """Validate aggregate functions."""
        func_name = node.sql_name().lower()

        if func_name not in SUPPORTED_AGGREGATES:
            suggestion = UNSUPPORTED_AGGREGATES.get(func_name, {}).get("suggestion")
            msg = f"Aggregate function '{func_name.upper()}' not supported"
            if suggestion:
                msg += f". Try: {suggestion}"
            return msg
        return None

    def _validate_function(self, node: exp.Func) -> Optional[str]:
        """Validate general functions."""
        func_name = node.sql_name().lower()

        # Skip aggregate functions (handled separately)
        if isinstance(node, exp.AggFunc):
            return None

        if func_name in SUPPORTED_FUNCTIONS:
            return None
        elif func_name in UNSUPPORTED_FUNCTIONS:
            info = UNSUPPORTED_FUNCTIONS[func_name]
            msg = f"Function '{func_name.upper()}' not supported"
            if info.get("reason"):
                msg += f": {info['reason']}"
            if info.get("suggestion"):
                msg += f". Try: {info['suggestion']}"
            return msg
        else:
            return f"Unknown function '{func_name.upper()}' - not in supported function list"

    def _validate_operator(self, node: exp.Expression) -> Optional[str]:
        """Validate operators."""
        node_type = type(node)

        if node_type in SUPPORTED_OPERATORS:
            return None
        elif node_type in UNSUPPORTED_OPERATORS:
            info = UNSUPPORTED_OPERATORS[node_type]
            msg = f"Operator '{info['name']}' not supported"
            if info.get("reason"):
                msg += f": {info['reason']}"
            if info.get("suggestion"):
                msg += f". Try: {info['suggestion']}"
            return msg
        return None

    def _validate_sql_construct(self, node: exp.Expression) -> Optional[str]:
        """Validate SQL language constructs."""
        node_type = type(node)

        if node_type in SUPPORTED_CONSTRUCTS:
            return None
        elif node_type in UNSUPPORTED_CONSTRUCTS:
            info = UNSUPPORTED_CONSTRUCTS[node_type]
            msg = f"SQL feature '{info['name']}' not supported"
            if info.get("reason"):
                msg += f": {info['reason']}"
            if info.get("suggestion"):
                msg += f". Try: {info['suggestion']}"
            return msg
        return None

    def _validate_data_type(self, node: exp.DataType) -> Optional[str]:
        """Validate data types."""
        type_name = node.this.name.lower() if node.this else "unknown"

        if type_name not in SUPPORTED_DATA_TYPES:
            suggestion = UNSUPPORTED_DATA_TYPES.get(type_name, {}).get("suggestion")
            msg = f"Data type '{type_name.upper()}' not supported"
            if suggestion:
                msg += f". Try: {suggestion}"
            return msg
        return None

    def _raise_validation_error(self, violations: List[str]) -> None:
        """Raise a comprehensive validation error."""
        error_msg = "SQL validation failed with the following issues:\n"
        for i, violation in enumerate(violations, 1):
            error_msg += f"  {i}. {violation}\n"
        error_msg += "\nSee Ray Data SQL documentation for supported features."
        raise NotImplementedError(error_msg)


# ============================================================================
# FEATURE WHITELISTS AND BLACKLISTS
# ============================================================================

# Supported aggregate functions
SUPPORTED_AGGREGATES: Set[str] = {
    "sum",
    "min",
    "max",
    "count",
    "avg",
    "mean",
    "std",
    "stddev",
}

# Unsupported aggregate functions with suggestions
UNSUPPORTED_AGGREGATES: Dict[str, Dict[str, str]] = {
    "median": {
        "reason": "Statistical functions not fully implemented",
        "suggestion": "Use percentile functions or compute manually",
    },
    "mode": {
        "reason": "Statistical functions not fully implemented",
        "suggestion": "Use groupby with count to find most frequent values",
    },
    "variance": {
        "reason": "Use std() instead",
        "suggestion": "STD() for standard deviation",
    },
    "var": {
        "reason": "Use std() instead",
        "suggestion": "STD() for standard deviation",
    },
    "percentile": {
        "reason": "Percentile functions not implemented",
        "suggestion": "Use approximation with sorting operations",
    },
    "array_agg": {
        "reason": "Array aggregation not supported",
        "suggestion": "Use groupby with collect operations",
    },
    "string_agg": {
        "reason": "String aggregation not supported",
        "suggestion": "Use map operations to concatenate strings",
    },
}

# Supported string and mathematical functions
SUPPORTED_FUNCTIONS: Set[str] = {
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
    "split",
    "regexp_replace",
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
    "degrees",
    "radians",
    # Date/time functions (basic)
    "date",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "date_add",
    "date_sub",
    "datediff",
    # Type conversion
    "cast",
    "convert",
    "try_cast",
    # Conditional functions
    "coalesce",
    "nullif",
    "greatest",
    "least",
    # Utility functions
    "row_number",  # Limited window function support
}

# Unsupported functions with reasons and suggestions
UNSUPPORTED_FUNCTIONS: Dict[str, Dict[str, str]] = {
    # Window functions (mostly unsupported)
    "lag": {
        "reason": "Window functions not fully implemented",
        "suggestion": "Use self-joins or manual ordering operations",
    },
    "lead": {
        "reason": "Window functions not fully implemented",
        "suggestion": "Use self-joins or manual ordering operations",
    },
    "rank": {
        "reason": "Advanced window functions not supported",
        "suggestion": "Use ROW_NUMBER() with ordering",
    },
    "dense_rank": {
        "reason": "Advanced window functions not supported",
        "suggestion": "Use ROW_NUMBER() with ordering",
    },
    "ntile": {
        "reason": "Window functions not fully implemented",
        "suggestion": "Use manual bucketing with modulo operations",
    },
    # Complex string functions
    "regexp_extract": {
        "reason": "Complex regex operations not supported",
        "suggestion": "Use simpler string functions like SPLIT or REPLACE",
    },
    "json_extract": {
        "reason": "JSON functions not implemented",
        "suggestion": "Parse JSON data before loading into Ray Data",
    },
    "xml_extract": {
        "reason": "XML functions not implemented",
        "suggestion": "Parse XML data before loading into Ray Data",
    },
    # Array/Map functions
    "array_contains": {
        "reason": "Array functions not implemented",
        "suggestion": "Use standard column operations",
    },
    "map_keys": {
        "reason": "Map functions not implemented",
        "suggestion": "Use standard column operations",
    },
    # Advanced date functions
    "date_trunc": {
        "reason": "Advanced date functions not implemented",
        "suggestion": "Use basic date functions like YEAR(), MONTH(), DAY()",
    },
    "extract": {
        "reason": "Complex date extraction not supported",
        "suggestion": "Use YEAR(), MONTH(), DAY(), HOUR(), etc.",
    },
}

# Supported operators
SUPPORTED_OPERATORS: Set[Type[exp.Expression]] = {
    # Arithmetic operators
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.Mod,
    # Comparison operators
    exp.EQ,
    exp.NEQ,
    exp.LT,
    exp.LTE,
    exp.GT,
    exp.GTE,
    # Logical operators
    exp.And,
    exp.Or,
    exp.Not,
    # Other operators
    exp.Is,
    exp.Like,
    exp.ILike,
    exp.Between,
    exp.Bracket,
    # Null checks
    exp.Null,
}

# Unsupported operators with reasons and suggestions
UNSUPPORTED_OPERATORS: Dict[Type[exp.Expression], Dict[str, str]] = {
    exp.In: {
        "name": "IN",
        "reason": "IN operator not implemented",
        "suggestion": "Use multiple OR conditions or JOIN operations",
    },
    exp.Exists: {
        "name": "EXISTS",
        "reason": "EXISTS subqueries not supported",
        "suggestion": "Use JOIN operations instead",
    },
    exp.All: {
        "name": "ALL",
        "reason": "ALL operator not implemented",
        "suggestion": "Use aggregation with MIN/MAX functions",
    },
    exp.Any: {
        "name": "ANY/SOME",
        "reason": "ANY/SOME operators not implemented",
        "suggestion": "Use aggregation or JOIN operations",
    },
    exp.Distinct: {
        "name": "DISTINCT",
        "reason": "DISTINCT not implemented",
        "suggestion": "Use GROUP BY or unique() operations on Dataset",
    },
}

# Supported SQL language constructs
SUPPORTED_CONSTRUCTS: Set[Type[exp.Expression]] = {
    # Core query components
    exp.Select,
    exp.From,
    exp.Where,
    exp.Group,
    exp.Having,
    exp.Order,
    exp.Limit,
    # Join operations
    exp.Join,
    # Expressions
    exp.Column,
    exp.Literal,
    exp.Star,
    exp.Alias,
    exp.Case,
    exp.When,
    exp.If,
    # Parentheses and grouping
    exp.Paren,
    exp.Tuple,
    # Basic table references
    exp.Table,
    exp.TableAlias,
}

# Unsupported SQL constructs with reasons
UNSUPPORTED_CONSTRUCTS: Dict[Type[exp.Expression], Dict[str, str]] = {
    exp.CTE: {
        "name": "Common Table Expressions (WITH)",
        "reason": "CTEs not implemented",
        "suggestion": "Break complex queries into multiple steps using intermediate tables",
    },
    exp.Union: {
        "name": "UNION",
        "reason": "Set operations not implemented",
        "suggestion": "Use Dataset.union() method instead",
    },
    exp.Intersect: {
        "name": "INTERSECT",
        "reason": "Set operations not implemented",
        "suggestion": "Use JOIN operations to find common records",
    },
    exp.Except: {
        "name": "EXCEPT",
        "reason": "Set operations not implemented",
        "suggestion": "Use LEFT JOIN with WHERE IS NULL to exclude records",
    },
    exp.Window: {
        "name": "Window functions",
        "reason": "Advanced window operations not supported",
        "suggestion": "Use GROUP BY or manual partitioning operations",
    },
    exp.Values: {
        "name": "VALUES clause",
        "reason": "VALUES clause not implemented",
        "suggestion": "Create Dataset from Python data structures",
    },
    exp.Insert: {
        "name": "INSERT statements",
        "reason": "DML operations not supported",
        "suggestion": "Use Dataset write operations",
    },
    exp.Update: {
        "name": "UPDATE statements",
        "reason": "DML operations not supported",
        "suggestion": "Use Dataset transformation operations",
    },
    exp.Delete: {
        "name": "DELETE statements",
        "reason": "DML operations not supported",
        "suggestion": "Use Dataset filter operations",
    },
    exp.Create: {
        "name": "CREATE statements",
        "reason": "DDL operations not supported",
        "suggestion": "Use register_table() to register existing Datasets",
    },
    exp.Drop: {
        "name": "DROP statements",
        "reason": "DDL operations not supported",
        "suggestion": "Use clear_tables() or Python del statements",
    },
}

# Supported data types
SUPPORTED_DATA_TYPES: Set[str] = {
    # Numeric types
    "int",
    "integer",
    "bigint",
    "smallint",
    "tinyint",
    "float",
    "double",
    "decimal",
    "numeric",
    "real",
    # String types
    "string",
    "varchar",
    "char",
    "text",
    # Boolean
    "boolean",
    "bool",
    # Date/time types
    "date",
    "time",
    "timestamp",
    "datetime",
    # Binary
    "binary",
    "varbinary",
    # Arrow-native types
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
}

# Unsupported data types with suggestions
UNSUPPORTED_DATA_TYPES: Dict[str, Dict[str, str]] = {
    "json": {
        "reason": "JSON type not supported",
        "suggestion": "Use STRING and parse manually",
    },
    "xml": {
        "reason": "XML type not supported",
        "suggestion": "Use STRING and parse manually",
    },
    "array": {
        "reason": "Complex array types not fully supported",
        "suggestion": "Use simpler types or nested datasets",
    },
    "map": {
        "reason": "Map types not fully supported",
        "suggestion": "Use separate columns or JSON strings",
    },
    "struct": {
        "reason": "Complex struct types have limited support",
        "suggestion": "Use separate columns or nested datasets",
    },
    "geometry": {
        "reason": "Spatial types not supported",
        "suggestion": "Use coordinate columns (latitude, longitude)",
    },
    "uuid": {"reason": "UUID type not supported", "suggestion": "Use STRING type"},
}


def get_supported_features_summary() -> Dict[str, List[str]]:
    """Get a summary of all supported SQL features.

    Returns:
        Dictionary categorizing supported features by type.
    """
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
        "data_types": sorted(SUPPORTED_DATA_TYPES),
    }


def get_feature_alternatives(feature_name: str) -> Optional[str]:
    """Get suggested alternatives for unsupported features.

    Args:
        feature_name: Name of the unsupported feature.

    Returns:
        Suggestion string if available, None otherwise.
    """
    feature_lower = feature_name.lower()

    # Check in all unsupported feature dictionaries
    for feature_dict in [
        UNSUPPORTED_AGGREGATES,
        UNSUPPORTED_FUNCTIONS,
        UNSUPPORTED_DATA_TYPES,
    ]:
        if feature_lower in feature_dict:
            return feature_dict[feature_lower].get("suggestion")

    # Check operators and constructs by examining their info dicts
    for info_dict in [UNSUPPORTED_OPERATORS, UNSUPPORTED_CONSTRUCTS]:
        for info in info_dict.values():
            if info["name"].lower() == feature_lower:
                return info.get("suggestion")

    return None
