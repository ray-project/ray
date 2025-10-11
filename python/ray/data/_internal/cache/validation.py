"""
Cache value validation for Ray Data caching.

This module validates that cached values are reasonable and consistent
with the operation that produced them. It helps catch bugs where cached
values become corrupted or inconsistent.

Two Types of Validation:
1. Value validation: Is this value valid for the operation? (e.g., count must be int ≥ 0)
2. Consistency validation: Is the relationship between values valid? (e.g., limit can't increase count)
"""

from typing import Any, Callable, Dict

# =============================================================================
# VALUE VALIDATORS
# =============================================================================


def _validate_positive_int(value: Any) -> bool:
    """Validate value is a non-negative integer.

    Used for: count, size_bytes, num_blocks

    Args:
        value: Value to validate

    Returns:
        True if value is int ≥ 0, False otherwise
    """
    return isinstance(value, int) and value >= 0


def _validate_list(value: Any) -> bool:
    """Validate value is a list.

    Used for: columns, input_files, take results

    Args:
        value: Value to validate

    Returns:
        True if value is a list, False otherwise
    """
    return isinstance(value, list)


def _validate_non_none(value: Any) -> bool:
    """Validate value is not None.

    This is the fallback validator for operations we don't have specific
    validation rules for. It just ensures the value exists.

    Args:
        value: Value to validate

    Returns:
        True if value is not None, False otherwise
    """
    return value is not None


# =============================================================================
# VALIDATION DISPATCH TABLE
# =============================================================================

# Maps operation names to their validation functions
# This is a dispatch table for type-safe, efficient validation
_VALIDATION_RULES: Dict[str, Callable[[Any], bool]] = {
    # Metadata operations that return integers
    "count": _validate_positive_int,
    # Metadata operations that return lists
    "columns": _validate_list,
    "input_files": _validate_list,
    # Data operations that return lists
    "take": _validate_list,
    "take_batch": _validate_list,
    "take_all": _validate_list,
    # All other operations use the fallback validator
}


def validate_cached_value(operation_name: str, value: Any) -> bool:
    """Validate that a cached value is reasonable for the operation.

    This is the main entry point for cache value validation. It uses the
    dispatch table to find the appropriate validator for the operation.

    Args:
        operation_name: Name of the operation (e.g., "count", "schema")
        value: The cached value to validate

    Returns:
        True if value is valid, False if it's corrupted or invalid

    Examples:
        >>> validate_cached_value("count", 100)
        True
        >>> validate_cached_value("count", -5)  # Negative count!
        False
        >>> validate_cached_value("columns", ["id", "value"])
        True
        >>> validate_cached_value("columns", "not_a_list")
        False
    """
    # Look up the validator for this operation (or use fallback)
    validator = _VALIDATION_RULES.get(operation_name, _validate_non_none)

    # Run the validator
    return validator(value)


# =============================================================================
# COUNT CONSISTENCY VALIDATION
# =============================================================================

# Operations that preserve count (produce exactly one output row per input row)
# These are 1:1 transformations
_COUNT_PRESERVING_OPS = {
    "map",  # 1:1 row transformation
    "add_column",  # Adds column to each row (1:1)
    "drop_columns",  # Removes columns from each row (1:1)
    "select_columns",  # Keeps only specified columns (1:1)
    "rename_columns",  # Renames columns (1:1)
    "with_column",  # Adds/replaces column (1:1)
    "repartition",  # Just redistributes rows (1:1)
    "sort",  # Just reorders rows (1:1)
    "random_shuffle",  # Just reorders rows (1:1)
    "randomize_block_order",  # Just reorders blocks (1:1)
}

# Operations that can reduce count (but never increase it)
# These are filtering operations
_COUNT_REDUCING_OPS = {
    "limit",  # Takes first N rows (reduces count)
    "filter",  # Keeps rows matching predicate (reduces count)
    "random_sample",  # Keeps random fraction of rows (reduces count)
}


def validate_count_consistency(
    source_count: Any, target_count: Any, operation_name: str
) -> bool:
    """Check if count relationship makes sense for the given operation.

    This validates that the source→target count relationship is consistent
    with the operation's semantics. It helps catch bugs where smart count
    updates produce incorrect values.

    Validation Rules:
        - Count-preserving ops (map, add_column, etc.): target == source
        - Count-reducing ops (limit, filter): target ≤ source
        - Other ops: any relationship is valid (we don't have specific rules)

    Args:
        source_count: Count before the transformation
        target_count: Count after the transformation
        operation_name: Name of the transformation (e.g., "limit", "map")

    Returns:
        True if the count relationship is valid, False if it's inconsistent

    Examples:
        >>> # map is 1:1, so count must be preserved
        >>> validate_count_consistency(100, 100, "map")
        True
        >>> validate_count_consistency(100, 50, "map")  # Bug!
        False

        >>> # limit can only reduce count
        >>> validate_count_consistency(100, 50, "limit")
        True
        >>> validate_count_consistency(100, 150, "limit")  # Bug!
        False

        >>> # filter can only reduce count
        >>> validate_count_consistency(100, 75, "filter")
        True
        >>> validate_count_consistency(100, 100, "filter")  # No rows filtered
        True
    """
    # -------------------------------------------------------------------------
    # Basic type and value validation
    # -------------------------------------------------------------------------
    if not isinstance(source_count, int) or not isinstance(target_count, int):
        return False  # Counts must be integers

    if source_count < 0 or target_count < 0:
        return False  # Counts can't be negative

    # -------------------------------------------------------------------------
    # Count-preserving operations must have equal counts
    # -------------------------------------------------------------------------
    if operation_name in _COUNT_PRESERVING_OPS:
        # These are 1:1 transformations, so target must equal source
        # Example: ds.map(fn) with 100 rows → must produce 100 rows
        return target_count == source_count

    # -------------------------------------------------------------------------
    # Count-reducing operations must not increase count
    # -------------------------------------------------------------------------
    if operation_name in _COUNT_REDUCING_OPS:
        # These can reduce count but never increase it
        # Example: ds.limit(50) with 100 rows → must produce ≤ 50 rows
        # Example: ds.filter(fn) with 100 rows → must produce ≤ 100 rows
        return target_count <= source_count

    # -------------------------------------------------------------------------
    # Unknown operations - accept any valid relationship (safe default)
    # -------------------------------------------------------------------------
    # For operations we don't have specific rules for, any valid count
    # relationship is acceptable. This is the safe default.
    return True
