"""
Simple cache value validation for Ray Data caching.
"""

from typing import Any, Callable, Dict



def _validate_positive_int(value: Any) -> bool:
    """Validate value is a non-negative integer."""
    return isinstance(value, int) and value >= 0


def _validate_list(value: Any) -> bool:
    """Validate value is a list."""
    return isinstance(value, list)


def _validate_non_none(value: Any) -> bool:
    """Validate value is not None."""
    return value is not None


# Validation dispatch table for type-safe validation
_VALIDATION_RULES: Dict[str, Callable[[Any], bool]] = {
    "count": _validate_positive_int,
    "columns": _validate_list,
    "input_files": _validate_list,
    "take": _validate_list,
    "take_batch": _validate_list,
    "take_all": _validate_list,
}


def validate_cached_value(operation_name: str, value: Any) -> bool:
    """Validate cached value is reasonable for the operation using dispatch table."""
    validator = _VALIDATION_RULES.get(operation_name, _validate_non_none)
    return validator(value)


# Operations that preserve count (1:1 mapping)
_COUNT_PRESERVING_OPS = {
    "map",
    "add_column",
    "drop_columns",
    "select_columns",
    "rename_columns",
    "with_column",
    "repartition",
    "sort",
    "random_shuffle",
    "randomize_block_order",
}

# Operations that can reduce count
_COUNT_REDUCING_OPS = {"limit", "filter", "random_sample"}


def validate_count_consistency(
    source_count: Any, target_count: Any, operation_name: str
) -> bool:
    """Check if count relationship makes sense for given operation.

    This validates that the source->target count relationship is consistent
    with the operation's semantics.
    """
    # Basic type and value validation
    if not isinstance(source_count, int) or not isinstance(target_count, int):
        return False
    if source_count < 0 or target_count < 0:
        return False

    # Count-preserving operations must have equal counts
    if operation_name in _COUNT_PRESERVING_OPS:
        return target_count == source_count

    # Count-reducing operations must not increase count
    if operation_name in _COUNT_REDUCING_OPS:
        return target_count <= source_count

    # Unknown operations - accept any valid relationship (safe default)
    return True
