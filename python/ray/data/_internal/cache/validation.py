"""
Simple cache value validation for Ray Data caching.
"""

from typing import Any


def validate_cached_value(operation_name: str, value: Any) -> bool:
    """Validate cached value is reasonable for the operation."""
    if operation_name == "count":
        return isinstance(value, int) and value >= 0
    elif operation_name in ["columns", "input_files"]:
        return isinstance(value, list)
    elif operation_name in ["take", "take_batch", "take_all"]:
        return isinstance(value, list)
    else:
        return value is not None  # Basic non-None check for everything else


def validate_count_consistency(
    source_count: Any, target_count: Any, operation_name: str
) -> bool:
    """Check if count relationship makes sense."""
    if not isinstance(source_count, int) or not isinstance(target_count, int):
        return False
    if source_count < 0 or target_count < 0:
        return False

    # Count-preserving operations
    if operation_name in [
        "map",
        "add_column",
        "drop_columns",
        "select_columns",
        "rename_columns",
        "repartition",
        "sort",
        "random_shuffle",
    ]:
        return target_count == source_count

    # Count-reducing operations
    elif operation_name in ["limit", "filter"]:
        return target_count <= source_count

    return True
