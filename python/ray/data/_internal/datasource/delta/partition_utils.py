"""Partition utilities for Hive-style partitioning in Delta Lake."""

import math
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

MAX_PARTITION_PATH_LENGTH = 200
MAX_PARTITION_COLUMNS = 10


def build_partition_path(
    cols: List[str], values: tuple
) -> Tuple[str, Dict[str, Optional[str]]]:
    """Build Hive-style partition path (col1=val1/col2=val2/).

    Returns (path_string, partition_dict).
    None values use __HIVE_DEFAULT_PARTITION__.
    NaN values use __HIVE_DEFAULT_PARTITION__ (similar to NULL handling).
    Values are URL-encoded to handle special characters.
    """
    if not cols:
        return "", {}
    if not values:
        return "", {}

    parts = []
    part_dict: Dict[str, Optional[str]] = {}

    for col, val in zip(cols, values):
        if val is None:
            parts.append(f"{col}=__HIVE_DEFAULT_PARTITION__")
            part_dict[col] = None
        elif isinstance(val, float) and math.isnan(val):
            # NaN handled like NULL in partitioning
            parts.append(f"{col}=__HIVE_DEFAULT_PARTITION__")
            part_dict[col] = None
        else:
            validate_partition_value(val)
            # URL-encode the value to handle special characters
            encoded_val = urllib.parse.quote(str(val), safe="")
            parts.append(f"{col}={encoded_val}")
            part_dict[col] = str(val)

    path = "/".join(parts) + "/"
    if len(path) > MAX_PARTITION_PATH_LENGTH:
        raise ValueError(f"Partition path too long: {len(path)} chars")

    return path, part_dict


def validate_partition_value(value: Any) -> None:
    """Validate partition value is safe for use in paths.

    Args:
        value: Partition value to validate.

    Raises:
        ValueError: If value contains invalid characters or is too long.
    """
    if value is None:
        return

    # Check for NaN - valid but handled specially in build_partition_path
    if isinstance(value, float) and math.isnan(value):
        return

    val_str = str(value)

    # Check for empty string
    if not val_str:
        raise ValueError("Partition value cannot be empty string")

    # Check for path traversal and separators
    if ".." in val_str:
        raise ValueError(f"Partition value contains '..': {value}")
    if "/" in val_str:
        raise ValueError(f"Partition value contains '/': {value}")
    if "\\" in val_str:
        raise ValueError(f"Partition value contains '\\': {value}")

    # Check for null byte
    if "\x00" in val_str:
        raise ValueError(f"Partition value contains null byte: {value}")

    # Check length
    if len(val_str) > MAX_PARTITION_PATH_LENGTH:
        raise ValueError(
            f"Partition value too long ({len(val_str)} chars, max {MAX_PARTITION_PATH_LENGTH})"
        )


def validate_partition_column_names(cols: List[str]) -> List[str]:
    """Validate partition column names.

    Args:
        cols: List of column names to validate.

    Returns:
        Validated list of column names.

    Raises:
        ValueError: If column names are invalid.
    """
    if not isinstance(cols, list):
        raise ValueError(f"Partition columns must be a list, got {type(cols).__name__}")

    if len(cols) > MAX_PARTITION_COLUMNS:
        raise ValueError(
            f"Too many partition columns ({len(cols)}). Maximum: {MAX_PARTITION_COLUMNS}"
        )

    # Check for duplicates
    seen = set()
    for col in cols:
        if not isinstance(col, str):
            raise ValueError(
                f"Partition column name must be string, got {type(col).__name__}: {col}"
            )
        if not col or not col.strip():
            raise ValueError("Partition column name cannot be empty")
        if "/" in col or "\\" in col or ".." in col:
            raise ValueError(f"Invalid characters in partition column name: {col}")
        if "=" in col:
            raise ValueError(
                f"Partition column name cannot contain '=': {col} "
                "(conflicts with Hive-style partitioning)"
            )
        if col in seen:
            raise ValueError(f"Duplicate partition column: {col}")
        seen.add(col)

    return cols


def validate_partition_columns_in_table(cols: List[str], table: pa.Table) -> None:
    """Validate partition columns exist in table with valid types.

    Partition columns must:
    - Exist in the table
    - Not be nested types (struct, list, map)
    - Not be dictionary-encoded types
    """
    if not cols:
        return

    missing = [c for c in cols if c not in table.column_names]
    if missing:
        raise ValueError(f"Missing partition columns: {missing}")

    for col in cols:
        col_type = table.schema.field(col).type
        if pa.types.is_nested(col_type):
            raise ValueError(
                f"Partition column '{col}' has nested type {col_type}. "
                "Nested types are not allowed for partition columns."
            )
        if pa.types.is_dictionary(col_type):
            raise ValueError(
                f"Partition column '{col}' has dictionary type {col_type}. "
                "Dictionary-encoded types are not allowed for partition columns."
            )
