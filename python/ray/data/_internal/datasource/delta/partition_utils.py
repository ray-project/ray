"""Partition utilities for Hive-style partitioning in Delta Lake."""

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
    """
    if not cols or not values:
        return "", {}

    parts = []
    part_dict: Dict[str, Optional[str]] = {}

    for col, val in zip(cols, values):
        if val is None:
            parts.append(f"{col}=__HIVE_DEFAULT_PARTITION__")
            part_dict[col] = None
        else:
            validate_partition_value(val)
            parts.append(f"{col}={val}")
            part_dict[col] = str(val)

    path = "/".join(parts) + "/"
    if len(path) > MAX_PARTITION_PATH_LENGTH:
        raise ValueError(f"Partition path too long: {len(path)} chars")

    return path, part_dict


def validate_partition_value(value: Any) -> None:
    """Validate partition value is safe for use in paths."""
    if value is None:
        return
    val_str = str(value)
    if ".." in val_str or "/" in val_str or "\\" in val_str:
        raise ValueError(f"Invalid partition value: {value}")
    if len(val_str) > MAX_PARTITION_PATH_LENGTH:
        raise ValueError(f"Partition value too long: {len(val_str)} chars")


def validate_partition_column_names(cols: List[str]) -> List[str]:
    """Validate partition column names."""
    if len(cols) > MAX_PARTITION_COLUMNS:
        raise ValueError(f"Too many partition columns: {len(cols)}")
    for col in cols:
        if not isinstance(col, str) or not col:
            raise ValueError(f"Invalid partition column: {col}")
        if "/" in col or "\\" in col or ".." in col:
            raise ValueError(f"Invalid characters in column name: {col}")
    return cols


def validate_partition_columns_in_table(cols: List[str], table: pa.Table) -> None:
    """Validate partition columns exist in table with valid types."""
    if not cols:
        return

    missing = [c for c in cols if c not in table.column_names]
    if missing:
        raise ValueError(f"Missing partition columns: {missing}")

    for col in cols:
        col_type = table.schema.field(col).type
        if pa.types.is_nested(col_type):
            raise ValueError(f"Nested type not allowed for partition: {col}")
