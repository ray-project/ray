"""File utilities for Delta Lake path operations and statistics."""

import json
import math
import os
import posixpath
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs


def join_delta_path(base: str, relative: str) -> str:
    """Join base path and relative path, handling URI schemes."""
    base = base.rstrip("/")
    relative = relative.lstrip("/")
    if "://" in base:
        scheme, rest = base.split("://", 1)
        return f"{scheme}://{posixpath.join(rest, relative)}"
    return posixpath.join(base, relative)


def safe_dirname(path: str) -> str:
    """Get directory portion of path, handling URI schemes."""
    if "://" in path:
        scheme, rest = path.split("://", 1)
        directory = posixpath.dirname(rest)
        return f"{scheme}://{directory}" if directory else ""
    return os.path.dirname(path)


def get_file_info_with_retry(
    fs: pa_fs.FileSystem, path: str, max_retries: int = 3
) -> pa_fs.FileInfo:
    """Get file info with retries for transient errors."""
    last_error: Optional[Exception] = None
    for _ in range(max_retries):
        try:
            return fs.get_file_info(path)
        except Exception as e:
            last_error = e
    raise last_error or RuntimeError(f"Failed to get file info: {path}")


def validate_file_path(path: str, max_length: int = 500) -> None:
    """Validate file path is safe for use."""
    if not isinstance(path, str):
        raise ValueError(f"Path must be string, got {type(path).__name__}")
    if ".." in path:
        raise ValueError(f"Path contains '..': {path}")
    if path.startswith("/"):
        raise ValueError(f"Absolute path not allowed: {path}")
    if len(path) > max_length:
        raise ValueError(f"Path too long ({len(path)} chars): {path}")
    if "\x00" in path:
        raise ValueError(f"Path contains null byte: {path}")


def compute_parquet_statistics(table: pa.Table) -> str:
    """Compute Delta Lake statistics JSON for a table.

    Returns JSON with numRecords, minValues, maxValues, and nullCount.
    """
    # Delta statistics format: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
    stats: Dict[str, Any] = {"numRecords": table.num_rows}
    null_counts: Dict[str, int] = {}
    min_vals: Dict[str, Any] = {}
    max_vals: Dict[str, Any] = {}

    for i, col in enumerate(table.columns):
        name = table.schema.field(i).name
        col_type = col.type

        # Null count
        null_count = pc.sum(pc.is_null(col)).as_py()
        if null_count is not None:
            null_counts[name] = null_count

        # Min/max for numeric and string types
        if _is_numeric(col_type):
            _add_numeric_stats(col, name, min_vals, max_vals)
        elif _is_string(col_type):
            _add_string_stats(col, name, min_vals, max_vals)

    if min_vals:
        stats["minValues"] = min_vals
    if max_vals:
        stats["maxValues"] = max_vals
    if null_counts:
        stats["nullCount"] = null_counts

    return json.dumps(stats)


# Private helpers


def _is_numeric(t: pa.DataType) -> bool:
    return pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t)


def _is_string(t: pa.DataType) -> bool:
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def _add_numeric_stats(col: pa.ChunkedArray, name: str, mins: Dict, maxs: Dict) -> None:
    min_val = pc.min(col).as_py()
    max_val = pc.max(col).as_py()
    if isinstance(min_val, float) and not math.isfinite(min_val):
        min_val = None
    if isinstance(max_val, float) and not math.isfinite(max_val):
        max_val = None
    if min_val is not None:
        mins[name] = min_val
    if max_val is not None:
        maxs[name] = max_val


def _add_string_stats(col: pa.ChunkedArray, name: str, mins: Dict, maxs: Dict) -> None:
    min_val = pc.min(col).as_py()
    max_val = pc.max(col).as_py()
    if min_val is not None:
        mins[name] = str(min_val)
    if max_val is not None:
        maxs[name] = str(max_val)
