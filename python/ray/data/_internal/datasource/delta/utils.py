"""Utility functions for Delta Lake datasource operations.

This is the MVP set of utilities required by APPEND-mode writes. Subsequent
PRs (OVERWRITE, partitioning, schema evolution, cloud-credential
auto-detection) will extend this module.

Delta Lake: https://delta.io/
delta-rs Python library: https://delta-io.github.io/delta-rs/python/
"""

import json
import math
import os
import posixpath
from typing import TYPE_CHECKING, Any, Dict, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

if TYPE_CHECKING:
    from deltalake import DeltaTable


# ----------------------------------------------------------------------
# Path helpers.
# ----------------------------------------------------------------------


def safe_dirname(path: str) -> str:
    """Get directory portion of path, handling URI schemes."""
    if "://" in path:
        scheme, rest = path.split("://", 1)
        directory = posixpath.dirname(rest)
        return f"{scheme}://{directory}" if directory else ""
    return os.path.dirname(path)


def resolve_under_table_root(table_root: Optional[str], rel_path: str) -> str:
    """Join a Delta-relative path to an absolute local table directory.

    When ``table_root`` is ``None`` (cloud / custom filesystem), ``rel_path``
    is returned unchanged.
    """
    if not table_root:
        return rel_path
    return os.path.normpath(os.path.join(table_root, rel_path))


def validate_file_path(path: str, max_length: int = 500) -> None:
    """Validate file path is safe for use."""
    if not isinstance(path, str):
        raise ValueError(f"Path must be string, got {type(path).__name__}")
    if not path or not path.strip():
        raise ValueError("Path cannot be empty")
    if ".." in path:
        raise ValueError(f"Path contains '..': {path}")
    if path.startswith("/"):
        raise ValueError(f"Absolute path not allowed: {path}")
    if len(path) > max_length:
        raise ValueError(f"Path too long ({len(path)} chars): {path}")
    if "\x00" in path:
        raise ValueError(f"Path contains null byte: {path}")
    for char in '<>:"|?*':
        if char in path:
            raise ValueError(f"Path contains invalid character '{char}': {path}")


def get_file_info_with_retry(
    fs: pa_fs.FileSystem, path: str, max_retries: int = 3, base_delay: float = 0.1
) -> pa_fs.FileInfo:
    """Get file info with retries and exponential backoff."""
    from ray._common.retry import call_with_retry
    from ray.data.context import DataContext

    data_context = DataContext.get_current()
    max_backoff_s = (
        base_delay * (2 ** (max_retries - 1)) if max_retries > 1 else base_delay
    )
    return call_with_retry(
        lambda: fs.get_file_info(path),
        description=f"get file info for '{path}'",
        match=data_context.retried_io_errors,
        max_attempts=max_retries,
        # call_with_retry caps backoff in whole seconds; round the sub-second
        # computed value up so a small base_delay still yields a >=1s cap.
        max_backoff_s=math.ceil(max_backoff_s),
    )


# ----------------------------------------------------------------------
# Storage options (MVP: pass-through; cloud auto-detection added in PR 7).
# ----------------------------------------------------------------------


def get_storage_options(
    path: str, provided: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Return storage options dict for the given path. MVP just passes
    through the caller-supplied dict; PR 7 adds boto3 / google-auth /
    azure-identity auto-detection for cloud paths."""
    return dict(provided or {})


# ----------------------------------------------------------------------
# DeltaTable helpers.
# ----------------------------------------------------------------------


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional["DeltaTable"]:
    """Return DeltaTable if it exists, None otherwise."""
    from deltalake import DeltaTable

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except FileNotFoundError:
        return None
    except Exception as e:
        # deltalake raises TableNotFoundError (an optional import) when the
        # table doesn't exist; treat that like a missing path. Anything else
        # is a real error and must propagate.
        if type(e).__name__ == "TableNotFoundError":
            return None
        raise


# ----------------------------------------------------------------------
# Type compatibility helpers.
# ----------------------------------------------------------------------


def _safe_type_check(t: pa.DataType, check_func) -> bool:
    """Safely check type, handling arro3 types that don't have 'id' attribute."""
    try:
        return check_func(t)
    except AttributeError:
        if not hasattr(t, "id"):
            try:
                pa_type = pa.DataType._import_from_c(t._export_to_c())
                return check_func(pa_type)
            except (AttributeError, TypeError):
                return False
        raise


def is_string_type(t: pa.DataType) -> bool:
    return (
        pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or (hasattr(pa.types, "is_string_view") and pa.types.is_string_view(t))
    )


def is_binary_type(t: pa.DataType) -> bool:
    return (
        pa.types.is_binary(t)
        or pa.types.is_large_binary(t)
        or (hasattr(pa.types, "is_binary_view") and pa.types.is_binary_view(t))
        or pa.types.is_fixed_size_binary(t)
    )


def is_date_type(t: pa.DataType) -> bool:
    return pa.types.is_date32(t) or pa.types.is_date64(t)


def is_numeric_type(t: pa.DataType) -> bool:
    return pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t)


def is_temporal_type(t: pa.DataType) -> bool:
    return pa.types.is_date(t) or pa.types.is_timestamp(t)


def types_compatible(expected: pa.DataType, actual: pa.DataType) -> bool:
    """Check if actual type can be written to expected type column."""
    if expected == actual:
        return True
    if _safe_type_check(expected, pa.types.is_integer) and _safe_type_check(
        actual, pa.types.is_integer
    ):
        if _safe_type_check(expected, pa.types.is_signed_integer) != _safe_type_check(
            actual, pa.types.is_signed_integer
        ):
            return False
        return getattr(actual, "bit_width", 64) <= getattr(expected, "bit_width", 64)
    if _safe_type_check(expected, pa.types.is_floating) and _safe_type_check(
        actual, pa.types.is_floating
    ):
        return True
    if is_string_type(expected) and is_string_type(actual):
        return True
    if is_binary_type(expected) and is_binary_type(actual):
        return True
    if _safe_type_check(expected, pa.types.is_boolean) and _safe_type_check(
        actual, pa.types.is_boolean
    ):
        return True
    if is_date_type(expected) and is_date_type(actual):
        return True
    if _safe_type_check(expected, pa.types.is_timestamp) and _safe_type_check(
        actual, pa.types.is_timestamp
    ):
        return getattr(expected, "tz", None) == getattr(actual, "tz", None)
    if _safe_type_check(expected, pa.types.is_decimal) and _safe_type_check(
        actual, pa.types.is_decimal
    ):
        return expected.precision == actual.precision and expected.scale == actual.scale
    return False


def validate_schema_type_compatibility(
    existing_schema: pa.Schema, incoming_schema: pa.Schema
) -> None:
    """Validate that incoming schema is compatible with existing schema."""
    existing_cols = {f.name: f.type for f in existing_schema}
    incoming_cols = {f.name: f.type for f in incoming_schema}

    mismatches = [
        c
        for c in existing_cols
        if c in incoming_cols
        and not types_compatible(existing_cols[c], incoming_cols[c])
    ]
    if mismatches:
        raise ValueError(
            f"Schema mismatch: type mismatches for existing columns: {mismatches}"
        )


# ----------------------------------------------------------------------
# Schema conversion (PyArrow <-> Delta Lake).
# ----------------------------------------------------------------------


def pyarrow_type_to_delta_type(pa_type: pa.DataType) -> str:
    """Convert PyArrow type to Delta Lake type string."""
    if pa.types.is_int8(pa_type):
        return "byte"
    if pa.types.is_int16(pa_type):
        return "short"
    if pa.types.is_int32(pa_type):
        return "integer"
    if pa.types.is_int64(pa_type):
        return "long"
    if pa.types.is_uint8(pa_type):
        return "short"
    if pa.types.is_uint16(pa_type):
        return "integer"
    if pa.types.is_uint32(pa_type):
        return "long"
    if pa.types.is_uint64(pa_type):
        return "decimal(20,0)"
    if pa.types.is_float16(pa_type) or pa.types.is_float32(pa_type):
        return "float"
    if pa.types.is_float64(pa_type):
        return "double"
    if is_string_type(pa_type):
        return "string"
    if is_binary_type(pa_type):
        return "binary"
    if pa.types.is_boolean(pa_type):
        return "boolean"
    if is_date_type(pa_type):
        return "date"
    if pa.types.is_timestamp(pa_type):
        return "timestamp"
    if pa.types.is_time(pa_type) or pa.types.is_null(pa_type):
        return "string"
    if pa.types.is_duration(pa_type):
        return "long"
    if pa.types.is_decimal(pa_type):
        return f"decimal({pa_type.precision},{pa_type.scale})"
    raise ValueError(f"Unsupported PyArrow type for Delta Lake: {pa_type}")


def convert_schema_to_delta(schema: pa.Schema) -> Any:
    """Convert PyArrow schema to Delta Lake schema."""
    from deltalake import Schema as DeltaSchema

    if schema is None:
        raise ValueError("Cannot convert None schema to Delta Lake schema")

    converted_fields = []
    for schema_field in schema:
        field_type = schema_field.type
        if pa.types.is_timestamp(field_type) and field_type.unit == "s":
            tz = field_type.tz if hasattr(field_type, "tz") else None
            field_type = pa.timestamp("us", tz=tz)
        converted_fields.append(
            pa.field(
                schema_field.name,
                field_type,
                schema_field.nullable,
                schema_field.metadata,
            )
        )

    converted_schema = pa.schema(converted_fields)

    try:
        return DeltaSchema.from_arrow(converted_schema)
    except (ValueError, TypeError):
        fields = [
            {
                "name": f.name,
                "type": pyarrow_type_to_delta_type(f.type),
                "nullable": f.nullable,
                "metadata": {},
            }
            for f in converted_schema
        ]
        return DeltaSchema.from_json(json.dumps({"type": "struct", "fields": fields}))


def to_pyarrow_schema(delta_schema: Any) -> pa.Schema:
    """Convert Delta Lake schema to PyArrow schema."""
    if delta_schema is None:
        raise ValueError("Cannot convert None to PyArrow schema")
    if isinstance(delta_schema, pa.Schema):
        return delta_schema

    out = None
    if hasattr(delta_schema, "to_pyarrow"):
        out = delta_schema.to_pyarrow()
    elif hasattr(delta_schema, "to_arrow"):
        # delta-rs newer builds expose to_arrow(); value may be arro3 Schema,
        # not pa.Schema.
        out = delta_schema.to_arrow()

    if out is not None:
        if isinstance(out, pa.Schema):
            return out
        # arro3 (and other Arrow C Data exporters): PyArrow accepts these in
        # pa.schema().
        try:
            return pa.schema(out)
        except (TypeError, ValueError):
            pass

    raise AttributeError(
        f"Cannot convert {type(delta_schema).__name__} to PyArrow schema"
    )


# ----------------------------------------------------------------------
# Parquet statistics for Delta file metadata.
# ----------------------------------------------------------------------


def _to_json_serializable(val: Any) -> Any:
    import math
    from decimal import Decimal

    if val is None:
        return None
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, float) and not math.isfinite(val):
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


def compute_parquet_statistics(table: pa.Table) -> str:
    """Compute Delta Lake statistics JSON for a table.

    Delta-rs expects per-column stats only for primitive (non-nested) types
    and rejects flat-integer ``nullCount`` for struct / list / map columns
    (it requires a recursive struct-shaped value for those). To keep the
    implementation simple and conformant, we skip stats entirely for
    nested columns -- delta-rs reads them as "no statistics available",
    which is harmless and disables only the row-group skipping optimisation
    for those columns.
    """
    stats: Dict[str, Any] = {"numRecords": table.num_rows}
    null_counts: Dict[str, int] = {}
    min_vals: Dict[str, Any] = {}
    max_vals: Dict[str, Any] = {}

    for i, col in enumerate(table.columns):
        name = table.schema.field(i).name
        col_type = col.type
        if pa.types.is_nested(col_type):
            # Skip stats for struct / list / map / fixed_size_list etc.
            # Delta-rs expects nested null-counts for nested columns and
            # rejects the flat integer form we'd otherwise emit.
            continue
        null_count = pc.sum(pc.is_null(col)).as_py()
        if null_count is not None and null_count >= 0:
            null_counts[name] = null_count
        if (
            is_numeric_type(col_type)
            or is_string_type(col_type)
            or is_temporal_type(col_type)
        ):
            min_val = _to_json_serializable(pc.min(col).as_py())
            max_val = _to_json_serializable(pc.max(col).as_py())
            if min_val is not None:
                min_vals[name] = min_val
            if max_val is not None:
                max_vals[name] = max_val

    if min_vals:
        stats["minValues"] = min_vals
    if max_vals:
        stats["maxValues"] = max_vals
    if null_counts:
        stats["nullCount"] = null_counts
    return json.dumps(stats)
