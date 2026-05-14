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
import urllib.parse
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

if TYPE_CHECKING:
    from deltalake import DeltaTable


# ----------------------------------------------------------------------
# Partitioning constants and helpers.
# ----------------------------------------------------------------------

MAX_PARTITION_PATH_LENGTH = 200
MAX_PARTITION_COLUMNS = 10


def build_partition_path(
    cols: List[str], values: tuple
) -> Tuple[str, Dict[str, Optional[str]]]:
    """Build a Hive-style partition path (``col1=val1/col2=val2/``).

    NULL values use ``__HIVE_DEFAULT_PARTITION__``; float NaN uses the
    literal string ``"NaN"`` (distinct from string ``"NaN"``).
    """
    if not cols or not values:
        return "", {}

    parts = []
    part_dict: Dict[str, Optional[str]] = {}
    for col, val in zip(cols, values):
        if val is None:
            parts.append(f"{col}=__HIVE_DEFAULT_PARTITION__")
            part_dict[col] = None
        elif isinstance(val, float) and math.isnan(val):
            parts.append(f"{col}=NaN")
            part_dict[col] = "NaN"
        else:
            validate_partition_value(val)
            encoded_val = urllib.parse.quote(str(val), safe="")
            parts.append(f"{col}={encoded_val}")
            part_dict[col] = str(val)

    path = "/".join(parts) + "/"
    if len(path) > MAX_PARTITION_PATH_LENGTH:
        raise ValueError(f"Partition path too long: {len(path)} chars")
    return path, part_dict


def validate_partition_value(value: Any) -> None:
    """Validate a partition value is safe for use in paths."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return
    val_str = str(value)
    if not val_str:
        raise ValueError("Partition value cannot be empty string")
    if ".." in val_str or "/" in val_str or "\\" in val_str:
        raise ValueError(f"Partition value contains invalid characters: {value}")
    if "\x00" in val_str:
        raise ValueError(f"Partition value contains null byte: {value}")
    if len(val_str) > MAX_PARTITION_PATH_LENGTH:
        raise ValueError(f"Partition value too long ({len(val_str)} chars)")


def validate_partition_column_names(cols: List[str]) -> List[str]:
    """Validate partition column names."""
    if not isinstance(cols, list):
        raise ValueError(f"Partition columns must be a list, got {type(cols).__name__}")
    if len(cols) > MAX_PARTITION_COLUMNS:
        raise ValueError(f"Too many partition columns ({len(cols)})")
    seen = set()
    for col in cols:
        if not isinstance(col, str) or not col.strip():
            raise ValueError(f"Invalid partition column name: {col}")
        if "/" in col or "\\" in col or ".." in col or "=" in col:
            raise ValueError(f"Invalid characters in partition column name: {col}")
        if col in seen:
            raise ValueError(f"Duplicate partition column: {col}")
        seen.add(col)
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
            raise ValueError(f"Partition column '{col}' has nested type {col_type}")
        if pa.types.is_dictionary(col_type):
            raise ValueError(f"Partition column '{col}' has dictionary type")


def infer_partition_type(value: Any) -> pa.DataType:
    """Infer PyArrow type from a partition value (best-effort)."""
    if value is None:
        return pa.string()
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, str):
        if value.lower() in ("true", "false"):
            return pa.bool_()
        try:
            int(value)
            if "." not in value and "e" not in value.lower():
                return pa.int64()
        except ValueError:
            pass
        try:
            float(value)
            return pa.float64()
        except ValueError:
            pass
    return pa.string()


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
    """Resolve storage options, auto-detecting cloud credentials where possible.

    Caller-supplied options always win. Auto-detected entries fill in
    missing keys for s3://, abfs[s]://, gs://, gcs:// paths.
    """
    options = dict(provided or {})
    path_lower = path.lower()
    if path_lower.startswith(("s3://", "s3a://")):
        options = {**_get_aws_credentials(), **options}
    elif path_lower.startswith(("abfss://", "abfs://")):
        options = {**_get_azure_credentials(), **options}
    elif path_lower.startswith(("gs://", "gcs://")):
        options = {**_get_gcs_credentials(), **options}
    return options


def _get_aws_credentials() -> Dict[str, str]:
    try:
        import boto3

        session = boto3.Session()
        creds = session.get_credentials()
        if not creds:
            return {}
        result = {
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_REGION": session.region_name or "us-east-1",
        }
        if creds.token:
            result["AWS_SESSION_TOKEN"] = creds.token
        return result
    except Exception:
        return {}


def _get_azure_credentials() -> Dict[str, str]:
    try:
        from azure.identity import DefaultAzureCredential

        credential = DefaultAzureCredential()
        token = credential.get_token("https://storage.azure.com/.default")
        return {"AZURE_STORAGE_TOKEN": token.token}
    except Exception:
        return {}


def _get_gcs_credentials() -> Dict[str, str]:
    try:
        import google.auth
        from google.auth.transport.requests import Request

        credentials, project = google.auth.default()
        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        result: Dict[str, str] = {}
        if project:
            result["GOOGLE_CLOUD_PROJECT"] = project
        if hasattr(credentials, "token") and credentials.token:
            result["GOOGLE_SERVICE_ACCOUNT_TOKEN"] = credentials.token
        return result
    except Exception:
        return {}


def create_filesystem_from_storage_options(
    path: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional[pa_fs.FileSystem]:
    """Build a PyArrow filesystem from storage_options for cloud paths.

    Returns None when no usable credentials are present so PyArrow falls
    back to its default resolution chain.
    """
    if not storage_options:
        return None
    path_lower = path.lower()
    if path_lower.startswith(("s3://", "s3a://")):
        access_key = storage_options.get("AWS_ACCESS_KEY_ID")
        secret_key = storage_options.get("AWS_SECRET_ACCESS_KEY")
        session_token = storage_options.get("AWS_SESSION_TOKEN")
        region = storage_options.get("AWS_REGION")
        if access_key or secret_key or session_token or region:
            return pa_fs.S3FileSystem(
                access_key=access_key,
                secret_key=secret_key,
                session_token=session_token,
                region=region,
            )
    elif path_lower.startswith(("abfss://", "abfs://")):
        account_name = storage_options.get("AZURE_STORAGE_ACCOUNT_NAME")
        token = storage_options.get("AZURE_STORAGE_TOKEN")
        if account_name and token:
            return pa_fs.AzureFileSystem(account_name=account_name, sas_token=token)
        if account_name:
            return pa_fs.AzureFileSystem(account_name=account_name)
    elif path_lower.startswith(("gs://", "gcs://")):
        service_account = storage_options.get("GOOGLE_SERVICE_ACCOUNT")
        anonymous = storage_options.get("GOOGLE_ANONYMOUS", "").lower() == "true"
        if service_account:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account
        if service_account or anonymous:
            return pa_fs.GcsFileSystem(anonymous=anonymous)
    return None


# ----------------------------------------------------------------------
# Commit properties (idempotency).
# ----------------------------------------------------------------------


def normalize_commit_properties(commit_properties):
    """Normalise commit_properties for delta-rs transactions."""
    if commit_properties is None:
        return None
    from deltalake.transaction import CommitProperties

    if isinstance(commit_properties, CommitProperties):
        return commit_properties
    if not isinstance(commit_properties, dict):
        raise TypeError(
            "commit_properties must be a dict of string keys/values or "
            "a deltalake.transaction.CommitProperties object"
        )
    commit_properties = dict(commit_properties)
    max_commit_retries = commit_properties.pop("max_commit_retries", None)
    if max_commit_retries is not None:
        try:
            max_commit_retries = int(max_commit_retries)
        except (ValueError, TypeError) as e:
            raise TypeError(
                "commit_properties['max_commit_retries'] must be an integer"
            ) from e

    custom_metadata: Dict[str, str] = {}
    for key, value in commit_properties.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError(
                "commit_properties must be a dict of string keys and values"
            )
        custom_metadata[key] = value

    return CommitProperties(
        custom_metadata=custom_metadata or None,
        max_commit_retries=max_commit_retries,
        app_transactions=None,
    )


def create_app_transaction_id(write_uuid: Optional[str]):
    """Create an idempotent app_transactions entry from a per-write UUID."""
    from deltalake.transaction import Transaction

    if write_uuid is None:
        import time

        write_uuid = f"ray_data_write_{int(time.time() * 1_000_000)}"
    return Transaction(
        app_id=f"ray.data.write_delta:{write_uuid}",
        version=1,
        last_updated=None,
    )


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
