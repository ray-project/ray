"""Utility functions for Delta Lake datasource operations.

This module consolidates file, schema, and storage utilities for
Delta Lake integration with Ray Data.

Delta Lake: https://delta.io/
delta-rs Python library: https://delta-io.github.io/delta-rs/python/
PyArrow: https://arrow.apache.org/docs/python/
"""

import json
import math
import os
import posixpath
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction, CommitProperties, Transaction


@dataclass
class DeltaWriteResult:
    """Result from writing blocks to Delta Lake storage.

    Attributes:
        add_actions: File metadata for Delta transaction log.
        schemas: Schemas from written blocks.
        written_files: List of full file paths written by this worker.
        write_uuid: Unique identifier for this write operation (for app_transactions).
    """

    add_actions: List["AddAction"] = field(default_factory=list)
    schemas: List[pa.Schema] = field(default_factory=list)
    written_files: List[str] = field(default_factory=list)
    write_uuid: Optional[str] = None


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
    fs: pa_fs.FileSystem, path: str, max_retries: int = 3, base_delay: float = 0.1
) -> pa_fs.FileInfo:
    """Get file info with retries and exponential backoff.

    Uses Ray's standard call_with_retry utility for consistent retry behavior.
    """
    from ray._common.retry import call_with_retry
    from ray.data.context import DataContext

    data_context = DataContext.get_current()

    def _get_file_info():
        return fs.get_file_info(path)

    max_backoff_s = (
        base_delay * (2 ** (max_retries - 1)) if max_retries > 1 else base_delay
    )

    return call_with_retry(
        _get_file_info,
        description=f"get file info for '{path}'",
        match=data_context.retried_io_errors,
        max_attempts=max_retries,
        max_backoff_s=max_backoff_s,
    )


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


def _safe_type_check(t: pa.DataType, check_func) -> bool:
    """Safely check type, handling arro3 types that don't have 'id' attribute."""
    try:
        return check_func(t)
    except AttributeError:
        # arro3 types don't have 'id' attribute - convert to pyarrow type first
        if not hasattr(t, "id"):
            try:
                pa_type = pa.DataType._import_from_c(t._export_to_c())
                return check_func(pa_type)
            except (AttributeError, TypeError):
                return False
        raise


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


def normalize_commit_properties(
    commit_properties: Optional[Dict[str, str]],
) -> Optional["CommitProperties"]:
    """Normalize commit properties for Delta Lake transactions.

    Args:
        commit_properties: Optional dict of commit properties or CommitProperties.

    Returns:
        CommitProperties object or None.
    """
    if commit_properties is None:
        return None

    from deltalake.transaction import CommitProperties

    if isinstance(commit_properties, CommitProperties):
        return commit_properties

    if not isinstance(commit_properties, dict):
        raise TypeError(
            "commit_properties must be a dict of string keys and values or "
            "a deltalake.transaction.CommitProperties object"
        )

    commit_properties = commit_properties.copy()
    max_commit_retries = commit_properties.pop("max_commit_retries", None)
    if max_commit_retries is not None:
        try:
            max_commit_retries = int(max_commit_retries)
        except (ValueError, TypeError):
            raise TypeError(
                "commit_properties['max_commit_retries'] must be an integer"
            )

    custom_metadata = {}
    for key, value in commit_properties.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError(
                "commit_properties must be a dict of string keys and values"
            )
        custom_metadata[key] = value

    return CommitProperties(
        custom_metadata=custom_metadata if custom_metadata else None,
        max_commit_retries=max_commit_retries,
        app_transactions=None,
    )


def validate_schema_type_compatibility(
    existing_schema: pa.Schema, incoming_schema: pa.Schema
) -> None:
    """Validate that incoming schema is compatible with existing schema.

    Checks for type mismatches in existing columns. New columns in incoming
    data are not validated (only overlapping columns are checked).

    Args:
        existing_schema: Schema from existing Delta table.
        incoming_schema: Schema from incoming data.

    Raises:
        ValueError: If type mismatches are found in existing columns.
    """
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


def convert_schema_to_delta(schema: pa.Schema) -> Any:
    """Convert PyArrow schema to Delta Lake schema."""
    from deltalake import Schema as DeltaSchema

    if schema is None:
        raise ValueError("Cannot convert None schema to Delta Lake schema")

    # Convert timestamp[s] to timestamp[us] since Delta Lake doesn't support
    # second precision. Delta Lake supports timestamp[us] and timestamp[ns].
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
        return DeltaSchema.from_json(
            json.dumps({"type": "struct", "fields": fields})
        )


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


# Type checking helpers
def is_string_type(t: pa.DataType) -> bool:
    """Check if type is a string type (including string_view)."""
    return (
        pa.types.is_string(t)
        or pa.types.is_large_string(t)
        or (hasattr(pa.types, "is_string_view") and pa.types.is_string_view(t))
    )


def is_binary_type(t: pa.DataType) -> bool:
    """Check if type is a binary type."""
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


def create_filesystem_from_storage_options(
    path: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional[pa_fs.FileSystem]:
    """Create PyArrow filesystem from storage options if credentials are provided.

    Args:
        path: Path to determine filesystem type (S3, Azure, GCS).
        storage_options: Storage options dict with credentials.

    Returns:
        PyArrow filesystem if credentials are available, None otherwise.
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
            return pa_fs.AzureFileSystem(
                account_name=account_name,
                sas_token=token,
            )
        elif account_name:
            return pa_fs.AzureFileSystem(account_name=account_name)
    elif path_lower.startswith(("gs://", "gcs://")):
        service_account = storage_options.get("GOOGLE_SERVICE_ACCOUNT")
        anonymous = storage_options.get("GOOGLE_ANONYMOUS", "").lower() == "true"
        if service_account:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account
        if service_account or anonymous:
            return pa_fs.GcsFileSystem(anonymous=anonymous)

    return None


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional["DeltaTable"]:
    """Return DeltaTable if it exists, None otherwise.

    Only catches exceptions that indicate "table not found" to avoid hiding
    real errors like auth failures, corrupt logs, etc.
    """
    from deltalake import DeltaTable

    not_found_exceptions = []
    not_found_exceptions.append(FileNotFoundError)

    try:
        from deltalake.exceptions import TableNotFoundError

        not_found_exceptions.append(TableNotFoundError)
    except ImportError:
        pass

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except tuple(not_found_exceptions):
        return None


def to_pyarrow_schema(delta_schema: Any) -> pa.Schema:
    """Convert Delta Lake schema to PyArrow schema.

    Handles both PyArrow schemas and arro3 Schema objects from deltalake.
    Ensures all DataType objects are PyArrow types, not arro3 types.
    """
    if delta_schema is None:
        raise ValueError("Cannot convert None to PyArrow schema")
    if isinstance(delta_schema, pa.Schema):
        needs_conversion = False
        fields = []
        for f in delta_schema:
            field_type = f.type
            if not hasattr(field_type, "id"):
                try:
                    pa_type = pa.DataType._import_from_c(field_type._export_to_c())
                    fields.append(
                        pa.field(f.name, pa_type, nullable=f.nullable)
                    )
                    needs_conversion = True
                except (AttributeError, TypeError):
                    fields.append(f)
            else:
                fields.append(f)
        if needs_conversion:
            return pa.schema(fields)
        return delta_schema

    schema_type_name = type(delta_schema).__name__
    schema_module = getattr(type(delta_schema), "__module__", "")

    if schema_type_name == "Schema" and "deltalake" in schema_module:
        if hasattr(delta_schema, "fields"):
            fields = []
            for f in delta_schema.fields:
                field_name = getattr(f, "name", None)
                field_nullable = getattr(f, "nullable", True)
                field_type = getattr(f, "type", None)

                if field_name is None or field_type is None:
                    continue

                type_str = str(field_type).lower()
                pa_type = None

                if "long" in type_str or "int64" in type_str:
                    pa_type = pa.int64()
                elif "integer" in type_str or "int32" in type_str:
                    pa_type = pa.int32()
                elif "short" in type_str or "int16" in type_str:
                    pa_type = pa.int16()
                elif "byte" in type_str or "int8" in type_str:
                    pa_type = pa.int8()
                elif "float" in type_str or "double" in type_str:
                    if "double" in type_str or "float64" in type_str:
                        pa_type = pa.float64()
                    else:
                        pa_type = pa.float32()
                elif "string" in type_str or "utf8" in type_str:
                    pa_type = pa.string()
                elif "boolean" in type_str or "bool" in type_str:
                    pa_type = pa.bool_()
                elif "binary" in type_str:
                    pa_type = pa.binary()
                elif "date" in type_str:
                    pa_type = pa.date32()
                elif "timestamp" in type_str:
                    if "microsecond" in type_str or "us" in type_str:
                        pa_type = pa.timestamp("us")
                    elif "millisecond" in type_str or "ms" in type_str:
                        pa_type = pa.timestamp("ms")
                    else:
                        pa_type = pa.timestamp("us")
                elif "decimal" in type_str:
                    pa_type = pa.decimal128(38, 18)

                if pa_type is None:
                    raise AttributeError(
                        f"Cannot convert deltalake field type '{type_str}' "
                        f"to PyArrow type for field '{field_name}'"
                    )

                fields.append(
                    pa.field(field_name, pa_type, nullable=field_nullable)
                )

            if fields:
                return pa.schema(fields)

    if schema_type_name == "Schema" and (
        "arro3" in schema_module or not schema_module
    ):
        fields = []
        try:
            for f in delta_schema:
                field_name = getattr(f, "name", None)
                field_nullable = getattr(f, "nullable", True)
                field_type = getattr(f, "type", None)

                if field_name is None or field_type is None:
                    continue

                try:
                    pa_type = pa.DataType._import_from_c(field_type._export_to_c())
                    fields.append(
                        pa.field(field_name, pa_type, nullable=field_nullable)
                    )
                except (AttributeError, TypeError) as e:
                    raise AttributeError(
                        f"Cannot convert arro3 field type "
                        f"{type(field_type).__name__} "
                        f"to PyArrow type for field '{field_name}': {e}"
                    )
        except (TypeError, AttributeError):
            if hasattr(delta_schema, "fields"):
                for f in delta_schema.fields:
                    field_name = getattr(f, "name", None)
                    field_nullable = getattr(f, "nullable", True)
                    field_type = getattr(f, "type", None)

                    if field_name is None or field_type is None:
                        continue

                    try:
                        pa_type = pa.DataType._import_from_c(
                            field_type._export_to_c()
                        )
                        fields.append(
                            pa.field(field_name, pa_type, nullable=field_nullable)
                        )
                    except (AttributeError, TypeError) as e:
                        raise AttributeError(
                            f"Cannot convert arro3 field type "
                            f"{type(field_type).__name__} "
                            f"to PyArrow type for field '{field_name}': {e}"
                        )

        if fields:
            return pa.schema(fields)

    if hasattr(delta_schema, "to_pyarrow"):
        schema = delta_schema.to_pyarrow()
        return to_pyarrow_schema(schema)
    raise AttributeError(
        f"Cannot convert {type(delta_schema).__name__} to PyArrow schema"
    )


def get_storage_options(
    path: str, provided: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Get storage options with auto-detection for cloud paths."""
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
        result = {}
        if project:
            result["GOOGLE_CLOUD_PROJECT"] = project
        if hasattr(credentials, "token") and credentials.token:
            result["GOOGLE_SERVICE_ACCOUNT_TOKEN"] = credentials.token
        return result
    except Exception:
        return {}


def create_app_transaction_id(write_uuid: Optional[str]) -> "Transaction":
    """Create app_transaction for idempotent commits.

    Args:
        write_uuid: Unique identifier for this write operation.

    Returns:
        Transaction object for app_transactions in CommitProperties.
    """
    from deltalake.transaction import Transaction

    if write_uuid is None:
        import time

        write_uuid = f"ray_data_write_{int(time.time() * 1000000)}"

    app_id = f"ray.data.write_delta:{write_uuid}"
    version = 1

    return Transaction(
        app_id=app_id,
        version=version,
        last_updated=None,
    )


def _to_json_serializable(val: Any) -> Any:
    """Convert value to JSON-serializable type."""
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
    """Compute Delta Lake statistics JSON for a table."""
    stats: Dict[str, Any] = {"numRecords": table.num_rows}
    null_counts: Dict[str, int] = {}
    min_vals: Dict[str, Any] = {}
    max_vals: Dict[str, Any] = {}

    for i, col in enumerate(table.columns):
        name = table.schema.field(i).name
        col_type = col.type

        if not pa.types.is_nested(col_type):
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
