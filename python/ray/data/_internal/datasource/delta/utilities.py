"""Utility functions for Delta Lake table operations."""

from typing import TYPE_CHECKING, Any, Dict, Optional

import pyarrow as pa

if TYPE_CHECKING:
    from deltalake import DeltaTable


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional["DeltaTable"]:
    """Return DeltaTable if it exists, None otherwise.

    Args:
        table_uri: Path to Delta table.
        storage_options: Cloud storage credentials.

    Returns:
        DeltaTable object or None if table doesn't exist.
    """
    # deltalake: https://delta-io.github.io/delta-rs/python/
    from deltalake import DeltaTable
    from deltalake.exceptions import DeltaError, TableNotFoundError

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except (FileNotFoundError, OSError, ValueError, TableNotFoundError, DeltaError):
        return None


def to_pyarrow_schema(delta_schema: Any) -> pa.Schema:
    """Convert Delta Lake schema to PyArrow schema.

    Args:
        delta_schema: Delta Lake schema, PyArrow schema, or schema-like object.

    Returns:
        PyArrow Schema.

    Raises:
        ValueError: If delta_schema is None.
        AttributeError: If delta_schema cannot be converted.
    """
    if delta_schema is None:
        raise ValueError("Cannot convert None to PyArrow schema")
    if isinstance(delta_schema, pa.Schema):
        return delta_schema
    if hasattr(delta_schema, "to_pyarrow"):
        return delta_schema.to_pyarrow()
    if hasattr(delta_schema, "to_arrow"):
        return delta_schema.to_arrow()
    raise AttributeError(
        f"Cannot convert {type(delta_schema).__name__} to PyArrow schema"
    )


def get_storage_options(
    path: str, provided: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Get storage options with auto-detection for cloud paths.

    Attempts to auto-detect credentials for S3, Azure, and GCS paths using
    boto3, azure.identity, and google.auth respectively.

    Args:
        path: Path to Delta table (local or cloud).
        provided: User-provided storage options (take precedence).

    Returns:
        Dict of storage options with auto-detected credentials merged.
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
    """Get AWS credentials from boto3 session."""
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
    """Get Azure credentials from DefaultAzureCredential."""
    try:
        from azure.identity import DefaultAzureCredential

        credential = DefaultAzureCredential()
        token = credential.get_token("https://storage.azure.com/.default")
        return {"AZURE_STORAGE_TOKEN": token.token}
    except ImportError:
        return {}
    except Exception:
        return {}


def _get_gcs_credentials() -> Dict[str, str]:
    """Get GCS credentials from google.auth default credentials."""
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
    except ImportError:
        return {}
    except Exception:
        return {}
