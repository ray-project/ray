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
    """Convert Delta Lake schema to PyArrow schema."""
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

    Attempts to auto-detect credentials for S3 and Azure paths using
    boto3 and azure.identity respectively.
    """
    options = dict(provided or {})

    if path.lower().startswith(("s3://", "s3a://")):
        options = {**_get_aws_credentials(), **options}
    elif path.lower().startswith(("abfss://", "abfs://")):
        options = {**_get_azure_credentials(), **options}

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
    except Exception:
        return {}
