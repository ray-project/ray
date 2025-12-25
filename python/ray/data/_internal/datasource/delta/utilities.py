"""
Delta Lake utility functions for credential management and table operations.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import pyarrow as pa

if TYPE_CHECKING:
    from deltalake import DeltaTable


def convert_pyarrow_filter_to_sql(
    filters: Optional[
        List[Union[Tuple[str, str, Any], Tuple[Tuple[str, str, Any], ...]]]
    ],
) -> Optional[str]:
    """Convert PyArrow partition filters to Delta Lake SQL predicate format."""
    if not filters:
        return None

    def fmt_val(v: Any) -> str:
        if v is None:
            return "NULL"
        elif isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        elif isinstance(v, str):
            escaped = v.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(v, (int, float)):
            return str(v)
        escaped = str(v).replace("'", "''")
        return f"'{escaped}'"

    def fmt_cond(col: str, op: str, val: Any) -> str:
        op_upper = op.upper()
        if op_upper in ("IN", "NOT IN"):
            if not isinstance(val, (list, tuple)):
                raise ValueError(
                    f"IN/NOT IN requires list/tuple, got {type(val).__name__}"
                )
            return f"{col} {op_upper} ({', '.join(fmt_val(v) for v in val)})"
        return f"{col} {op} {fmt_val(val)}"

    parts = []
    for item in filters:
        if not isinstance(item, (tuple, list)):
            raise ValueError(f"Filter must be tuple/list, got {type(item).__name__}")

        if len(item) > 0 and isinstance(item[0], (tuple, list)):
            conds = []
            for cond in item:
                if not isinstance(cond, (tuple, list)) or len(cond) != 3:
                    raise ValueError(f"Condition must be 3-tuple, got {cond}")
                conds.append(fmt_cond(*cond))
            parts.append(f"({' AND '.join(conds)})")
        else:
            if len(item) != 3:
                raise ValueError(f"Filter must be 3-tuple, got {len(item)} elements")
            parts.append(fmt_cond(*item))

    return parts[0] if len(parts) == 1 else " OR ".join(parts)


def _get_aws_storage_options() -> Dict[str, str]:
    """Get S3 storage options from boto3 credentials."""
    import boto3

    session = boto3.Session()
    credentials = session.get_credentials()
    if not credentials:
        return {}

    storage_options = {
        "AWS_ACCESS_KEY_ID": credentials.access_key,
        "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
        "AWS_REGION": session.region_name or "us-east-1",
    }
    if credentials.token:
        storage_options["AWS_SESSION_TOKEN"] = credentials.token
    return storage_options


def _get_azure_storage_options() -> Dict[str, str]:
    """Get Azure storage options from DefaultAzureCredential."""
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return {"AZURE_STORAGE_TOKEN": token.token}


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional["DeltaTable"]:
    """Get a DeltaTable object if it exists, return None otherwise."""
    from deltalake import DeltaTable
    from deltalake.exceptions import DeltaError, TableNotFoundError

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except (FileNotFoundError, OSError, ValueError, TableNotFoundError, DeltaError):
        # Table not found or unreachable with the provided options; callers treat this as
        # "table does not exist". Other errors should surface to fail fast.
        return None


def to_pyarrow_schema(delta_schema: Any) -> pa.Schema:
    """Convert a Delta Lake schema object to a PyArrow schema.

    delta-rs schema objects may expose either ``to_pyarrow`` (newer) or
    ``to_arrow`` (older) helpers. Fall back to returning the input if it
    is already a PyArrow schema.
    """
    if isinstance(delta_schema, pa.Schema):
        return delta_schema
    if hasattr(delta_schema, "to_pyarrow"):
        return delta_schema.to_pyarrow()
    if hasattr(delta_schema, "to_arrow"):
        return delta_schema.to_arrow()
    raise AttributeError(
        "Delta schema object does not support to_pyarrow() or to_arrow(). "
        f"Type: {type(delta_schema).__name__}"
    )


def get_storage_options(
    path: str, provided: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Get storage options with auto-detection for cloud paths."""
    provided = provided or {}
    auto_options = {}

    if path.lower().startswith(("s3://", "s3a://")):
        try:
            auto_options = _get_aws_storage_options()
        except Exception:
            pass
    elif path.lower().startswith(("abfss://", "abfs://")):
        try:
            auto_options = _get_azure_storage_options()
        except Exception:
            pass

    return {**auto_options, **provided}
