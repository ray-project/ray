"""
Delta Lake utility functions for credential management and table operations.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    from deltalake import DeltaTable


def convert_pyarrow_filter_to_sql(
    filters: Optional[
        List[Union[Tuple[str, str, Any], Tuple[Tuple[str, str, Any], ...]]]
    ]
) -> Optional[str]:
    """
    Convert PyArrow partition filters to Delta Lake SQL predicate format.

    Args:
        filters: List of filter tuples in PyArrow format.
            Simple filter: ("column", "operator", value)
            Conjunctive filter (AND): (("col1", "op1", val1), ("col2", "op2", val2))
            Multiple filters are combined with OR

    Returns:
        SQL predicate string or None if no filters

    Examples:
        >>> convert_pyarrow_filter_to_sql([("year", "=", "2024")])
        "year = '2024'"
        >>> convert_pyarrow_filter_to_sql([("age", ">", 18)])
        "age > 18"
        >>> convert_pyarrow_filter_to_sql([("id", "in", [1, 2, 3])])
        "id IN (1, 2, 3)"
        >>> convert_pyarrow_filter_to_sql([(("year", "=", 2024), ("month", ">", 6))])
        "(year = 2024 AND month > 6)"
    """
    if not filters:
        return None

    def format_value(value: Any) -> str:
        """Format a value for SQL, properly escaping strings."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, str):
            # Escape single quotes by doubling them (SQL standard)
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # Convert to string and escape
            str_value = str(value)
            escaped_value = str_value.replace("'", "''")
            return f"'{escaped_value}'"

    def format_condition(col: str, op: str, value: Any) -> str:
        """Format a single condition."""
        op_upper = op.upper()

        if op_upper in ("IN", "NOT IN"):
            if not isinstance(value, (list, tuple)):
                raise ValueError(
                    f"IN/NOT IN operator requires list/tuple value, got {type(value)}"
                )
            formatted_values = ", ".join(format_value(v) for v in value)
            return f"{col} {op_upper} ({formatted_values})"
        else:
            return f"{col} {op} {format_value(value)}"

    sql_parts = []

    for filter_item in filters:
        if not isinstance(filter_item, (tuple, list)):
            raise ValueError(
                f"Each filter must be a tuple or list, got {type(filter_item)}"
            )

        # Check if it's a conjunctive filter (AND) or simple filter
        if len(filter_item) > 0 and isinstance(filter_item[0], (tuple, list)):
            # Conjunctive filter: (("col1", "op1", val1), ("col2", "op2", val2))
            conditions = []
            for condition in filter_item:
                if not isinstance(condition, (tuple, list)) or len(condition) != 3:
                    raise ValueError(
                        f"Each condition in conjunctive filter must be a 3-tuple, "
                        f"got {condition}"
                    )
                col, op, value = condition
                conditions.append(format_condition(col, op, value))
            sql_parts.append(f"({' AND '.join(conditions)})")
        else:
            # Simple filter: ("col", "op", value)
            if len(filter_item) != 3:
                raise ValueError(
                    f"Simple filter must be a 3-tuple (column, operator, value), "
                    f"got {len(filter_item)} elements"
                )
            col, op, value = filter_item
            sql_parts.append(format_condition(col, op, value))

    # Multiple filters are combined with OR
    if len(sql_parts) == 1:
        return sql_parts[0]
    else:
        return " OR ".join(sql_parts)


class AWSUtilities:
    """AWS credential management."""

    @staticmethod
    def get_s3_storage_options() -> Dict[str, str]:
        """Get S3 storage options with automatic credential detection."""
        try:
            import boto3

            session = boto3.Session()
            credentials = session.get_credentials()

            if credentials:
                storage_options = {
                    "AWS_ACCESS_KEY_ID": credentials.access_key,
                    "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
                    "AWS_REGION": session.region_name or "us-east-1",
                }
                # Only include session token if it exists (for temporary credentials)
                if credentials.token:
                    storage_options["AWS_SESSION_TOKEN"] = credentials.token
                return storage_options
        except Exception:
            pass
        return {}


class GCPUtilities:
    """GCP credential management."""

    pass


class AzureUtilities:
    """Azure credential management."""

    @staticmethod
    def get_azure_storage_options() -> Dict[str, str]:
        """Get Azure storage options with automatic credential detection."""
        try:
            from azure.identity import DefaultAzureCredential

            credential = DefaultAzureCredential()
            token = credential.get_token("https://storage.azure.com/.default")
            return {"AZURE_STORAGE_TOKEN": token.token}
        except Exception:
            pass
        return {}


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional["DeltaTable"]:
    """
    Try to get a DeltaTable object, returning None if it doesn't exist.

    Args:
        table_uri: Path to the Delta table
        storage_options: Storage options for cloud authentication

    Returns:
        DeltaTable object if successful, None otherwise
    """
    try:
        from deltalake import DeltaTable

        return DeltaTable(table_uri, storage_options=storage_options)
    except Exception:
        return None


class DeltaUtilities:
    """Utility class for Delta Lake operations."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]] = None):
        """
        Initialize Delta utilities.

        Args:
            path: Path to the Delta table
            storage_options: Storage options for cloud authentication
        """
        self.path = path
        self.storage_options = self._get_storage_options(path, storage_options or {})

    def _get_storage_options(
        self, path: str, provided: Dict[str, str]
    ) -> Dict[str, str]:
        """Get storage options with auto-detection and user overrides."""
        auto_options = {}

        # Auto-detect based on path scheme
        if path.lower().startswith(("s3://", "s3a://")):
            auto_options = AWSUtilities.get_s3_storage_options()
        elif path.lower().startswith(("abfss://", "abfs://")):
            auto_options = AzureUtilities.get_azure_storage_options()

        # Merge with provided (provided takes precedence)
        return {**auto_options, **provided}

    def get_table(self) -> Optional["DeltaTable"]:
        """Get the DeltaTable object."""
        return try_get_deltatable(self.path, self.storage_options)
