"""
Delta Lake utility functions for credential management and table operations.

This module provides helper functions for:
- Converting PyArrow filters to Delta Lake SQL predicates
- Managing cloud storage credentials (AWS, GCP, Azure)
- Delta table operations and utilities

Dependencies:
- boto3 for AWS credential detection: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- azure-identity for Azure authentication: https://learn.microsoft.com/en-us/python/api/azure-identity/
- deltalake Python package: https://delta-io.github.io/delta-rs/python/
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from deltalake import DeltaTable


def convert_pyarrow_filter_to_sql(
    filters: Optional[
        List[Union[Tuple[str, str, Any], Tuple[Tuple[str, str, Any], ...]]]
    ]
) -> Optional[str]:
    """
    Convert PyArrow partition filters to Delta Lake SQL predicate format.

    This function translates PyArrow's filter format (used by ray.data.read_parquet)
    into SQL WHERE clause format (used by Delta Lake's partition filtering).

    Filter Format:
    - Simple filter: ("column", "operator", value) → "column op value"
    - Conjunctive filter (AND): (("col1", "op", val1), ("col2", "op", val2))
      → "(col1 op val1 AND col2 op val2)"
    - Multiple filters in list are combined with OR

    Supported Operators:
    - Comparison: =, !=, <, <=, >, >=
    - Membership: IN, NOT IN (requires list/tuple value)
    - Boolean: AND (implicit in tuple of tuples)

    Args:
        filters: List of filter expressions in PyArrow format. None or empty list
            returns None (no filtering).

    Returns:
        SQL predicate string suitable for Delta Lake partition filtering,
        or None if no filters provided.

    Examples:
        Simple equality filter:
        >>> convert_pyarrow_filter_to_sql([("year", "=", "2024")])
        "year = '2024'"

        Numeric comparison:
        >>> convert_pyarrow_filter_to_sql([("age", ">", 18)])
        'age > 18'

        IN operator with list:
        >>> convert_pyarrow_filter_to_sql([("id", "in", [1, 2, 3])])
        'id IN (1, 2, 3)'

        Conjunctive filter (AND):
        >>> convert_pyarrow_filter_to_sql([(("year", "=", 2024), ("month", ">", 6))])
        '(year = 2024 AND month > 6)'

        Multiple filters (OR):
        >>> convert_pyarrow_filter_to_sql([("year", "=", 2023), ("year", "=", 2024)])
        'year = 2023 OR year = 2024'

    Raises:
        ValueError: If filter format is invalid or IN/NOT IN used with non-list value
    """
    if not filters:
        return None

    def format_value(value: Any) -> str:
        """
        Format a single value for SQL with proper escaping.

        Handles NULL, boolean, string, numeric, and other types.
        Strings are SQL-escaped (single quotes doubled) and wrapped in quotes.

        Args:
            value: Python value to format

        Returns:
            SQL-formatted string representation
        """
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            # Boolean values use SQL TRUE/FALSE keywords
            return "TRUE" if value else "FALSE"
        elif isinstance(value, str):
            # Escape single quotes by doubling them (SQL standard: '' represents ')
            # Example: "it's" → "it''s" → SQL: 'it''s' (displays as: it's)
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, (int, float)):
            # Numeric values don't need quoting
            return str(value)
        else:
            # Fallback for other types: convert to string and escape
            str_value = str(value)
            escaped_value = str_value.replace("'", "''")
            return f"'{escaped_value}'"

    def format_condition(col: str, op: str, value: Any) -> str:
        """
        Format a single filter condition as SQL expression.

        Args:
            col: Column name
            op: Operator (=, >, <, IN, etc.)
            value: Filter value

        Returns:
            SQL condition string like "age > 18" or "id IN (1, 2, 3)"

        Raises:
            ValueError: If IN/NOT IN operator used with non-list value
        """
        op_upper = op.upper()

        if op_upper in ("IN", "NOT IN"):
            # IN operator requires list or tuple of values
            if not isinstance(value, (list, tuple)):
                raise ValueError(
                    f"IN/NOT IN operator requires list or tuple value, "
                    f"got {type(value).__name__}. "
                    f"Example: ('id', 'IN', [1, 2, 3])"
                )
            # Format each value in the list and join with commas
            formatted_values = ", ".join(format_value(v) for v in value)
            return f"{col} {op_upper} ({formatted_values})"
        else:
            # Standard comparison operator (=, !=, <, >, <=, >=)
            return f"{col} {op} {format_value(value)}"

    sql_parts = []

    # Process each filter item (can be simple or conjunctive)
    for filter_item in filters:
        if not isinstance(filter_item, (tuple, list)):
            raise ValueError(
                f"Each filter must be a tuple or list, "
                f"got {type(filter_item).__name__}. "
                f"Expected format: ('column', 'op', value) or "
                f"(('col1', 'op1', val1), ('col2', 'op2', val2))"
            )

        # Distinguish between conjunctive (AND) and simple filters
        # Conjunctive filter has tuples as first element, simple filter has string
        if len(filter_item) > 0 and isinstance(filter_item[0], (tuple, list)):
            # Conjunctive filter: (("col1", "op1", val1), ("col2", "op2", val2))
            # Multiple conditions joined with AND within parentheses
            conditions = []
            for condition in filter_item:
                if not isinstance(condition, (tuple, list)) or len(condition) != 3:
                    raise ValueError(
                        f"Each condition in conjunctive filter must be a 3-tuple "
                        f"(column, operator, value), got {condition}"
                    )
                col, op, value = condition
                conditions.append(format_condition(col, op, value))
            # Wrap AND conditions in parentheses for proper precedence
            sql_parts.append(f"({' AND '.join(conditions)})")
        else:
            # Simple filter: ("col", "op", value)
            # Single condition without parentheses
            if len(filter_item) != 3:
                raise ValueError(
                    f"Simple filter must be a 3-tuple (column, operator, value), "
                    f"got {len(filter_item)} elements: {filter_item}"
                )
            col, op, value = filter_item
            sql_parts.append(format_condition(col, op, value))

    # Combine multiple filters with OR (each element in filters list is ORed)
    # Single filter doesn't need OR
    if len(sql_parts) == 1:
        return sql_parts[0]
    else:
        # Join with OR, e.g., "year = 2023 OR year = 2024"
        return " OR ".join(sql_parts)


class AWSUtilities:
    """
    AWS credential management for Delta Lake S3 access.

    Provides automatic credential detection using boto3's credential chain:
    1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    2. ~/.aws/credentials file
    3. IAM role (when running on EC2/ECS/Lambda)
    4. Container credentials (ECS task role)
    """

    @staticmethod
    def get_s3_storage_options() -> Dict[str, str]:
        """
        Get S3 storage options with automatic credential detection.

        Uses boto3's credential chain to automatically discover and use
        AWS credentials from various sources.

        Returns:
            Dictionary with AWS credentials in Delta Lake format:
            - AWS_ACCESS_KEY_ID: AWS access key
            - AWS_SECRET_ACCESS_KEY: AWS secret key
            - AWS_SESSION_TOKEN: Session token (for temporary credentials only)
            - AWS_REGION: AWS region (defaults to us-east-1)

            Returns empty dict if boto3 not installed or credentials not found.

        Example:
            >>> # On EC2 with IAM role or with credentials in environment:
            >>> AWSUtilities.get_s3_storage_options()
            {'AWS_ACCESS_KEY_ID': '...', 'AWS_SECRET_ACCESS_KEY': '...', ...}
        """
        try:
            import boto3

            # Create boto3 session to access credential chain
            session = boto3.Session()
            credentials = session.get_credentials()

            if credentials:
                # Build storage options dictionary for Delta Lake
                storage_options = {
                    "AWS_ACCESS_KEY_ID": credentials.access_key,
                    "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
                    "AWS_REGION": session.region_name or "us-east-1",
                }

                # Include session token only for temporary credentials
                # (IAM role, STS, etc.) - not present for long-term credentials
                if credentials.token:
                    storage_options["AWS_SESSION_TOKEN"] = credentials.token

                return storage_options
        except Exception:
            # Handle import errors, network issues, or credential problems gracefully
            pass
        return {}


class GCPUtilities:
    """
    GCP credential management for Delta Lake GCS access.

    Placeholder for future GCS-specific credential management.
    Currently uses default Google Cloud credentials (GOOGLE_APPLICATION_CREDENTIALS).
    """

    pass


class AzureUtilities:
    """
    Azure credential management for Delta Lake Azure Data Lake Storage access.

    Placeholder for future Azure-specific credential management.
    Currently uses environment variables or default Azure credentials.
    """

    @staticmethod
    def get_azure_storage_options() -> Dict[str, str]:
        """
        Get Azure storage options with automatic credential detection.

        Returns:
            Dictionary with Azure credentials in Delta Lake format.
            Returns empty dict if credentials not available.
        """
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
