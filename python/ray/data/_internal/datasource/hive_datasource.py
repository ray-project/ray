import logging
import re
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

import ray
from ray._common.retry import call_with_retry

if TYPE_CHECKING:
    import pyarrow.fs

logger = logging.getLogger(__name__)

# PyHive documentation: https://github.com/dropbox/PyHive
# Hive Metastore: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL

# Valid authentication methods for PyHive (must be uppercase)
_VALID_AUTH_METHODS = {"NONE", "NOSASL", "KERBEROS", "LDAP", "CUSTOM"}

# Connection retry configuration
_CONNECTION_MAX_ATTEMPTS = 3
_CONNECTION_MAX_BACKOFF_S = 4

# Validation constants
_MAX_IDENTIFIER_LENGTH = 128  # Reasonable limit for database/table names
_MIN_PORT = 1
_MAX_PORT = 65535
_DEFAULT_TIMEOUT_SECONDS = 30


def _validate_port(port: int) -> None:
    """Validate that port is in valid range."""
    if not isinstance(port, int):
        raise TypeError(f"Port must be an integer, got {type(port).__name__}")
    if port < _MIN_PORT or port > _MAX_PORT:
        raise ValueError(
            f"Port must be between {_MIN_PORT} and {_MAX_PORT}, got {port}"
        )


def _validate_host(host: str) -> str:
    """Validate host format and return normalized value.

    Args:
        host: Host string to validate

    Returns:
        Normalized host string (stripped of whitespace)

    Raises:
        TypeError: If host is not a string
        ValueError: If host is invalid
    """
    if not isinstance(host, str):
        raise TypeError(f"Host must be a string, got {type(host).__name__}")
    host = host.strip()
    if not host:
        raise ValueError("Host cannot be empty or whitespace-only")
    # Check for potentially dangerous characters
    if any(c in host for c in ["\n", "\r", "\t", "\0"]):
        raise ValueError(
            "Host cannot contain newlines, carriage returns, or null bytes"
        )
    # Basic validation - should not be a URL (user should extract hostname)
    if host.startswith(("http://", "https://")):
        raise ValueError(
            "Host should not include protocol (http:// or https://). "
            "Extract the hostname from the URL."
        )
    return host


def _validate_identifier(name: str, identifier_type: str) -> str:
    """Validate database or table identifier and return normalized value.

    Args:
        name: Identifier string to validate
        identifier_type: Type of identifier (e.g., "Database", "Table") for error messages

    Returns:
        Normalized identifier string (stripped of whitespace)

    Raises:
        TypeError: If name is not a string
        ValueError: If identifier is invalid
    """
    if not isinstance(name, str):
        raise TypeError(
            f"{identifier_type} must be a string, got {type(name).__name__}"
        )
    name = name.strip()
    if not name:
        raise ValueError(f"{identifier_type} cannot be empty or whitespace-only")
    if len(name) > _MAX_IDENTIFIER_LENGTH:
        raise ValueError(
            f"{identifier_type} exceeds maximum length of {_MAX_IDENTIFIER_LENGTH} characters"
        )
    # Check for dangerous characters that could be used for SQL injection
    # Allow backticks as they will be escaped
    dangerous_chars = ["\n", "\r", "\0", ";", "--", "/*", "*/"]
    for char in dangerous_chars:
        if char in name:
            raise ValueError(
                f"{identifier_type} contains invalid character sequence: {char!r}"
            )
    return name


def _escape_identifier(identifier: str) -> str:
    """
    Escape identifier for safe use in SQL queries.

    Escapes backticks by doubling them, which is the standard way to escape
    identifiers in SQL when using backticks for quoting.
    """
    if not identifier:
        return identifier
    # Escape backticks by doubling them
    return identifier.replace("`", "``")


def _validate_auth_params(
    auth: str, username: Optional[str], password: Optional[str]
) -> None:
    """Validate authentication parameters."""
    auth_upper = auth.upper()
    if auth_upper not in _VALID_AUTH_METHODS:
        raise ValueError(
            f"Invalid auth method: '{auth}'. "
            f"Supported methods: {', '.join(sorted(_VALID_AUTH_METHODS))}"
        )
    if auth_upper == "LDAP":
        if not username or not username.strip():
            raise ValueError("Username is required for LDAP authentication")
        if not password:
            raise ValueError("Password is required for LDAP authentication")
    elif auth_upper in ("NONE", "NOSASL", "KERBEROS"):
        if username or password:
            logger.warning(
                f"Username and password are ignored for {auth_upper} authentication"
            )


def _validate_kerberos_service_name(service_name: str) -> str:
    """Validate Kerberos service name format and return normalized value.

    Args:
        service_name: Kerberos service name to validate

    Returns:
        Normalized service name string (stripped of whitespace)

    Raises:
        TypeError: If service_name is not a string
        ValueError: If service name is invalid
    """
    if not isinstance(service_name, str):
        raise TypeError(
            f"Kerberos service name must be a string, got {type(service_name).__name__}"
        )
    service_name = service_name.strip()
    if not service_name:
        raise ValueError("Kerberos service name cannot be empty or whitespace-only")
    # Kerberos service names typically follow certain patterns
    # Allow alphanumeric, hyphens, underscores, dots
    if not re.match(r"^[a-zA-Z0-9._-]+$", service_name):
        raise ValueError(
            "Kerberos service name contains invalid characters. "
            "Only alphanumeric, dots, hyphens, and underscores are allowed."
        )
    return service_name


def _validate_configuration(configuration: Optional[dict]) -> None:
    """Validate Hive configuration dictionary."""
    if configuration is None:
        return
    if not isinstance(configuration, dict):
        raise TypeError(
            f"Configuration must be a dictionary, got {type(configuration).__name__}"
        )
    # Check that all keys and values are strings (Hive config requirement)
    for key, value in configuration.items():
        if not isinstance(key, str):
            raise TypeError(
                f"Configuration keys must be strings, got {type(key).__name__} for key {key!r}"
            )
        if not isinstance(value, str):
            raise TypeError(
                f"Configuration values must be strings, got {type(value).__name__} for key {key!r}"
            )
        # Basic validation - keys should look like Hive config keys
        if not key.strip():
            raise ValueError("Configuration keys cannot be empty or whitespace-only")


def _validate_storage_location(location: str) -> str:
    """
    Validate and normalize storage location.

    Args:
        location: Storage location string to validate

    Returns:
        Normalized location string

    Raises:
        ValueError: If location is invalid or potentially dangerous
    """
    if not isinstance(location, str):
        raise TypeError(
            f"Storage location must be a string, got {type(location).__name__}"
        )
    location = location.strip()
    if not location:
        raise ValueError("Storage location cannot be empty or whitespace-only")
    
    # Check for dangerous patterns that could indicate path traversal
    # Only reject patterns when they're actually used as path traversal operators,
    # not when they appear as part of legitimate directory/bucket names
    location_lower = location.lower()
    
    # Check for "../" or "..\\" as path traversal (after / or \ or at start of path segment)
    # This regex matches "../" or "..\\" when preceded by / or \ or start of string,
    # but not when part of directory names like "data../" or "user..name"
    if re.search(r"(^|[/\\])\.\.(/|\\\\)", location):
        raise ValueError(
            "Storage location contains path traversal pattern '../' or '..\\'. "
            "This could indicate a path traversal attempt."
        )
    
    # Check for "~/" at start (home directory reference)
    if location.startswith("~/"):
        raise ValueError(
            "Storage location starts with '~/'. "
            "This could indicate an attempt to access home directory."
        )
    
    # Check for dangerous file:// paths
    if re.search(r"file:///(etc|proc)/", location_lower):
        raise ValueError(
            "Storage location contains potentially dangerous file:// path. "
            "This could indicate an attempt to access system directories."
        )
    
    return location


def _validate_partition_columns(partition_cols: List[str]) -> List[str]:
    """Validate partition column names."""
    if not partition_cols:
        return partition_cols
    validated = []
    for col in partition_cols:
        if not isinstance(col, str):
            raise TypeError(
                f"Partition column names must be strings, got {type(col).__name__}"
            )
        col = col.strip()
        if not col:
            logger.warning("Skipping empty partition column name")
            continue
        validated.append(col)
    return validated


def _sanitize_log_message(message: str) -> str:
    """
    Remove potentially sensitive information from log messages.

    This prevents accidental credential exposure in logs.
    """
    # Remove common credential patterns
    # Remove user:pass@host patterns
    message = re.sub(r"://[^:]+:[^@]+@", "://***:***@", message)
    # Remove password= patterns
    message = re.sub(
        r"password=['\"]?[^'\"]+['\"]?", "password=***", message, flags=re.IGNORECASE
    )
    return message


def _connect_to_hive(
    host: str,
    port: int,
    auth_upper: str,
    username: Optional[str],
    password: Optional[str],
    configuration: Optional[dict],
    kerberos_service_name: str,
    timeout: Optional[int] = None,
) -> Any:
    """
    Connect to HiveServer2 with retry logic.

    Uses Ray's call_with_retry utility for consistent retry behavior across
    the codebase.

    Args:
        host: HiveServer2 host address
        port: HiveServer2 port
        auth_upper: Normalized authentication method (uppercase)
        username: Username for LDAP authentication
        password: Password for LDAP authentication
        configuration: Optional Hive configuration parameters
        kerberos_service_name: Kerberos service name
        timeout: Connection timeout in seconds (optional)

    Returns:
        Hive connection object

    References:
        - PyHive: https://github.com/dropbox/PyHive
    """
    # Import here to avoid requiring pyhive at module import time
    from pyhive import hive

    # Validate and normalize inputs
    host = _validate_host(host)
    _validate_port(port)
    kerberos_service_name = _validate_kerberos_service_name(kerberos_service_name)
    _validate_configuration(configuration)

    def _connect():
        try:
            # PyHive connection parameters
            connect_params = {
                "host": host,
                "port": port,
                "auth": auth_upper,
                "configuration": configuration or {},
                "kerberos_service_name": kerberos_service_name,
            }
            # Only add username/password if provided (for LDAP auth)
            if username is not None:
                connect_params["username"] = username
            if password is not None:
                connect_params["password"] = password
            # Add timeout if specified (some PyHive versions support this)
            if timeout is not None:
                connect_params["timeout"] = timeout

            return hive.connect(**connect_params)
        except Exception as e:
            logger.debug(f"Connection attempt failed: {e}")
            raise

    sanitized_msg = _sanitize_log_message(
        f"Connecting to HiveServer2 at {host}:{port}, auth={auth_upper}"
    )
    logger.debug(sanitized_msg)
    return call_with_retry(
        _connect,
        description=f"connect to HiveServer2 at {host}:{port}",
        max_attempts=_CONNECTION_MAX_ATTEMPTS,
        max_backoff_s=_CONNECTION_MAX_BACKOFF_S,
    )


def _parse_describe_formatted_output(
    rows: List[Tuple[Any, ...]], database: str, table: str
) -> Tuple[str, str, List[str]]:
    """
    Parse DESCRIBE FORMATTED output to extract table metadata.

    Args:
        rows: Output rows from DESCRIBE FORMATTED command
        database: Database name for error messages
        table: Table name for error messages

    Returns:
        Tuple of (storage_location, file_format, partition_columns)

    Raises:
        ValueError: If required metadata cannot be extracted
    """
    if not rows:
        raise ValueError(
            f"DESCRIBE FORMATTED returned no rows for table {database}.{table}. "
            "The table may not exist or you may not have permissions to describe it."
        )

    location = None
    file_format = None
    partition_cols = []
    in_partition_info = False
    locations_found = []
    input_formats_found = []

    for row in rows:
        if not row or len(row) < 2:
            continue

        col_name = str(row[0]).strip() if row[0] else ""
        data_type = str(row[1]).strip() if row[1] else ""

        # Check for section headers
        if "# Partition Information" in col_name:
            in_partition_info = True
            continue
        elif col_name.startswith("#"):
            in_partition_info = False
            continue

        # Extract location (may appear multiple times, collect all)
        if col_name.lower() in ("location:", "location"):
            if data_type:
                locations_found.append(data_type)
                # Use the first non-empty location found
                if location is None:
                    location = data_type
                    logger.debug(f"Found table location: {location}")

        # Extract input format (determines file type)
        if col_name.lower() in ("inputformat:", "inputformat"):
            detected_format = _detect_file_format_from_input_format(data_type)
            if detected_format:
                input_formats_found.append(detected_format)
                # Use the first detected format found
                if file_format is None:
                    file_format = detected_format
                    logger.debug(
                        f"Detected file format: {file_format} from {data_type}"
                    )

        # Extract partition columns
        if in_partition_info and col_name and not col_name.startswith("#"):
            partition_cols.append(col_name)

    # Validate location
    if not location:
        raise ValueError(
            f"Could not determine storage location for table {database}.{table}. "
            f"DESCRIBE FORMATTED returned {len(rows)} rows but no location field was found. "
            "Ensure the table exists and you have permissions to describe it."
        )

    # Validate and normalize location
    try:
        location = _validate_storage_location(location)
    except ValueError as e:
        raise ValueError(
            f"Invalid storage location for table {database}.{table}: {e}"
        ) from e

    # Warn if multiple locations found
    if len(locations_found) > 1:
        logger.warning(
            f"Multiple location values found for table {database}.{table}. "
            f"Using first: {location}"
        )

    # Validate file format detection
    if not file_format:
        raise ValueError(
            f"Could not determine file format for table {database}.{table} "
            f"from DESCRIBE FORMATTED output. "
            f"Found {len(input_formats_found)} input format(s): {input_formats_found if input_formats_found else 'none'}. "
            f"Ensure the table has a recognized InputFormat (ParquetInputFormat, AvroInputFormat, TextInputFormat, etc.)."
        )
    elif len(input_formats_found) > 1:
        logger.warning(
            f"Multiple input formats detected for table {database}.{table}. "
            f"Using first detected format: {file_format}"
        )

    # Validate partition columns
    partition_cols = _validate_partition_columns(partition_cols)

    return location, file_format, partition_cols


def _detect_file_format_from_input_format(input_format: str) -> Optional[str]:
    """
    Map Hive InputFormat class name to file format string.

    Uses more specific matching to avoid false positives. For example,
    "ParquetInputFormat" should match but "CustomParquetInputFormat" should
    be handled more carefully.

    Args:
        input_format: Hive InputFormat class name (e.g.,
            "org.apache.hadoop.mapred.ParquetInputFormat")

    Returns:
        File format string ("parquet", "orc", "avro", "text") or None
    """
    if not input_format:
        return None

    input_format_lower = input_format.lower()
    # Use word boundaries or specific patterns to avoid false positives
    # Check for common Hive InputFormat patterns
    format_patterns = [
        ("parquet", ["parquetinputformat", "parquetinput", ".parquet"]),
        ("orc", ["orcinputformat", "orcinput", ".orc"]),
        ("avro", ["avroinputformat", "avroinput", ".avro"]),
        ("text", ["textinputformat", "textinput"]),
    ]

    for format_name, patterns in format_patterns:
        for pattern in patterns:
            if pattern in input_format_lower:
                return format_name

    return None


def _get_table_location_and_format(
    host: str,
    port: int,
    database: str,
    table: str,
    auth: str,
    username: Optional[str],
    password: Optional[str],
    configuration: Optional[dict],
    kerberos_service_name: str,
    timeout: Optional[int] = None,
) -> Tuple[str, str, List[str]]:
    """
    Query Hive metastore to get table storage location and file format.

    Uses PyHive to connect to HiveServer2 and execute DESCRIBE FORMATTED
    to extract table metadata.

    Args:
        host: HiveServer2 host address
        port: HiveServer2 port
        database: Hive database name
        table: Hive table name
        auth: Authentication mechanism
        username: Username for LDAP authentication
        password: Password for LDAP authentication
        configuration: Optional Hive configuration parameters
        kerberos_service_name: Kerberos service name
        timeout: Connection timeout in seconds (optional)

    Returns:
        Tuple of (storage_location, file_format, partition_columns)

    Raises:
        ValueError: If input validation fails
        RuntimeError: If connection or query execution fails

    References:
        - PyHive: https://github.com/dropbox/PyHive
        - Hive Metastore: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
    """
    # Validate and normalize all inputs
    host = _validate_host(host)
    _validate_port(port)
    database = _validate_identifier(database, "Database")
    table = _validate_identifier(table, "Table")
    _validate_auth_params(auth, username, password)
    kerberos_service_name = _validate_kerberos_service_name(kerberos_service_name)
    _validate_configuration(configuration)

    # Normalize auth parameter (PyHive requires uppercase)
    auth_upper = auth.upper()

    # Connect to Hive with retry logic
    connection = None
    try:
        connection = _connect_to_hive(
            host=host,
            port=port,
            auth_upper=auth_upper,
            username=username,
            password=password,
            configuration=configuration,
            kerberos_service_name=kerberos_service_name,
            timeout=timeout,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to connect to HiveServer2 at {host}:{port}. " f"Error: {e}"
        ) from e

    cursor = None
    try:
        try:
            cursor = connection.cursor()
        except Exception as e:
            raise RuntimeError(
                f"Failed to create cursor for HiveServer2 connection. Error: {e}"
            ) from e

        # Get detailed table information using fully qualified table name
        # Escape identifiers to prevent SQL injection
        # Note: PyHive doesn't support parameterized queries for DESCRIBE statements,
        # so we use escaping instead
        escaped_database = _escape_identifier(database)
        escaped_table = _escape_identifier(table)
        fully_qualified_table = f"`{escaped_database}`.`{escaped_table}`"

        logger.debug(f"Executing: DESCRIBE FORMATTED {fully_qualified_table}")
        try:
            cursor.execute(f"DESCRIBE FORMATTED {fully_qualified_table}")
        except Exception as e:
            raise RuntimeError(
                f"Failed to execute DESCRIBE FORMATTED for table {database}.{table}. "
                f"Error: {e}. Ensure the table exists and you have permissions to describe it."
            ) from e

        try:
            rows = cursor.fetchall()
        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch DESCRIBE FORMATTED results for table {database}.{table}. "
                f"Error: {e}"
            ) from e

        # Parse DESCRIBE FORMATTED output
        try:
            location, file_format, partition_cols = _parse_describe_formatted_output(
                rows, database, table
            )
        except ValueError:
            # Re-raise ValueError as-is (these are validation errors)
            raise
        except Exception as e:
            raise RuntimeError(
                f"Failed to parse DESCRIBE FORMATTED output for table {database}.{table}. "
                f"Error: {e}"
            ) from e

        return location, file_format, partition_cols

    finally:
        # Clean up cursor
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                logger.warning(f"Error closing cursor: {e}", exc_info=True)

        # Clean up connection
        if connection is not None:
            try:
                connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}", exc_info=True)


def read_hive_table(
    table: str,
    *,
    host: str,
    port: int = 10000,
    database: str = "default",
    auth: str = "NONE",
    username: Optional[str] = None,
    password: Optional[str] = None,
    configuration: Optional[dict] = None,
    kerberos_service_name: str = "hive",
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
    timeout: Optional[int] = None,
    **reader_kwargs,
) -> "ray.data.Dataset":
    """
    Read a Hive table by querying metastore and reading files directly from storage.

    This function uses PyHive to connect to the Hive metastore, determines the table's
    storage location and file format, then uses Ray Data's native readers to read
    directly from S3, HDFS, or other storage. This is much more efficient than reading
    through HiveServer2.

    Args:
        table: Hive table name. Must be a valid identifier (no SQL injection characters).
        host: HiveServer2 host address (IP or hostname, no protocol prefix).
        port: HiveServer2 port (1-65535). Defaults to 10000.
        database: Hive database name. Defaults to "default".
        auth: Authentication mechanism - "NONE", "NOSASL", "KERBEROS", "LDAP", or "CUSTOM".
            Defaults to "NONE".
        username: Username for LDAP authentication. Required if auth="LDAP".
        password: Password for LDAP authentication. Required if auth="LDAP".
        configuration: Optional Hive configuration parameters as a dictionary.
            All keys and values must be strings.
        kerberos_service_name: Kerberos service name. Defaults to "hive".
        filesystem: Optional PyArrow filesystem for accessing storage. If not provided,
            Ray Data will infer the filesystem from the storage location URL.
        timeout: Connection timeout in seconds. Defaults to None (no timeout).
            Note: Timeout support depends on PyHive version.
        **reader_kwargs: Additional arguments passed to the Ray Data reader
            (e.g., columns, partitioning, override_num_blocks). See the specific reader
            documentation for supported arguments.

    Returns:
        Ray Dataset containing the table data

    Raises:
        ValueError: If input validation fails (invalid host, port, table name, etc.)
        RuntimeError: If connection or metadata retrieval fails
        TypeError: If input types are incorrect

    Examples:
        Basic usage with default authentication:

        >>> import ray
        >>> ds = ray.data.read_hive(
        ...     table="sales",
        ...     host="hive.example.com",
        ...     database="analytics"
        ... )

        With LDAP authentication:

        >>> ds = ray.data.read_hive(
        ...     table="users",
        ...     host="hive.example.com",
        ...     auth="LDAP",
        ...     username="analyst",
        ...     password="secret"
        ... )

        With column selection:

        >>> ds = ray.data.read_hive(
        ...     table="transactions",
        ...     host="hive.example.com",
        ...     columns=["user_id", "amount", "timestamp"]
        ... )

    References:
        - PyHive: https://github.com/dropbox/PyHive
        - Hive Metastore: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
        - Ray Data Parquet Reader: https://docs.ray.io/en/latest/data/api/doc/ray.data.read_parquet.html
    """
    # Check for required dependency (following pattern from read_delta)
    # _check_import expects an object with __class__, so we implement our own check
    import importlib

    try:
        importlib.import_module("pyhive")
    except ImportError:
        raise ImportError(
            "`ray.data.read_hive` depends on 'pyhive', but 'pyhive' "
            "couldn't be imported. You can install 'pyhive' by running `pip install pyhive`."
        )

    # Validate and normalize inputs (additional validation happens in _get_table_location_and_format)
    host = _validate_host(host)
    _validate_port(port)
    table = _validate_identifier(table, "Table")
    database = _validate_identifier(database, "Database")
    _validate_auth_params(auth, username, password)
    kerberos_service_name = _validate_kerberos_service_name(kerberos_service_name)
    _validate_configuration(configuration)

    # Sanitize log message to avoid credential exposure
    sanitized_msg = _sanitize_log_message(
        f"Reading Hive table {database}.{table} from {host}:{port}"
    )
    logger.info(sanitized_msg)

    # Get table metadata from Hive metastore
    try:
        location, file_format, partition_cols = _get_table_location_and_format(
            host=host,
            port=port,
            database=database,
            table=table,
            auth=auth,
            username=username,
            password=password,
            configuration=configuration,
            kerberos_service_name=kerberos_service_name,
            timeout=timeout,
        )
    except (ValueError, RuntimeError):
        # Re-raise validation and connection errors with context
        raise
    except Exception as e:
        raise RuntimeError(
            f"Unexpected error while retrieving metadata for table {database}.{table}: {e}"
        ) from e

    sanitized_location = _sanitize_log_message(location)
    logger.info(
        f"Table location: {sanitized_location}, format: {file_format}, "
        f"partitions: {partition_cols if partition_cols else 'none'}"
    )

    # Map file format to Ray Data reader
    reader_map = {
        "parquet": ray.data.read_parquet,
        "avro": ray.data.read_avro,
        "text": ray.data.read_text,
    }

    if file_format not in reader_map:
        # Provide helpful error message for unsupported formats
        if file_format == "orc":
            raise ValueError(
                "ORC file format is not currently supported by Ray Data. "
                "Consider converting your Hive table to Parquet format for better compatibility."
            )
        raise ValueError(
            f"Unsupported file format: {file_format}. "
            f"Supported formats: {list(reader_map.keys())}"
        )

    reader = reader_map[file_format]

    # Handle partitioning
    # If user provided partitioning, warn if it conflicts with detected partitioning
    if "partitioning" in reader_kwargs:
        if partition_cols:
            logger.warning(
                f"User-provided partitioning overrides auto-detected partition columns: "
                f"{partition_cols}"
            )
    elif partition_cols:
        # Only add auto-detected partitioning if user didn't provide their own
        from ray.data.datasource.partitioning import Partitioning

        logger.info(f"Table is partitioned by: {partition_cols}")
        reader_kwargs["partitioning"] = Partitioning("hive", field_names=partition_cols)

    # Handle filesystem
    # If user provided filesystem, warn that it overrides location-based inference
    if filesystem and "filesystem" in reader_kwargs:
        logger.warning(
            "User-provided filesystem in reader_kwargs overrides filesystem parameter"
        )
    elif filesystem:
        # Only add filesystem if user didn't provide it in kwargs
        reader_kwargs["filesystem"] = filesystem

    # Read directly from storage using Ray Data native reader
    try:
        logger.info(f"Reading {file_format} files from {sanitized_location}")
        return reader(location, **reader_kwargs)
    except Exception as e:
        raise RuntimeError(
            f"Failed to read {file_format} files from location {sanitized_location}. "
            f"Error: {e}. "
            f"Ensure the location is accessible and contains valid {file_format} files."
        ) from e
