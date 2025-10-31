import logging
import time
from typing import TYPE_CHECKING, Optional

import ray
from ray.data._internal.util import _check_import

if TYPE_CHECKING:
    import pyarrow.fs

logger = logging.getLogger(__name__)

# PyHive documentation: https://github.com/dropbox/PyHive
# Hive Metastore: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL

# Valid authentication methods for PyHive (must be uppercase)
_VALID_AUTH_METHODS = {"NONE", "NOSASL", "KERBEROS", "LDAP", "CUSTOM"}


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
):
    """
    Query Hive metastore to get table storage location and file format.

    Uses PyHive to connect to HiveServer2 and execute DESCRIBE FORMATTED
    to extract table metadata.

    Returns:
        Tuple of (storage_location, file_format, partition_columns)
    """
    from pyhive import hive

    # Validate and normalize auth parameter (PyHive requires uppercase)
    auth_upper = auth.upper()
    if auth_upper not in _VALID_AUTH_METHODS:
        raise ValueError(
            f"Invalid auth method: '{auth}'. "
            f"Supported methods: {', '.join(sorted(_VALID_AUTH_METHODS))}"
        )

    # Connect to Hive with retry logic
    max_retries = 3
    connection = None
    last_error = None

    for attempt in range(max_retries):
        try:
            logger.debug(
                f"Connecting to HiveServer2 at {host}:{port}, "
                f"database={database}, auth={auth_upper} (attempt {attempt + 1}/{max_retries})"
            )
            connection = hive.connect(
                host=host,
                port=port,
                auth=auth_upper,
                username=username,
                password=password,
                configuration=configuration or {},
                kerberos_service_name=kerberos_service_name,
            )
            break  # Connection successful
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                logger.warning(
                    f"Connection attempt {attempt + 1} failed: {e}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                raise RuntimeError(
                    f"Failed to connect to HiveServer2 at {host}:{port} after {max_retries} attempts. "
                    f"Last error: {last_error}"
                ) from last_error

    try:
        cursor = connection.cursor()

        # Get detailed table information using fully qualified table name
        # Escape backticks in database and table names to prevent SQL injection
        escaped_database = database.replace("`", "``")
        escaped_table = table.replace("`", "``")
        fully_qualified_table = f"`{escaped_database}`.`{escaped_table}`"

        logger.debug(f"Executing: DESCRIBE FORMATTED {fully_qualified_table}")
        cursor.execute(f"DESCRIBE FORMATTED {fully_qualified_table}")
        rows = cursor.fetchall()

        # Parse DESCRIBE FORMATTED output
        location = None
        file_format = None
        partition_cols = []

        # Flag to track if we're in partition information section
        in_partition_info = False

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

            # Extract location
            if col_name.lower() in ("location:", "location"):
                location = data_type
                logger.debug(f"Found table location: {location}")

            # Extract input format (determines file type)
            if col_name.lower() in ("inputformat:", "inputformat"):
                # Map Hive input format to file format
                if "parquet" in data_type.lower():
                    file_format = "parquet"
                elif "orc" in data_type.lower():
                    file_format = "orc"
                elif "avro" in data_type.lower():
                    file_format = "avro"
                elif "text" in data_type.lower():
                    file_format = "text"
                logger.debug(f"Detected file format: {file_format} from {data_type}")

            # Extract partition columns
            if in_partition_info and col_name and not col_name.startswith("#"):
                partition_cols.append(col_name)

        cursor.close()

        if not location:
            raise ValueError(
                f"Could not determine storage location for table {database}.{table}. "
                f"Ensure the table exists and you have permissions to describe it."
            )

        if not file_format:
            # Default to parquet if we can't determine format
            logger.warning(
                f"Could not determine file format for table {database}.{table}, "
                f"defaulting to parquet"
            )
            file_format = "parquet"

        return location, file_format, partition_cols

    finally:
        connection.close()


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
    **reader_kwargs,
) -> "ray.data.Dataset":
    """
    Read a Hive table by querying metastore and reading files directly from storage.

    This function uses PyHive to connect to the Hive metastore, determines the table's
    storage location and file format, then uses Ray Data's native readers to read
    directly from S3, HDFS, or other storage. This is much more efficient than reading
    through HiveServer2.

    Args:
        table: Hive table name
        host: HiveServer2 host address
        port: HiveServer2 port (default: 10000)
        database: Hive database name (default: "default")
        auth: Authentication mechanism - "NONE", "NOSASL", "KERBEROS", "LDAP", or "CUSTOM"
        username: Username for LDAP authentication
        password: Password for LDAP authentication
        configuration: Optional Hive configuration parameters
        kerberos_service_name: Kerberos service name (default: "hive")
        filesystem: Optional PyArrow filesystem for accessing storage
        **reader_kwargs: Additional arguments passed to the Ray Data reader
            (e.g., columns, partitioning, override_num_blocks)

    Returns:
        Ray Dataset containing the table data

    References:
        - PyHive: https://github.com/dropbox/PyHive
        - Hive Metastore: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
        - Ray Data Parquet Reader: https://docs.ray.io/en/latest/data/api/doc/ray.data.read_parquet.html
    """
    _check_import(None, module="pyhive", package="hive")

    logger.info(f"Reading Hive table {database}.{table} from {host}:{port}")

    # Get table metadata from Hive metastore
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
    )

    logger.info(
        f"Table location: {location}, format: {file_format}, "
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

    # Add partition columns to reader kwargs if available
    if partition_cols and "partitioning" not in reader_kwargs:
        from ray.data.datasource.partitioning import Partitioning

        logger.info(f"Table is partitioned by: {partition_cols}")
        reader_kwargs["partitioning"] = Partitioning("hive", field_names=partition_cols)

    # Add filesystem to reader kwargs if provided
    if filesystem and "filesystem" not in reader_kwargs:
        reader_kwargs["filesystem"] = filesystem

    # Read directly from storage using Ray Data native reader
    logger.info(f"Reading {file_format} files from {location}")
    return reader(location, **reader_kwargs)
