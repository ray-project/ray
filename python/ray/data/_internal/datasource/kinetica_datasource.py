"""
Kinetica Datasource for Ray Data.

This module provides a Ray Data Datasource implementation for reading data
from Kinetica databases.
"""

import logging
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from ray.data._internal.util import _check_import
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask

if TYPE_CHECKING:
    import pyarrow as pa
    from ray.data.context import DataContext


logger = logging.getLogger(__name__)


# Characters that are potentially dangerous in filter expressions
_UNSAFE_FILTER_CHARS = re.compile(r"[;{}]")


def _is_filter_safe(filter_expr: str) -> bool:
    """
    Check if a filter expression is safe from SQL injection.

    Args:
        filter_expr: The filter expression to check.

    Returns:
        True if the filter appears safe, False otherwise.
    """
    if not filter_expr:
        return True

    if _UNSAFE_FILTER_CHARS.search(filter_expr):
        return False

    return True


class KineticaDatasource(Datasource):
    """
    A Ray Data Datasource for reading from Kinetica databases.

    This datasource supports parallel reads by partitioning data across
    multiple read tasks using offset-based pagination.

    Example:
        >>> import ray
        >>> from ray.data._internal.datasource.kinetica_datasource import (
        ...     KineticaDatasource,
        ... )
        >>>
        >>> ds = ray.data.read_datasource(
        ...     KineticaDatasource(
        ...         url="http://localhost:9191",
        ...         table_name="my_table",
        ...         username="admin",
        ...         password="password",
        ...         columns=["id", "name", "value"],
        ...         filter_expression="value > 100",
        ...     )
        ... )
        >>> ds.show()
    """

    # Default batch size for pagination (10,000 records per request)
    DEFAULT_BATCH_SIZE = 10000

    def __init__(
        self,
        url: str,
        table_name: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filter_expression: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "ascending",
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the Kinetica datasource.

        Args:
            url: URL of the Kinetica server (e.g., "http://localhost:9191").
            table_name: Name of the table to read from.
            username: Username for authentication.
            password: Password for authentication.
            columns: Optional list of column names to read. If None, reads all columns.
            filter_expression: Optional SQL WHERE clause filter (without the WHERE keyword).
            sort_by: Optional column name to sort results by.
            sort_order: Sort order, either "ascending" or "descending".
            limit: Optional maximum number of records to read.
            batch_size: Number of records to fetch per request for pagination.
                        Defaults to 10,000.
            options: Additional GPUdb client options.
        """
        _check_import(self, module="gpudb", package="gpudb")

        self._url = url
        self._table_name = table_name
        self._username = username
        self._password = password
        self._columns = columns
        self._filter_expression = filter_expression
        self._sort_by = sort_by
        self._sort_order = sort_order
        self._limit = limit
        self._batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        self._options = options or {}

        # Validate filter expression
        if filter_expression and not _is_filter_safe(filter_expression):
            raise ValueError(
                f"Filter expression contains potentially unsafe characters: {filter_expression}"
            )

        # Cache for schema and record type
        self._arrow_schema: Optional["pa.Schema"] = None
        self._record_type = None
        self._total_count: Optional[int] = None

    def _init_client(self):
        """Create and return a GPUdb client instance."""
        from gpudb import GPUdb

        gpudb_options = GPUdb.Options()

        if self._username:
            gpudb_options.username = self._username
        if self._password:
            gpudb_options.password = self._password

        # Apply additional options
        for key, value in self._options.items():
            if hasattr(gpudb_options, key):
                setattr(gpudb_options, key, value)

        return GPUdb(host=self._url, options=gpudb_options)

    def _get_table_info(self, client) -> tuple:
        """
        Get table schema and record count.

        Returns:
            Tuple of (GPUdbRecordType, total_record_count, arrow_schema)
        """
        from gpudb import GPUdbRecordType, GPUdbRecordColumn, GPUdbException
        from ray.data._internal.datasource.kinetica_type_utils import (
            kinetica_type_to_arrow_schema,
        )
        import json

        # Get table info
        response = client.show_table(
            table_name=self._table_name,
            options={"get_column_info": "true"},
        )

        # Get record type from schema
        type_schemas = response.get("type_schemas", [])
        properties_list = response.get("properties", [[]])

        if not type_schemas:
            raise GPUdbException(
                f"Could not retrieve schema for table '{self._table_name}'"
            )

        # Parse the Avro schema
        schema_dict = json.loads(type_schemas[0])

        columns = []
        props_dict = properties_list[0] if properties_list else {}

        for field_def in schema_dict.get("fields", []):
            field_name = field_def.get("name")
            field_type = field_def.get("type")

            # Skip columns not in the requested list
            if self._columns and field_name not in self._columns:
                continue

            # Handle union types (for nullability)
            is_nullable = False
            if isinstance(field_type, list):
                non_null_types = [t for t in field_type if t != "null"]
                is_nullable = "null" in field_type
                field_type = non_null_types[0] if non_null_types else "string"

            # Map Avro type to Kinetica column type
            if field_type == "int":
                col_type = GPUdbRecordColumn._ColumnType.INT
            elif field_type == "long":
                col_type = GPUdbRecordColumn._ColumnType.LONG
            elif field_type == "float":
                col_type = GPUdbRecordColumn._ColumnType.FLOAT
            elif field_type == "double":
                col_type = GPUdbRecordColumn._ColumnType.DOUBLE
            elif field_type == "bytes":
                col_type = GPUdbRecordColumn._ColumnType.BYTES
            else:
                col_type = GPUdbRecordColumn._ColumnType.STRING

            col_properties = props_dict.get(field_name, [])
            if not isinstance(col_properties, list):
                col_properties = []

            column = GPUdbRecordColumn(
                name=field_name,
                column_type=col_type,
                column_properties=col_properties,
                is_nullable=is_nullable,
            )
            columns.append(column)

        # Handle empty tables (no columns)
        if not columns:
            import pyarrow as pa
            return None, 0, pa.schema([])

        record_type = GPUdbRecordType(columns=columns, label=self._table_name)

        # Get total count (accounting for filter)
        if self._filter_expression:
            count_response = client.get_records(
                table_name=self._table_name,
                offset=0,
                limit=0,
                options={"expression": self._filter_expression},
            )
            total_count = count_response.get("total_number_of_records", 0)
        else:
            total_count = response.get("total_size", 0)
            if isinstance(total_count, list):
                total_count = total_count[0] if total_count else 0

        # Apply limit if specified
        if self._limit is not None:
            total_count = min(total_count, self._limit)

        # Convert to Arrow schema
        arrow_schema = kinetica_type_to_arrow_schema(record_type)

        return record_type, total_count, arrow_schema

    def _estimate_row_size(self, client, sample_size: int = 100) -> int:
        """
        Estimate the average row size by sampling.

        Args:
            client: GPUdb client.
            sample_size: Number of rows to sample.

        Returns:
            Estimated average row size in bytes.
        """
        try:
            options = (
                {"expression": self._filter_expression}
                if self._filter_expression
                else {}
            )
            if self._columns is not None:
                options["select_columns"] = ",".join(self._columns)

            response = client.get_records(
                table_name=self._table_name,
                offset=0,
                limit=sample_size,
                options=options,
            )

            records_json = response.get("records_json", [])
            if not records_json:
                return 256

            total_size = sum(len(str(r)) for r in records_json)
            return total_size // len(records_json)

        except Exception:
            return 256

    def _create_read_fn(
        self,
        offset: int,
        limit: int,
    ) -> Callable[[], Iterable["pa.Table"]]:
        """
        Create a read function for a specific data partition.

        Args:
            offset: Starting offset for this partition.
            limit: Number of records to read.

        Returns:
            A callable that returns an iterable of Arrow tables.
        """
        # Capture instance variables for the closure
        url = self._url
        username = self._username
        password = self._password
        table_name = self._table_name
        columns = self._columns
        filter_expression = self._filter_expression
        sort_by = self._sort_by
        sort_order = self._sort_order
        client_options = self._options
        arrow_schema = self._arrow_schema
        batch_size = self._batch_size

        def read_fn() -> Iterable["pa.Table"]:
            import pyarrow as pa
            from gpudb import GPUdb
            from ray.data._internal.datasource.kinetica_type_utils import (
                convert_records_to_arrow_table,
            )
            import json

            # Create client in the worker
            gpudb_options = GPUdb.Options()
            if username:
                gpudb_options.username = username
            if password:
                gpudb_options.password = password
            for key, value in client_options.items():
                if hasattr(gpudb_options, key):
                    setattr(gpudb_options, key, value)

            client = GPUdb(host=url, options=gpudb_options)

            # Build request options
            options = {}
            if filter_expression:
                options["expression"] = filter_expression
            if columns is not None:
                options["select_columns"] = ",".join(columns)
            if sort_by:
                options["sort_by"] = sort_by
                options["sort_order"] = sort_order

            # Track how many records we've read and need to read
            records_remaining = limit
            current_offset = offset
            has_yielded = False

            try:
                while records_remaining > 0:
                    fetch_count = min(batch_size, records_remaining)

                    response = client.get_records(
                        table_name=table_name,
                        offset=current_offset,
                        limit=fetch_count,
                        encoding="json",
                        options=options,
                    )

                    records_json = response.get("records_json", [])

                    if not records_json:
                        break

                    # Parse JSON records
                    records = [
                        json.loads(r) if isinstance(r, str) else r for r in records_json
                    ]

                    # Convert to Arrow table and yield
                    table = convert_records_to_arrow_table(records, arrow_schema)
                    yield table
                    has_yielded = True

                    # Update counters
                    fetched_count = len(records)
                    records_remaining -= fetched_count
                    current_offset += fetched_count

                    if fetched_count < fetch_count:
                        break

                # If we never yielded anything, yield an empty table
                if not has_yielded:
                    empty_arrays = [
                        pa.array([], type=field.type) for field in arrow_schema
                    ]
                    yield pa.Table.from_arrays(empty_arrays, schema=arrow_schema)

            except Exception as e:
                logger.error(f"Error reading from Kinetica: {e}")
                raise

        return read_fn

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ) -> List[ReadTask]:
        """
        Generate read tasks for parallel data loading.

        Args:
            parallelism: Desired level of parallelism.
            per_task_row_limit: Optional max rows per task.
            data_context: Optional Ray data context.

        Returns:
            List of ReadTask objects.
        """
        client = self._init_client()

        try:
            # Get table info
            self._record_type, self._total_count, self._arrow_schema = (
                self._get_table_info(client)
            )

            if self._total_count == 0:
                return []

            # Estimate row size for metadata
            avg_row_size = self._estimate_row_size(client)

            effective_parallelism = parallelism
            if self._filter_expression and not self._sort_by:
                logger.warning(
                    "Filter expression without sort_by may produce inconsistent "
                    "results with parallelism > 1. Consider specifying sort_by."
                )

            # Calculate partition sizes
            records_per_task = max(1, self._total_count // effective_parallelism)

            # Ensure we don't create too many tiny tasks
            min_records_per_task = 1000
            if (
                records_per_task < min_records_per_task
                and self._total_count > min_records_per_task
            ):
                effective_parallelism = max(
                    1, self._total_count // min_records_per_task
                )
                records_per_task = self._total_count // effective_parallelism

            read_tasks = []
            offset = 0

            for i in range(effective_parallelism):
                if i == effective_parallelism - 1:
                    partition_size = self._total_count - offset
                else:
                    partition_size = records_per_task

                if partition_size <= 0:
                    break

                metadata = BlockMetadata(
                    num_rows=partition_size,
                    size_bytes=partition_size * avg_row_size,
                    input_files=None,
                    exec_stats=None,
                )

                read_fn = self._create_read_fn(offset, partition_size)

                read_tasks.append(ReadTask(read_fn, metadata))
                offset += partition_size

            return read_tasks

        finally:
            pass

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate the in-memory size of the data.

        Returns:
            Estimated size in bytes, or None if unknown.
        """
        if self._total_count is None:
            client = self._init_client()
            try:
                _, self._total_count, _ = self._get_table_info(client)
            finally:
                pass

        if self._total_count is None:
            return None

        return self._total_count * 256

    def get_name(self) -> str:
        """Return the name of this datasource."""
        return f"Kinetica({self._table_name})"
