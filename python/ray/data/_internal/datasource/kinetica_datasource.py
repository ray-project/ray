"""
Kinetica Datasource for Ray Data.

This module provides a Ray Data Datasource implementation for reading data
from Kinetica databases using the GPUdbTable class for idiomatic API usage.
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


# Patterns that are potentially dangerous in filter expressions
# Note: This is a defense-in-depth measure. Kinetica's expression parser
# provides the primary protection, but we block obvious injection attempts.
_UNSAFE_FILTER_PATTERNS = [
    re.compile(r"[;{}]"),  # Statement terminators and code blocks
    re.compile(r"--"),  # SQL comment sequences
    re.compile(r"/\*"),  # Block comment start
    re.compile(r"\*/"),  # Block comment end
    re.compile(r"\bUNION\b", re.IGNORECASE),  # UNION-based injection
    re.compile(r"\bINSERT\b", re.IGNORECASE),  # INSERT statements
    re.compile(r"\bUPDATE\b", re.IGNORECASE),  # UPDATE statements
    re.compile(r"\bDELETE\b", re.IGNORECASE),  # DELETE statements
    re.compile(r"\bDROP\b", re.IGNORECASE),  # DROP statements
    re.compile(r"\bCREATE\b", re.IGNORECASE),  # CREATE statements
    re.compile(r"\bALTER\b", re.IGNORECASE),  # ALTER statements
    re.compile(r"\bTRUNCATE\b", re.IGNORECASE),  # TRUNCATE statements
    re.compile(r"\bEXEC\b", re.IGNORECASE),  # EXEC/EXECUTE
    re.compile(r"\bEXECUTE\b", re.IGNORECASE),
    re.compile(r"\bINTO\s+OUTFILE\b", re.IGNORECASE),  # File operations
    re.compile(r"\bLOAD_FILE\b", re.IGNORECASE),
]


def _has_balanced_quotes(expr: str) -> bool:
    """
    Check if string literals in the expression are properly balanced.

    This detects unclosed string literals which could be used to bypass
    the string stripping logic in filter safety checks.

    Args:
        expr: The expression to check.

    Returns:
        True if quotes are balanced, False if there are unclosed strings.
    """
    in_single = False
    in_double = False
    i = 0
    while i < len(expr):
        char = expr[i]
        if not in_double and char == "'":
            # Check for escaped quote '' only when inside a single-quoted string
            if in_single and i + 1 < len(expr) and expr[i + 1] == "'":
                i += 2  # Skip escaped quote
                continue
            in_single = not in_single
        elif not in_single and char == '"':
            # Check for escaped quote "" only when inside a double-quoted string
            if in_double and i + 1 < len(expr) and expr[i + 1] == '"':
                i += 2  # Skip escaped quote
                continue
            in_double = not in_double
        i += 1
    return not in_single and not in_double


def _strip_string_literals(expr: str) -> str:
    """
    Remove content inside string literals to avoid false positives.

    This handles single-quoted strings (SQL standard) and double-quoted strings.
    Escaped quotes within strings are handled ('' for single, "" for double).

    Args:
        expr: The expression to process.

    Returns:
        The expression with string literal contents replaced by empty strings.
    """
    # Replace single-quoted strings (handling escaped quotes '')
    expr = re.sub(r"'(?:[^']|'')*'", "''", expr)
    # Replace double-quoted strings (handling escaped quotes "")
    expr = re.sub(r'"(?:[^"]|"")*"', '""', expr)
    return expr


def _is_filter_safe(filter_expr: str) -> bool:
    """
    Check if a filter expression is safe from SQL injection.

    This function provides defense-in-depth validation against common SQL
    injection patterns. It blocks:
    - Unbalanced quotes (unclosed string literals)
    - Statement terminators (;) and code blocks ({})
    - SQL comments (-- and /* */)
    - DDL/DML keywords (UNION, INSERT, UPDATE, DELETE, DROP, etc.)
    - File operation functions

    String literals are stripped before checking, so legitimate values like
    "city = 'Union City'" won't trigger false positives for the UNION keyword.

    Note: Kinetica's expression parser provides the primary security layer.
    This validation is an additional safeguard against obvious attacks.

    Args:
        filter_expr: The filter expression to check.

    Returns:
        True if the filter appears safe, False otherwise.
    """
    if not filter_expr:
        return True

    # First check for unbalanced quotes - unclosed strings could bypass stripping
    if not _has_balanced_quotes(filter_expr):
        return False

    # Strip string literals to avoid false positives on keywords in data values
    # e.g., city = 'Union City' should not trigger on UNION
    expr_without_strings = _strip_string_literals(filter_expr)

    for pattern in _UNSAFE_FILTER_PATTERNS:
        if pattern.search(expr_without_strings):
            return False

    return True


class KineticaDatasource(Datasource):
    """
    A Ray Data Datasource for reading from Kinetica databases.

    This datasource uses GPUdbTable for reading data, providing automatic
    type handling, schema management, and optional multihead I/O support.
    It supports parallel reads by partitioning data across multiple read
    tasks using offset-based pagination.

    Example:
        >>> import ray  # doctest: +SKIP
        >>> ds = ray.data.read_datasource(  # doctest: +SKIP
        ...     KineticaDatasource(
        ...         url="http://localhost:9191",
        ...         table_name="my_table",
        ...         username="admin",
        ...         password="password",
        ...     )
        ... )
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
        use_multihead_io: bool = False,
        convert_special_types: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the Kinetica datasource.

        Args:
            url: URL of the Kinetica server (e.g., "http://localhost:9191").
            table_name: Name of the table to read from.
            username: Username for authentication.
            password: Password for authentication.
            columns: Optional list of column names to read. If None, reads all
                columns.
            filter_expression: Optional SQL WHERE clause filter (without the
                WHERE keyword).
            sort_by: Optional column name to sort results by.
            sort_order: Sort order, either "ascending" or "descending".
            limit: Optional maximum number of records to read.
            batch_size: Number of records to fetch per request for pagination.
                        Defaults to 10,000.
            use_multihead_io: If True, enables multihead I/O for parallel reads
                from multiple Kinetica nodes. Can improve performance for large
                datasets on clustered deployments.
            convert_special_types: If True, converts special types (arrays, JSON)
                on retrieval. Defaults to True.
            options: Additional GPUdb client options.
        """
        super().__init__()
        _check_import(self, module="gpudb", package="gpudb")

        self._url = url
        self._table_name = table_name
        self._username = username
        self._password = password
        self._filter_expression = filter_expression
        self._sort_by = sort_by
        self._limit = limit
        if batch_size is not None:
            if batch_size <= 0:
                raise ValueError(
                    f"batch_size must be a positive integer, got {batch_size}"
                )
            self._batch_size = batch_size
        else:
            self._batch_size = self.DEFAULT_BATCH_SIZE
        self._use_multihead_io = use_multihead_io
        self._convert_special_types = convert_special_types
        self._options = options or {}

        # Validate columns - empty list is likely a mistake
        # None means "all columns", non-empty list means "specific columns"
        if columns is not None and len(columns) == 0:
            raise ValueError(
                "columns cannot be an empty list. "
                "Use None to select all columns, or provide a non-empty list "
                "of column names to select specific columns."
            )
        self._columns = columns

        # Validate sort_order
        valid_sort_orders = ("ascending", "descending")
        if sort_order not in valid_sort_orders:
            raise ValueError(
                f"Invalid sort_order '{sort_order}'. "
                f"Must be one of: {', '.join(valid_sort_orders)}"
            )
        self._sort_order = sort_order

        # Validate filter expression
        if filter_expression and not _is_filter_safe(filter_expression):
            raise ValueError(
                "Filter expression contains potentially unsafe patterns and was "
                "rejected. Please review your filter for SQL injection patterns "
                "such as statement terminators, comments, or DDL/DML keywords."
            )

        # Cache for schema and record count
        self._arrow_schema: Optional["pa.Schema"] = None
        self._total_count: Optional[int] = None

    def _init_client(self):
        """Create and return a GPUdb client instance."""
        from ray.data._internal.datasource.kinetica_type_utils import (
            create_gpudb_client,
        )

        return create_gpudb_client(
            url=self._url,
            username=self._username,
            password=self._password,
            options=self._options,
        )

    def _create_gpudb_table(self, client: Any):
        """
        Create a GPUdbTable instance for reading records.

        Args:
            client: GPUdb client instance.

        Returns:
            GPUdbTable instance configured for reading.
        """
        from gpudb import GPUdbTable

        # Create GPUdbTable for an existing table (pass None for _type)
        gpudb_table = GPUdbTable(
            _type=None,
            name=self._table_name,
            db=client,
            use_multihead_io=self._use_multihead_io,
            convert_special_types_on_retrieval=self._convert_special_types,
        )

        return gpudb_table

    def _get_table_info(self, client: Any) -> tuple:
        """
        Get table schema and record count using GPUdbTable.

        Args:
            client: GPUdb client instance.

        Returns:
            Tuple of (total_record_count, arrow_schema)
        """
        from gpudb import GPUdbException

        from ray.data._internal.datasource.kinetica_type_utils import (
            kinetica_type_to_arrow_schema,
        )

        # Create GPUdbTable to get schema information
        gpudb_table = self._create_gpudb_table(client)

        # Get record type from GPUdbTable
        record_type = gpudb_table.gpudbrecord_type

        if record_type is None:
            raise GPUdbException(
                f"Could not retrieve schema for table '{self._table_name}'"
            )

        # If columns are specified, filter the schema
        if self._columns:
            from gpudb import GPUdbRecordType

            # Get actual column names from the table
            actual_column_names = {col.name for col in record_type.columns}
            requested_columns = set(self._columns)

            # Find columns that don't exist in the table
            invalid_columns = requested_columns - actual_column_names
            if invalid_columns:
                raise ValueError(
                    f"The following columns were requested but do not exist in "
                    f"table '{self._table_name}': {sorted(invalid_columns)}. "
                    f"Available columns are: {sorted(actual_column_names)}"
                )

            # Build column map for lookup
            column_map = {col.name: col for col in record_type.columns}

            # Preserve user-specified column order
            filtered_columns = [column_map[name] for name in self._columns]

            # Create a filtered record type for Arrow schema conversion
            record_type = GPUdbRecordType(
                columns=filtered_columns, label=self._table_name
            )

        # Get total count (accounting for filter)
        if self._filter_expression:
            # Use get_records with limit=0 to get filtered count
            count_response = client.get_records(
                table_name=self._table_name,
                offset=0,
                limit=0,
                options={"expression": self._filter_expression},
            )
            total_count = count_response.get("total_number_of_records", 0)
        else:
            # Get count from show_table for unfiltered reads
            response = client.show_table(
                table_name=self._table_name,
                options={"get_sizes": "true"},
            )
            total_count = response.get("total_size", 0)
            if isinstance(total_count, list):
                total_count = total_count[0] if total_count else 0

        # Apply limit if specified
        if self._limit is not None:
            total_count = min(total_count, self._limit)

        # Convert to Arrow schema
        arrow_schema = kinetica_type_to_arrow_schema(record_type)

        return total_count, arrow_schema

    def _estimate_row_size(self, client: Any, sample_size: int = 100) -> int:
        """
        Estimate the average row size by sampling using GPUdbTable.

        Args:
            client: GPUdb client instance.
            sample_size: Number of rows to sample.

        Returns:
            Estimated average row size in bytes.
        """
        try:
            gpudb_table = self._create_gpudb_table(client)

            # Build options for the query
            options = {}
            if self._filter_expression:
                options["expression"] = self._filter_expression
            if self._sort_by:
                options["sort_by"] = self._sort_by
                options["sort_order"] = self._sort_order

            # Use get_records_by_column if specific columns requested
            if self._columns:
                records = gpudb_table.get_records_by_column(
                    column_names=self._columns,
                    offset=0,
                    limit=sample_size,
                    options=options,
                    get_column_major=False,  # Get row-major for size estimation
                    force_primitive_return_types=True,
                )
            else:
                records = gpudb_table.get_records(
                    offset=0,
                    limit=sample_size,
                    options=options,
                    force_primitive_return_types=True,
                )

            if not records:
                return 256

            # Estimate size based on string representation
            total_size = sum(len(str(dict(r))) for r in records)
            return total_size // len(records)

        except Exception:
            return 256

    def _create_read_fn(
        self,
        offset: int,
        limit: int,
    ) -> Callable[[], Iterable["pa.Table"]]:
        """
        Create a read function for a specific data partition using GPUdbTable.

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
        use_multihead_io = self._use_multihead_io
        convert_special_types = self._convert_special_types

        def read_fn() -> Iterable["pa.Table"]:
            import pyarrow as pa
            from gpudb import GPUdbTable

            from ray.data._internal.datasource.kinetica_type_utils import (
                create_gpudb_client,
            )

            # Create client in the worker
            client = create_gpudb_client(
                url=url,
                username=username,
                password=password,
                options=client_options,
            )

            # Create GPUdbTable for reading
            gpudb_table = GPUdbTable(
                _type=None,
                name=table_name,
                db=client,
                use_multihead_io=use_multihead_io,
                convert_special_types_on_retrieval=convert_special_types,
            )

            # Build request options
            options = {}
            if filter_expression:
                options["expression"] = filter_expression
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

                    # Use appropriate method based on column selection
                    if columns:
                        # get_records_by_column for specific columns
                        records = gpudb_table.get_records_by_column(
                            column_names=columns,
                            offset=current_offset,
                            limit=fetch_count,
                            options=options,
                            get_column_major=False,  # Row-major format
                            force_primitive_return_types=True,
                        )
                    else:
                        # get_records for all columns
                        records = gpudb_table.get_records(
                            offset=current_offset,
                            limit=fetch_count,
                            options=options,
                            force_primitive_return_types=True,
                        )

                    if not records:
                        break

                    # Convert records to Arrow table
                    # Records are OrderedDict or Record objects
                    from ray.data._internal.datasource.kinetica_type_utils import (
                        convert_records_to_arrow_table,
                    )

                    record_dicts = [dict(r) for r in records]
                    table = convert_records_to_arrow_table(record_dicts, arrow_schema)
                    yield table
                    has_yielded = True

                    # Update counters
                    fetched_count = len(records)
                    records_remaining -= fetched_count
                    current_offset += fetched_count

                    # If we got fewer records than requested, we're done
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

        # Get table info using GPUdbTable
        self._total_count, self._arrow_schema = self._get_table_info(client)

        if self._total_count == 0:
            return []

        # Estimate row size for metadata
        avg_row_size = self._estimate_row_size(client)

        # Ensure parallelism is at least 1 to handle unresolved or invalid values
        effective_parallelism = max(1, parallelism)
        if not self._sort_by and effective_parallelism > 1:
            # Without a deterministic sort order, offset-based pagination
            # with parallelism > 1 will produce incorrect results (duplicates
            # or missing rows) because Kinetica does not guarantee a stable
            # row order across paginated get_records calls. Force single-task
            # execution unless sort_by is specified.
            logger.warning(
                "Parallel reads without sort_by may produce non-deterministic "
                "results (duplicate or missing rows). Reducing parallelism "
                f"from {effective_parallelism} to 1. Specify sort_by to enable "
                "parallel reads with consistent ordering."
            )
            effective_parallelism = 1

        # Calculate partition sizes
        records_per_task = max(1, self._total_count // effective_parallelism)

        # Ensure we don't create too many tiny tasks
        min_records_per_task = 1000
        if (
            records_per_task < min_records_per_task
            and self._total_count > min_records_per_task
        ):
            effective_parallelism = max(1, self._total_count // min_records_per_task)
            records_per_task = self._total_count // effective_parallelism

        # Cap effective_parallelism to total_count to avoid empty tasks
        # This handles the case where parallelism > total_count
        effective_parallelism = min(effective_parallelism, self._total_count)

        read_tasks = []
        offset = 0

        for i in range(effective_parallelism):
            if i == effective_parallelism - 1:
                partition_size = self._total_count - offset
            else:
                partition_size = records_per_task

            # Skip if we've already assigned all records
            if partition_size <= 0 or offset >= self._total_count:
                break

            metadata = BlockMetadata(
                num_rows=partition_size,
                size_bytes=partition_size * avg_row_size,
                input_files=None,
                exec_stats=None,
            )

            read_fn = self._create_read_fn(offset, partition_size)

            read_tasks.append(
                ReadTask(
                    read_fn,
                    metadata,
                    schema=self._arrow_schema,
                    per_task_row_limit=per_task_row_limit,
                )
            )
            offset += partition_size

        return read_tasks

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """
        Estimate the in-memory size of the data.

        Returns:
            Estimated size in bytes, or None if unknown.
        """
        if self._total_count is None:
            client = self._init_client()
            self._total_count, _ = self._get_table_info(client)

        if self._total_count is None:
            return None

        return self._total_count * 256

    def get_name(self) -> str:
        """Return the name of this datasource."""
        return f"Kinetica({self._table_name})"
