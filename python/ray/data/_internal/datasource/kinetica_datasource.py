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
# Valid SQL identifier pattern: starts with letter or underscore,
# followed by letters, digits, or underscores. No spaces or special chars.
_VALID_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

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


def _is_valid_identifier(name: str) -> bool:
    """
    Check if a string is a valid SQL identifier (column/table name).

    Valid identifiers start with a letter or underscore, followed by
    letters, digits, or underscores. This prevents injection attacks
    when interpolating column names into expressions.

    Args:
        name: The identifier to validate.

    Returns:
        True if the identifier is valid, False otherwise.
    """
    if not name:
        return False
    return _VALID_IDENTIFIER_PATTERN.match(name) is not None


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
        partition_column: Optional[str] = None,
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
            partition_column: Optional column name for hash-based partitioning
                in parallel reads. When specified, rows are deterministically
                assigned to read tasks using MOD(HASH(column), parallelism),
                guaranteeing each row is read by exactly one task. This enables
                safe parallel reads without requiring a unique sort key. Should
                be a column with good value distribution (e.g., primary key).
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

        # Validate limit - None means no limit, positive integer means specific limit
        if limit is not None and limit <= 0:
            raise ValueError(
                f"limit must be a positive integer or None, got {limit}. "
                "Use None for no limit."
            )
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

        # Validate and set partition column for hash-based parallel reads
        if partition_column is not None:
            if not _is_valid_identifier(partition_column):
                raise ValueError(
                    f"Invalid partition_column '{partition_column}'. "
                    "Column names must start with a letter or underscore and "
                    "contain only letters, digits, and underscores."
                )
        self._partition_column = partition_column

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
        task_id: Optional[int] = None,
        total_tasks: Optional[int] = None,
    ) -> Callable[[], Iterable["pa.Table"]]:
        """
        Create a read function for a specific data partition using GPUdbTable.

        Args:
            offset: Starting offset for this partition (offset-based reads).
            limit: Number of records to read (offset-based reads).
            task_id: Task index for hash-based partitioning (0 to total_tasks-1).
            total_tasks: Total number of parallel tasks for hash partitioning.

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
        partition_column = self._partition_column

        # Determine if using hash-based partitioning
        use_hash_partitioning = (
            partition_column is not None
            and task_id is not None
            and total_tasks is not None
        )

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

            # Build the filter expression, combining user filter with hash partition
            effective_filter = filter_expression

            if use_hash_partitioning:
                # Hash-based partitioning: each task reads rows where
                # MOD(HASH(partition_column), total_tasks) = task_id
                # This guarantees each row is read by exactly one task.
                hash_filter = (
                    f"MOD(HASH({partition_column}), {total_tasks}) = {task_id}"
                )
                if effective_filter:
                    # Combine user filter with hash filter
                    effective_filter = f"({effective_filter}) AND ({hash_filter})"
                else:
                    effective_filter = hash_filter

            if effective_filter:
                options["expression"] = effective_filter

            if sort_by:
                options["sort_by"] = sort_by
                options["sort_order"] = sort_order

            # Track how many records we've read and need to read
            # For hash partitioning, we don't know exact count per task upfront,
            # so we use a large limit and read until exhausted.
            records_remaining = limit
            current_offset = offset
            has_yielded = False

            # When sort_by is not provided and we need to paginate (limit > batch_size),
            # offset-based pagination without a stable sort order can produce
            # duplicate or missing rows. To avoid this, fetch all records in one
            # request when sort_by is not specified.
            effective_batch_size = batch_size
            if not sort_by and limit > batch_size:
                logger.info(
                    f"Fetching all {limit} records in a single request because "
                    "sort_by is not specified (pagination without sort_by can "
                    "produce inconsistent results). Specify sort_by to enable "
                    "batched fetching."
                )
                effective_batch_size = limit

            try:
                while records_remaining > 0:
                    fetch_count = min(effective_batch_size, records_remaining)

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

        # Handle parallelism=-1 (auto-detect) by computing based on data size.
        # Use min_records_per_task to determine a reasonable parallelism.
        min_records_per_task = 1000
        if parallelism == -1:
            # Auto-detect: aim for ~min_records_per_task rows per task
            effective_parallelism = max(1, self._total_count // min_records_per_task)
        else:
            # Ensure parallelism is at least 1 to handle invalid values
            effective_parallelism = max(1, parallelism)

        # Determine partitioning strategy for parallel reads
        use_hash_partitioning = self._partition_column is not None

        if effective_parallelism > 1:
            if use_hash_partitioning:
                if self._limit is not None:
                    # Hash partitioning with a global limit cannot be parallelized
                    # correctly - each task reads its hash partition independently,
                    # so we can't enforce a global row limit across tasks.
                    logger.warning(
                        "Cannot use parallel hash partitioning with a global limit. "
                        f"Reducing parallelism from {effective_parallelism} to 1. "
                        "Remove the limit parameter to enable parallel reads with "
                        "partition_column."
                    )
                    effective_parallelism = 1
                    use_hash_partitioning = False
                else:
                    # Hash-based partitioning using MOD(HASH(column), parallelism)
                    # guarantees each row goes to exactly one task - safe for parallel
                    logger.info(
                        f"Using hash-based partitioning on column "
                        f"'{self._partition_column}' for {effective_parallelism} "
                        "parallel read tasks."
                    )
            elif not self._sort_by:
                # Without partition_column or sort_by, offset-based pagination
                # will produce non-deterministic results (duplicates or missing
                # rows) because Kinetica does not guarantee stable row ordering.
                logger.warning(
                    "Parallel reads without partition_column or sort_by may "
                    "produce non-deterministic results (duplicate or missing "
                    f"rows). Reducing parallelism from {effective_parallelism} "
                    "to 1. Specify partition_column for safe parallel reads."
                )
                effective_parallelism = 1
            else:
                # sort_by specified but no partition_column - offset-based
                # pagination requires unique sort key for consistency
                logger.warning(
                    f"Parallel reads with sort_by='{self._sort_by}' require "
                    "the sort column to have unique values for consistent "
                    "results. Consider using partition_column instead for "
                    "guaranteed correctness."
                )

        # Calculate partition sizes
        records_per_task = max(1, self._total_count // effective_parallelism)

        # Ensure we don't create too many tiny tasks
        if (
            records_per_task < min_records_per_task
            and self._total_count > min_records_per_task
        ):
            effective_parallelism = max(1, self._total_count // min_records_per_task)
            records_per_task = self._total_count // effective_parallelism

        # Cap effective_parallelism to total_count to avoid empty tasks
        # This handles the case where parallelism > total_count
        effective_parallelism = min(effective_parallelism, self._total_count)

        # Recompute records_per_task after capping parallelism to ensure
        # even distribution of work across tasks
        records_per_task = max(1, self._total_count // effective_parallelism)

        read_tasks = []

        if use_hash_partitioning:
            # Hash-based partitioning: each task gets rows where
            # MOD(HASH(partition_column), parallelism) = task_id
            # Row counts are estimated (hash distribution is approximately even)
            estimated_rows_per_task = max(1, self._total_count // effective_parallelism)

            for task_id in range(effective_parallelism):
                metadata = BlockMetadata(
                    num_rows=estimated_rows_per_task,
                    size_bytes=estimated_rows_per_task * avg_row_size,
                    input_files=None,
                    exec_stats=None,
                )

                read_fn = self._create_read_fn(
                    offset=0,
                    limit=self._total_count,
                    task_id=task_id,
                    total_tasks=effective_parallelism,
                )

                read_tasks.append(
                    ReadTask(
                        read_fn,
                        metadata,
                        schema=self._arrow_schema,
                        per_task_row_limit=per_task_row_limit,
                    )
                )
        else:
            # Offset-based partitioning (requires unique sort_by for consistency)
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
