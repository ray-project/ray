"""
Delta Lake datasink implementation with two-phase commit for ACID compliance.

This module implements write support for Delta Lake tables using the
deltalake Python package. The implementation follows a two-phase commit
protocol to ensure ACID guarantees for distributed writes.

Delta Lake documentation: https://delta.io/
Python deltalake package: https://delta-io.github.io/delta-rs/python/
"""

import json
import logging
import os
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray.data._internal.datasource.delta.config import DeltaWriteConfig, WriteMode
from ray.data._internal.datasource.delta.utilities import (
    DeltaUtilities,
    try_get_deltatable,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


class DeltaDatasink(Datasink[List["AddAction"]]):
    """
    Ray Data datasink for Delta Lake tables using two-phase commit.

    Ensures ACID compliance at dataset level:
    1. write(): Per-task Parquet writes, returns AddAction metadata
    2. on_write_complete(): Collects all AddActions, single transaction commit

    Supports: append, overwrite, error, ignore modes
    Storage: Local, S3, GCS, Azure, HDFS
    """

    def __init__(
        self,
        path: str,
        *,
        mode: str = WriteMode.APPEND.value,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        **write_kwargs,
    ):
        _check_import(self, module="deltalake", package="deltalake")

        self.path = path
        self.mode = self._validate_mode(mode)
        self.partition_cols = partition_cols or []
        self.schema = schema
        self.write_kwargs = write_kwargs
        self._skip_write = False

        # Initialize Delta utilities
        self.delta_utils = DeltaUtilities(
            path, storage_options=write_kwargs.get("storage_options")
        )
        self.storage_options = self.delta_utils.storage_options

        # Delta write configuration
        self.delta_write_config = DeltaWriteConfig(
            mode=self.mode,
            partition_cols=partition_cols,
            schema=schema,
            **write_kwargs,
        )

        # Set up filesystem
        self.filesystem = filesystem or pa_fs.FileSystem.from_uri(path)[0]

    def _validate_mode(self, mode: str) -> WriteMode:
        """Validate and return WriteMode."""
        if mode not in ["append", "overwrite", "error", "ignore"]:
            if mode == "merge":
                raise ValueError(
                    "Merge mode not supported in v1. Use 'append' or 'overwrite' modes."
                )
            raise ValueError(
                f"Invalid mode '{mode}'. "
                f"Supported: 'append', 'overwrite', 'error', 'ignore'"
            )
        return WriteMode(mode)

    def on_write_start(self) -> None:
        """Check ERROR and IGNORE modes before writing files to prevent wasted I/O."""
        _check_import(self, module="deltalake", package="deltalake")

        existing_table = try_get_deltatable(self.path, self.storage_options)

        # For ERROR mode, fail immediately if table exists
        if self.mode == WriteMode.ERROR and existing_table:
            raise ValueError(
                f"Delta table already exists at {self.path}. "
                f"Use mode='append' or 'overwrite'."
            )

        # For IGNORE mode, skip write if table exists to prevent wasted computation
        # Note: This is handled by setting _skip_write flag that write() will check
        if self.mode == WriteMode.IGNORE and existing_table:
            logger.info(
                f"Delta table already exists at {self.path}. "
                f"Skipping write due to mode='ignore'."
            )
            self._skip_write = True
        else:
            self._skip_write = False

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["AddAction"]:
        """
        Phase 1: Write Parquet files, return AddAction metadata (no commit).

        This is the first phase of the two-phase commit protocol. Each worker task:
        1. Converts Ray Data blocks to PyArrow tables
        2. Combines tables and validates partition columns
        3. Partitions data if needed (Hive-style partitioning)
        4. Writes Parquet files to storage
        5. Returns AddAction metadata (files not yet visible in Delta table)

        The actual commit happens in on_write_complete() after all tasks finish.

        Args:
            blocks: Ray Data blocks to write (may contain multiple blocks per task)
            ctx: Task context providing task_idx for unique file naming

        Returns:
            List of AddAction objects describing written files with:
            - path: Relative path to Parquet file
            - size: File size in bytes
            - partition_values: Dict of partition column values
            - stats: JSON statistics (min/max/null counts)
            - modification_time: Write timestamp in milliseconds

        Raises:
            ValueError: If partition columns are not in table schema

        Example:
            >>> # Internal method called by Ray Data during write
            >>> # Returns AddActions like:
            >>> # [AddAction(
            >>> #     path="year=2024/month=10/part-00001-abc123.parquet",
            >>> #     size=1024000,
            >>> #     partition_values={"year": "2024", "month": "10"},
            >>> #     stats='{"numRecords": 1000, ...}',
            >>> #     modification_time=1696896000000
            >>> # )]
        """
        # Skip write if table exists in IGNORE mode (checked in on_write_start)
        if self._skip_write:
            return []

        _check_import(self, module="deltalake", package="deltalake")

        # Convert Ray Data blocks to PyArrow tables
        # Filter out empty blocks to avoid unnecessary processing
        combined_table = self._convert_blocks_to_table(blocks)
        if combined_table is None:
            return []

        # Validate partition columns exist in table schema
        self._validate_partition_columns(combined_table)

        # Write partitioned or non-partitioned data
        return self._write_table_data(combined_table, ctx.task_idx)

    def _convert_blocks_to_table(self, blocks: Iterable[Block]) -> Optional[pa.Table]:
        """
        Convert Ray Data blocks to a single PyArrow table.

        Args:
            blocks: Iterable of Ray Data blocks

        Returns:
            Combined PyArrow table, or None if all blocks are empty
        """
        tables = []
        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            # Skip empty blocks to avoid unnecessary concatenation overhead
            if block_accessor.num_rows() > 0:
                tables.append(block_accessor.to_arrow())

        if not tables:
            return None

        # Concatenate all tables into single table for this task
        # PyArrow handles schema alignment automatically
        return pa.concat_tables(tables)

    def _validate_partition_columns(self, table: pa.Table) -> None:
        """
        Validate that all partition columns exist in the table schema.

        Args:
            table: PyArrow table to validate

        Raises:
            ValueError: If any partition columns are missing from table schema
        """
        if not self.partition_cols:
            return

        missing_cols = [
            col for col in self.partition_cols if col not in table.column_names
        ]
        if missing_cols:
            raise ValueError(
                f"Partition columns {missing_cols} not found in table schema. "
                f"Available columns: {table.column_names}. "
                f"Check that partition_cols matches your data schema."
            )

    def _write_table_data(self, table: pa.Table, task_idx: int) -> List["AddAction"]:
        """
        Write table data as partitioned or non-partitioned Parquet files.

        Args:
            table: PyArrow table to write
            task_idx: Task index for unique file naming

        Returns:
            List of AddAction objects for written files
        """
        file_actions = []

        if self.partition_cols:
            # Partition table using Hive-style partitioning (column=value)
            # Each partition becomes a separate Parquet file
            partitioned_tables = self._partition_table(table, self.partition_cols)
            for partition_values, partition_table in partitioned_tables.items():
                action = self._write_partition(
                    partition_table, partition_values, task_idx
                )
                file_actions.append(action)
        else:
            # Write entire table as single Parquet file (no partitioning)
            action = self._write_partition(table, (), task_idx)
            file_actions.append(action)

        return file_actions

    def _partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[tuple, pa.Table]:
        """
        Partition table by columns efficiently using vectorized operations.

        Uses different strategies based on number of partition columns:
        - Single column: PyArrow compute unique() + filter() for optimal performance
        - Multiple columns: Vectorized to_pylist() + take() to minimize data copies

        This produces Hive-style partitioning where each unique combination of
        partition column values gets its own Parquet file.

        Args:
            table: PyArrow table to partition
            partition_cols: List of column names to partition by

        Returns:
            Dictionary mapping partition values tuple to corresponding table slice
            Example: {("2024", "10"): <PyArrow Table>, ("2024", "11"): <Table>}

        Example:
            >>> # For table with columns: [year, month, value]
            >>> # partition_cols = ["year", "month"]
            >>> # Returns:
            >>> # {
            >>> #   ("2024", "10"): Table with rows where year=2024, month=10
            >>> #   ("2024", "11"): Table with rows where year=2024, month=11
            >>> # }
        """
        from collections import defaultdict

        import pyarrow.compute as pc

        partitions = {}

        if len(partition_cols) == 1:
            # Optimized single-column path using PyArrow compute functions
            # This avoids converting to Python and is faster for large tables
            col_name = partition_cols[0]
            unique_values = pc.unique(table[col_name])

            for partition_value in unique_values:
                # Convert to Python for dictionary key
                partition_value_py = partition_value.as_py()

                # Create boolean mask for rows matching this partition value
                row_mask = pc.equal(table[col_name], partition_value)

                # Filter table to get rows for this partition
                # Store with tuple key for consistency with multi-column case
                partitions[(partition_value_py,)] = table.filter(row_mask)
        else:
            # Multi-column partitioning using vectorized operations
            # Convert all partition columns to Python lists in one pass
            # This is more efficient than processing row-by-row
            partition_values_lists = [table[col].to_pylist() for col in partition_cols]

            # Group row indices by partition key
            # zip(*lists) creates tuples of (col1_val, col2_val, ...) for each row
            partition_indices = defaultdict(list)
            for row_idx, partition_tuple in enumerate(zip(*partition_values_lists)):
                partition_indices[partition_tuple].append(row_idx)

            # Create table slices for each partition using take()
            # take() is efficient as it creates views without copying data when possible
            for partition_tuple, row_indices in partition_indices.items():
                partitions[partition_tuple] = table.take(row_indices)

        return partitions

    def _write_partition(
        self,
        table: pa.Table,
        partition_values: tuple,
        task_idx: int,
    ) -> "AddAction":
        """
        Write a single partition to Parquet file and create AddAction metadata.

        This method handles:
        1. Generating unique filename with task index and UUID
        2. Building Hive-style partition path (e.g., year=2024/month=10/)
        3. Writing Parquet file to storage
        4. Computing file statistics for query optimization
        5. Creating AddAction metadata for Delta transaction log

        Args:
            table: PyArrow table containing partition data
            partition_values: Tuple of partition column values
                (empty for non-partitioned tables)
            task_idx: Task index for generating unique filenames across workers

        Returns:
            AddAction object containing file metadata for Delta transaction log

        Example:
            >>> # For partitioned write with partition_cols=["year", "month"]
            >>> # partition_values = ("2024", "10")
            >>> # Returns AddAction(
            >>> #     path="year=2024/month=10/part-00001-abc123.parquet",
            >>> #     size=1024000,
            >>> #     partition_values={"year": "2024", "month": "10"},
            >>> #     ...
            >>> # )
        """
        from deltalake.transaction import AddAction

        # Generate unique filename using task index and UUID
        # Format: part-00001-abc123.parquet
        # Task index ensures no conflicts across workers, UUID ensures uniqueness
        filename = self._generate_filename(task_idx)

        # Build Hive-style partition path and dict for Delta metadata
        partition_path, partition_dict = self._build_partition_path(partition_values)

        # Write Parquet file and get file size
        relative_path = partition_path + filename
        full_path = os.path.join(self.path, relative_path)
        file_size = self._write_parquet_file(table, full_path)

        # Compute statistics for query optimization (min/max/null counts)
        file_statistics = self._compute_statistics(table)

        # Create AddAction for Delta transaction log
        return AddAction(
            path=relative_path,
            size=file_size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),  # Milliseconds since epoch
            data_change=True,  # This write changes data (not just metadata)
            stats=file_statistics,
        )

    def _generate_filename(self, task_idx: int) -> str:
        """
        Generate unique Parquet filename.

        Uses task index (5 digits, zero-padded) and UUID to ensure uniqueness
        across distributed workers and prevent filename collisions.

        Args:
            task_idx: Task index from Ray execution context

        Returns:
            Filename string like "part-00001-abc123def456.parquet"
        """
        return f"part-{task_idx:05d}-{uuid.uuid4().hex}.parquet"

    def _build_partition_path(
        self, partition_values: tuple
    ) -> tuple[str, Dict[str, Optional[str]]]:
        """
        Build Hive-style partition path and dictionary for Delta metadata.

        Args:
            partition_values: Tuple of partition values matching partition_cols order

        Returns:
            Tuple of (partition_path, partition_dict) where:
            - partition_path: String like "year=2024/month=10/" or "" if not partitioned
            - partition_dict: Dict like {"year": "2024", "month": "10"}

        Example:
            Given partition_cols=["year", "month"] and partition_values=("2024", "10"),
            this returns ("year=2024/month=10/", {"year": "2024", "month": "10"}).
            For non-partitioned tables, it returns ("", {}).
        """
        if not self.partition_cols or not partition_values:
            return "", {}

        partition_path_components = []
        partition_dict = {}

        for col_name, col_value in zip(self.partition_cols, partition_values):
            # Handle NULL partition values (represented as empty string in path)
            value_str = "" if col_value is None else str(col_value)

            # Build Hive-style path component: column_name=value
            partition_path_components.append(f"{col_name}={value_str}")

            # Store in dict for Delta metadata (NULL as None, others as strings)
            partition_dict[col_name] = None if col_value is None else str(col_value)

        # Join with / and add trailing slash
        partition_path = "/".join(partition_path_components) + "/"

        return partition_path, partition_dict

    def _write_parquet_file(self, table: pa.Table, file_path: str) -> int:
        """
        Write PyArrow table to Parquet file and return file size.

        This method:
        1. Drops partition columns (they're in the path, not the file)
        2. Creates parent directories if needed
        3. Writes Parquet file with compression
        4. Returns file size for AddAction metadata

        Args:
            table: PyArrow table to write
            file_path: Full path where file should be written

        Returns:
            File size in bytes

        Note:
            Partition columns are removed from the file because they're encoded
            in the directory structure (Hive-style partitioning). This avoids
            data duplication and follows Delta Lake conventions.
        """
        # Prepare table for writing by removing partition columns
        # These columns are encoded in the directory path, not stored in the file
        table_to_write = self._prepare_table_for_write(table)

        # Ensure parent directories exist
        self._ensure_parent_directory(file_path)

        # Write Parquet file with compression and statistics
        pq.write_table(
            table_to_write,
            file_path,
            filesystem=self.filesystem,
            compression=self.write_kwargs.get("compression", "snappy"),
            write_statistics=True,  # Enable Parquet statistics for query optimization
        )

        # Get file size for AddAction metadata
        return self.filesystem.get_file_info(file_path).size

    def _prepare_table_for_write(self, table: pa.Table) -> pa.Table:
        """
        Prepare table for writing by removing partition columns.

        Partition columns are not stored in Parquet files because they're
        encoded in the directory structure (e.g., year=2024/month=10/).

        Args:
            table: Original PyArrow table

        Returns:
            Table with partition columns removed
        """
        if not self.partition_cols:
            return table

        # Drop all partition columns in one call
        return table.drop(self.partition_cols)

    def _ensure_parent_directory(self, file_path: str) -> None:
        """
        Create parent directory for file if it doesn't exist.

        Args:
            file_path: Full file path

        Note:
            Silently succeeds if directory already exists (expected for
            concurrent writes to same partition).
        """
        parent_dir = os.path.dirname(file_path)
        if not parent_dir:
            return

        try:
            self.filesystem.create_dir(parent_dir, recursive=True)
        except Exception:
            # Ignore errors - directory may already exist from concurrent write
            # The actual write will fail with a clearer error if there's a real issue
            pass

    def _compute_statistics(self, table: pa.Table) -> str:
        """
        Compute file-level statistics for Delta Lake transaction log.

        These statistics enable query optimization features like:
        - Data skipping: Skip files based on min/max values
        - Partition pruning: Filter partitions efficiently
        - Query planning: Estimate row counts and data distribution

        The statistics follow Delta Lake's JSON format and include:
        - numRecords: Total row count
        - minValues: Minimum value per column (numeric and string types)
        - maxValues: Maximum value per column (numeric and string types)
        - nullCount: NULL count per column

        Args:
            table: PyArrow table to compute statistics for

        Returns:
            JSON string with statistics in Delta Lake format

        Example:
            >>> # Returns JSON like:
            >>> # {
            >>> #   "numRecords": 1000,
            >>> #   "minValues": {"age": 18, "name": "Alice"},
            >>> #   "maxValues": {"age": 65, "name": "Zoe"},
            >>> #   "nullCount": {"age": 0, "name": 5}
            >>> # }
        """

        # Initialize statistics dictionary
        statistics = {"numRecords": len(table)}
        min_values = {}
        max_values = {}
        null_counts = {}

        # Compute per-column statistics
        for col_name in table.column_names:
            column = table[col_name]

            # Always compute null count (used for data skipping)
            null_counts[col_name] = column.null_count

            # Compute min/max only if column has non-null values
            # Skip all-null columns to avoid compute errors
            if column.null_count < len(column):
                min_val, max_val = self._compute_column_min_max(column)
                if min_val is not None:
                    min_values[col_name] = min_val
                if max_val is not None:
                    max_values[col_name] = max_val

        # Add non-empty statistics to result
        if min_values:
            statistics["minValues"] = min_values
        if max_values:
            statistics["maxValues"] = max_values
        if null_counts:
            statistics["nullCount"] = null_counts

        return json.dumps(statistics)

    def _compute_column_min_max(
        self, column: pa.Array
    ) -> tuple[Optional[Any], Optional[Any]]:
        """
        Compute min and max values for a single column.

        Supports numeric types (integer, floating) and string types.
        Silently returns (None, None) for unsupported types or on compute errors.

        Args:
            column: PyArrow array (column from table)

        Returns:
            Tuple of (min_value, max_value), or (None, None) if not computable
        """
        import pyarrow.compute as pc

        try:
            col_type = column.type

            # Numeric types: integer and floating point
            if pa.types.is_integer(col_type) or pa.types.is_floating(col_type):
                min_val = pc.min(column).as_py()
                max_val = pc.max(column).as_py()
                return min_val, max_val

            # String types: string and large_string
            elif pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
                min_val = pc.min(column).as_py()
                max_val = pc.max(column).as_py()
                # Ensure string representation for JSON serialization
                # Use explicit 'is not None' check to preserve falsy values like ""
                return (
                    str(min_val) if min_val is not None else None,
                    str(max_val) if max_val is not None else None,
                )

            # Unsupported type for min/max
            return None, None

        except Exception:
            # Silently handle compute errors (e.g., unsupported operations)
            # The write should succeed even if statistics computation fails
            return None, None

    def on_write_complete(self, write_result: WriteResult[List["AddAction"]]) -> None:
        """
        Phase 2: Commit all files in single ACID transaction.

        This is the second phase of two-phase commit. After all tasks have written
        their Parquet files (Phase 1), this method:
        1. Collects all AddAction metadata from all tasks
        2. Checks if table exists
        3. Creates new table OR appends to existing table
        4. Commits transaction (makes files visible atomically)

        The transaction commit is atomic - either all files become visible or none do.
        This ensures ACID properties even with distributed writes.

        Args:
            write_result: Aggregated results from all Ray tasks containing AddActions

        Returns:
            None

        Raises:
            ValueError: If ERROR mode and table was created during write
                (race condition)

        Note:
            For IGNORE mode, silently skips commit if table was created during write.
            This handles concurrent write scenarios gracefully.

        Example workflow:
            1. write() on Task 1: Writes files, returns [AddAction1, AddAction2]
            2. write() on Task 2: Writes files, returns [AddAction3, AddAction4]
            3. on_write_complete(): Collects all AddActions, single commit
            4. Result: All files visible atomically in Delta table
        """

        # Aggregate AddActions from all distributed tasks
        all_file_actions = self._collect_file_actions(write_result)
        if not all_file_actions:
            # No data to commit (all blocks were empty)
            logger.info(f"No files to commit for Delta table at {self.path}")
            return

        # Check if table exists (may have been created concurrently)
        existing_table = try_get_deltatable(self.path, self.storage_options)

        if existing_table:
            # Table exists: Append or overwrite using transaction
            self._commit_to_existing_table(existing_table, all_file_actions)
        else:
            # Table doesn't exist: Create new table with files
            self._create_table_with_files(all_file_actions)

    def _collect_file_actions(
        self, write_result: WriteResult[List["AddAction"]]
    ) -> List["AddAction"]:
        """
        Collect all AddAction objects from distributed write tasks.

        Args:
            write_result: Write results from all tasks

        Returns:
            Flattened list of all AddAction objects
        """
        all_file_actions = []
        for task_file_actions in write_result.write_returns:
            all_file_actions.extend(task_file_actions)
        return all_file_actions

    def _create_table_with_files(self, file_actions: List["AddAction"]) -> None:
        """
        Create new Delta table and commit files in single transaction.

        Args:
            file_actions: List of AddAction objects for all written files

        Note:
            Schema is inferred from first Parquet file plus partition columns.
            All table metadata (name, description, configuration) is set atomically.
        """
        from deltalake import Schema as DeltaSchema
        from deltalake.transaction import create_table_with_add_actions

        # Infer schema from written files
        table_schema = self._infer_schema(file_actions)
        delta_schema = DeltaSchema.from_arrow(table_schema)

        # Create table with all files in single atomic transaction
        # This writes the _delta_log/00000000000000000000.json file
        create_table_with_add_actions(
            table_uri=self.path,
            schema=delta_schema,
            add_actions=file_actions,
            mode=self.mode.value,
            partition_by=self.partition_cols or None,
            name=self.delta_write_config.name,
            description=self.delta_write_config.description,
            configuration=self.delta_write_config.configuration,
            storage_options=self.storage_options,
            commit_properties=self.delta_write_config.commit_properties,
            post_commithook_properties=self.delta_write_config.post_commithook_properties,
        )

        logger.info(
            f"Created Delta table at {self.path} with {len(file_actions)} files"
        )

    def _commit_to_existing_table(
        self, existing_table: "DeltaTable", file_actions: List["AddAction"]
    ) -> None:
        """
        Commit files to existing Delta table using write transaction.

        Handles race conditions where table was created between write start
        and complete.

        Args:
            existing_table: Existing Delta table object
            file_actions: List of AddAction objects for written files

        Raises:
            ValueError: If ERROR mode (table shouldn't have been created)
        """
        # Handle race condition checks for ERROR and IGNORE modes
        if self.mode == WriteMode.ERROR:
            raise ValueError(
                f"Race condition detected: Delta table was created at {self.path} "
                f"after write started. Files have been written but not committed to "
                f"the transaction log. Use mode='append' or 'overwrite' if concurrent "
                f"writes are expected."
            )

        if self.mode == WriteMode.IGNORE:
            # Silently skip commit if table was created during write
            logger.info(
                f"Table created at {self.path} during write. "
                f"Skipping commit due to mode='ignore'."
            )
            return

        # Determine transaction mode (append or overwrite)
        transaction_mode = "overwrite" if self.mode == WriteMode.OVERWRITE else "append"

        # Create write transaction with all file actions
        # The transaction will be appended to _delta_log as new JSON file
        transaction = existing_table.create_write_transaction(
            actions=file_actions,
            mode=transaction_mode,
            schema=existing_table.schema(),
            partition_by=self.partition_cols or None,
            commit_properties=self.delta_write_config.commit_properties,
            post_commithook_properties=self.delta_write_config.post_commithook_properties,
        )

        # Atomic commit - all files become visible at once
        # This writes new _delta_log JSON file (e.g., 00000000000000000001.json)
        transaction.commit()

        logger.info(
            f"Committed {len(file_actions)} files to Delta table at {self.path} "
            f"(mode={transaction_mode})"
        )

    def _infer_schema(self, add_actions: List["AddAction"]) -> pa.Schema:
        """Infer schema from first file and partition columns."""
        if self.schema:
            return self.schema

        # Read schema from first file
        first_file = os.path.join(self.path, add_actions[0].path)
        file_obj = self.filesystem.open_input_file(first_file)
        parquet_file = pq.ParquetFile(file_obj)
        schema = parquet_file.schema_arrow

        # Add partition columns
        if self.partition_cols:
            for col in self.partition_cols:
                if col in add_actions[0].partition_values:
                    val = add_actions[0].partition_values[col]
                    col_type = self._infer_partition_type(val)
                    schema = schema.append(pa.field(col, col_type))

        return schema

    def _infer_partition_type(self, value: Optional[str]) -> pa.DataType:
        """Infer PyArrow type from partition value."""
        if not value:
            return pa.string()

        try:
            int(value)
            return pa.int64()
        except ValueError:
            pass

        try:
            float(value)
            return pa.float64()
        except ValueError:
            pass

        return pa.string()

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure."""
        logger.error(
            f"Delta write failed for {self.path}: {error}. "
            f"Uncommitted files will be cleaned by Delta vacuum."
        )

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return None

    def get_name(self) -> str:
        return "Delta"
