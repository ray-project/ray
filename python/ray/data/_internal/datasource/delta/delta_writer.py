"""File writing logic for Delta Lake datasink.

This module handles writing Parquet files to storage. It's designed to be
streaming-safe and can be used independently of the main datasink class.
"""

import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ray.data._internal.datasource.delta.utils import (
    build_partition_path,
    compute_parquet_statistics,
    get_file_info_with_retry,
    safe_dirname,
    validate_file_path,
    validate_partition_value,
)
from ray.data._internal.datasource.parquet_datasink import (
    WRITE_FILE_MAX_ATTEMPTS,
    WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
)
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)

# Maximum number of partitions to prevent filesystem issues
_MAX_PARTITIONS = 10000


class DeltaFileWriter:
    """Handles writing Parquet files for Delta Lake tables.

    This class is stateless (except for filesystem) and streaming-safe.
    It can be used independently for writing files without committing to Delta log.

    PyArrow Parquet: https://arrow.apache.org/docs/python/parquet.html
    """

    def __init__(
        self,
        table_uri: str,
        filesystem: Optional[pa.fs.FileSystem],
        partition_cols: Optional[List[str]] = None,
        write_uuid: Optional[str] = None,
        write_kwargs: Optional[Dict[str, Any]] = None,
        written_files: Optional[Set[str]] = None,
    ):
        """Initialize file writer.

        Args:
            table_uri: Base URI for Delta table.
            filesystem: PyArrow filesystem for writing files.
            partition_cols: List of partition column names.
            write_uuid: Unique identifier for this write operation.
            write_kwargs: Additional write options (compression, etc.).
            written_files: Set to track written file paths (for cleanup).
        """
        self.table_uri = table_uri
        self.filesystem = filesystem or _resolve_paths_and_filesystem(table_uri)[1]
        self.partition_cols = partition_cols or []
        self.write_uuid = write_uuid
        self.write_kwargs = write_kwargs or {}
        self.written_files = written_files or set()

    def write_table_data(
        self, table: pa.Table, task_idx: int, block_idx: int = 0
    ) -> List["AddAction"]:
        """Write table data as partitioned or non-partitioned Parquet files.

        Args:
            table: PyArrow table to write.
            task_idx: Task index for filename generation.
            block_idx: Block index for filename generation.

        Returns:
            List of AddAction objects with file metadata.
        """
        if self.partition_cols:
            partitioned_tables = self.partition_table(table, self.partition_cols)
            actions = [
                self.write_partition(
                    partition_table, partition_values, task_idx, block_idx
                )
                for partition_values, partition_table in partitioned_tables.items()
            ]
            return [a for a in actions if a is not None]
        # For non-partitioned writes, pass empty tuple for partition_values
        action = self.write_partition(table, (), task_idx, block_idx)
        return [action] if action is not None else []

    def partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[Tuple, pa.Table]:
        """Partition table by columns efficiently using Arrow-native operations.

        Uses PyArrow compute functions for efficient O(n) partitioning:
        - Single column: Uses pc.unique() + pc.filter() (O(n) + O(k*n) where k is unique count)
        - Multiple columns: Uses struct column + dictionary encoding for O(n) grouping

        PyArrow compute functions: https://arrow.apache.org/docs/python/api/compute.html

        Args:
            table: PyArrow table to partition.
            partition_cols: List of partition column names.

        Returns:
            Dictionary mapping partition value tuples to partitioned tables.
        """
        if len(table) == 0:
            return {}

        partitions = {}
        if len(partition_cols) == 1:
            # Single column: use unique() + filter (already efficient)
            col = partition_cols[0]
            unique_vals = pc.unique(table[col])
            if len(unique_vals) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition values ({len(unique_vals)}). Max: {_MAX_PARTITIONS}"
                )
            for val in unique_vals:
                val_py = val.as_py()
                validate_partition_value(val_py)
                if val_py is None:
                    filtered = table.filter(pc.is_null(table[col]))
                elif pa.types.is_floating(table[col].type) and (
                    isinstance(val_py, float) and val_py != val_py
                ):  # NaN check: val_py != val_py is True only for NaN
                    filtered = table.filter(pc.is_nan(table[col]))
                else:
                    filtered = table.filter(pc.equal(table[col], val))
                if len(filtered) > 0:
                    partitions[(val_py,)] = filtered
        else:
            # Multiple columns: use struct + dictionary encoding for O(n) grouping
            # This avoids converting to Python lists (which is O(n) and slow for large datasets)
            # Create struct column from partition columns
            struct_fields = [
                pa.field(col, table.schema.field(col).type, nullable=True)
                for col in partition_cols
            ]
            # Convert ChunkedArrays to Arrays (from_arrays requires Array objects)
            partition_arrays = [
                table[col].combine_chunks() if isinstance(table[col], pa.ChunkedArray) else table[col]
                for col in partition_cols
            ]
            struct_array = pa.StructArray.from_arrays(
                partition_arrays, fields=struct_fields
            )

            # Use dictionary encode to get unique groups and indices
            # This gives us O(n) grouping instead of O(k*n) Python loops
            dict_encoded = pc.dictionary_encode(struct_array)
            dictionary = (
                dict_encoded.dictionary
            )  # Unique struct values (partition keys)
            indices = dict_encoded.indices  # Dictionary index for each row

            # Check partition count
            if len(dictionary) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition combinations ({len(dictionary)}). Max: {_MAX_PARTITIONS}"
                )

            # For each unique partition key, filter rows using vectorized operations
            # This is O(k*n) but uses Arrow-native operations (much faster than Python loops)
            for dict_idx in range(len(dictionary)):
                # Create mask: rows that belong to this partition
                mask = pc.equal(indices, dict_idx)

                # Filter table using mask (Arrow-native, very fast)
                partitioned = table.filter(mask)

                if len(partitioned) > 0:
                    # Extract partition key from dictionary struct
                    struct_val = dictionary[dict_idx]
                    partition_key = tuple(
                        struct_val[i].as_py() if struct_val[i].is_valid else None
                        for i in range(len(partition_cols))
                    )

                    # Validate partition values
                    for v in partition_key:
                        if v is not None:
                            validate_partition_value(v)

                    partitions[partition_key] = partitioned

        return partitions

    def write_partition(
        self,
        table: pa.Table,
        partition_values: Tuple,
        task_idx: int,
        block_idx: int = 0,
    ) -> Optional["AddAction"]:
        """Write a single partition to Parquet file and create AddAction metadata.

        Writes Parquet file to storage, computes statistics, and creates AddAction
        object with file metadata. AddAction is used later to commit to Delta log.

        Note: Partition columns are removed from the data written to Parquet files
        per Delta Lake convention - partition values are stored only in directory
        paths and AddAction metadata.

        deltalake AddAction: https://delta-io.github.io/delta-rs/python/api/deltalake.transaction.html#deltalake.transaction.AddAction

        Args:
            table: PyArrow table to write.
            partition_values: Tuple of partition values for this partition.
            task_idx: Task index for filename generation.
            block_idx: Block index for filename generation.

        Returns:
            AddAction object with file metadata, or None if table is empty.
        """
        # Import AddAction - handle different deltalake versions
        # AddAction is used to create file metadata for Delta transaction log
        # deltalake.transaction.AddAction: https://delta-io.github.io/delta-rs/python/api/deltalake.transaction.html#deltalake.transaction.AddAction
        try:
            from deltalake.transaction import AddAction
        except ImportError:
            # Fallback: try importing from deltalake directly (some versions)
            try:
                from deltalake import AddAction
            except ImportError:
                # Last resort: try deltalake.writer (older versions)
                try:
                    from deltalake.writer import AddAction
                except ImportError:
                    raise ImportError(
                        "Could not import AddAction from deltalake. "
                        "This usually means deltalake is not installed or is an incompatible version. "
                        "Please install/upgrade deltalake: pip install --upgrade deltalake"
                    ) from None

        if len(table) == 0:
            return None

        # Remove partition columns from data before writing (Delta Lake convention)
        # Partition values are stored in directory paths and AddAction metadata
        if self.partition_cols:
            data_cols = [c for c in table.column_names if c not in self.partition_cols]
            table = table.select(data_cols)

        filename = self.generate_filename(task_idx, block_idx)
        partition_path, partition_dict = build_partition_path(
            self.partition_cols, partition_values
        )
        relative_path = partition_path + filename

        validate_file_path(relative_path)
        # filesystem is SubTreeFileSystem rooted at table path, so use relative_path directly
        # relative_path is already validated to not contain ".."
        self.written_files.add(relative_path)
        file_size = self.write_parquet_file(table, relative_path)
        file_statistics = compute_parquet_statistics(table)

        return AddAction(
            path=relative_path,
            size=file_size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=file_statistics,
        )

    def generate_filename(self, task_idx: int, block_idx: int = 0) -> str:
        """Generate unique Parquet filename.

        Format: part-{write_uuid[:8]}-{task_idx:05d}-{block_idx:05d}-{unique_id}.parquet
        Includes write_uuid prefix for correlation, task and block indices for debugging,
        plus per-file UUID for uniqueness.

        Args:
            task_idx: Task index.
            block_idx: Block index.

        Returns:
            Unique filename string.
        """
        unique_id = uuid.uuid4().hex[:16]
        # Handle None, empty string, or too-short write_uuid
        if self.write_uuid and len(self.write_uuid) >= 8:
            write_prefix = self.write_uuid[:8]
        elif self.write_uuid:
            write_prefix = self.write_uuid.ljust(8, "0")
        else:
            write_prefix = "00000000"
        return f"part-{write_prefix}-{task_idx:05d}-{block_idx:05d}-{unique_id}.parquet"

    def write_parquet_file(self, table: pa.Table, file_path: str) -> int:
        """Write PyArrow table to Parquet file and return file size.

        Uses PyArrow Parquet writer with compression and statistics enabled.
        Uses call_with_retry for robust retry behavior with exponential backoff.

        PyArrow Parquet: https://arrow.apache.org/docs/python/parquet.html

        Args:
            table: PyArrow table to write.
            file_path: Relative path from table root (filesystem is SubTreeFileSystem).

        Returns:
            Size of written file in bytes.
        """
        from ray._common.retry import call_with_retry

        compression = self.write_kwargs.get("compression", "snappy")
        valid_compressions = ["snappy", "gzip", "brotli", "zstd", "lz4", "none"]
        if compression not in valid_compressions:
            raise ValueError(
                f"Invalid compression '{compression}'. Supported: {valid_compressions}"
            )

        self.ensure_parent_directory(file_path)
        write_statistics = self.write_kwargs.get("write_statistics", True)
        data_context = DataContext.get_current()

        # Store result in list to capture from inner function
        file_size_result = [0]

        def write_and_verify():
            pq.write_table(
                table,
                file_path,
                filesystem=self.filesystem,
                compression=compression,
                write_statistics=write_statistics,
            )
            file_info = get_file_info_with_retry(self.filesystem, file_path)
            if file_info.size == 0:
                try:
                    self.filesystem.delete_file(file_path)
                except Exception as delete_err:
                    logger.debug(
                        f"Failed to delete empty file {file_path}: {delete_err}"
                    )
                raise RuntimeError(f"Written file is empty: {file_path}")
            file_size_result[0] = file_info.size

        call_with_retry(
            write_and_verify,
            description=f"write Parquet file '{file_path}'",
            match=data_context.retried_io_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )

        return file_size_result[0]

    def ensure_parent_directory(self, file_path: str) -> None:
        """Create parent directory for file if needed."""
        parent_dir = safe_dirname(file_path)
        if parent_dir:
            try:
                self.filesystem.create_dir(parent_dir, recursive=True)
            except (FileExistsError, Exception):
                pass  # OK - directory exists or cloud storage doesn't need it
