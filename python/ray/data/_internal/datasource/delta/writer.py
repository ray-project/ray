"""File writing logic for Delta Lake datasink.

This module handles writing Parquet files to storage for Delta Lake tables.
It's designed to be streaming-safe and can be used independently of the main
datasink class.

Delta Lake: https://delta.io/
PyArrow Parquet: https://arrow.apache.org/docs/python/parquet.html
"""

import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ray._common.retry import call_with_retry
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

logger = logging.getLogger(__name__)

_MAX_PARTITIONS = 10_000
_VALID_COMPRESSIONS = {"snappy", "gzip", "brotli", "zstd", "lz4", "none"}


def _import_add_action():
    """Import AddAction from deltalake, handling different versions."""
    for mod in ("deltalake.transaction", "deltalake", "deltalake.writer"):
        try:
            m = __import__(mod, fromlist=["AddAction"])
            return m.AddAction
        except Exception:
            continue
    raise ImportError(
        "Could not import AddAction from deltalake. Please install/upgrade deltalake."
    )


class DeltaFileWriter:
    """Handles writing Parquet files for Delta Lake tables.

    This class is stateless (except for filesystem) and streaming-safe.
    Writes one file per table/partition combination - no buffering.
    """

    def __init__(
        self,
        *,
        filesystem: pa.fs.FileSystem,
        partition_cols: List[str],
        write_uuid: Optional[str],
        write_kwargs: Dict[str, Any],
        written_files: Set[str],
    ):
        """Initialize file writer.

        Args:
            filesystem: PyArrow filesystem for writing files.
            partition_cols: List of partition column names.
            write_uuid: Unique identifier for this write operation.
            write_kwargs: Additional write options (compression, etc.).
            written_files: Set to track written file paths (for cleanup).
        """
        self.filesystem = filesystem
        self.partition_cols = partition_cols
        self.write_uuid = write_uuid
        self.write_kwargs = write_kwargs
        self.written_files = written_files
        self._AddAction = _import_add_action()
        self._file_seq = 0  # monotonically increasing per task for stable filenames

        # Validate compression early with friendly error
        compression = self.write_kwargs.get("compression", "snappy")
        if compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression '{compression}'. Supported: {sorted(_VALID_COMPRESSIONS)}"
            )

    def add_table(self, table: pa.Table, task_idx: int) -> List[Any]:
        """Write table data immediately (one file per partition, no buffering).

        Args:
            table: PyArrow table to write.
            task_idx: Task index for filename generation.

        Returns:
            List of AddAction objects.
        """
        if len(table) == 0:
            return []
        self._file_seq += 1
        return self.write_table_data(table, task_idx, block_idx=self._file_seq)

    def write_table_data(
        self, table: pa.Table, task_idx: int, block_idx: int
    ) -> List[Any]:
        """Write table data as partitioned or non-partitioned Parquet files.

        Args:
            table: PyArrow table to write.
            task_idx: Task index for filename generation.
            block_idx: Block index for filename generation.

        Returns:
            List of AddAction objects with file metadata.
        """
        if len(table) == 0:
            return []
        if self.partition_cols:
            parts = self.partition_table(table, self.partition_cols)
            return [
                a
                for a in (
                    self.write_partition(t, k, task_idx, block_idx)
                    for k, t in parts.items()
                )
                if a
            ]
        a = self.write_partition(table, (), task_idx, block_idx)
        return [a] if a else []

    def partition_table(
        self, table: pa.Table, cols: List[str]
    ) -> Dict[Tuple, pa.Table]:
        """Partition table by columns efficiently using Arrow-native operations.

        Args:
            table: PyArrow table to partition.
            cols: List of partition column names.

        Returns:
            Dictionary mapping partition value tuples to partitioned tables.
        """
        if len(table) == 0:
            return {}

        if len(cols) == 1:
            col = cols[0]
            unique_vals = pc.unique(table[col])
            if len(unique_vals) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition values ({len(unique_vals)}). Max: {_MAX_PARTITIONS}"
                )
            out = {}
            for v in unique_vals:
                vpy = v.as_py()
                validate_partition_value(vpy)
                if vpy is None:
                    sub = table.filter(pc.is_null(table[col]))
                elif (
                    pa.types.is_floating(table[col].type)
                    and isinstance(vpy, float)
                    and vpy != vpy
                ):
                    sub = table.filter(pc.is_nan(table[col]))
                else:
                    sub = table.filter(pc.equal(table[col], v))
                if len(sub) > 0:
                    out[(vpy,)] = sub
            return out

        # multi-col: struct -> dictionary encode
        arrays = []
        fields = []
        for c in cols:
            arr = table[c]
            if isinstance(arr, pa.ChunkedArray):
                arr = arr.combine_chunks()
            arrays.append(arr)
            fields.append(pa.field(c, table.schema.field(c).type, nullable=True))

        struct_arr = pa.StructArray.from_arrays(arrays, fields=fields)

        try:
            enc = pc.dictionary_encode(struct_arr)
            dictionary, indices = enc.dictionary, enc.indices
            use_dictionary_keys = True
        except pa.ArrowNotImplementedError:
            # Some PyArrow builds don't support dictionary_encode on struct types.
            # Fallback: manual grouping by iterating through rows and grouping by partition values.
            use_dictionary_keys = False

            # Build groups manually by iterating through rows
            # Extract columns as arrays for efficient access
            col_arrays = [table[c] for c in cols]
            # Handle ChunkedArrays by combining chunks if needed
            col_arrays = [
                arr.combine_chunks() if isinstance(arr, pa.ChunkedArray) else arr
                for arr in col_arrays
            ]

            groups: Dict[Tuple, List[int]] = {}
            for i in range(len(table)):
                key = tuple(
                    arr[i].as_py() if arr[i].is_valid else None for arr in col_arrays
                )
                if key not in groups:
                    groups[key] = []
                groups[key].append(i)

            if len(groups) > _MAX_PARTITIONS:
                raise ValueError(
                    f"Too many partition combinations ({len(groups)}). Max: {_MAX_PARTITIONS}"
                )

            # Convert groups to dictionary of tables
            out = {}
            for key, indices_list in groups.items():
                for v in key:
                    if v is not None:
                        validate_partition_value(v)
                # Use take() to extract rows by index
                sub = table.take(pa.array(indices_list))
                if len(sub) > 0:
                    out[key] = sub
            return out

        if len(dictionary) > _MAX_PARTITIONS:
            raise ValueError(
                f"Too many partition combinations ({len(dictionary)}). Max: {_MAX_PARTITIONS}"
            )

        out = {}
        for dict_idx in range(len(dictionary)):
            sub = table.filter(pc.equal(indices, dict_idx))
            if len(sub) == 0:
                continue

            if use_dictionary_keys:
                key_struct = dictionary[dict_idx]
                key = tuple(
                    key_struct[i].as_py() if key_struct[i].is_valid else None
                    for i in range(len(cols))
                )
            else:
                # Derive actual partition key from the first row of the group.
                first = sub.slice(0, 1)
                key = tuple(first[c][0].as_py() for c in cols)

            for v in key:
                if v is not None:
                    validate_partition_value(v)
            out[key] = sub
        return out

    def write_partition(
        self,
        table: pa.Table,
        partition_values: Tuple,
        task_idx: int,
        block_idx: int,
    ):
        """Write a single partition to Parquet file and create AddAction metadata.

        Args:
            table: PyArrow table to write.
            partition_values: Tuple of partition values for this partition.
            task_idx: Task index for filename generation.
            block_idx: Block index for filename generation.

        Returns:
            AddAction object with file metadata, or None if table is empty.
        """
        if len(table) == 0:
            return None

        # Drop partition columns from file payload (Delta convention)
        if self.partition_cols:
            data_cols = [c for c in table.column_names if c not in self.partition_cols]
            table = table.select(data_cols)

        filename = self._filename(task_idx, block_idx)
        partition_path, partition_dict = build_partition_path(
            self.partition_cols, partition_values
        )
        rel_path = partition_path + filename

        validate_file_path(rel_path)
        self.written_files.add(rel_path)

        size = self._write_parquet(table, rel_path)
        stats = compute_parquet_statistics(table)

        return self._AddAction(
            path=rel_path,
            size=size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=stats,
        )

    def _filename(self, task_idx: int, block_idx: int) -> str:
        """Generate unique Parquet filename.

        Args:
            task_idx: Task index.
            block_idx: Block index.

        Returns:
            Unique filename string.
        """
        uid = uuid.uuid4().hex[:16]
        prefix = self.write_uuid or "00000000"
        prefix = prefix[:8].ljust(8, "0")
        return f"part-{prefix}-{task_idx:05d}-{block_idx:05d}-{uid}.parquet"

    def _write_parquet(self, table: pa.Table, rel_path: str) -> int:
        """Write PyArrow table to Parquet file and return file size.

        Args:
            table: PyArrow table to write.
            rel_path: Relative path from table root.

        Returns:
            Size of written file in bytes.
        """
        compression = self.write_kwargs.get(
            "compression", "snappy"
        )  # already validated
        write_statistics = self.write_kwargs.get("write_statistics", True)

        parent = safe_dirname(rel_path)
        if parent:
            try:
                self.filesystem.create_dir(parent, recursive=True)
            except Exception:
                pass

        ctx = DataContext.get_current()
        result = {"size": 0}

        def _write_and_verify():
            pq.write_table(
                table,
                rel_path,
                filesystem=self.filesystem,
                compression=compression,
                write_statistics=write_statistics,
            )
            info = get_file_info_with_retry(self.filesystem, rel_path)
            if info.size == 0:
                try:
                    self.filesystem.delete_file(rel_path)
                except Exception:
                    pass
                raise RuntimeError(f"Written file is empty: {rel_path}")
            result["size"] = info.size

        call_with_retry(
            _write_and_verify,
            description=f"write Parquet file '{rel_path}'",
            match=ctx.retried_io_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )
        return result["size"]
