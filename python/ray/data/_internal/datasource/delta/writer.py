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
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa
import pyarrow.parquet as pq

from ray._common.retry import call_with_retry
from ray.data._internal.datasource.delta.utils import (
    compute_parquet_statistics,
    get_file_info_with_retry,
    safe_dirname,
    validate_file_path,
)
from ray.data._internal.datasource.parquet_datasink import (
    WRITE_FILE_MAX_ATTEMPTS,
    WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
)
from ray.data.context import DataContext

logger = logging.getLogger(__name__)

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
        "Could not import AddAction from deltalake. "
        "Please install/upgrade deltalake."
    )


class DeltaFileWriter:
    """Handles writing Parquet files for Delta Lake tables.

    This class is stateless (except for filesystem) and streaming-safe.
    """

    def __init__(
        self,
        *,
        filesystem: pa.fs.FileSystem,
        write_uuid: Optional[str],
        write_kwargs: Dict[str, Any],
        written_files: Set[str],
    ):
        """Initialize file writer.

        Args:
            filesystem: PyArrow filesystem for writing files.
            write_uuid: Unique identifier for this write operation.
            write_kwargs: Additional write options (compression, etc.).
            written_files: Set to track written file paths (for cleanup).
        """
        self.filesystem = filesystem
        self.write_uuid = write_uuid
        self.write_kwargs = write_kwargs
        self.written_files = written_files
        self._AddAction = _import_add_action()
        self._file_seq = 0  # monotonically increasing per task for stable filenames

        # Validate compression early with friendly error
        compression = self.write_kwargs.get("compression", "snappy")
        if compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression '{compression}'. "
                f"Supported: {sorted(_VALID_COMPRESSIONS)}"
            )

    def write_table(self, table: pa.Table, task_idx: int) -> List[Any]:
        """Write a table as a single Parquet file (no partitioning).

        Args:
            table: PyArrow table to write.
            task_idx: Task index for filename generation.

        Returns:
            List of AddAction objects with file metadata.
        """
        if len(table) == 0:
            return []
        self._file_seq += 1
        action = self._write_file(table, task_idx, self._file_seq)
        return [action] if action else []

    def _write_file(
        self,
        table: pa.Table,
        task_idx: int,
        block_idx: int,
    ):
        """Write a single Parquet file and create AddAction metadata.

        Args:
            table: PyArrow table to write.
            task_idx: Task index for filename generation.
            block_idx: Block index for filename generation.

        Returns:
            AddAction object with file metadata, or None if table is empty.
        """
        if len(table) == 0:
            return None

        filename = self._filename(task_idx, block_idx)

        validate_file_path(filename)
        self.written_files.add(filename)

        size = self._write_parquet(table, filename)
        stats = compute_parquet_statistics(table)

        return self._AddAction(
            path=filename,
            size=size,
            partition_values={},
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
        compression = self.write_kwargs.get("compression", "snappy")
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
