"""Parquet writer for the Delta Lake datasink (MVP, APPEND-only).

This MVP build writes one Parquet file per Arrow table handed to
``add_table`` -- no partitioning and no buffered/large-file mode. PR 5
extends this writer with Hive-style partitioning and the
``target_file_size_bytes`` buffered path.

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
    resolve_under_table_root,
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
    """Import AddAction from deltalake, handling different package layouts."""
    for mod in ("deltalake.transaction", "deltalake", "deltalake.writer"):
        try:
            m = __import__(mod, fromlist=["AddAction"])
            return m.AddAction
        except Exception:
            continue
    raise ImportError(
        "Could not import AddAction from deltalake. Install/upgrade deltalake."
    )


class DeltaFileWriter:
    """MVP DeltaFileWriter.

    Writes one Parquet file per ``add_table`` call. No partitioning. No
    buffering. PR 5 adds those features.
    """

    def __init__(
        self,
        *,
        filesystem: pa.fs.FileSystem,
        write_uuid: Optional[str],
        write_kwargs: Dict[str, Any],
        written_files: Set[str],
        local_filesystem_root: Optional[str] = None,
    ):
        self.filesystem = filesystem
        self.write_uuid = write_uuid
        self.write_kwargs = write_kwargs
        self.written_files = written_files
        self._AddAction = _import_add_action()
        self._file_seq = 0
        self._local_filesystem_root = local_filesystem_root

        compression = self.write_kwargs.get("compression", "snappy")
        if compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"Invalid compression '{compression}'. "
                f"Supported: {sorted(_VALID_COMPRESSIONS)}"
            )

    def add_table(self, table: pa.Table, task_idx: int) -> List[Any]:
        """Write a single Arrow table as one Parquet file."""
        if len(table) == 0:
            return []
        self._file_seq += 1
        action = self._write_one(table, task_idx, self._file_seq)
        return [action] if action is not None else []

    def flush(self, task_idx: int) -> List[Any]:
        """No-op in MVP (no buffering)."""
        return []

    # ------------------------------------------------------------------

    def _write_one(
        self, table: pa.Table, task_idx: int, block_idx: int
    ) -> Optional[Any]:
        filename = self._filename(task_idx, block_idx)
        rel_path = filename

        validate_file_path(rel_path)
        self.written_files.add(rel_path)

        size = self._write_parquet(table, rel_path)
        stats = compute_parquet_statistics(table)

        return self._AddAction(
            path=rel_path,
            size=size,
            partition_values={},
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=stats,
        )

    def _filename(self, task_idx: int, block_idx: int) -> str:
        uid = uuid.uuid4().hex[:16]
        prefix = self.write_uuid or "00000000"
        prefix = prefix[:8].ljust(8, "0")
        return f"part-{prefix}-{task_idx:05d}-{block_idx:05d}-{uid}.parquet"

    def _write_parquet(self, table: pa.Table, rel_path: str) -> int:
        compression = self.write_kwargs.get("compression", "snappy")
        write_statistics = self.write_kwargs.get("write_statistics", True)

        phys_path = resolve_under_table_root(self._local_filesystem_root, rel_path)
        parent = safe_dirname(rel_path)
        phys_parent = (
            resolve_under_table_root(self._local_filesystem_root, parent)
            if self._local_filesystem_root and parent
            else parent
        )
        if phys_parent:
            try:
                self.filesystem.create_dir(phys_parent, recursive=True)
            except Exception:
                pass

        ctx = DataContext.get_current()
        result = {"size": 0}

        def _write_and_verify() -> None:
            pq.write_table(
                table,
                phys_path,
                filesystem=self.filesystem,
                compression=compression,
                write_statistics=write_statistics,
            )
            info = get_file_info_with_retry(self.filesystem, phys_path)
            if info.size == 0:
                try:
                    self.filesystem.delete_file(phys_path)
                except Exception:
                    pass
                raise RuntimeError(f"Written file is empty: {phys_path}")
            result["size"] = info.size

        call_with_retry(
            _write_and_verify,
            description=f"write Parquet file '{phys_path}'",
            match=ctx.retried_io_errors,
            max_attempts=WRITE_FILE_MAX_ATTEMPTS,
            max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
        )
        return result["size"]
