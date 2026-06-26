"""Shared low-level Parquet I/O primitives.

This module hosts the small, format-agnostic pieces of Parquet writing that
both ``ParquetDatasink`` (used by ``Dataset.write_parquet``) and the
``DeltaFileWriter`` (used by ``Dataset.write_delta``) want to share:

* the retry-wrapped ``pq.write_table`` call,
* the per-write UUID-tagged filename builder,
* the retry tunables.

Higher-level concerns -- partitioning, file-action assembly, stats blobs --
stay in their respective datasinks because they're format-specific.

Addresses the review feedback that asked for the Delta Parquet writer to
reuse the writer powering ``write_parquet``. ``ParquetDatasink`` is not
modified in this commit (it sits on top of ``pyarrow.dataset.write_dataset``
rather than ``pq.write_table``); the convergence work for ``write_parquet``
itself is a follow-up.
"""

import uuid
from typing import Any, Callable, Dict, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray._common.retry import call_with_retry

# Re-exported so existing call sites can keep importing these from the
# original location (parquet_datasink.py) without churn.
from ray.data._internal.datasource.parquet_datasink import (  # noqa: F401
    WRITE_FILE_MAX_ATTEMPTS,
    WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
)
from ray.data.context import DataContext

__all__ = [
    "WRITE_FILE_MAX_ATTEMPTS",
    "WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS",
    "build_uuid_filename",
    "write_parquet_with_retry",
]


def build_uuid_filename(
    write_uuid: Optional[str],
    task_idx: int,
    block_idx: int,
    suffix: str = ".parquet",
) -> str:
    """Generate a stable, write-UUID-tagged filename.

    The format is ``part-<uuid8>-<task:05d>-<block:05d>-<rand16>.parquet``.
    Used by both ``DeltaFileWriter`` and (eventually) the Parquet writer.
    """
    uid = uuid.uuid4().hex[:16]
    prefix = write_uuid or "00000000"
    prefix = prefix[:8].ljust(8, "0")
    return f"part-{prefix}-{task_idx:05d}-{block_idx:05d}-{uid}{suffix}"


def write_parquet_with_retry(
    table: pa.Table,
    rel_path: str,
    filesystem: pa_fs.FileSystem,
    *,
    compression: str = "snappy",
    write_statistics: bool = True,
    verify_size: bool = True,
    extra_pq_kwargs: Optional[Dict[str, Any]] = None,
    on_each_attempt: Optional[Callable[[], None]] = None,
) -> int:
    """Write a PyArrow table to ``rel_path`` with retry and size verification.

    Returns the written file size in bytes. Raises if the file ends up empty
    (treated as a transient failure and retried; final empty is a hard error).
    """
    extra = dict(extra_pq_kwargs or {})
    ctx = DataContext.get_current()
    result: Dict[str, int] = {"size": 0}

    def _attempt() -> None:
        if on_each_attempt is not None:
            on_each_attempt()
        pq.write_table(
            table,
            rel_path,
            filesystem=filesystem,
            compression=compression,
            write_statistics=write_statistics,
            **extra,
        )
        info = filesystem.get_file_info(rel_path)
        if verify_size and info.size == 0:
            try:
                filesystem.delete_file(rel_path)
            except Exception:
                pass
            raise RuntimeError(f"Written file is empty: {rel_path}")
        result["size"] = info.size

    call_with_retry(
        _attempt,
        description=f"write Parquet file '{rel_path}'",
        match=ctx.retried_io_errors,
        max_attempts=WRITE_FILE_MAX_ATTEMPTS,
        max_backoff_s=WRITE_FILE_RETRY_MAX_BACKOFF_SECONDS,
    )
    return result["size"]
