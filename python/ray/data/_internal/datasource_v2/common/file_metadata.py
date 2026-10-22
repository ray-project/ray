"""Validate optional planning metadata before execution consumes it."""

from typing import List, Optional

from pyarrow.fs import FileSystem, FileType

from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileMetadata
from ray.data._internal.util import call_with_retry


def get_cached_file_metadata(
    file: FileInfo,
    filesystem: FileSystem,
    retried_io_errors: List[str],
) -> Optional[FileMetadata]:
    """Return a matching cache entry; misses use the reader's normal I/O path.

    Existence checks preserve missing-file errors even for metadata-only count.
    Size and available mtime detect ordinary replacement, not an atomic snapshot.
    """
    metadata = file.metadata
    if metadata is None or (
        metadata.num_rows is None and metadata.parquet_footer is None
    ):
        return None
    info = call_with_retry(
        lambda: filesystem.get_file_info(file.path),
        description=f"check planning metadata for {file.path}",
        match=retried_io_errors,
    )
    if info.type == FileType.NotFound:
        raise FileNotFoundError(file.path)
    if info.type != FileType.File:
        return None
    if info.size != file.size:
        return None
    if metadata.mtime_ns is not None and info.mtime_ns != metadata.mtime_ns:
        return None
    return metadata
