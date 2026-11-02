"""Byte-range chunks of line-delimited files and record-boundary alignment.

A ``LineDelimitedFileChunk`` is what :class:`RecordAlignedCSVFileIndexer`
stores in a manifest's chunk column and what :class:`CSVFileReader` reads
back, the way ``FileChunk`` does for Parquet row-group runs. The alignment
helpers are shared by both so a boundary is defined in exactly one place.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.interfaces.file_manifest import ChunkMetadata

_BOUNDARY_SCAN_SIZE = 64 * 1024


@dataclass(frozen=True)
class LineDelimitedFileChunk:
    """A byte range of a line-delimited file (CSV, JSONL).

    Produced by indexers that split large uncompressed files for parallel
    reads. The range is aligned to record boundaries by the indexer; the reader
    aligns it again before parsing, so a stale or unaligned range is never
    read twice or skipped.
    """

    chunk_byte_start_idx: int
    chunk_byte_end_idx: int

    def to_metadata(self) -> ChunkMetadata:
        return asdict(self)

    @classmethod
    def from_metadata(cls, metadata: Mapping[str, Any]) -> "LineDelimitedFileChunk":
        # Values come back from the manifest block as numpy scalars.
        return cls(
            chunk_byte_start_idx=int(metadata["chunk_byte_start_idx"]),
            chunk_byte_end_idx=int(metadata["chunk_byte_end_idx"]),
        )


def _is_record_boundary(file: pa.NativeFile, offset: int, file_size: int) -> bool:
    if offset <= 0 or offset >= file_size:
        return True

    previous = file.read_at(1, offset - 1)
    if previous == b"\n":
        return True
    if previous == b"\r":
        # ``offset`` points between CR and LF in a CRLF terminator.
        return file.read_at(1, offset) != b"\n"
    return False


def _find_record_boundary(file: pa.NativeFile, offset: int, file_size: int) -> int:
    """Return the first byte after a record terminator at/after ``offset``."""
    position = offset
    while position < file_size:
        data = file.read_at(min(_BOUNDARY_SCAN_SIZE, file_size - position), position)
        for index, value in enumerate(data):
            absolute = position + index
            if value == ord("\n"):
                return absolute + 1
            if value == ord("\r"):
                if absolute + 1 < file_size and file.read_at(1, absolute + 1) == b"\n":
                    return absolute + 2
                return absolute + 1
        if not data:
            break
        position += len(data)
    return file_size


def _align_chunk_range(
    file: pa.NativeFile,
    metadata: Mapping[str, Any],
    file_size: int,
) -> Tuple[int, int]:
    """Align an estimated byte range to complete CSV record boundaries."""
    chunk = LineDelimitedFileChunk.from_metadata(metadata)
    start = min(chunk.chunk_byte_start_idx, file_size)
    end = min(chunk.chunk_byte_end_idx, file_size)

    if not _is_record_boundary(file, start, file_size):
        start = _find_record_boundary(file, start, file_size)
    if not _is_record_boundary(file, end, file_size):
        end = _find_record_boundary(file, end, file_size)

    return start, max(start, end)
