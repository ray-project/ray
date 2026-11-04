"""Byte-range chunking for newline-delimited files (CSV and JSONL).

Purpose: let several read tasks share one large CSV or JSONL file. Parquet
and ORC split at row groups or stripes listed in the file footer; text files
have no such index, so this module splits them by byte offset and fixes the
line boundaries at read time.

How it works:

- Listing (:func:`split_into_line_chunks`) is arithmetic on the file size and
  reads no bytes. Chunk ``i`` is the range ``[i * chunk_size,
  min((i + 1) * chunk_size, file_size))``, stored as a ``FileChunk`` whose
  only unit id is ``i``. :func:`line_chunk_byte_range` maps the id back to
  the range, so listing rows carry no offsets.
- Reading (:func:`read_line_aligned`) turns a range into whole lines by one
  rule: a line belongs to the chunk that holds its first byte. A chunk skips
  the partial line it starts inside and finishes the line that crosses its
  end. Joining every chunk of a file gives back the file exactly. For
  ``b"a\\nbb\\nc\\n"`` with ``chunk_size=3`` the ranges ``[0, 3)``,
  ``[3, 6)`` and ``[6, 7)`` read as ``b"a\\nbb\\n"``, ``b"c\\n"`` and
  ``b""``.

Design decisions:

- Cuts are fixed-size, not placed at newlines: finding newlines while
  listing would read the whole dataset in the single listing task.
- The start check looks at byte ``start - 1``. When a line begins exactly at
  ``start``, that byte is the previous line's ``\\n``, so the line stays in
  this chunk and the previous chunk stops at that ``\\n``.
- One ``read_at`` covers ``[start - 1, end + 64 KiB)``, so the line crossing
  ``end`` rarely needs a second request; longer lines finish in 64 KiB steps.
- A line longer than a chunk leaves the chunks it covers empty. They return
  no bytes instead of failing.
- Correct only when every ``\\n`` ends a row. Compressed files and CSV with
  quoted newlines (``newlines_in_values=True``) must be read whole.
"""

import logging
import math
from typing import TYPE_CHECKING, List, Tuple

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
from ray.data._internal.util import KiB, NumBytes

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)

# Bytes read past a chunk's end in the same request, so the line that crosses
# the end is usually finished without a second request.
_LINE_PROBE_BYTES = 64 * KiB


def line_chunk_byte_range(
    index: int, *, chunk_size: NumBytes, file_size: NumBytes
) -> Tuple[int, int]:
    """Return the ``[start, end)`` byte range of chunk ``index``, before line alignment.

    Args:
        index: Position of the chunk in its file, the ``FileChunk`` unit id.
        chunk_size: Bytes per chunk; the last chunk of a file may be shorter.
        file_size: Size of the file in bytes.

    Returns:
        The chunk's start offset and its exclusive end offset.
    """
    start = index * chunk_size
    assert (
        0 <= start < file_size
    ), f"chunk {index} of size {chunk_size} starts outside a {file_size}-byte file"
    return start, min(start + chunk_size, file_size)


def split_into_line_chunks(
    file_size: NumBytes, *, chunk_size: NumBytes
) -> List[FileChunk]:
    """Cut a file into one ``FileChunk`` per ``chunk_size`` bytes, in file order.

    No bytes are read; :func:`read_line_aligned` finds the lines at read time.
    The row count of a chunk is unknown, so ``num_rows`` is 0 and
    ``fully_matched`` is False, which tells limit push-down not to rely on it.

    Args:
        file_size: Size of the file in bytes.
        chunk_size: Bytes per chunk.

    Returns:
        The file's chunks, each holding one unit id: the chunk's index.
    """
    assert chunk_size > 0, f"chunk_size must be positive, got {chunk_size}"
    num_chunks = math.ceil(file_size / chunk_size)
    chunks = []
    for index in range(num_chunks):
        start, end = line_chunk_byte_range(
            index=index, chunk_size=chunk_size, file_size=file_size
        )
        chunks.append(
            FileChunk(
                unit_ids=(index,),
                num_rows=0,
                size_bytes=end - start,
                fully_matched=False,
            )
        )
    return chunks


def read_line_aligned(
    file: "pyarrow.NativeFile", *, start: int, end: int, file_size: NumBytes
) -> memoryview:
    """Read the whole lines that begin in ``[start, end)``.

    1. The first line starts at byte 0, or else just after the first ``\\n``
       at or after ``start - 1``. Looking one byte back keeps a line that
       begins exactly at ``start``; the previous chunk stops before it.
    2. The last line ends just after the first ``\\n`` at or after
       ``end - 1``, or at the end of the file. That finishes the line that
       crosses ``end``.

    Args:
        file: The file, opened for random access.
        start: First byte of the chunk's range.
        end: One past the last byte of the chunk's range.
        file_size: Size of the file in bytes.

    Returns:
        The chunk's lines; empty when no line begins in the range, which
        happens when a line is longer than a chunk.
    """
    # One request covers the byte before the range, the range itself, and
    # usually the rest of the line that crosses ``end``.
    fetch_start = max(start - 1, 0)
    fetch_end = min(end + _LINE_PROBE_BYTES, file_size)
    data = file.read_at(nbytes=fetch_end - fetch_start, offset=fetch_start)

    # Index in ``data`` where the range's first whole line begins. With no
    # ``\n`` after byte ``start - 1`` in ``data``, that line runs past
    # ``fetch_end`` and no line begins in the range.
    first_line_start = 0
    if start > 0:
        previous_newline = data.find(b"\n")
        first_line_start = len(data) if previous_newline == -1 else previous_newline + 1
    if fetch_start + first_line_start >= end:
        logger.debug("Line chunk [%d, %d) holds no line start", start, end)
        return memoryview(b"")

    last_newline = data.find(b"\n", end - 1 - fetch_start)
    if last_newline != -1:
        lines = memoryview(data)[first_line_start : last_newline + 1]
    elif fetch_end == file_size:
        # The file's last line has no trailing newline.
        lines = memoryview(data)[first_line_start:]
    else:
        tail = _read_through_newline(file=file, offset=fetch_end, file_size=file_size)
        lines = memoryview(b"".join((memoryview(data)[first_line_start:], tail)))
    aligned_start = fetch_start + first_line_start
    logger.debug(
        "Line chunk [%d, %d) aligned to [%d, %d)",
        start,
        end,
        aligned_start,
        aligned_start + len(lines),
    )
    return lines


def _read_through_newline(
    file: "pyarrow.NativeFile", *, offset: int, file_size: NumBytes
) -> bytes:
    """Read from ``offset`` through the next ``\\n``, or to the end of the file.

    The ``\\n`` is kept, as in :func:`read_line_aligned`, so each chunk ends
    where the next one starts.
    """
    pieces = []
    while offset < file_size:
        piece = file.read_at(
            nbytes=min(_LINE_PROBE_BYTES, file_size - offset), offset=offset
        )
        if not piece:
            break
        newline = piece.find(b"\n")
        if newline != -1:
            pieces.append(piece[: newline + 1])
            break
        pieces.append(piece)
        offset += len(piece)
    return b"".join(pieces)
