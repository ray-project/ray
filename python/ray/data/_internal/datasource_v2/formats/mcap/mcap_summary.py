"""An MCAP file's summary, and the names of its chunks and messages.

An MCAP file can end with a summary section, found through the footer. It holds
the schemas and channels, one ``ChunkIndex`` per chunk (byte offset, sizes,
log-time bounds, channels) and a ``Statistics`` record (message counts, time
bounds). Reading it takes one to three ranged reads (``mcap_io``), so a read can
be planned without reading any message. ``read_summaries`` reads the summaries
of many files concurrently, with retries. The listing reads them with
``read_listed_summaries``, which keeps each raw section for the read tasks.

Format specification: https://mcap.dev/spec
"""

import math
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Callable,
    Deque,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from pyarrow.fs import FileSystem

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    parse_summary_records,
    read_summary_from,
    read_summary_section_from,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    from mcap.records import ChunkIndex, Statistics
    from mcap.summary import Summary

_T = TypeVar("_T")
_R = TypeVar("_R")

# Peak memory of reading a chunk, as a multiple of the bytes its rows take as
# tables: the table being built, the chunk being decompressed and the ranged
# read that fetched it (``mcap_io.ChunkReads``). Reads of one 128 MiB table
# peaked at 1.14 to 1.20 times their tables.
_READ_PEAK_FACTOR = 1.2

# A ``MessageIndex`` record after a chunk: opcode (1), length (8), channel id
# (2) and entries length (4), then a log time and an offset (16) per message.
_MESSAGE_INDEX_HEADER_BYTES = 15
_MESSAGE_INDEX_ENTRY_BYTES = 16

# Summaries read concurrently within one listing task. A summary read is an
# open and one to three small ranged reads, so a listing waits on request
# latency. Up to twice this many read summaries are held at once.
_DEFAULT_SUMMARY_IO_CONCURRENCY = env_integer(
    "RAY_DATA_MCAP_SUMMARY_IO_CONCURRENCY", 32
)


@dataclass(frozen=True)
class ListedSummary:
    """A file's summary as the listing reads it: parsed, and as raw records.

    ``section`` is the raw summary section, from which the listing copies the
    records each read task needs into the manifest (``mcap_task_summary``).
    """

    summary: "Summary"
    section: memoryview


def read_summary(filesystem: FileSystem, path: str) -> Optional["Summary"]:
    """Return the summary of the MCAP file at ``path``, or ``None`` if it has none.

    See ``mcap_io.read_summary_from``.
    """
    with filesystem.open_input_file(path) as f:
        return read_summary_from(f, path)


def read_listed_summary(filesystem: FileSystem, path: str) -> Optional[ListedSummary]:
    """Like ``read_summary``, but also keep the raw summary section."""
    with filesystem.open_input_file(path) as f:
        section = read_summary_section_from(f, path)
    if section is None:
        return None
    return ListedSummary(parse_summary_records(section, path), section)


def _read_with_retry(
    read: Callable[["FileSystem", str], _R],
    filesystem: "FileSystem",
    path: str,
    retried_io_errors: List[str],
) -> _R:
    """Read one file's summary, retrying errors that match ``retried_io_errors``."""
    return call_with_retry(
        lambda: read(filesystem, path),
        description=f"read MCAP summary for {path}",
        match=retried_io_errors,
    )


def read_summaries(
    filesystem: "FileSystem",
    items: Iterable[_T],
    path: Callable[[_T], str],
    io_concurrency: int = _DEFAULT_SUMMARY_IO_CONCURRENCY,
) -> Iterator[Tuple[_T, "Future[Optional[Summary]]"]]:
    """Read the summaries of ``items`` several at a time, yielding them in order.

    Each item comes with its pending read, so the caller decides what a failed
    read means. A bounded window of in-flight reads keeps memory flat for a long
    listing.
    """
    return _read_concurrently(read_summary, filesystem, items, path, io_concurrency)


def read_listed_summaries(
    filesystem: "FileSystem",
    items: Iterable[_T],
    path: Callable[[_T], str],
    io_concurrency: int = _DEFAULT_SUMMARY_IO_CONCURRENCY,
) -> Iterator[Tuple[_T, "Future[Optional[ListedSummary]]"]]:
    """``read_summaries``, keeping each raw section (``read_listed_summary``)."""
    return _read_concurrently(
        read_listed_summary, filesystem, items, path, io_concurrency
    )


def _read_concurrently(
    read: Callable[["FileSystem", str], _R],
    filesystem: "FileSystem",
    items: Iterable[_T],
    path: Callable[[_T], str],
    io_concurrency: int,
) -> Iterator[Tuple[_T, "Future[_R]"]]:
    """Run ``read`` on the path of each item, in a bounded window, in order."""
    from ray.data.context import DataContext

    retried_io_errors = DataContext.get_current().retried_io_errors
    window = max(1, io_concurrency * 2)
    pending: Deque[Tuple[_T, Future]] = deque()
    with ThreadPoolExecutor(max_workers=max(1, io_concurrency)) as pool:
        for item in items:
            summary_read = pool.submit(
                _read_with_retry, read, filesystem, path(item), retried_io_errors
            )
            pending.append((item, summary_read))
            if len(pending) >= window:
                yield pending.popleft()
        while pending:
            yield pending.popleft()


def chunk_unit_id(path: str, chunk_start_offset: int) -> str:
    """Stable ``ReadUnit.id`` of one chunk: the file plus the chunk's byte offset.

    The offset names the chunk for as long as the file is not rewritten. The
    indexer leaves out a chunk whose id is in ``excluded_read_unit_ids``.
    """
    return f"{path}#c={chunk_start_offset}"


def message_row_id(path: str, chunk_start_offset: int, index_in_chunk: int) -> str:
    """Deterministic id of one message: its chunk and its position in the chunk.

    ``index_in_chunk`` counts every message of the chunk, selected or not, so
    the id does not depend on the read's filters.
    """
    return f"{path}#c={chunk_start_offset}:{index_in_chunk}"


def unindexed_message_row_id(path: str, ordinal: int) -> str:
    """Deterministic id of one message in a file without a chunk index.

    The id is the message's position among all messages of the file, selected
    or not.
    """
    return f"{path}#m={ordinal}"


def estimate_chunk_rows(
    chunk_index: "ChunkIndex",
    statistics: Optional["Statistics"],
    total_uncompressed_size: int,
) -> int:
    """Estimate how many messages a chunk holds.

    The chunk's message indexes hold 16 bytes per message, so their length
    gives the count of every channel. A file written without them has the count
    from ``Statistics`` split across chunks in proportion to uncompressed size.
    The listing row marks the count inexact (``fully_matched=False``), since it
    includes unselected channels, so a pushed-down limit never relies on it.
    """
    channels = len(chunk_index.message_index_offsets)
    indexed = chunk_index.message_index_length - _MESSAGE_INDEX_HEADER_BYTES * channels
    if channels and indexed > 0:
        return indexed // _MESSAGE_INDEX_ENTRY_BYTES
    if (
        statistics is None
        or statistics.message_count == 0
        or total_uncompressed_size <= 0
    ):
        return 0
    share = chunk_index.uncompressed_size / total_uncompressed_size
    return max(1, round(statistics.message_count * share))


def chunk_run(
    chunk_index: "ChunkIndex",
    statistics: Optional["Statistics"],
    total_uncompressed_size: int,
    row_bytes: int,
) -> FileChunk:
    """Build the listing row for one chunk.

    ``size_bytes`` estimates the peak memory of reading the chunk, which is
    what the partitioner budgets on: ``_READ_PEAK_FACTOR`` times the bytes its
    rows take as tables. Those are the chunk's uncompressed records plus
    ``row_bytes`` per message for the columns a row adds (see
    ``MessageRowLayout.row_bytes``). A chunk that records no size counts as one
    byte.
    """
    num_rows = estimate_chunk_rows(chunk_index, statistics, total_uncompressed_size)
    table_bytes = chunk_index.uncompressed_size + num_rows * row_bytes
    return FileChunk(
        unit_ids=(chunk_index.chunk_start_offset,),
        num_rows=num_rows,
        size_bytes=max(math.ceil(_READ_PEAK_FACTOR * table_bytes), 1),
        fully_matched=False,
    )
