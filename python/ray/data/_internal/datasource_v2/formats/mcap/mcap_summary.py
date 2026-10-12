"""An MCAP file's summary, and the names of its chunks and messages.

An MCAP file can end with a summary section, found through the footer. It holds
the schemas and channels, one ``ChunkIndex`` per chunk (byte offset, sizes,
log-time bounds, channels) and a ``Statistics`` record (message counts, time
bounds). Reading it takes two seeks, so a read can be planned without reading
any message. ``read_summaries`` reads the summaries of many files concurrently,
with retries.

Format specification: https://mcap.dev/spec
"""

from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from pyarrow.fs import FileSystem

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    from mcap.records import ChunkIndex, Statistics
    from mcap.summary import Summary

_T = TypeVar("_T")

# Summaries read concurrently within one listing task. A summary read is two
# small ranged requests, so it is latency-bound on remote storage.
_DEFAULT_SUMMARY_IO_CONCURRENCY = env_integer(
    "RAY_DATA_MCAP_SUMMARY_IO_CONCURRENCY", 16
)


def read_summary(filesystem: FileSystem, path: str) -> Optional["Summary"]:
    """Return the summary of the MCAP file at ``path``, or ``None`` if it has none.

    The summary section is optional. A file without one cannot be split or
    pruned, and is read whole by a linear scan.
    """
    from mcap.reader import SeekingReader

    with filesystem.open_input_file(path) as f:
        return SeekingReader(f).get_summary()


def _read_summary_with_retry(
    filesystem: "FileSystem", path: str, retried_io_errors: List[str]
) -> Optional["Summary"]:
    """Read one file's summary, retrying errors that match ``retried_io_errors``."""
    return call_with_retry(
        lambda: read_summary(filesystem, path),
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
    from ray.data.context import DataContext

    retried_io_errors = DataContext.get_current().retried_io_errors
    window = max(1, io_concurrency * 2)
    pending: Deque[Tuple[_T, Future]] = deque()
    with ThreadPoolExecutor(max_workers=max(1, io_concurrency)) as pool:
        for item in items:
            summary_read = pool.submit(
                _read_summary_with_retry, filesystem, path(item), retried_io_errors
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


def topic_unit_id(path: str, topic: str) -> str:
    """Stable ``ReadUnit.id`` of one topic of a file, at topic granularity."""
    return f"{path}#t={topic}"


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

    A chunk index records sizes but no message count. The file's count from
    ``Statistics`` is split across chunks in proportion to uncompressed size.
    The listing row marks the estimate inexact (``fully_matched=False``), so a
    pushed-down limit never relies on it.
    """
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
) -> FileChunk:
    """Build the listing row for one chunk.

    ``size_bytes`` is the chunk's uncompressed size, which is what a task holds
    in memory to read it and what the partitioner budgets on. A chunk that
    records no size counts as one byte.
    """
    return FileChunk(
        unit_ids=(chunk_index.chunk_start_offset,),
        num_rows=estimate_chunk_rows(chunk_index, statistics, total_uncompressed_size),
        size_bytes=max(chunk_index.uncompressed_size, 1),
        fully_matched=False,
    )


def topic_run_metadata(
    chunk_indexes: "List[ChunkIndex]",
    topic: str,
    num_rows: int,
) -> Dict[str, Any]:
    """The listing row for one topic of a file, at topic granularity.

    A :class:`FileChunk` row with the ``topic`` added, so the reader knows
    which topic to read. At this granularity listing rows reach the reader
    unchanged, so the extra key survives.
    """
    run = FileChunk(
        unit_ids=tuple(c.chunk_start_offset for c in chunk_indexes),
        num_rows=num_rows,
        size_bytes=max(sum(c.uncompressed_size for c in chunk_indexes), 1),
        fully_matched=False,
    )
    metadata = run.to_metadata()
    metadata["topic"] = topic
    return metadata
