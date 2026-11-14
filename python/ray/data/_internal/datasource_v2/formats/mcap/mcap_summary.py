"""An MCAP file's summary, and the names of its chunks and messages.

An MCAP file can end with a summary section, found through the footer. It holds
the schemas and channels, one ``ChunkIndex`` per chunk (byte offset, sizes,
log-time bounds, channels) and a ``Statistics`` record (message counts, time
bounds). Reading it takes one to three ranged reads (``mcap_io``), so a read can
be planned without reading any message. ``read_summaries`` fetches many files'
sections concurrently within a byte budget, and ``chunk_runs`` weighs each chunk
by the peak memory of the read task that reads it.

Format specification: https://mcap.dev/spec
"""

import itertools
import math
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

from pyarrow.fs import FileSystem

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    ChunkReads,
    SummaryTail,
    parse_summary_records,
    read_section,
    read_summary_from,
    read_tail,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    from mcap.records import ChunkIndex, Statistics
    from mcap.summary import Summary

_T = TypeVar("_T")

# Terms of a read task's peak memory (``chunk_runs``). The parsed sizes are
# upper bounds for CPython 3.10 to 3.12 with mcap 1.x.
# A message record's bytes besides its payload: opcode (1), length (8),
# channel id (2), sequence (4), and log and publish times (16).
_MESSAGE_RECORD_HEADER_BYTES = 31
# Bytes one parsed ``mcap.records.Message`` holds beyond its payload.
_PARSED_MESSAGE_BYTES = 295
# Bytes one entry of the log-time merge's heap holds, beyond the path in its
# ``row_id`` (``mcap_chunks``).
_HEAP_ENTRY_BYTES = 250
# Most spare capacity of a ``bytearray`` grown by ``+=``, as a share of its bytes.
_BUFFER_GROWTH = 1 / 8

# A ``MessageIndex`` record after a chunk: opcode (1), length (8), channel id
# (2) and entries length (4), then a log time and an offset (16) per message.
_MESSAGE_INDEX_HEADER_BYTES = 15
_MESSAGE_INDEX_ENTRY_BYTES = 16

# Summary tail reads in flight within one listing task. A read is an open and
# one or two small ranged reads, so a listing waits on request latency. Up to
# twice this many fetched sections are held at once (``read_summaries``).
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


def parse_listed_summary(
    section: Optional[memoryview], path: str
) -> Optional[ListedSummary]:
    """Parse a section that ``read_summaries`` fetched; ``None`` stays ``None``."""
    if section is None:
        return None
    return ListedSummary(parse_summary_records(section, path), section)


def read_summary(filesystem: FileSystem, path: str) -> Optional["Summary"]:
    """Return the summary of the MCAP file at ``path``, or ``None`` if it has none.

    See ``mcap_io.read_summary_from``.
    """
    with filesystem.open_input_file(path) as f:
        return read_summary_from(f, path)


def read_summaries(
    filesystem: "FileSystem",
    items: Iterable[_T],
    path: Callable[[_T], str],
    budget_bytes: int,
    io_concurrency: int = _DEFAULT_SUMMARY_IO_CONCURRENCY,
) -> Iterator[Tuple[_T, "Future[Optional[memoryview]]"]]:
    """Fetch the summary sections of ``items`` concurrently, yielding them in order.

    Each item comes with its pending section, ``None`` for a file without one.
    Threads only fetch; the caller parses (``parse_listed_summary``), since
    parsing holds the GIL. Sections are fetched in file order while they fit
    ``budget_bytes``, and the next file always fits when nothing is held. A
    section counts against the budget until the caller asks for the next item.
    """
    from ray.data.context import DataContext

    retried_io_errors = DataContext.get_current().retried_io_errors
    queue = iter(items)
    with ThreadPoolExecutor(max_workers=max(1, io_concurrency)) as pool:
        window = _SectionWindow(
            pool, filesystem, retried_io_errors, budget_bytes, io_concurrency
        )
        while True:
            window.start_tails(queue, path)
            window.admit_sections()
            fetched = window.next_section()
            if fetched is None:
                return
            yield fetched.item, fetched.read
            window.release(fetched)


@dataclass(frozen=True)
class _Fetch(Generic[_T]):
    """One item's pending read: its tail, or its section of ``nbytes`` bytes."""

    item: _T
    read: "Future[Any]"
    nbytes: int = 0


class _SectionWindow(Generic[_T]):
    """The tails and sections ``read_summaries`` holds, in file order."""

    def __init__(
        self,
        pool: ThreadPoolExecutor,
        filesystem: "FileSystem",
        retried_io_errors: List[str],
        budget_bytes: int,
        io_concurrency: int,
    ):
        self._pool = pool
        self._filesystem = filesystem
        self._retried = retried_io_errors
        self._budget = budget_bytes
        self._concurrency = max(1, io_concurrency)
        self._tails: Deque[_Fetch[_T]] = deque()
        self._sections: Deque[_Fetch[_T]] = deque()
        # Bytes of the sections admitted and not yet released.
        self._held = 0

    def start_tails(self, queue: Iterator[_T], path: Callable[[_T], str]) -> None:
        """Keep ``io_concurrency`` tail reads in flight."""
        for item in itertools.islice(queue, self._concurrency - len(self._tails)):
            read = self._pool.submit(
                _read_tail, self._filesystem, path(item), self._retried
            )
            self._tails.append(_Fetch(item, read))

    def admit_sections(self) -> None:
        """Start section reads in file order while their sections fit the budget."""
        while self._tails and len(self._sections) < 2 * self._concurrency:
            tail = self._tails[0]
            if self._sections and not tail.read.done():
                return  # Hand out a fetched section rather than wait.
            nbytes = _section_bytes(tail.read)
            if self._sections and self._held + nbytes > self._budget:
                return
            self._tails.popleft()
            self._held += nbytes
            read = self._pool.submit(
                _read_section, self._filesystem, tail.read, self._retried
            )
            self._sections.append(_Fetch(tail.item, read, nbytes))

    def next_section(self) -> Optional[_Fetch[_T]]:
        """The next admitted section in file order, or ``None`` when all are out."""
        return self._sections.popleft() if self._sections else None

    def release(self, fetched: _Fetch[_T]) -> None:
        """Stop counting a section the caller is done with."""
        self._held -= fetched.nbytes


def _read_tail(
    filesystem: "FileSystem", path: str, retried_io_errors: List[str]
) -> SummaryTail:
    """Read one file's tail, retrying errors that match ``retried_io_errors``."""

    def read() -> SummaryTail:
        with filesystem.open_input_file(path) as f:
            return read_tail(f, path)

    return call_with_retry(
        read, description=f"read MCAP summary for {path}", match=retried_io_errors
    )


def _read_section(
    filesystem: "FileSystem",
    tail_read: "Future[SummaryTail]",
    retried_io_errors: List[str],
) -> Optional[memoryview]:
    """Read the section a finished tail read locates, or raise the tail's error."""
    tail = tail_read.result()
    if tail.holds_section:
        return read_section(None, tail)

    def read() -> Optional[memoryview]:
        with filesystem.open_input_file(tail.path) as f:
            return read_section(f, tail)

    return call_with_retry(
        read,
        description=f"read MCAP summary for {tail.path}",
        match=retried_io_errors,
    )


def _section_bytes(tail_read: "Future[SummaryTail]") -> int:
    """The length of the section a tail read locates; 0 when the read failed."""
    if tail_read.exception() is not None:
        return 0
    return tail_read.result().section_bytes


def topic_chunks(summary: "Summary", channel_ids: Set[int]) -> List["ChunkIndex"]:
    """The chunks that may hold a topic whose channels are ``channel_ids``.

    A chunk may hold it when its message indexes name one of those channels, or
    name none (``MCAPSelection.chunk_may_match``). A summary that lists no channel cannot tell, so every chunk may.
    One that lists others but none of the topic's says the file lacks the topic.
    """
    if not summary.channels:
        return list(summary.chunk_indexes)
    if not channel_ids:
        return []
    any_time = MCAPSelection()
    return [
        c for c in summary.chunk_indexes if any_time.chunk_may_match(c, channel_ids)
    ]


def channel_ids_by_topic(summary: "Summary") -> Dict[str, Set[int]]:
    """The ids of each topic's channels in ``summary``."""
    by_topic: Dict[str, Set[int]] = {}
    for channel_id, channel in summary.channels.items():
        by_topic.setdefault(channel.topic, set()).add(channel_id)
    return by_topic


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


@dataclass(frozen=True)
class ReadMemory:
    """How the listing sizes a read task's memory, to weigh its chunks.

    Attributes:
        task_bytes: The partitioner's budget per read task.
        log_time_order: Whether the read merges chunks by log time.
        ranged_read_bytes: Most bytes one ranged read spans.
    """

    task_bytes: int
    log_time_order: bool
    ranged_read_bytes: int = ChunkReads().max_bytes


def chunk_runs(
    path: str,
    summary: "Summary",
    chunk_indexes: Sequence["ChunkIndex"],
    row_bytes: int,
    memory: ReadMemory,
    payload_factor: int = 1,
) -> List[FileChunk]:
    """Build the listing rows of one file's ``chunk_indexes``, weighted for packing.

    A task's peak is its table plus W + R (``read_overhead``). A chunk's share
    of the table is its uncompressed records plus ``row_bytes`` per message,
    plus ``_BUFFER_GROWTH``. Each payload byte counts ``payload_factor`` bytes,
    for payloads the reader decodes into larger values. Its row weighs that share times
    ``task_bytes / (task_bytes - W - R)``, so a full task leaves room for W + R,
    or a whole task when W + R fills one.
    """
    total_uncompressed = sum(c.uncompressed_size for c in summary.chunk_indexes)
    counts = [
        estimate_chunk_rows(c, summary.statistics, total_uncompressed)
        for c in chunk_indexes
    ]
    room = memory.task_bytes - read_overhead(path, chunk_indexes, counts, memory)
    runs = []
    for chunk_index, num_rows in zip(chunk_indexes, counts):
        payload = max(
            0, chunk_index.uncompressed_size - num_rows * _MESSAGE_RECORD_HEADER_BYTES
        )
        records = chunk_index.uncompressed_size + (payload_factor - 1) * payload
        table = (records + num_rows * row_bytes) * (1 + _BUFFER_GROWTH)
        weight = table * memory.task_bytes / room if room > 0 else memory.task_bytes
        runs.append(
            FileChunk(
                unit_ids=(chunk_index.chunk_start_offset,),
                num_rows=num_rows,
                size_bytes=max(math.ceil(weight), 1),
                fully_matched=False,
            )
        )
    return runs


def read_overhead(
    path: str,
    chunk_indexes: Sequence["ChunkIndex"],
    counts: Sequence[int],
    memory: ReadMemory,
) -> int:
    """W + R of ``chunk_runs``: the largest chunk working set, plus one ranged
    read when the chunks share reads."""
    working_set = max(
        _working_set(c, n, path, memory.log_time_order)
        for c, n in zip(chunk_indexes, counts)
    )
    return working_set + _ranged_read(chunk_indexes, memory.ranged_read_bytes)


def _working_set(
    chunk_index: "ChunkIndex", num_messages: int, path: str, log_time_order: bool
) -> int:
    """Most bytes one chunk holds while it is read, its rows aside.

    The chunk record is held throughout, with either its span of the ranged
    read or its decompressed and parsed messages, plus a heap entry each when
    merging by log time.
    """
    payload = max(
        0, chunk_index.uncompressed_size - num_messages * _MESSAGE_RECORD_HEADER_BYTES
    )
    decompressed = chunk_index.uncompressed_size if chunk_index.compression else 0
    per_message = _PARSED_MESSAGE_BYTES
    if log_time_order:
        per_message += _HEAP_ENTRY_BYTES + len(path)
    parsed = decompressed + payload + num_messages * per_message
    return chunk_index.compressed_size + max(chunk_index.chunk_length, parsed)


def _ranged_read(chunk_indexes: Sequence["ChunkIndex"], max_bytes: int) -> int:
    """Most bytes a ranged read holds while chunks shorter than ``max_bytes``
    share it, within the chunks' extent (``mcap_io.plan_chunk_reads``)."""
    if sum(c.chunk_length < max_bytes for c in chunk_indexes) < 2:
        return 0
    start = min(c.chunk_start_offset for c in chunk_indexes)
    end = max(c.chunk_start_offset + c.chunk_length for c in chunk_indexes)
    return min(max_bytes, end - start)
