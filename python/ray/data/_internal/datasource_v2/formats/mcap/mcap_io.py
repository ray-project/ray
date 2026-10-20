"""Reads of an MCAP file's summary and chunk records, in few requests.

mcap's readers parse a record a few bytes per ``read``. On remote storage each
read of a raw file is one request, so parsing a summary that way takes several
requests per chunk it indexes. Here the bytes are fetched first and parsed in
memory:

- ``read_summary_section_from`` checks both magic bytes and the footer, then
  returns the summary section, all in one to three ranged reads.
  ``read_summary_from`` also parses it.
- ``summary_records`` splits a section into its raw records, and
  ``parse_summary_records`` parses records copied out of a section.
- ``read_chunks`` fetches chunk records that lie close together in one ranged
  read, as ``ChunkReads`` allows; ``read_chunk`` fetches one chunk record.
- ``read_message_index`` fetches one channel's Message Index record of a chunk
  in one ranged read.
- ``buffered`` wraps a file for a linear scan, so it reads in large blocks.

Format specification: https://mcap.dev/spec
"""

import io
import struct
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from ray._common.utils import env_integer
from ray.data._internal.util import KiB, MiB

if TYPE_CHECKING:
    from mcap.records import Chunk, ChunkIndex
    from mcap.summary import Summary

# The first summary read takes this many bytes from the end of the file: the
# footer and, when it fits, the whole summary section. A larger section takes
# one more read.
_SUMMARY_TAIL_BYTES = 64 * KiB

# Defaults of ``ChunkReads``. A request to object storage costs a fixed delay
# (16-32 ms on S3) plus its bytes over the connection's bandwidth (about
# 64 MiB/s). Reading through a gap costs less than the request it saves while
# the gap is under delay x bandwidth: 1 MiB at 16 ms. A 16 MiB read spends about
# 90% of its time moving bytes. A first read of 4 MiB keeps a read that stops
# early, such as under a row limit, from fetching far more than it parses.
_CHUNK_READ_BYTES = env_integer("RAY_DATA_MCAP_CHUNK_READ_BYTES", 16 * MiB)
_CHUNK_READ_GAP_BYTES = env_integer("RAY_DATA_MCAP_CHUNK_READ_GAP_BYTES", 1 * MiB)
_CHUNK_READ_FIRST_BYTES = env_integer("RAY_DATA_MCAP_CHUNK_READ_FIRST_BYTES", 4 * MiB)

_MAGIC = b"\x89MCAP0\r\n"
# A footer record is the opcode, an 8-byte length and 20 bytes of fields:
# ``summary_start`` (8), ``summary_offset_start`` (8) and ``summary_crc`` (4).
_FOOTER_OPCODE = 0x02
_FOOTER_BYTES = 1 + 8 + 20
_RECORD_PREFIX_BYTES = 1 + 8  # opcode and record length


@dataclass(frozen=True)
class ChunkReads:
    """How ``read_chunks`` groups chunk records into ranged reads.

    Attributes:
        max_bytes: Most bytes one read spans, gaps included. A chunk record
            longer than this is read alone, and 0 reads every chunk alone.
            A read task holds one read's bytes at a time.
        max_gap_bytes: Longest gap between two chunk records that one read
            spans; the gap's bytes are read and dropped. Neighbouring chunks
            are apart by their message index records (16 bytes per message),
            and a chunk the read does not need is a gap as large as itself.
        first_bytes: Most bytes the first read of a file spans. Each later
            read may span twice as many as the one before, up to
            ``max_bytes``, so a read that stops early fetches little.
    """

    max_bytes: int = _CHUNK_READ_BYTES
    max_gap_bytes: int = _CHUNK_READ_GAP_BYTES
    first_bytes: int = _CHUNK_READ_FIRST_BYTES


@dataclass(frozen=True)
class RangedRead:
    """Bytes ``[start, end)`` of a file, which hold the ``chunk_indexes``' records."""

    start: int
    end: int
    chunk_indexes: Tuple["ChunkIndex", ...]


def plan_chunk_reads(
    chunk_indexes: Iterable["ChunkIndex"], reads: ChunkReads
) -> List[RangedRead]:
    """Group chunks, taken in the order given, into ranged reads.

    A chunk joins the read before it if it starts at or after that read's end,
    at most ``max_gap_bytes`` past it, and the read then spans at most its
    size limit: ``first_bytes`` for the first read, then twice the previous
    limit, up to ``max_bytes``. Otherwise it starts a new read.
    """
    planned: List[RangedRead] = []
    group: List["ChunkIndex"] = []
    limit = min(reads.first_bytes, reads.max_bytes)
    for chunk_index in chunk_indexes:
        if group and not _extends(group, chunk_index, reads.max_gap_bytes, limit):
            planned.append(_ranged_read(group))
            group = []
            limit = min(2 * limit, reads.max_bytes)
        group.append(chunk_index)
    if group:
        planned.append(_ranged_read(group))
    return planned


# Takes a chunk that cannot be decoded and the error, in a read that skips
# corrupt chunks (``SelectedMessageReader.iter_chunks``).
OnCorrupt = Callable[["ChunkIndex", Exception], None]


def read_chunks(
    f: Any,
    chunk_indexes: Iterable["ChunkIndex"],
    reads: ChunkReads,
    on_corrupt: Optional[OnCorrupt] = None,
) -> Iterator[Tuple["ChunkIndex", Optional["Chunk"]]]:
    """Yield the chunk record of each of ``chunk_indexes``, in that order.

    Each read of ``plan_chunk_reads`` is made when its first chunk is needed,
    and its bytes are released once its last chunk is parsed. With
    ``on_corrupt``, a chunk record that does not parse is handed to it and
    yielded as ``None``; an error reading ``f`` still raises.
    """
    for ranged_read in plan_chunk_reads(chunk_indexes, reads):
        # ``BytesIO`` shares the bytes rather than copying them; parsing a
        # chunk copies only the chunk's records.
        stream = io.BytesIO(_read_range(f, ranged_read.start, ranged_read.end))
        last = len(ranged_read.chunk_indexes) - 1
        for i, chunk_index in enumerate(ranged_read.chunk_indexes):
            position = chunk_index.chunk_start_offset - ranged_read.start
            chunk = _parse_chunk_or_report(stream, position, chunk_index, on_corrupt)
            if i == last:
                # Free the bytes now, not when the next read replaces them, so
                # two reads are never held at once.
                stream.close()
            yield chunk_index, chunk


def read_summary_from(f: Any, path: str) -> Optional["Summary"]:
    """Return the summary of the open MCAP file ``f``, or ``None`` if it has none.

    The summary section is optional. A file without one cannot be split or
    pruned, and is read whole by a linear scan.

    Raises ``ValueError`` naming ``path`` when the file does not start and end
    like an MCAP file or its summary section does not parse, as in a truncated
    recording. Errors reading ``f`` propagate unchanged, so callers can retry
    them.
    """
    section = read_summary_section_from(f, path)
    return None if section is None else _parse_summary(section, path)


def read_summary_section_from(f: Any, path: str) -> Optional[memoryview]:
    """Return the raw summary section of the open MCAP file ``f``, or ``None``.

    The section runs from the first summary record through the footer. Raises
    as ``read_summary_from`` does, except that the records are not parsed.
    """
    size = f.seek(0, io.SEEK_END)
    tail_start = max(0, size - _SUMMARY_TAIL_BYTES)
    tail = _read_range(f, tail_start, size)
    head = tail if tail_start == 0 else _read_range(f, 0, len(_MAGIC))
    if not head.startswith(_MAGIC):
        raise ValueError(
            f"{path!r} is not an MCAP file: it does not start with the MCAP "
            "magic bytes."
        )
    summary_start = _summary_start(tail, size, path)
    if summary_start == 0:
        return None
    if summary_start < tail_start:
        tail = _read_range(f, summary_start, tail_start) + tail
        tail_start = summary_start
    end = len(tail) - len(_MAGIC)
    return memoryview(tail)[summary_start - tail_start : end]


def summary_records(section: memoryview) -> Iterator[Tuple[int, memoryview]]:
    """Yield ``(opcode, record)`` for each record of ``section`` before its footer.

    ``record`` is the whole raw record: opcode, length and body. The framing is
    not checked here, so split only a section that parsed.
    """
    position = 0
    while position < len(section):
        opcode = section[position]
        if opcode == _FOOTER_OPCODE:
            return
        length = int.from_bytes(section[position + 1 : position + 9], "little")
        end = position + _RECORD_PREFIX_BYTES + length
        yield opcode, section[position:end]
        position = end


def parse_summary_records(records: Union[bytes, memoryview], path: str) -> "Summary":
    """Parse a summary section of ``path``, or records copied out of one.

    Copied records are whole raw records, as ``summary_records`` yields them,
    and end without a footer. Raises as ``read_summary_from`` does.
    """
    return _parse_summary(memoryview(records), path)


def read_chunk(f: Any, chunk_index: "ChunkIndex") -> "Chunk":
    """Read the chunk record that ``chunk_index`` points to, in one ranged read."""
    start = chunk_index.chunk_start_offset
    stream = io.BytesIO(_read_range(f, start, start + chunk_index.chunk_length))
    return _parse_chunk(stream, 0)


def _parse_chunk_or_report(
    stream: io.BytesIO,
    position: int,
    chunk_index: "ChunkIndex",
    on_corrupt: Optional[OnCorrupt],
) -> Optional["Chunk"]:
    """``_parse_chunk``, or ``None`` after handing a corrupt record to
    ``on_corrupt``. Without ``on_corrupt`` the error raises."""
    try:
        return _parse_chunk(stream, position)
    except Exception as e:  # noqa: BLE001 - any parse failure is a corrupt record
        if on_corrupt is None:
            raise
        on_corrupt(chunk_index, e)
        return None


def _parse_chunk(stream: io.BytesIO, position: int) -> "Chunk":
    """Parse the chunk record that starts at ``position`` of ``stream``."""
    from mcap.data_stream import ReadDataStream
    from mcap.records import Chunk

    stream.seek(position + _RECORD_PREFIX_BYTES)
    return Chunk.read(ReadDataStream(stream))


def _record_end(chunk_index: "ChunkIndex") -> int:
    """The offset just past the chunk record that ``chunk_index`` points to."""
    return chunk_index.chunk_start_offset + chunk_index.chunk_length


def _extends(
    group: List["ChunkIndex"], chunk_index: "ChunkIndex", max_gap: int, limit: int
) -> bool:
    """Whether ``chunk_index`` can join the read of ``group`` (``plan_chunk_reads``)."""
    end = _record_end(group[-1])
    return (
        end <= chunk_index.chunk_start_offset <= end + max_gap
        and _record_end(chunk_index) - group[0].chunk_start_offset <= limit
    )


def _ranged_read(group: List["ChunkIndex"]) -> RangedRead:
    """The read that spans the chunk records of ``group``, in file order."""
    return RangedRead(group[0].chunk_start_offset, _record_end(group[-1]), tuple(group))


def read_message_index(
    f: Any, chunk_index: "ChunkIndex", channel_id: int
) -> List[Tuple[int, int]]:
    """The ``(log_time, offset)`` entries of a channel's Message Index record.

    The records follow their chunk back to back, so one ends where the next
    starts, or where the chunk's message index section ends: one ranged read.
    A record longer than that takes a second read. Empty when the record at
    the indexed offset is not a Message Index.
    """
    from mcap.data_stream import ReadDataStream
    from mcap.opcode import Opcode
    from mcap.records import MessageIndex

    offsets = chunk_index.message_index_offsets
    start = offsets[channel_id]
    section_end = (
        chunk_index.chunk_start_offset
        + chunk_index.chunk_length
        + chunk_index.message_index_length
    )
    end = min((o for o in offsets.values() if o > start), default=section_end)
    data = _read_range(f, start, max(end, start + _RECORD_PREFIX_BYTES))
    opcode, length = struct.unpack_from("<BQ", data)
    if opcode != Opcode.MESSAGE_INDEX:
        return []
    body_end = _RECORD_PREFIX_BYTES + length
    if body_end > len(data):
        data += _read_range(f, start + len(data), start + body_end)
    body = io.BytesIO(data[_RECORD_PREFIX_BYTES:body_end])
    return MessageIndex.read(ReadDataStream(body)).records


def buffered(f: Any, buffer_size: int) -> io.BufferedReader:
    """``f`` behind a read buffer of ``buffer_size`` bytes, for a linear scan.

    Closing or dropping the buffer leaves ``f`` open; its owner closes it.
    """
    return io.BufferedReader(_RawView(f), buffer_size=buffer_size)


def _read_range(f: Any, start: int, end: int) -> bytes:
    """Bytes ``[start, end)`` of ``f``, in one read."""
    f.seek(start)
    return f.read(end - start)


def _summary_start(tail: bytes, size: int, path: str) -> int:
    """The footer's ``summary_start``, after checking the end of the file.

    ``tail`` is the last bytes of the file, which is ``size`` bytes long: they
    must end with the footer record and then the magic bytes.
    """
    footer_start = len(tail) - len(_MAGIC) - _FOOTER_BYTES
    footer = tail[max(0, footer_start) : len(tail) - len(_MAGIC)]
    length = int.from_bytes(footer[1:9], "little")
    if (
        footer_start < 0
        or not tail.endswith(_MAGIC)
        or footer[0] != _FOOTER_OPCODE
        or length != _FOOTER_BYTES - _RECORD_PREFIX_BYTES
    ):
        raise ValueError(
            f"{path!r} is not a complete MCAP file: it does not end with a footer "
            "and the MCAP magic bytes. The recording may have been cut short."
        )
    summary_start = int.from_bytes(footer[9:17], "little")
    if summary_start > size - len(_MAGIC) - _FOOTER_BYTES:
        raise ValueError(
            f"The footer of MCAP file {path!r} puts the summary at byte "
            f"{summary_start}, past the end of the file's {size} bytes."
        )
    return summary_start


def _parse_summary(section: memoryview, path: str) -> "Summary":
    """Collect the records of a summary section, up to its footer or its end.

    Raises ``ValueError`` naming ``path`` when the section does not parse.
    """
    try:
        return _collect_summary_records(section)
    except Exception as e:
        raise ValueError(
            f"Cannot parse the summary section of MCAP file {path!r} "
            f"({_describe(e)}). The file may be truncated or corrupt."
        ) from e


def _collect_summary_records(section: memoryview) -> "Summary":
    """The summary records of ``section``, gathered as mcap's readers do."""
    from mcap.records import (
        AttachmentIndex,
        Channel,
        ChunkIndex,
        Footer,
        MetadataIndex,
        Schema,
        Statistics,
    )
    from mcap.stream_reader import StreamReader
    from mcap.summary import Summary

    summary = Summary()
    if not section:
        return summary
    stream = io.BytesIO(section)
    for record in StreamReader(stream, skip_magic=True).records:
        if isinstance(record, Footer):
            break
        if isinstance(record, Statistics):
            summary.statistics = record
        elif isinstance(record, Schema):
            summary.schemas[record.id] = record
        elif isinstance(record, Channel):
            summary.channels[record.id] = record
        elif isinstance(record, ChunkIndex):
            summary.chunk_indexes.append(record)
        elif isinstance(record, AttachmentIndex):
            summary.attachment_indexes.append(record)
        elif isinstance(record, MetadataIndex):
            summary.metadata_indexes.append(record)
        # Records copied out of a section end without a footer. The reader has
        # consumed exactly the records yielded so far.
        if stream.tell() == len(section):
            break
    return summary


def _describe(e: Exception) -> str:
    """``Type: message``, or just ``Type`` for an exception without a message."""
    return f"{type(e).__name__}: {e}" if str(e) else type(e).__name__


class _RawView(io.RawIOBase):
    """``f`` as a raw stream for ``io.BufferedReader``, which never closes it."""

    def __init__(self, f: Any):
        self._f = f

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._f.seek(offset, whence)

    def tell(self) -> int:
        return self._f.tell()

    def readinto(self, buffer: Any) -> int:
        return self._f.readinto(buffer)
