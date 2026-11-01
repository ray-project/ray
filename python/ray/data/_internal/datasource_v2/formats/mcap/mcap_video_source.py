"""What a decoding read task reads: its own messages and what its decoders need.

A task owns some chunks of a file, but a video decoder needs the frames back to
a keyframe. So, per video channel, a message task reads back up to the look-back
cap (``RAY_DATA_MCAP_MAX_LEAD_IN_S``) before its first owned message (the
lead-in), and before an owned message that follows another task's messages (a
gap). The channel's decoder starts at the last keyframe among them. With a cap
of four frames (K is a keyframe, p another picture)::

    frame       0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19 20 21 22 23
    /camera     K  p  p  p  p  K  p  p  p  p  K  p  p  p  p  K  p  p  p  p  K  p  p  p
    chunks      [     other task    ][  this task  ][     other task    ][  this task  ]
    reads                [ lead-in  ][    owned    ]         [   gap    ][    owned    ]

An H.264 or H.265 decoder also needs the stream's parameter sets before its
first picture. Most encoders repeat them on every keyframe. Some write them only
before the first one, so a decoder whose first keyframe comes without them reads
them where the stream starts (``StreamStart``): one more read, for such a stream
only (K* carries the parameter sets)::

    frame       0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19
    /camera     K* p  p  p  p  K  p  p  p  p  K  p  p  p  p  K  p  p  p  p
    chunks      [ first ][        other tasks         ][     this task     ]
    reads       [ start ]                  [ lead-in  ][      owned        ]

These reads lie in chunks the task does not own, so the source plans with the
file's whole summary, not the records a task carries (``mcap_task_summary``).
A sample read reads back to each camera's keyframe itself
(``mcap_decoded_samples``), and finds parameter sets with ``StreamStart`` too.
"""

import bisect
import itertools
import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
)

from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
    chunk_may_hold,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import buffered, read_chunk
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    NalFormat,
    VideoCodec,
    is_keyframe,
    parameter_set_kinds,
    parameter_set_types,
    parameter_sets,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import ChunkIndex
    from mcap.summary import Summary

logger = logging.getLogger(__name__)


def stream_start_chunks(summary: "Summary", channel_id: int) -> List["ChunkIndex"]:
    """The chunks where the channel's stream may start, in file order: those
    written without message indexes, which may hold any channel, up to the first
    whose index names the channel."""
    chunks = []
    for chunk in sorted(summary.chunk_indexes, key=lambda c: c.chunk_start_offset):
        if not chunk.message_index_offsets:
            chunks.append(chunk)
        elif channel_id in chunk.message_index_offsets:
            chunks.append(chunk)
            break
    return chunks


@dataclass(frozen=True)
class StreamStart:
    """Reads a video stream's parameter sets where the stream starts.

    A decoder whose first keyframe comes without them asks once
    (``FrameDecoder``). They are taken from the channel's messages up to its
    first keyframe, in the first chunk that holds the channel
    (``stream_start_chunks``), or from the start of a file without a chunk index.
    Such a file is read whole before its frames are decoded, so reading its start
    again leaves the task's own read alone.
    """

    f: Any
    path: str
    summary: Optional["Summary"]

    def read(self, channel_id: int, topic: str, codec: VideoCodec) -> bytes:
        """The parameter sets at the start of the channel's stream, with start codes.

        Warns, naming the file and the topic, when a kind is missing there or
        the records there do not parse. The frames then cannot be decoded until
        a keyframe that carries them.
        """
        try:
            sets = _first_parameter_sets(
                self._stream_head(channel_id), channel_id, codec
            )
        except OSError:
            raise  # an I/O failure: the task's read is retried
        except Exception as e:  # noqa: BLE001 - corrupt records: decode without them
            logger.debug("Cannot read the start of %r in %r: %s", topic, self.path, e)
            sets = b""
        if parameter_set_kinds(sets, codec) != parameter_set_types(codec):
            _warn_missing_parameter_sets(topic, self.path, codec)
        return sets

    def _stream_head(self, channel_id: int) -> Iterable[Any]:
        """The records where the channel's stream starts, in file order: those
        of the first chunk with a message on the channel."""
        from mcap.records import Message
        from mcap.stream_reader import breakup_chunk

        if self.summary is None or not self.summary.chunk_indexes:
            return _records_from_start(self.f)
        for chunk in stream_start_chunks(self.summary, channel_id):
            records = list(breakup_chunk(read_chunk(self.f, chunk)))
            if any(
                isinstance(r, Message) and r.channel_id == channel_id for r in records
            ):
                return records
        return []


def _first_parameter_sets(
    records: Iterable[Any], channel_id: int, codec: VideoCodec
) -> bytes:
    """The parameter sets in the channel's messages up to its first keyframe.

    Each kind comes from the first message that holds it. Length-prefixed NAL
    units are read as such (``NalFormat``).
    """
    from mcap.records import Message

    needed = parameter_set_types(codec)
    nal_format = NalFormat(codec)
    found: List[bytes] = []
    kinds: Set[int] = set()
    for record in records:
        if not isinstance(record, Message) or record.channel_id != channel_id:
            continue
        payload = nal_format.annex_b(record.data)
        sets = parameter_sets(payload, codec, skip=kinds)
        found.append(sets)
        kinds |= parameter_set_kinds(sets, codec)
        if kinds >= needed or is_keyframe(payload, codec):
            break
    return b"".join(found)


def _records_from_start(f: Any) -> Iterator[Any]:
    """The records of ``f`` from its start, read through a buffer of
    ``DataContext.streaming_read_buffer_size`` bytes."""
    from mcap.stream_reader import StreamReader

    from ray.data.context import DataContext

    f.seek(0)
    stream = buffered(f, DataContext.get_current().streaming_read_buffer_size)
    yield from StreamReader(stream).records


def _warn_missing_parameter_sets(topic: str, path: str, codec: VideoCodec) -> None:
    """Warn once per topic of a file that its frames lack parameter sets."""
    if log_once(f"mcap_missing_parameter_sets:{path}:{topic}"):
        logger.warning(
            "Video topic %r in %r has a keyframe without the stream's %s parameter "
            "sets, and they cannot be read from the start of the stream either. "
            "Its frames are skipped until a keyframe that carries them.",
            topic,
            path,
            codec.name,
        )


@dataclass(frozen=True)
class _Gap:
    """A channel's skipped messages between two of its owned ones."""

    messages: List[_Selected]
    # Whether the unread part of the gap, beyond the look-back cap, may hold
    # messages of the channel.
    clipped: bool


@dataclass(frozen=True)
class _ChunkSpans:
    """The log-time spans of some chunks, searched by bisection."""

    # Chunk start times in ascending order, and the latest end time among the
    # chunks up to each one.
    starts: List[int]
    latest_ends: List[int]

    @classmethod
    def of(cls, chunks: Iterable["ChunkIndex"]) -> "_ChunkSpans":
        spans = sorted((c.message_start_time, c.message_end_time) for c in chunks)
        return cls(
            [start for start, _ in spans],
            list(itertools.accumulate((end for _, end in spans), max)),
        )

    def any_between(self, low: int, high: int) -> bool:
        """Whether a span holds a log time strictly between ``low`` and ``high``."""
        count = bisect.bisect_left(self.starts, high)
        return count > 0 and self.latest_ends[count - 1] > low


class VideoMessageSource:
    """What a decoded message task reads of one file.

    The task's own messages, and per video channel the messages before them
    that prime its decoder: the lead-in before the channel's first owned
    message, and each gap, the channel's messages another task owns between
    two of ours. Beyond those, it reads only the stream's parameter sets
    where it starts, when a decoder lacks them (``stream_start``).
    """

    def __init__(
        self,
        message_reader: SelectedMessageReader,
        selection: MCAPSelection,
        max_lead_in_ns: int,
        f: Any,
        path: str,
        offsets: Optional[Set[int]],
        summary: Optional["Summary"],
    ):
        """Set up the reads of the task's chunks at ``offsets`` of ``path``.

        ``offsets`` is ``None`` when the task reads the whole file, and
        ``summary`` is the file's whole summary, or ``None`` without an index.
        """
        self._message_reader = message_reader
        self._selection = selection
        self._f = f
        self._path = path
        self._offsets = offsets
        self._summary = summary
        self._lead_ns = max_lead_in_ns
        self.stream_start = StreamStart(f, path, summary)
        # Per channel of a file without an index: its messages before the
        # time range, set aside by ``owned``.
        self._unindexed_lead: Dict[int, List[_Selected]] = {}
        # Per channel: the spans of the chunks another task owns that may hold
        # it, built on first use.
        self._unowned_spans: Dict[int, _ChunkSpans] = {}

    def owned(self) -> Iterator[_Selected]:
        """The task's messages in log-time order.

        A file without an index is one task and is read whole. The messages
        before the time range are set aside as each channel's lead-in.
        """
        from ray.data.context import DataContext

        selection = self._selection
        if self._summary is None:
            start_time, end_time = selection.start_time, selection.end_time
            low = max(0, start_time - self._lead_ns) if start_time is not None else None
            buffer_size = DataContext.get_current().streaming_read_buffer_size
            # Streamed in file order and sorted here, so the file is held once.
            items = sorted(
                self._message_reader.iter_unindexed(
                    buffered(self._f, buffer_size),
                    self._path,
                    time_bounds=(low, end_time),
                    log_time_order=False,
                ),
                key=lambda item: item[2].log_time,
            )
            if start_time is not None:
                for item in items:
                    if item[2].log_time < start_time:
                        self._unindexed_lead.setdefault(item[1].id, []).append(item)
                items = [m for m in items if m[2].log_time >= start_time]
            return iter(items)
        return self._message_reader.iter_chunks(
            self._f,
            self._path,
            self._summary,
            self._offsets,
            selected=selection.selected_channel_ids(
                self._summary.channels, self._summary.schemas
            ),
            log_time_order=True,
        )

    def lead_in(self, channel_id: int, first_time: int) -> List[_Selected]:
        """The channel's messages in the look-back span before its first owned one."""
        if self._summary is None:
            low = first_time - self._lead_ns
            lead = self._unindexed_lead.get(channel_id, [])
            return [m for m in lead if m[2].log_time >= low]
        if not self._lead_ns:
            return []
        return self._channel_messages(
            channel_id, max(0, first_time - self._lead_ns), first_time
        )

    def may_have_gap(self, channel_id: int, last_time: int, next_time: int) -> bool:
        """Whether another task's chunk may hold the channel between two of ours.

        Both of ours can come from one chunk, when the channel was written out
        of log-time order.
        """
        spans = self._unowned_spans.get(channel_id)
        if spans is None:
            spans = self._unowned_spans[channel_id] = self._unowned_chunk_spans(
                channel_id
            )
        return spans.any_between(last_time, next_time)

    def gap(self, channel_id: int, last_time: int, next_time: int) -> _Gap:
        """The channel's messages another task owns between two of ours.

        At most the look-back cap before ``next_time`` is read.
        """
        if self._summary is None or self._offsets is None or next_time <= last_time + 1:
            return _Gap([], clipped=False)
        low = max(last_time + 1, next_time - self._lead_ns)
        # Clipped only if another task's chunk may hold the channel in the
        # unread part of the gap, as its message index tells. A channel that
        # only pauses keeps the decoder's state, even when other topics fill
        # the pause, and so does a jump between consecutive chunks.
        clipped = low > last_time + 1 and any(
            c.chunk_start_offset not in self._offsets
            and c.message_end_time > last_time
            and c.message_start_time < low
            and chunk_may_hold(self._f, c, channel_id, last_time + 1, low)
            for c in self._summary.chunk_indexes
        )
        # With the look-back cap at zero nothing is read back, but a skipped
        # span still leaves the channel cold.
        messages = (
            self._channel_messages(
                channel_id, low, next_time, skip_offsets=self._offsets
            )
            if low < next_time
            else []
        )
        return _Gap(messages, clipped)

    def _unowned_chunk_spans(self, channel_id: int) -> _ChunkSpans:
        """The spans of the chunks another task owns that may hold the channel."""
        if self._summary is None or self._offsets is None:
            return _ChunkSpans.of([])
        offsets = self._offsets
        return _ChunkSpans.of(
            c
            for c in self._summary.chunk_indexes
            if c.chunk_start_offset not in offsets
            and (not c.message_index_offsets or channel_id in c.message_index_offsets)
        )

    def _channel_messages(
        self,
        channel_id: int,
        low: int,
        high: int,
        skip_offsets: Optional[Set[int]] = None,
    ) -> List[_Selected]:
        """The channel's messages with ``low <= log_time < high`` of an indexed file.

        They are read from every chunk that may hold the channel in that span,
        except ``skip_offsets``. That includes owned chunks, which can hold
        messages before the time range, and chunks the listing never considered.
        """
        summary = self._summary
        assert summary is not None
        if low >= high:
            return []
        chunks = sorted(
            (
                c
                for c in summary.chunk_indexes
                if c.message_end_time >= low
                and c.message_start_time < high
                and (skip_offsets is None or c.chunk_start_offset not in skip_offsets)
                and (
                    not c.message_index_offsets or channel_id in c.message_index_offsets
                )
            ),
            key=lambda c: c.chunk_start_offset,
        )
        if not chunks:
            return []
        messages = self._message_reader.iter_chunks(
            self._f,
            self._path,
            summary,
            None,
            chunk_indexes=chunks,
            selected={channel_id},
            time_bounds=(low, high),
            log_time_order=True,
        )
        # A channel the summary does not list passes the selection on its own
        # schema and topic, so another camera's messages are dropped here.
        return [m for m in messages if m[1].id == channel_id]
