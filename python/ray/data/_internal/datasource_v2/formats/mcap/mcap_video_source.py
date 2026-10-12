"""What a decoding read task reads: its own messages and what its decoders need.

A task owns some chunks of a file, but a video decoder needs the frames back to
a keyframe. So, per video channel, a message task reads back up to the look-back
cap (``RAY_DATA_MCAP_MAX_LEAD_IN_S``) before its first owned message (the
lead-in), and before an owned message that follows another task's messages (a
gap). The channel's decoder starts at the last keyframe among them. Beyond the
cap only the message index is read, for the latest log time that ``fps``
thinning must count. With a cap of four frames (K is a keyframe, p another
picture)::

    frame       0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19 20 21 22 23
    /camera     K  p  p  p  p  K  p  p  p  p  K  p  p  p  p  K  p  p  p  p  K  p  p  p
    chunks      [     other task    ][  this task  ][     other task    ][  this task  ]
    reads       [ index ][ lead-in  ][    owned    ][ index ][   gap    ][    owned    ]

A window task reads the span of its windows itself (``CoarseRows``), and seeds
``fps`` thinning with ``FpsSeed`` too.
"""

import bisect
import itertools
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
    Tuple,
)

from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
    chunk_may_hold,
    latest_indexed_log_time,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_rows import RowSettings
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import FrameThinner

if TYPE_CHECKING:
    from mcap.records import ChunkIndex
    from mcap.summary import Summary

    from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import _Assignment


@dataclass(frozen=True)
class FpsSeed:
    """Tells ``fps`` thinning about the frames a task does not read.

    A task reads a channel back only to the look-back cap, but an ``fps``
    interval can be longer. A frame earlier in the task's first interval, out
    of that reach, still took the interval in a whole-file read. The message
    index gives its log time without decompressing a chunk. A file without a
    chunk index is read by one task, so it needs no seed.
    """

    f: Any
    summary: Optional["Summary"]

    def apply(
        self,
        thinner: FrameThinner,
        channel_id: int,
        next_time: int,
        unread: Tuple[int, int],
    ) -> None:
        """Observe the channel's latest unread message in the interval of ``next_time``.

        ``next_time`` is the owned message the channel feeds next. ``unread`` is
        the span ``[low, high)`` before it that the task does not read. The
        index has no payloads, so a message holding only parameter sets, or a
        corrupt still, counts as a frame.
        """
        start = thinner.interval_start(next_time)
        if self.summary is None or start is None:
            return
        low, high = max(unread[0], start), unread[1]
        if low >= high:
            return
        latest = latest_indexed_log_time(self.f, self.summary, channel_id, low, high)
        if latest is not None:
            thinner.observe_unread(latest)


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
    two of ours. Beyond those, it reads only log times from the message
    index, to seed ``fps`` thinning.
    """

    def __init__(
        self,
        message_reader: SelectedMessageReader,
        settings: RowSettings,
        f: Any,
        assignment: "_Assignment",
        summary: Optional["Summary"],
    ):
        self._message_reader = message_reader
        self._selection = settings.selection
        self._f = f
        self._path = assignment.path
        self._offsets = assignment.offsets
        self._summary = summary
        self._lead_ns = settings.max_lead_in_ns
        self._fps_seed = FpsSeed(f, summary)
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
        selection = self._selection
        if self._summary is None:
            start_time, end_time = selection.start_time, selection.end_time
            low = max(0, start_time - self._lead_ns) if start_time is not None else None
            items = sorted(
                self._message_reader.iter_unindexed(
                    self._f, self._path, time_bounds=(low, end_time)
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
            return self._unindexed_lead.get(channel_id, [])
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

    def seed_fps(
        self,
        thinner: FrameThinner,
        channel_id: int,
        next_time: int,
        last_time: Optional[int] = None,
    ) -> None:
        """Seed ``fps`` thinning for the channel's owned message at ``next_time``.

        As ``lead_in`` and ``gap`` do, the task reads the channel back to the
        look-back cap before it, and after ``last_time``, its previous owned
        message.
        """
        floor = 0 if last_time is None else last_time + 1
        read_from = max(floor, next_time - self._lead_ns)
        self._fps_seed.apply(thinner, channel_id, next_time, (floor, read_from))

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
        return list(
            self._message_reader.iter_chunks(
                self._f,
                self._path,
                summary,
                None,
                chunk_indexes=chunks,
                selected={channel_id},
                time_bounds=(low, high),
                log_time_order=True,
            )
        )
