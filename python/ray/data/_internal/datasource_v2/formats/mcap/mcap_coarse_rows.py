"""Window, topic and file rows, which pack many messages into one row.

This module decides what a task reads for its rows. A window task reads the
span of the windows it owns (``mcap_windows``), from one look-back cap earlier
when a selected channel may be video. A topic task reads its topic from the
chunks it owns, plus the look-back before the time range for a video topic. A
file task reads every selected message of the file. ``mcap_lead_in`` then picks
the frames a decoder needs, and ``mcap_coarse_layout`` builds the table. A topic
or file row whose payloads pass ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails the read.
"""

import bisect
import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_layout import (
    CoarseRow,
    CoarseRowBatch,
    _Entry,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_lead_in import (
    topic_from_keyframe,
    video_channels,
    window_lead_in,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    DEFAULT_MAX_LEAD_IN_NS,
    MCAPSelection,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import is_video_channel
from ray.data._internal.datasource_v2.formats.mcap.mcap_windows import (
    owner_offsets,
    place_windows,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import Channel, Message, Schema
    from mcap.summary import Summary

    from ray.data._internal.datasource_v2.formats.mcap.mcap_reader import _Assignment

logger = logging.getLogger(__name__)

# ``MCAPReader._finish``: appends a table's partition and synthesized columns
# and applies the projection. The int is ``rows_before``.
FinishTable = Callable[[pa.Table, "_Assignment", int], pa.Table]


@dataclass(frozen=True)
class RowSettings:
    """The reader options that rows are built with.

    ``MCAPReader`` builds one and hands it to each row builder. Each field is
    the reader argument of the same name.
    """

    selection: MCAPSelection
    granularity: str
    window: Optional[WindowSpec]
    video: Optional[VideoOptions]
    video_topics: FrozenSet[str]
    # With ``video`` at ``window`` granularity, the topics that get frame
    # columns. Settled at planning, so the schema is fixed before any task runs.
    decoded_topics: Tuple[str, ...]
    include_metadata: bool
    include_row_id: bool
    columns: Optional[List[str]]
    target_block_size: Optional[int]
    max_row_bytes: int
    max_lead_in_ns: int

    def is_video(self, channel: "Channel", schema: Optional["Schema"]) -> bool:
        """Whether the channel carries video (:func:`is_video_channel`)."""
        return is_video_channel(
            channel.topic, schema.name if schema else None, self.video_topics
        )

    @property
    def row_digest(self) -> str:
        """The digest in every coarse row's id.

        It covers the selection, and the options that change what a row holds:
        the listed video topics, the look-back cap and, for decoded rows, the
        ``video`` options. A checkpoint written under other options then never
        skips one of these rows.
        """
        options = []
        if self.video_topics:
            options.append("video_topics=" + ",".join(sorted(self.video_topics)))
        if self.max_lead_in_ns != DEFAULT_MAX_LEAD_IN_NS:
            options.append(f"max_lead_in_ns={self.max_lead_in_ns}")
        if self.video is not None:
            options.append(f"video=fps:{self.video.fps},resize:{self.video.resize}")
        return self.selection.digest(*options)


@dataclass
class _LogTimeSpan:
    """The first and last log time of the messages observed so far.

    Stands in for the file-wide span an indexed file's statistics give.
    """

    first: Optional[int] = None
    last: Optional[int] = None

    def observe(self, message: "Message") -> None:
        if self.first is None or self.last is None:
            self.first = self.last = message.log_time
        else:
            self.first = min(self.first, message.log_time)
            self.last = max(self.last, message.log_time)


@dataclass(frozen=True)
class _PlacedWindows:
    """Windows placed over a file, and the entries to cut into them."""

    entries: List[_Entry]
    windows: List[Tuple[int, int]]


class CoarseRows:
    """Builds the window, topic and file rows of one read task."""

    def __init__(
        self,
        settings: RowSettings,
        message_reader: SelectedMessageReader,
        finish: FinishTable,
    ):
        self._settings = settings
        self._message_reader = message_reader
        self._finish = finish

    def _span(self, summary: "Summary") -> Tuple[int, int]:
        """The file's first and last log time, clipped to the time range.

        The span is file-wide, not the selection's, so a recording's window
        grid does not move when a read selects other topics. ``file_start``
        means the file's first message, whatever is read.
        """
        statistics = summary.statistics
        if statistics is not None and statistics.message_count > 0:
            start, end = statistics.message_start_time, statistics.message_end_time
        else:
            start = min(c.message_start_time for c in summary.chunk_indexes)
            end = max(c.message_end_time for c in summary.chunk_indexes)
        return self._clip_span(start, end)

    def _clip_span(self, start: int, end: int) -> Tuple[int, int]:
        if self._settings.selection.start_time is not None:
            start = max(start, self._settings.selection.start_time)
        if self._settings.selection.end_time is not None:
            end = min(end, self._settings.selection.end_time - 1)
        return start, end

    def _entries(
        self, selected: Iterator[_Selected], what: Optional[str] = None
    ) -> List[_Entry]:
        """Collect the selected messages as entries.

        With ``what``, the row's name in the error, fail as soon as the payloads
        exceed the row limit rather than after holding them all.
        """
        entries: List[_Entry] = []
        payload_bytes = 0
        for schema, channel, message, _ in selected:
            if what is not None:
                payload_bytes += len(message.data)
                if payload_bytes > self._settings.max_row_bytes:
                    self._raise_row_too_large(payload_bytes, what, partial=True)
            entries.append((schema, channel, message))
        return entries

    def _read_entries_in_span(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        channel_ids: Set[int],
        low: int,
        high: int,
    ) -> List[_Entry]:
        """Read the channels' messages logged in ``[low, high)``, by log time.

        Reads every chunk that overlaps the span and may hold one of the
        channels, not only the listing's candidates: a lead-in can lie before
        the time range, in chunks the listing never considered.
        """
        chunks = sorted(
            (
                c
                for c in summary.chunk_indexes
                if c.message_end_time >= low
                and c.message_start_time < high
                and (
                    not c.message_index_offsets
                    or not channel_ids.isdisjoint(c.message_index_offsets)
                )
            ),
            key=lambda c: c.chunk_start_offset,
        )
        if not chunks:
            return []
        messages = self._message_reader.iter_chunks(
            f,
            path,
            summary,
            None,
            chunk_indexes=chunks,
            selected=channel_ids,
            time_bounds=(low, high),
            log_time_order=True,
        )
        # A channel that only a chunk declares passes ``selected`` unchecked.
        return self._entries(m for m in messages if m[1].id in channel_ids)

    def window_tables(
        self, f: Any, assignment: "_Assignment", summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Emit the window rows this task owns for one file."""
        placed = self.placed_windows(f, assignment, summary)
        if placed is not None:
            yield from self._window_rows(assignment, placed.entries, placed.windows)

    def placed_windows(
        self, f: Any, assignment: "_Assignment", summary: Optional["Summary"]
    ) -> Optional[_PlacedWindows]:
        """Place the windows this task owns in one file and read their entries.

        ``None`` when the task owns no window there.
        """
        assert self._settings.window is not None
        path = assignment.path
        if summary is None:
            return self._read_unindexed_windows(f, path, self._settings.window)
        selected = self._settings.selection.selected_channel_ids(
            summary.channels, summary.schemas
        )
        windows = self._owned_windows(
            summary, selected, assignment.offsets, self._settings.window
        )
        if not windows:
            return None
        lead_in = self._lead_in_span_ns(summary, selected)
        low = windows[0][0] - lead_in
        high = max(end for _, end in windows)
        if self._settings.selection.end_time is not None:
            high = min(high, self._settings.selection.end_time)
        entries = self._read_entries_in_span(f, path, summary, selected, low, high)
        return _PlacedWindows(entries, windows)

    def _lead_in_span_ns(self, summary: "Summary", selected: Set[int]) -> int:
        """How far before a window this task reads: zero without video topics.

        A channel whose schema record only a chunk holds may be video, so the
        span is read for it too.
        """
        for cid in selected:
            channel = summary.channels[cid]
            schema = summary.schemas.get(channel.schema_id)
            unknown = bool(channel.schema_id) and schema is None
            if unknown or self._settings.is_video(channel, schema):
                return self._settings.max_lead_in_ns
        return 0

    def _read_unindexed_windows(
        self, f: Any, path: str, spec: WindowSpec
    ) -> Optional[_PlacedWindows]:
        """Read a file without a chunk index whole and place windows over it.

        Such a file is one task, so every window is this task's. ``None`` when
        no selected message lies in the time range.
        """
        if log_once(f"mcap_window_unindexed:{path}"):
            logger.warning(
                "MCAP file %r has no chunk index; reading it whole to place "
                "windows. Rewrite the file with an index to split it across "
                "tasks.",
                path,
            )
        span = _LogTimeSpan()
        entries = self._entries(
            self._message_reader.iter_unindexed(
                f, path, (None, None), on_message=span.observe
            )
        )
        entries.sort(key=lambda e: e[2].log_time)
        if not entries or not any(
            self._settings.selection.in_time_range(e[2].log_time) for e in entries
        ):
            return None
        assert span.first is not None and span.last is not None
        start, end = self._clip_span(span.first, span.last)
        if start > end:
            return None
        return _PlacedWindows(entries, place_windows(spec, start, end))

    def _owned_windows(
        self,
        summary: "Summary",
        selected: Set[int],
        offsets: Optional[Set[int]],
        spec: WindowSpec,
    ) -> List[Tuple[int, int]]:
        """The windows of an indexed file that this task emits.

        A window belongs to the task owning the chunk its start falls in,
        judged over the candidate chunks the listing saw. ``offsets`` is
        ``None`` when the task owns the whole file.
        """
        candidates = self._message_reader.candidate_chunks(summary, selected)
        if not candidates:
            return []
        windows = place_windows(spec, *self._span(summary))
        owners = owner_offsets(candidates, [start for start, _ in windows])
        return [
            window
            for window, owner in zip(windows, owners)
            if offsets is None or owner in offsets
        ]

    def _window_rows(
        self,
        assignment: "_Assignment",
        entries: List[_Entry],
        windows: Sequence[Tuple[int, int]],
    ) -> Iterator[pa.Table]:
        """Cut ``entries`` (log-time ordered) into the given windows."""
        path = assignment.path
        digest = self._settings.row_digest
        times = [e[2].log_time for e in entries]
        channels = video_channels(
            entries, self._settings.is_video, self._settings.max_lead_in_ns
        )
        batch = self.new_batch()
        for start, end in windows:
            in_window = self._entries_in_window(entries, times, start, end)
            if not in_window:
                continue
            lead_in = window_lead_in(
                channels,
                start,
                in_window,
                range_start=self._settings.selection.start_time,
                max_lead_in_ns=self._settings.max_lead_in_ns,
            )
            batch.add(
                CoarseRow(
                    path=path,
                    row_id=f"{path}#[{start},{end})@{digest}",
                    messages=lead_in + in_window,
                    window=(start, end),
                    num_lead_in=len(lead_in),
                )
            )
            if (
                self._settings.target_block_size is not None
                and batch.payload_bytes >= self._settings.target_block_size
            ):
                yield self._finish(batch.build(), assignment, 0)
                batch = self.new_batch()
        if len(batch) > 0:
            yield self._finish(batch.build(), assignment, 0)

    def _entries_in_window(
        self, entries: List[_Entry], times: List[int], start: int, end: int
    ) -> List[_Entry]:
        """The entries logged in ``[start, end)`` that lie in the time range.

        ``times`` holds the entries' log times, in ascending order.
        """
        first = bisect.bisect_left(times, start)
        last = bisect.bisect_left(times, end)
        return [
            e
            for e in entries[first:last]
            if self._settings.selection.in_time_range(e[2].log_time)
        ]

    def topic_tables(
        self, f: Any, assignment: "_Assignment", summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Emit one row per topic this task was assigned (one, when indexed)."""
        path = assignment.path
        if summary is None:
            groups = self._group_unindexed_topics(f, assignment)
        else:
            assert assignment.topic is not None, "an indexed topic row names its topic"
            entries = self._read_indexed_topic(f, assignment, summary)
            groups = [(assignment.topic, entries)]
        digest = self._settings.row_digest
        batch = self.new_batch()
        for topic, topic_entries in groups:
            topic_entries, num_lead_in = self._decodable_topic(
                f, path, summary, topic, topic_entries
            )
            if not topic_entries:
                continue
            row = CoarseRow(
                path=path,
                row_id=f"{path}#{topic}@{digest}",
                messages=topic_entries,
                topic=topic,
                num_lead_in=num_lead_in,
            )
            self._check_row_size(row, f"topic {topic!r} of {path!r}")
            batch.add(row)
        if len(batch) > 0:
            yield self._finish(batch.build(), assignment, 0)

    def _group_unindexed_topics(
        self, f: Any, assignment: "_Assignment"
    ) -> List[Tuple[str, List[_Entry]]]:
        """Scan a file without a chunk index and group its entries by topic.

        Keeps the assigned topic, or every topic in name order when none is
        assigned. Each topic's entries are sorted by log time. Fails as soon as
        one topic's payloads exceed the row limit.
        """
        path = assignment.path
        by_topic: Dict[str, List[_Entry]] = {}
        sizes: Dict[str, int] = {}
        for schema, channel, message, _ in self._message_reader.iter_unindexed(f, path):
            topic = channel.topic
            if assignment.topic is not None and topic != assignment.topic:
                continue
            sizes[topic] = sizes.get(topic, 0) + len(message.data)
            if sizes[topic] > self._settings.max_row_bytes:
                self._raise_row_too_large(
                    sizes[topic], f"topic {topic!r} of {path!r}", partial=True
                )
            by_topic.setdefault(topic, []).append((schema, channel, message))
        for topic_entries in by_topic.values():
            topic_entries.sort(key=lambda e: e[2].log_time)
        topics = (
            [assignment.topic] if assignment.topic is not None else sorted(by_topic)
        )
        return [(topic, by_topic.get(topic, [])) for topic in topics]

    def _read_indexed_topic(
        self, f: Any, assignment: "_Assignment", summary: "Summary"
    ) -> List[_Entry]:
        """Read the assigned topic's selected messages from the owned chunks.

        Fails as soon as their payloads exceed the row limit.
        """
        path = assignment.path
        selected = self._settings.selection.selected_channel_ids(
            summary.channels, summary.schemas
        )
        topic_ids = {
            cid for cid in selected if summary.channels[cid].topic == assignment.topic
        }
        messages = self._message_reader.iter_chunks(
            f,
            path,
            summary,
            assignment.offsets,
            chunk_indexes=self._message_reader.candidate_chunks(summary, topic_ids),
            selected=topic_ids,
            log_time_order=True,
        )
        # A channel that only a chunk declares passes ``topic_ids`` unchecked,
        # and may belong to another topic.
        return self._entries(
            (m for m in messages if m[1].topic == assignment.topic),
            what=f"topic {assignment.topic!r} of {path!r}",
        )

    def _decodable_topic(
        self,
        f: Any,
        path: str,
        summary: Optional["Summary"],
        topic: str,
        entries: List[_Entry],
    ) -> Tuple[List[_Entry], int]:
        """A topic's messages with each video channel from a keyframe on, and how
        many are lead-in.

        Reads the video channels' look-back before the time range, where
        :func:`topic_from_keyframe` looks for their keyframes. A topic without
        video is returned as it is.
        """
        channels = {channel.id: (schema, channel) for schema, channel, _ in entries}
        video_ids = {
            channel_id
            for channel_id, (schema, channel) in channels.items()
            if self._settings.is_video(channel, schema)
        }
        if not video_ids:
            return entries, 0
        range_start = self._settings.selection.start_time
        lead: List[_Entry] = []
        if range_start is not None:
            lead = self._look_back(f, path, summary, video_ids, range_start)
        return topic_from_keyframe(
            path, topic, entries, lead, video_ids, self._settings.max_lead_in_ns
        )

    def _look_back(
        self,
        f: Any,
        path: str,
        summary: Optional["Summary"],
        channel_ids: Set[int],
        before: int,
    ) -> List[_Entry]:
        """The channels' messages in the look-back span before ``before``."""
        low = max(0, before - self._settings.max_lead_in_ns)
        if low >= before:
            return []
        if summary is None:
            # File order is not log-time order. Sort, so the walk back to a
            # keyframe goes by time.
            return sorted(
                (
                    (schema, channel, message)
                    for schema, channel, message, _ in self._message_reader.iter_unindexed(
                        f, path, time_bounds=(low, before)
                    )
                    if channel.id in channel_ids
                ),
                key=lambda e: e[2].log_time,
            )
        return self._read_entries_in_span(f, path, summary, channel_ids, low, before)

    def file_tables(
        self, f: Any, assignment: "_Assignment", summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Emit the one row holding every selected message of the file."""
        path = assignment.path
        what = f"file {path!r}"
        if summary is None:
            entries = self._entries(
                self._message_reader.iter_unindexed(f, path), what=what
            )
            entries.sort(key=lambda e: e[2].log_time)
        else:
            entries = self._entries(
                self._message_reader.iter_chunks(
                    f, path, summary, None, log_time_order=True
                ),
                what=what,
            )
        if not entries:
            return
        row = CoarseRow(
            path=path,
            row_id=f"{path}@{self._settings.row_digest}",
            messages=entries,
        )
        self._check_row_size(row, f"file {path!r}")
        batch = self.new_batch()
        batch.add(row)
        yield self._finish(batch.build(), assignment, 0)

    def new_batch(
        self, frame_shape: Optional[Dict[str, Tuple[int, int]]] = None
    ) -> CoarseRowBatch:
        columns = self._settings.columns
        batch = CoarseRowBatch(
            granularity=self._settings.granularity,
            include_metadata=self._settings.include_metadata,
            include_row_id=self._settings.include_row_id,
            columns=frozenset(columns) if columns is not None else None,
            decoded_topics=self._settings.decoded_topics,
        )
        if frame_shape is not None:
            batch.frame_shape = frame_shape
        return batch

    def _check_row_size(self, row: CoarseRow, what: str) -> None:
        if row.payload_bytes > self._settings.max_row_bytes:
            self._raise_row_too_large(row.payload_bytes, what, partial=False)

    def _raise_row_too_large(self, payload_bytes: int, what: str, partial: bool):
        raise ValueError(
            f"The row for {what} would carry "
            f"{'at least ' if partial else ''}{payload_bytes} bytes of payload, "
            f"over the {self._settings.max_row_bytes}-byte limit for one row "
            "(RAY_DATA_MCAP_MAX_ROW_BYTES). Read this data at "
            "read_granularity='window' or 'message' instead."
        )
