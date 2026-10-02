"""Options shared by the MCAP datasources.

``TimeRange``, ``WindowSpec`` and ``VideoOptions`` are the public option types
``read_mcap`` accepts; they are re-exported from ``ray.data.datasource``.

``MCAPSelection`` bundles the three message filters of ``read_mcap`` (``topics``,
``message_types``, ``time_range``) and answers, from summary records alone,
whether a file, a chunk or a channel can contribute rows. The indexer and the
reader both ask it, so a chunk the indexer keeps is one the reader scans and a
chunk it drops is one no task ever opens.

Nothing from the ``mcap`` package is imported at module level: ``read_api``
imports these types whether or not ``mcap`` is installed.
"""

import hashlib
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    FrozenSet,
    Iterable,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Schema, Statistics

# Name of the deterministic per-row id column (see ``MCAPReader``).
ROW_ID_COLUMN = "row_id"

# What one output row is. ``message`` is today's row; ``window``, ``topic`` and
# ``file`` pack the messages of a time window, a topic or a whole file into one
# row of lists; ``attachment`` and ``metadata`` return the file's Attachment and
# Metadata records, which live outside the message stream.
RowType = Literal["message", "window", "topic", "file", "attachment", "metadata"]
MESSAGE_GRANULARITY = "message"
WINDOW_GRANULARITY = "window"
TOPIC_GRANULARITY = "topic"
FILE_GRANULARITY = "file"
ATTACHMENT_GRANULARITY = "attachment"
METADATA_GRANULARITY = "metadata"
GRANULARITIES: Tuple[str, ...] = (
    MESSAGE_GRANULARITY,
    WINDOW_GRANULARITY,
    TOPIC_GRANULARITY,
    FILE_GRANULARITY,
    ATTACHMENT_GRANULARITY,
    METADATA_GRANULARITY,
)
RECORD_GRANULARITIES: Tuple[str, ...] = (ATTACHMENT_GRANULARITY, METADATA_GRANULARITY)

_NS_PER_S = 1_000_000_000


def _seconds_to_ns(seconds: float) -> int:
    return int(round(seconds * _NS_PER_S))


@dataclass
class TimeRange:
    """Time range for filtering MCAP messages.

    Attributes:
        start_time: Start time in nanoseconds (inclusive).
        end_time: End time in nanoseconds (exclusive).
    """

    start_time: int
    end_time: int

    def __post_init__(self):
        """Validate time range after initialization."""
        if self.start_time >= self.end_time:
            raise ValueError(
                f"start_time ({self.start_time}) must be less than "
                f"end_time ({self.end_time})"
            )
        if self.start_time < 0 or self.end_time < 0:
            raise ValueError(
                f"time values must be non-negative, got start_time={self.start_time}, "
                f"end_time={self.end_time}"
            )


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class WindowSpec:
    """How ``read_mcap(read_granularity="window")`` cuts a recording into rows.

    Every row is one half-open window ``[start, start + length_s)`` of log time
    within one file, holding every selected message logged inside it. Windows
    with no messages are not emitted.

    Attributes:
        length_s: Window length in seconds.
        stride_s: Distance between the starts of consecutive windows, in
            seconds. Defaults to ``length_s`` (back-to-back windows). Smaller
            gives overlapping windows, which duplicate messages on purpose.
        anchor: Where window 0 starts. ``"file_start"`` puts it at the file's
            first message; ``"epoch"`` aligns window starts to multiples of
            ``stride_s`` since the Unix epoch, so windows line up across files;
            an ``int`` is an absolute nanosecond timestamp that windows are
            aligned to, before and after it.
        drop_partial: Drop a window that runs past the file's last message
            instead of emitting it short.
    """

    length_s: float
    stride_s: Optional[float] = None
    anchor: Union[Literal["file_start", "epoch"], int] = "file_start"
    drop_partial: bool = False

    def __post_init__(self):
        if self.length_s <= 0:
            raise ValueError(f"length_s must be positive, got {self.length_s}")
        if self.stride_s is not None and self.stride_s <= 0:
            raise ValueError(f"stride_s must be positive, got {self.stride_s}")
        if isinstance(self.anchor, bool) or not (
            self.anchor in ("file_start", "epoch")
            or (isinstance(self.anchor, int) and self.anchor >= 0)
        ):
            raise ValueError(
                "anchor must be 'file_start', 'epoch' or a non-negative "
                f"nanosecond timestamp, got {self.anchor!r}"
            )

    @property
    def length_ns(self) -> int:
        return _seconds_to_ns(self.length_s)

    @property
    def stride_ns(self) -> int:
        return _seconds_to_ns(
            self.stride_s if self.stride_s is not None else self.length_s
        )


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class VideoOptions:
    """How ``read_mcap`` finds the bytes a video window needs to decode.

    A compressed video stream can only be decoded from a keyframe, and a
    window rarely starts on one. For every video topic a window row therefore
    also carries the frames from the last keyframe before the window start (the
    "lead-in"), counted by ``num_lead_in``. Without these options, video topics
    are recognised from the channel's schema name and keyframes are detected
    from the payload bytes (JPEG, PNG, H.264 and H.265 Annex-B).

    Attributes:
        topics: Topics to treat as video whatever their schema says. Any
            iterable of topic names; stored as a tuple.
        lead_in_s: Fixed lead-in in seconds, for codecs whose keyframes cannot
            be detected from the bytes. Replaces keyframe detection for every
            video topic.
        max_lead_in_s: How far back a window looks for a keyframe. Bounds how
            much a read task may have to read before a window.
    """

    topics: Optional[Iterable[str]] = None
    lead_in_s: Optional[float] = None
    max_lead_in_s: float = 10.0

    def __post_init__(self):
        if self.topics is not None:
            object.__setattr__(self, "topics", tuple(self.topics))
        if self.lead_in_s is not None and self.lead_in_s < 0:
            raise ValueError(f"lead_in_s must be non-negative, got {self.lead_in_s}")
        if self.max_lead_in_s < 0:
            raise ValueError(
                f"max_lead_in_s must be non-negative, got {self.max_lead_in_s}"
            )

    @property
    def lead_in_ns(self) -> Optional[int]:
        return _seconds_to_ns(self.lead_in_s) if self.lead_in_s is not None else None

    @property
    def max_lead_in_ns(self) -> int:
        """Bound on the lead-in: the fixed lead-in when set, else the search range."""
        if self.lead_in_s is not None:
            return _seconds_to_ns(self.lead_in_s)
        return _seconds_to_ns(self.max_lead_in_s)


@dataclass(frozen=True)
class MCAPSelection:
    """Which messages a read selects.

    Attributes:
        topics: Topics to keep, or ``None`` for every topic.
        message_types: Schema names to keep, or ``None`` for every schema. A
            channel without a schema is never rejected by this filter; that is
            what the legacy datasource does, and a schema-less channel has no
            name to compare.
        time_range: Half-open ``[start_time, end_time)`` window on ``log_time``,
            or ``None`` for the whole recording.
    """

    topics: Optional[FrozenSet[str]] = None
    message_types: Optional[FrozenSet[str]] = None
    time_range: Optional[TimeRange] = None

    @classmethod
    def create(
        cls,
        topics: Optional[Iterable[str]],
        time_range: Optional[TimeRange],
        message_types: Optional[Iterable[str]],
    ) -> "MCAPSelection":
        """Normalize ``read_mcap``'s arguments; an empty collection means no filter."""
        return cls(
            topics=frozenset(topics) if topics else None,
            message_types=frozenset(message_types) if message_types else None,
            time_range=time_range,
        )

    @property
    def is_empty(self) -> bool:
        """``True`` when no filter is set, so every message of every file is read."""
        return (
            self.topics is None
            and self.message_types is None
            and self.time_range is None
        )

    @property
    def start_time(self) -> Optional[int]:
        return self.time_range.start_time if self.time_range is not None else None

    @property
    def end_time(self) -> Optional[int]:
        return self.time_range.end_time if self.time_range is not None else None

    def digest(self) -> str:
        """Short, stable hash of the selection, part of every coarse row's id.

        A window, topic or file row's content depends on which messages were
        selected, so its id must change when the selection does; otherwise a
        checkpoint written under one selection would skip rows of another.
        Message rows do not need it: one message's content does not depend
        on what else was selected.
        """
        parts = [
            ",".join(sorted(self.topics)) if self.topics is not None else "*",
            ",".join(sorted(self.message_types))
            if self.message_types is not None
            else "*",
            f"{self.start_time}-{self.end_time}"
            if self.time_range is not None
            else "*",
        ]
        return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:8]

    def accepts_channel(self, channel: "Channel", schema: Optional["Schema"]) -> bool:
        """Whether messages on ``channel`` pass the topic and schema filters."""
        if self.topics is not None and channel.topic not in self.topics:
            return False
        if (
            self.message_types is not None
            and schema is not None
            and schema.name not in self.message_types
        ):
            return False
        return True

    def selected_channel_ids(
        self, channels: Mapping[int, "Channel"], schemas: Mapping[int, "Schema"]
    ) -> Set[int]:
        """Ids of the channels whose messages pass the topic and schema filters."""
        return {
            channel_id
            for channel_id, channel in channels.items()
            if self.accepts_channel(
                channel, schemas.get(channel.schema_id) if channel.schema_id else None
            )
        }

    def in_time_range(self, log_time: int) -> bool:
        """Whether a message logged at ``log_time`` is inside ``time_range``."""
        if self.time_range is None:
            return True
        return self.time_range.start_time <= log_time < self.time_range.end_time

    def overlaps(self, start_time: int, end_time: int) -> bool:
        """Whether the closed span ``[start_time, end_time]`` meets ``time_range``."""
        if self.time_range is None:
            return True
        return (
            end_time >= self.time_range.start_time
            and start_time < self.time_range.end_time
        )

    def chunk_may_match(
        self, chunk_index: "ChunkIndex", selected_channel_ids: Set[int]
    ) -> bool:
        """Whether a chunk can hold a selected message, judged from its index record.

        A chunk index carries the chunk's log-time bounds and, when the writer
        recorded message indexes, the ids of the channels it contains. Without
        the latter, membership is unknown until the chunk is scanned, so the
        chunk is kept.
        """
        if not self.overlaps(
            chunk_index.message_start_time, chunk_index.message_end_time
        ):
            return False
        if not chunk_index.message_index_offsets:
            return True
        return any(
            channel_id in selected_channel_ids
            for channel_id in chunk_index.message_index_offsets
        )

    def file_may_match(
        self,
        statistics: Optional["Statistics"],
        selected_channel_ids: Set[int],
        has_channels: bool,
    ) -> bool:
        """Whether a file can hold a selected message, judged from its summary.

        ``has_channels`` distinguishes a file whose summary lists channels, none
        of which is selected (prunable), from a file whose summary lists no
        channels at all (unknown, kept).
        """
        if has_channels and not selected_channel_ids:
            return False
        if statistics is not None and statistics.message_count > 0:
            return self.overlaps(
                statistics.message_start_time, statistics.message_end_time
            )
        return True
