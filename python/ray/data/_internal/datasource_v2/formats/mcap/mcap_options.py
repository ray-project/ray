"""Options of ``read_mcap``.

``TimeRange``, ``WindowSpec`` and ``VideoOptions`` are public option types of
``read_mcap``. All three are re-exported from ``ray.data.datasource``.

``MCAPSelection`` bundles the ``topics``, ``message_types`` and ``time_range``
filters. It decides from summary records whether a file, chunk or channel can
hold selected messages. The indexer and the reader both use it, so they agree
on which chunks can match.
"""

import hashlib
import math
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

from ray._common.utils import env_float
from ray.util.annotations import PublicAPI

# Type-only: ``import ray.data`` loads this module, and ``mcap`` is optional.
if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Schema, Statistics

# Name of the deterministic per-row id column (see ``MCAPReader``).
ROW_ID_COLUMN = "row_id"

# What one output row is. ``message`` is one message per row. The other three
# pack the messages of one time window, one topic or the whole of one file into
# a row of lists.
RowType = Literal["message", "window", "topic", "file"]
MESSAGE_GRANULARITY = "message"
WINDOW_GRANULARITY = "window"
TOPIC_GRANULARITY = "topic"
FILE_GRANULARITY = "file"
GRANULARITIES: Tuple[str, ...] = (
    MESSAGE_GRANULARITY,
    WINDOW_GRANULARITY,
    TOPIC_GRANULARITY,
    FILE_GRANULARITY,
)

_NS_PER_S = 1_000_000_000


def _seconds_to_ns(seconds: float) -> int:
    return int(round(seconds * _NS_PER_S))


# How far back, in seconds, a row looks for a keyframe on a video topic. It
# bounds what a read task reads before a window. A video topic whose codec is
# not recognised gets this whole span as its window lead-in. Not a
# ``read_mcap`` argument: ``RAY_DATA_MCAP_MAX_LEAD_IN_S`` overrides it.
DEFAULT_MAX_LEAD_IN_S = 10.0
DEFAULT_MAX_LEAD_IN_NS = _seconds_to_ns(DEFAULT_MAX_LEAD_IN_S)


def max_lead_in_ns() -> int:
    """The look-back cap in nanoseconds, honouring ``RAY_DATA_MCAP_MAX_LEAD_IN_S``."""
    seconds = env_float("RAY_DATA_MCAP_MAX_LEAD_IN_S", DEFAULT_MAX_LEAD_IN_S)
    if seconds < 0:
        raise ValueError(
            f"RAY_DATA_MCAP_MAX_LEAD_IN_S must be non-negative, got {seconds}"
        )
    return _seconds_to_ns(seconds)


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
            seconds. Defaults to ``length_s`` (back-to-back windows). A smaller
            stride gives overlapping windows, which share messages.
        anchor: Where window 0 starts. ``"file_start"`` puts it at the file's
            first message, or at the start of ``time_range`` if that is later.
            ``"epoch"`` aligns window starts to multiples of ``stride_s`` since
            the Unix epoch, so windows line up across files. An ``int`` is a
            nanosecond timestamp that windows are aligned to, before and after
            it.
        drop_partial: Drop a window that runs past the file's last message, or
            past the end of ``time_range``, instead of emitting it short.
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
        if self.length_ns < 1 or self.stride_ns < 1:
            # Windows would never move on from a stride of 0 ns.
            raise ValueError(
                "length_s and stride_s must be at least one nanosecond, got "
                f"length_s={self.length_s}, stride_s={self.stride_s}"
            )
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
    """Decode the video topics of a ``read_mcap`` inside the read task.

    At ``message`` granularity every selected topic must be video, and each
    row is one RGB frame in a ``frame`` column, ``uint8`` of shape
    ``(height, width, 3)``, in place of ``data``. At ``window`` granularity
    each row adds, per video topic, a ``frames:<topic>`` tensor of shape
    ``(n, height, width, 3)`` and the frames' log times in
    ``frame_times:<topic>``. The other topics stay in the message lists. A
    window whose decoded frames exceed ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails
    the read, so use ``fps`` and ``resize`` to keep windows small.

    A topic is video when its schema name is a known video schema or
    ``read_mcap(video_topics=...)`` lists it. Its codec comes from the
    message's ``format`` field or its bytes (JPEG, PNG, H.264, H.265, VP9,
    AV1). Every emitted frame is complete, even when a read task starts
    mid-stream. Requires ``av`` for H.264, H.265, VP9 and AV1, and ``Pillow``
    for JPEG and PNG.

    Attributes:
        fps: Keep at most one frame per ``1/fps`` seconds of ``log_time`` per
            topic. The intervals are aligned to the epoch, so the kept frames
            do not depend on how the read is split into tasks. ``None`` keeps
            every frame.
        resize: Scale frames to this ``(height, width)``. ``None`` keeps the
            coded size.
    """

    fps: Optional[float] = None
    resize: Optional[Tuple[int, int]] = None

    def __post_init__(self):
        if self.fps is not None and (isinstance(self.fps, bool) or self.fps <= 0):
            raise ValueError(f"fps must be a positive number, got {self.fps!r}")
        if self.fps is not None and not (
            math.isfinite(self.fps) and round(_NS_PER_S / self.fps) >= 1
        ):
            # Thinning would never move on from an interval of 0 ns.
            raise ValueError(
                "fps must be finite and leave at least one nanosecond between "
                f"frames, got {self.fps!r}"
            )
        if self.resize is not None:
            if len(self.resize) != 2 or any(
                isinstance(v, bool) or not isinstance(v, int) or v <= 0
                for v in self.resize
            ):
                raise ValueError(
                    "resize must be a (height, width) pair of positive integers, "
                    f"got {self.resize!r}"
                )
            object.__setattr__(self, "resize", tuple(self.resize))

    @property
    def fps_interval_ns(self) -> Optional[int]:
        """Minimum log-time distance between two emitted frames of a topic."""
        return int(round(_NS_PER_S / self.fps)) if self.fps is not None else None


@dataclass(frozen=True)
class MCAPSelection:
    """Which messages a read selects.

    Attributes:
        topics: Topics to keep, or ``None`` for every topic.
        message_types: Schema names to keep, or ``None`` for every schema. A
            channel without a schema always passes.
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
        """Build a selection from ``read_mcap``'s arguments.

        An empty ``topics`` or ``message_types`` means no filter.
        """
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

    def digest(self, *options: str) -> str:
        """Short, stable hash of the selection, part of every coarse row's id.

        A window, topic or file row holds the selected messages, so its id must
        change with the selection. Otherwise a checkpoint written under one
        selection would skip rows of another. ``options`` adds the other read
        options that change what a row holds. A message row's content does not
        depend on the selection, so its id needs no digest.
        """
        parts = [
            ",".join(sorted(self.topics)) if self.topics is not None else "*",
            ",".join(sorted(self.message_types))
            if self.message_types is not None
            else "*",
            f"{self.start_time}-{self.end_time}"
            if self.time_range is not None
            else "*",
            *options,
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

    def schemas_settle(
        self, channels: Mapping[int, "Channel"], schemas: Mapping[int, "Schema"]
    ) -> bool:
        """Whether ``schemas`` has every schema that ``message_types`` must check.

        A summary can list a channel but leave its schema record inside a chunk
        (``repeat_schemas=False``). Such a channel passes :meth:`accepts_channel`
        unchecked, so a caller that needs the exact selection must read the
        chunks instead. Always ``True`` without ``message_types``.
        """
        if self.message_types is None:
            return True
        return all(
            not channel.schema_id or channel.schema_id in schemas
            for channel in channels.values()
            if self.topics is None or channel.topic in self.topics
        )

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

        The index holds the chunk's log-time bounds and, if the writer wrote
        message indexes, the ids of its channels. Without message indexes, only
        the time bounds are checked.
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

        ``has_channels`` is whether the summary lists any channels. If it does
        and none is selected, the file cannot match. If it lists none, the
        channels are unknown and only the time bounds are checked.
        """
        if has_channels and not selected_channel_ids:
            return False
        if statistics is not None and statistics.message_count > 0:
            return self.overlaps(
                statistics.message_start_time, statistics.message_end_time
            )
        return True
