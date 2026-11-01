"""Message filters and video options of ``read_mcap``.

``TimeRange`` is the public time filter of ``read_mcap``, and ``VideoOptions``
how a read decodes its video topics. Both are re-exported from
``ray.data.datasource``.

``MCAPSelection`` bundles the ``topics``, ``message_types`` and ``time_range``
filters. It decides from summary records whether a file, chunk or channel can
hold selected messages. The indexer and the reader both use it, so they agree
on which chunks can match.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, FrozenSet, Iterable, Mapping, Optional, Set, Tuple

from ray._common.utils import env_float
from ray.util.annotations import PublicAPI

# Type-only: ``import ray.data`` loads this module, and ``mcap`` is optional.
if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Schema, Statistics

# Name of the deterministic per-row id column (see ``MCAPReader``).
ROW_ID_COLUMN = "row_id"

# What one output row of a read of paths is (``read_granularity``).
# ``message`` is one message per row. ``attachment`` and ``metadata`` are one
# Attachment or Metadata record per row. Those records live outside the message
# stream (``mcap_records``).
MESSAGE_GRANULARITY = "message"
ATTACHMENT_GRANULARITY = "attachment"
METADATA_GRANULARITY = "metadata"
GRANULARITIES: Tuple[str, ...] = (
    MESSAGE_GRANULARITY,
    ATTACHMENT_GRANULARITY,
    METADATA_GRANULARITY,
)
RECORD_GRANULARITIES: Tuple[str, ...] = (ATTACHMENT_GRANULARITY, METADATA_GRANULARITY)

_NS_PER_S = 1_000_000_000

# How far back, in seconds, a decoder looks for a keyframe before the first
# frame it outputs. Not a ``read_mcap`` argument:
# ``RAY_DATA_MCAP_MAX_LEAD_IN_S`` overrides it.
DEFAULT_MAX_LEAD_IN_S = 10.0
DEFAULT_MAX_LEAD_IN_NS = int(DEFAULT_MAX_LEAD_IN_S * _NS_PER_S)


def max_lead_in_ns() -> int:
    """The look-back cap in nanoseconds, honouring ``RAY_DATA_MCAP_MAX_LEAD_IN_S``."""
    seconds = env_float("RAY_DATA_MCAP_MAX_LEAD_IN_S", DEFAULT_MAX_LEAD_IN_S)
    if seconds < 0:
        raise ValueError(
            f"RAY_DATA_MCAP_MAX_LEAD_IN_S must be non-negative, got {seconds}"
        )
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
class VideoOptions:
    """Decode the video topics of a ``read_mcap`` inside the read task.

    In a read of paths every selected topic must be video, and each row is one
    RGB frame in a ``frame`` column, ``uint8`` of shape ``(height, width, 3)``,
    in place of ``data``. Every message's frame is kept: for frames at a lower
    rate, use a sample read with ``steps.across(fps=...)`` over clips of each
    recording. In a sample read (``read_mcap(samples=...)``), each
    video topic's steps become a ``frames:<topic>`` tensor of shape
    ``(steps, height, width, 3)``, and the other topics keep their payloads. A
    sample row whose frames exceed ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails the
    read, so use ``resize`` to keep rows small.

    A topic is video when its schema name is a known video schema or
    ``read_mcap(video_topics=...)`` lists it. Its codec comes from the
    message's ``format`` field or its bytes (JPEG, PNG, H.264, H.265, VP9,
    AV1). Every frame is complete, even when a read task starts mid-stream: it
    is decoded from the last keyframe before it, at most
    ``RAY_DATA_MCAP_MAX_LEAD_IN_S`` (10 s) back. Requires ``av`` for H.264,
    H.265, VP9 and AV1, and ``Pillow`` for JPEG and PNG.

    Attributes:
        resize: Scale frames to exactly this ``(height, width)``. A frame of
            another aspect ratio is stretched to it, not cropped or padded.
            ``None`` keeps the coded size.
    """

    resize: Optional[Tuple[int, int]] = None

    def __post_init__(self):
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
