"""Message filters of ``read_mcap``.

``TimeRange`` is the public time filter of ``read_mcap``. It is re-exported from
``ray.data.datasource``.

``MCAPSelection`` bundles the ``topics``, ``message_types`` and ``time_range``
filters. It decides from summary records whether a file, chunk or channel can
hold selected messages. The indexer and the reader both use it, so they agree
on which chunks can match.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, FrozenSet, Iterable, Mapping, Optional, Set

# Type-only: ``import ray.data`` loads this module, and ``mcap`` is optional.
if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Schema, Statistics

# Name of the deterministic per-row id column (see ``MCAPReader``).
ROW_ID_COLUMN = "row_id"


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
