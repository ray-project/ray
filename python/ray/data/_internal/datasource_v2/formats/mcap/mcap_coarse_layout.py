"""The Arrow layout of window, topic and file rows.

A coarse row packs many messages into one row of parallel lists: entry *i* of
each list column is the same message, in log-time order. ``CoarseRowBatch``
turns a task's rows into a table and builds only ``path`` and the projected
columns.
"""

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Tuple,
)

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    FILE_GRANULARITY,
    ROW_ID_COLUMN,
    TOPIC_GRANULARITY,
    WINDOW_GRANULARITY,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_windows import Window

if TYPE_CHECKING:
    from mcap.records import Channel, Message, Schema

# A message inside a coarse row: schema, channel, record.
_Entry = Tuple[Optional["Schema"], "Channel", "Message"]

CHANNEL_STRUCT = pa.struct(
    [
        pa.field("channel_id", pa.int32()),
        pa.field("topic", pa.string()),
        pa.field("message_encoding", pa.string()),
        pa.field("schema_name", pa.string()),
        pa.field("schema_encoding", pa.string()),
        pa.field("schema_data", pa.binary()),
        pa.field("metadata", pa.map_(pa.string(), pa.string())),
    ]
)

_MESSAGE_LISTS = [
    pa.field("channel_id", pa.list_(pa.int32())),
    pa.field("log_time", pa.list_(pa.int64())),
    pa.field("publish_time", pa.list_(pa.int64())),
    pa.field("sequence", pa.list_(pa.uint32())),
    # A row is one list element of one contiguous Arrow array. With int32
    # offsets, a row over 2 GiB fails in Arrow's builder or Ray's chunk
    # combiner. int64 offsets cost 4 bytes per message and keep the schema
    # independent of RAY_DATA_MCAP_MAX_ROW_BYTES, which only guards memory.
    pa.field("data", pa.large_list(pa.large_binary())),
]

# The columns between ``row_id`` and the message lists, per coarse granularity.
_GRANULARITY_FIELDS: Dict[str, List[pa.Field]] = {
    WINDOW_GRANULARITY: [
        pa.field("window_start", pa.int64()),
        pa.field("window_end", pa.int64()),
        pa.field("num_messages", pa.int64()),
        pa.field("num_lead_in", pa.int32()),
        pa.field("topic", pa.list_(pa.string())),
    ],
    TOPIC_GRANULARITY: [
        pa.field("topic", pa.string()),
        pa.field("start_time", pa.int64()),
        pa.field("end_time", pa.int64()),
        pa.field("num_messages", pa.int64()),
        pa.field("num_lead_in", pa.int32()),
    ],
    FILE_GRANULARITY: [
        pa.field("start_time", pa.int64()),
        pa.field("end_time", pa.int64()),
        pa.field("num_messages", pa.int64()),
        pa.field("topic", pa.list_(pa.string())),
    ],
}


def coarse_row_schema(
    granularity: str, *, include_metadata: bool, include_row_id: bool
) -> pa.Schema:
    """The schema of window, topic or file rows.

    Payloads stay encoded, in ``large_list<large_binary>`` so a row is not
    capped at 2 GiB. The ``channels`` column holds what decoding needs: one
    struct per channel in the row. ``path`` is always present, because a coarse
    row means little without its recording.

    Args:
        granularity: ``window``, ``topic`` or ``file``.
        include_metadata: Whether the ``channels`` column is present.
        include_row_id: Whether ``row_id`` is present.

    Returns:
        The schema, columns in output order.
    """
    if granularity not in _GRANULARITY_FIELDS:
        raise ValueError(f"not a coarse granularity: {granularity!r}")
    fields = [pa.field("path", pa.string())]
    if include_row_id:
        fields.append(pa.field(ROW_ID_COLUMN, pa.string()))
    fields += _GRANULARITY_FIELDS[granularity]
    fields += _MESSAGE_LISTS
    if include_metadata:
        fields.append(pa.field("channels", pa.list_(CHANNEL_STRUCT)))
    return pa.schema(fields)


@dataclass
class CoarseRow:
    """One window, topic or file row before it is turned into Arrow."""

    path: str
    row_id: str
    # Messages in log-time order, each with its schema and channel.
    messages: List[_Entry]
    # Window rows only.
    window: Optional[Window] = None
    # Window and topic rows: how many leading messages come before the row's
    # own span. They reach back to a keyframe, so the row decodes on its own.
    num_lead_in: int = 0
    # Topic rows only.
    topic: Optional[str] = None

    @property
    def payload_bytes(self) -> int:
        return sum(len(m.data) for _, _, m in self.messages)


@dataclass
class CoarseRowBatch:
    """Accumulates coarse rows and builds one Arrow table from them."""

    granularity: str
    include_metadata: bool
    include_row_id: bool
    # The projected columns, or ``None`` for all. ``path`` is always built, so
    # the row count survives a projection to no columns.
    columns: Optional[FrozenSet[str]] = None
    rows: List[CoarseRow] = field(default_factory=list)
    payload_bytes: int = 0

    def add(self, row: CoarseRow) -> None:
        self.rows.append(row)
        self.payload_bytes += row.payload_bytes

    def __len__(self) -> int:
        return len(self.rows)

    def build(self) -> pa.Table:
        """Turn the accumulated rows into one table, columns in schema order.

        Only ``path`` and the projected columns are built, so a ``count()``
        does not copy the payloads.
        """
        times = [[m.log_time for _, _, m in r.messages] for r in self.rows]
        builders: Dict[str, Callable[[], Any]] = {
            "path": lambda: pa.array([r.path for r in self.rows], pa.string())
        }
        if self.include_row_id:
            builders[ROW_ID_COLUMN] = lambda: pa.array(
                [r.row_id for r in self.rows], pa.string()
            )
        if self.granularity == WINDOW_GRANULARITY:
            builders.update(self._window_columns())
        else:
            builders.update(self._topic_or_file_columns(times))
        builders.update(self._message_list_columns(times))
        if self.include_metadata:
            builders["channels"] = lambda: pa.array(
                [_channel_structs(r.messages) for r in self.rows],
                pa.list_(CHANNEL_STRUCT),
            )
        return pa.table(
            {
                name: build()
                for name, build in builders.items()
                if name == "path" or self.columns is None or name in self.columns
            }
        )

    def _window_columns(self) -> Dict[str, Callable[[], Any]]:
        """The window rows' columns before the message lists."""
        windows = []
        for row in self.rows:
            assert row.window is not None, "a window row names its window"
            windows.append(row.window)
        return {
            "window_start": lambda: pa.array([w[0] for w in windows], pa.int64()),
            "window_end": lambda: pa.array([w[1] for w in windows], pa.int64()),
            "num_messages": lambda: pa.array(
                [len(r.messages) for r in self.rows], pa.int64()
            ),
            "num_lead_in": lambda: pa.array(
                [r.num_lead_in for r in self.rows], pa.int32()
            ),
            "topic": self._topic_lists,
        }

    def _topic_or_file_columns(
        self, times: List[List[int]]
    ) -> Dict[str, Callable[[], Any]]:
        """The topic or file rows' columns before the message lists."""
        columns: Dict[str, Callable[[], Any]] = {}
        if self.granularity == TOPIC_GRANULARITY:
            columns["topic"] = lambda: pa.array(
                [r.topic for r in self.rows], pa.string()
            )
        # The span is that of the row's own messages; a lead-in precedes it.
        columns["start_time"] = lambda: pa.array(
            [t[r.num_lead_in] for t, r in zip(times, self.rows)], pa.int64()
        )
        columns["end_time"] = lambda: pa.array([t[-1] for t in times], pa.int64())
        columns["num_messages"] = lambda: pa.array(
            [len(r.messages) for r in self.rows], pa.int64()
        )
        if self.granularity == TOPIC_GRANULARITY:
            columns["num_lead_in"] = lambda: pa.array(
                [r.num_lead_in for r in self.rows], pa.int32()
            )
        if self.granularity == FILE_GRANULARITY:
            columns["topic"] = self._topic_lists
        return columns

    def _message_list_columns(
        self, times: List[List[int]]
    ) -> Dict[str, Callable[[], Any]]:
        """The parallel lists: entry *i* of each is the row's *i*-th message."""
        return {
            "channel_id": lambda: pa.array(
                [[m.channel_id for _, _, m in r.messages] for r in self.rows],
                pa.list_(pa.int32()),
            ),
            "log_time": lambda: pa.array(times, pa.list_(pa.int64())),
            "publish_time": lambda: pa.array(
                [[m.publish_time for _, _, m in r.messages] for r in self.rows],
                pa.list_(pa.int64()),
            ),
            "sequence": lambda: pa.array(
                [[m.sequence for _, _, m in r.messages] for r in self.rows],
                pa.list_(pa.uint32()),
            ),
            "data": self._data_column,
        }

    def _topic_lists(self) -> pa.Array:
        return pa.array(
            [[c.topic for _, c, _ in r.messages] for r in self.rows],
            pa.list_(pa.string()),
        )

    def _data_column(self) -> pa.Array:
        return pa.array(
            [[m.data for _, _, m in r.messages] for r in self.rows],
            pa.large_list(pa.large_binary()),
        )


def _channel_structs(messages: Iterable[_Entry]) -> List[Dict[str, Any]]:
    """One struct per distinct channel among ``messages``, in first-seen order."""
    seen: Dict[int, Dict[str, Any]] = {}
    for schema, channel, _ in messages:
        if channel.id in seen:
            continue
        seen[channel.id] = {
            "channel_id": channel.id,
            "topic": channel.topic,
            "message_encoding": channel.message_encoding,
            "schema_name": schema.name if schema else None,
            "schema_encoding": schema.encoding if schema else None,
            "schema_data": schema.data if schema else None,
            "metadata": list(channel.metadata.items()),
        }
    return list(seen.values())
