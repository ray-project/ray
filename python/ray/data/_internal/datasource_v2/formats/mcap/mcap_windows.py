"""Window placement, window ownership and the coarse row layouts.

Window, topic and file rows pack many messages into one row of parallel lists:
entry *i* of each list column is the same message, in log-time order. The
functions here are pure, so the indexer, the reader and the tests reason about
the same windows and the same rows.
"""

import bisect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    FILE_GRANULARITY,
    ROW_ID_COLUMN,
    TOPIC_GRANULARITY,
    WINDOW_GRANULARITY,
    WindowSpec,
)

if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Message, Schema

# A window as ``[start, end)`` in nanoseconds.
Window = Tuple[int, int]


def place_windows(spec: WindowSpec, span_start: int, span_end: int) -> List[Window]:
    """The windows of ``spec`` that meet the closed span ``[span_start, span_end]``.

    ``span_start`` and ``span_end`` are the first and last selected log times of
    a file. With ``anchor="file_start"`` window 0 begins at ``span_start`` and
    nothing precedes it; with ``"epoch"`` or an absolute anchor, window starts
    are the anchor plus any whole number of strides, so windows line up across
    files. ``drop_partial`` drops the windows that end after the last message.
    """
    if span_end < span_start:
        return []
    length, stride = spec.length_ns, spec.stride_ns
    if spec.anchor == "file_start":
        base, first_k = span_start, 0
    else:
        base = 0 if spec.anchor == "epoch" else int(spec.anchor)
        # The first window that still reaches span_start: base + k*stride > span_start - length.
        first_k = -((span_start - length + 1 - base) // -stride)
    windows: List[Window] = []
    k = first_k
    while True:
        start = base + k * stride
        if start > span_end:
            break
        end = start + length
        if spec.drop_partial and end > span_end + 1:
            break
        windows.append((start, end))
        k += 1
    return windows


def owner_offsets(
    chunk_indexes: Sequence["ChunkIndex"], window_starts: Iterable[int]
) -> List[int]:
    """The byte offset of the chunk that owns each window.

    A window belongs to the last chunk, in log-time order of chunk starts,
    that starts at or before the window; a window before every chunk belongs
    to the first. The task that owns that chunk emits the window and reads
    past its own chunks to fill it. Every task derives this from the same
    summary, so each window is emitted exactly once.
    """
    ordered = sorted(
        chunk_indexes, key=lambda c: (c.message_start_time, c.chunk_start_offset)
    )
    starts = [c.message_start_time for c in ordered]
    owners = []
    for window_start in window_starts:
        index = max(bisect.bisect_right(starts, window_start) - 1, 0)
        owners.append(ordered[index].chunk_start_offset)
    return owners


# -- row layouts -------------------------------------------------------------

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
    pa.field("data", pa.list_(pa.binary())),
]


def coarse_row_schema(
    granularity: str, *, include_metadata: bool, include_row_id: bool
) -> pa.Schema:
    """The schema of window, topic or file rows.

    Payloads stay encoded (``list<binary>``): a row's decoding needs are met by
    its ``channels`` column, one struct per channel present in the row, which
    ``include_metadata=False`` drops. ``path`` is always present, because a
    coarse row is meaningless without the recording it came from.

    Args:
        granularity: ``window``, ``topic`` or ``file``.
        include_metadata: Whether the ``channels`` column is present.
        include_row_id: Whether ``row_id`` is present.

    Returns:
        The schema, columns in output order.
    """
    fields = [pa.field("path", pa.string())]
    if include_row_id:
        fields.append(pa.field(ROW_ID_COLUMN, pa.string()))
    if granularity == WINDOW_GRANULARITY:
        fields += [
            pa.field("window_start", pa.int64()),
            pa.field("window_end", pa.int64()),
            pa.field("num_messages", pa.int64()),
            pa.field("num_lead_in", pa.int32()),
            pa.field("topic", pa.list_(pa.string())),
        ]
    elif granularity == TOPIC_GRANULARITY:
        fields += [
            pa.field("topic", pa.string()),
            pa.field("start_time", pa.int64()),
            pa.field("end_time", pa.int64()),
            pa.field("num_messages", pa.int64()),
        ]
    elif granularity == FILE_GRANULARITY:
        fields += [
            pa.field("start_time", pa.int64()),
            pa.field("end_time", pa.int64()),
            pa.field("num_messages", pa.int64()),
            pa.field("topic", pa.list_(pa.string())),
        ]
    else:
        raise ValueError(f"not a coarse granularity: {granularity!r}")
    fields += _MESSAGE_LISTS
    if include_metadata:
        fields.append(pa.field("channels", pa.list_(CHANNEL_STRUCT)))
    return pa.schema(fields)


@dataclass
class CoarseRow:
    """One window, topic or file row before it is turned into Arrow."""

    path: str
    row_id: str
    # Messages in log-time order, with their channel and schema.
    messages: List[Tuple[Optional["Schema"], "Channel", "Message"]]
    # Window rows only.
    window: Optional[Window] = None
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
    rows: List[CoarseRow] = field(default_factory=list)
    payload_bytes: int = 0

    def add(self, row: CoarseRow) -> None:
        self.rows.append(row)
        self.payload_bytes += row.payload_bytes

    def __len__(self) -> int:
        return len(self.rows)

    def build(self) -> pa.Table:
        columns: Dict[str, Any] = {
            "path": pa.array([r.path for r in self.rows], pa.string())
        }
        if self.include_row_id:
            columns[ROW_ID_COLUMN] = pa.array(
                [r.row_id for r in self.rows], pa.string()
            )
        times = [[m.log_time for _, _, m in r.messages] for r in self.rows]
        if self.granularity == WINDOW_GRANULARITY:
            windows = []
            for row in self.rows:
                assert row.window is not None, "a window row names its window"
                windows.append(row.window)
            columns["window_start"] = pa.array([w[0] for w in windows], pa.int64())
            columns["window_end"] = pa.array([w[1] for w in windows], pa.int64())
            columns["num_messages"] = pa.array(
                [len(r.messages) for r in self.rows], pa.int64()
            )
            columns["num_lead_in"] = pa.array(
                [r.num_lead_in for r in self.rows], pa.int32()
            )
            columns["topic"] = pa.array(
                [[c.topic for _, c, _ in r.messages] for r in self.rows],
                pa.list_(pa.string()),
            )
        else:
            if self.granularity == TOPIC_GRANULARITY:
                columns["topic"] = pa.array([r.topic for r in self.rows], pa.string())
            columns["start_time"] = pa.array([t[0] for t in times], pa.int64())
            columns["end_time"] = pa.array([t[-1] for t in times], pa.int64())
            columns["num_messages"] = pa.array(
                [len(r.messages) for r in self.rows], pa.int64()
            )
            if self.granularity == FILE_GRANULARITY:
                columns["topic"] = pa.array(
                    [[c.topic for _, c, _ in r.messages] for r in self.rows],
                    pa.list_(pa.string()),
                )
        columns["channel_id"] = pa.array(
            [[m.channel_id for _, _, m in r.messages] for r in self.rows],
            pa.list_(pa.int32()),
        )
        columns["log_time"] = pa.array(times, pa.list_(pa.int64()))
        columns["publish_time"] = pa.array(
            [[m.publish_time for _, _, m in r.messages] for r in self.rows],
            pa.list_(pa.int64()),
        )
        columns["sequence"] = pa.array(
            [[m.sequence for _, _, m in r.messages] for r in self.rows],
            pa.list_(pa.uint32()),
        )
        columns["data"] = pa.array(
            [[m.data for _, _, m in r.messages] for r in self.rows],
            pa.list_(pa.binary()),
        )
        if self.include_metadata:
            columns["channels"] = pa.array(
                [_channel_structs(r.messages) for r in self.rows],
                pa.list_(CHANNEL_STRUCT),
            )
        return pa.table(columns)


def _channel_structs(
    messages: Iterable[Tuple[Optional["Schema"], "Channel", "Message"]]
) -> List[Dict[str, Any]]:
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
