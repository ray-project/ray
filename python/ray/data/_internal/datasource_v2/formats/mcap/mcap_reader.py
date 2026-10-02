"""Reads the chunks a listing row assigned to a task and builds rows.

``MCAPReader.read`` receives a ``FileManifest`` whose rows name files and, per
file, the byte offsets of the chunks this task owns. For each file it seeks to
those chunks, decompresses them, keeps the selected messages and yields Arrow
tables of about ``target_block_size`` bytes. With ``log_time_order`` the owned
chunks are merged by log time as they are read (a heap of chunk indexes and
messages, as the mcap library does for a whole file), so memory holds only the
chunks that overlap in time.

At ``message`` granularity a row carries the same columns the legacy datasource
produces, plus ``channel_metadata`` (a ``map<string, string>`` of the channel's
metadata) and, on request, ``row_id``: a deterministic name for the message
built from the file path, the chunk's byte offset and the message's position in
the chunk.

At ``window``, ``topic`` and ``file`` granularity a row packs many messages
into parallel list columns (see ``mcap_windows``). A window belongs to the
task owning the chunk its start falls in; that task reads on to the window's
end, and back to the last keyframe of every video topic, so the row is
decodable on its own.
"""

import bisect
import heapq
import json
import logging
from dataclasses import dataclass
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import pyarrow as pa
from pyarrow.fs import FileSystem, LocalFileSystem
from typing_extensions import override

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ATTACHMENT_GRANULARITY,
    MESSAGE_GRANULARITY,
    METADATA_GRANULARITY,
    ROW_ID_COLUMN,
    TOPIC_GRANULARITY,
    WINDOW_GRANULARITY,
    MCAPSelection,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_records import (
    RecordRowBatch,
    iter_records,
    read_record_at,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    attachment_unit_id,
    message_row_id,
    metadata_unit_id,
    topic_unit_id,
    unindexed_message_row_id,
    unindexed_record_row_id,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    detect_codec,
    is_keyframe,
    is_video_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_windows import (
    CoarseRow,
    CoarseRowBatch,
    owner_offsets,
    place_windows,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.tensor_extensions.arrow import convert_to_pyarrow_array
from ray.data._internal.util import GiB, iterate_with_retry
from ray.data.block import BlockMetadata
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Message, Schema
    from mcap.summary import Summary

logger = logging.getLogger(__name__)

# Rough in-memory cost of one row beyond its payload: the timestamps, the
# sequence number, the topic string and the Arrow offsets around them. Only
# used to decide when a table is big enough to yield.
_ROW_OVERHEAD_BYTES = 96

# Largest payload a topic or file row may carry. Such a row is one Arrow list
# cell; past this it stops being a unit anything downstream can hold.
DEFAULT_MAX_ROW_BYTES = GiB

# Columns a message row carries, in output order. Metadata columns are present
# only with ``include_metadata``; ``row_id`` only with ``include_row_id``.
DATA_COLUMNS = ("data", "topic", "log_time", "publish_time", "sequence")
METADATA_COLUMNS = (
    "channel_id",
    "message_encoding",
    "schema_name",
    "schema_encoding",
    "schema_data",
    "channel_metadata",
)

# One selected message as the reader sees it: its schema (``None`` for a
# schema-less channel), its channel, the record, and its ``row_id``.
_Selected = Tuple[Optional["Schema"], "Channel", "Message", str]
# A message inside a coarse row: schema, channel, record.
_Entry = Tuple[Optional["Schema"], "Channel", "Message"]


def message_schema(
    *,
    include_metadata: bool,
    include_row_id: bool,
    data_type: Optional[pa.DataType] = None,
) -> pa.Schema:
    """The Arrow schema of message rows, before partition and synthesized columns.

    Types follow what the legacy datasource's block builder infers from Python
    values (``int64`` for every integer), so a dataset read through either path
    concatenates. ``schema_data`` is dictionary-encoded: a schema definition is
    stored once per file but repeated on every row, and ROS 2 definitions run
    kilobytes each. ``channel_metadata`` is a ``map`` rather than an inferred
    ``struct`` so its type does not depend on which keys a recorder wrote.

    Args:
        include_metadata: Whether the per-channel and per-schema columns are present.
        include_row_id: Whether ``row_id`` is present.
        data_type: Type of the ``data`` column: ``binary``, or the type of the
            decoded JSON values when every selected channel is JSON-encoded (the
            caller sampled one message for it).

    Returns:
        The schema, columns in output order.
    """
    fields = [
        pa.field("data", data_type if data_type is not None else pa.binary()),
        pa.field("topic", pa.string()),
        pa.field("log_time", pa.int64()),
        pa.field("publish_time", pa.int64()),
        pa.field("sequence", pa.int64()),
    ]
    if include_metadata:
        fields += [
            pa.field("channel_id", pa.int64()),
            pa.field("message_encoding", pa.string()),
            pa.field("schema_name", pa.string()),
            pa.field("schema_encoding", pa.string()),
            pa.field("schema_data", pa.dictionary(pa.int32(), pa.binary())),
            pa.field("channel_metadata", pa.map_(pa.string(), pa.string())),
        ]
    if include_row_id:
        fields.append(pa.field(ROW_ID_COLUMN, pa.string()))
    return pa.schema(fields)


def decode_payload(channel: "Channel", data: bytes, where: str) -> Any:
    """Decode a JSON payload into Python values.

    Only called when the dataset's ``data`` column was planned as decoded JSON,
    which requires every selected channel of the sampled files to be
    JSON-encoded. Arrow has no column type for a mix of bytes and decoded
    values (the fallback is a pickled-object column, which the V2 read path
    refuses), so a channel of another encoding, or a message that is not valid
    JSON, fails the read naming the message instead of poisoning the column.

    Args:
        channel: The message's channel.
        data: The payload.
        where: What to name in the error: a ``row_id`` or a file.

    Returns:
        The decoded JSON value.
    """
    if channel.message_encoding != "json":
        raise ValueError(
            f"{where}: topic {channel.topic!r} is {channel.message_encoding!r}-encoded, "
            "but the dataset's `data` column holds decoded JSON because every "
            "selected channel of the sampled files was JSON-encoded. Select topics "
            "of one encoding per read (pass `topics=` or `message_types=`)."
        )
    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(
            f"{where}: message on JSON-encoded topic {channel.topic!r} is not valid "
            f"JSON: {e}"
        ) from e


def is_video_channel(
    channel: "Channel", schema: Optional["Schema"], video: Optional[VideoOptions]
) -> bool:
    """Whether a channel is treated as video: forced by name, or by its schema."""
    if video is not None and video.topics is not None and channel.topic in video.topics:
        return True
    return is_video_schema(schema.name if schema else None)


class _MessageTableBuilder:
    """Accumulates selected messages column by column and builds Arrow tables."""

    def __init__(
        self,
        *,
        columns: Optional[Set[str]],
        include_metadata: bool,
        include_row_id: bool,
        decode_json: bool,
    ):
        # ``None`` means every column. The set decides what is accumulated, so a
        # pruned read never decodes a JSON payload it will not return.
        self._want = (
            (lambda name: True) if columns is None else (lambda name: name in columns)
        )
        self._include_metadata = include_metadata
        self._include_row_id = include_row_id
        # Fixed by the planned schema: ``data`` is decoded JSON values or bytes
        # for every row of the dataset, never a mix.
        self._decode_json = decode_json
        self.reset()

    def reset(self) -> None:
        self.num_rows = 0
        self.estimated_bytes = 0
        self._columns: Dict[str, List[Any]] = {}

    def add(self, selected: _Selected) -> None:
        schema, channel, message, row_id = selected
        self.num_rows += 1
        self.estimated_bytes += len(message.data) + _ROW_OVERHEAD_BYTES
        put = self._columns.setdefault
        if self._want("data"):
            put("data", []).append(
                decode_payload(channel, message.data, row_id)
                if self._decode_json
                else message.data
            )
        if self._want("topic"):
            put("topic", []).append(channel.topic)
        if self._want("log_time"):
            put("log_time", []).append(message.log_time)
        if self._want("publish_time"):
            put("publish_time", []).append(message.publish_time)
        if self._want("sequence"):
            put("sequence", []).append(message.sequence)
        if self._include_metadata:
            if self._want("channel_id"):
                put("channel_id", []).append(message.channel_id)
            if self._want("message_encoding"):
                put("message_encoding", []).append(channel.message_encoding)
            if self._want("schema_name"):
                put("schema_name", []).append(schema.name if schema else None)
            if self._want("schema_encoding"):
                put("schema_encoding", []).append(schema.encoding if schema else None)
            if self._want("schema_data"):
                put("schema_data", []).append(schema.data if schema else None)
            if self._want("channel_metadata"):
                put("channel_metadata", []).append(list(channel.metadata.items()))
        if self._include_row_id and self._want(ROW_ID_COLUMN):
            put(ROW_ID_COLUMN, []).append(row_id)

    def build(self) -> pa.Table:
        n = self.num_rows
        cols = self._columns
        arrays: Dict[str, pa.Array] = {}
        if "data" in cols:
            if self._decode_json:
                # JSON payloads decoded to Python values: let Ray's converter
                # infer a struct, as the legacy block builder does. Blocks whose
                # structs differ (a key missing from some messages) are unified
                # downstream with nulls.
                arrays["data"] = convert_to_pyarrow_array(cols["data"], "data")
            else:
                arrays["data"] = pa.array(cols["data"], type=pa.binary())
        for name, type_ in (
            ("topic", pa.string()),
            ("log_time", pa.int64()),
            ("publish_time", pa.int64()),
            ("sequence", pa.int64()),
            ("channel_id", pa.int64()),
            ("message_encoding", pa.string()),
            ("schema_name", pa.string()),
            ("schema_encoding", pa.string()),
        ):
            if name in cols:
                arrays[name] = pa.array(cols[name], type=type_)
        if "schema_data" in cols:
            arrays["schema_data"] = pa.array(
                cols["schema_data"], type=pa.binary()
            ).dictionary_encode()
        if "channel_metadata" in cols:
            arrays["channel_metadata"] = pa.array(
                cols["channel_metadata"], type=pa.map_(pa.string(), pa.string())
            )
        if ROW_ID_COLUMN in cols:
            arrays[ROW_ID_COLUMN] = pa.array(cols[ROW_ID_COLUMN], type=pa.string())
        if not arrays:
            # Every column was pruned (``count()`` projects to nothing): keep
            # the row count through a stub column, as ``FileReader`` does.
            return pa.table({_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(n)})
        return pa.table(arrays)


@dataclass(frozen=True)
class _Assignment:
    """What one task reads of one file: its chunks, and at topic granularity
    the topic. ``offsets`` is ``None`` for a whole-file listing row."""

    path: str
    offsets: Optional[Set[int]]
    topic: Optional[str] = None

    @property
    def unit(self) -> ReadUnit:
        if self.topic is not None:
            return ReadUnit(id=topic_unit_id(self.path, self.topic), source=self.path)
        return ReadUnit(id=self.path, source=self.path, count=1)


@dataclass
class _ChannelMessages:
    """One video channel's messages in a read range, for lead-in lookups."""

    channel: "Channel"
    schema: Optional["Schema"]
    times: List[int]
    entries: List[_Entry]
    codec: Optional[VideoCodec]
    # Cached ``is_keyframe`` per entry; windows overlap, so each is asked often.
    keyframe: List[Optional[bool]]

    def is_keyframe_at(self, index: int) -> bool:
        cached = self.keyframe[index]
        if cached is None:
            assert self.codec is not None
            cached = is_keyframe(self.entries[index][2].data, self.codec)
            self.keyframe[index] = cached
        return cached


@DeveloperAPI
class MCAPReader(Reader[FileManifest], SupportsMetadata):
    """Reads the chunks of MCAP files a manifest assigns to one task.

    Created by ``MCAPScanner.create_reader`` with every pushdown applied:
    the message selection, the projected columns and the per-task row limit.
    Also answers ``count()`` from the summaries (:meth:`read_metadata`) when
    the selection can be counted there.
    """

    # Files whose summaries one count task reads. A summary read is two small
    # ranged requests, so several per task amortize the task overhead.
    _COUNT_ROWS_BATCH_SIZE = 16

    def __init__(
        self,
        *,
        selection: MCAPSelection,
        granularity: str = MESSAGE_GRANULARITY,
        window: Optional[WindowSpec] = None,
        video: Optional[VideoOptions] = None,
        include_metadata: bool = True,
        include_row_id: bool = False,
        log_time_order: bool = True,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        synthesized_columns: Sequence[SynthesizedColumn] = (),
        target_block_size: Optional[int] = None,
        schema: Optional[pa.Schema] = None,
        decode_json: bool = False,
        max_row_bytes: int = DEFAULT_MAX_ROW_BYTES,
    ):
        """Initialize the reader.

        Args:
            selection: Which messages to keep.
            granularity: What one row is: ``message``, ``window``, ``topic`` or
                ``file``.
            window: Window placement, required at ``window`` granularity.
            video: How video topics are recognised and how far their lead-in
                reaches; only used at ``window`` and ``topic`` granularity.
            include_metadata: Whether to emit the channel and schema columns.
            include_row_id: Whether to emit ``row_id``.
            log_time_order: Whether a task's message rows come out in ascending
                ``log_time`` order (its chunks merged by time) or in file order.
                Coarse rows are always in log-time order.
            columns: Columns to produce, in order; ``None`` for all of them.
            limit: Stop after this many rows per manifest.
            filesystem: Filesystem the paths resolve against; local when ``None``.
            partitioning: Path partitioning whose values become string columns.
            synthesized_columns: Columns appended to every table rather than
                read, e.g. ``PathColumn`` for ``include_paths``.
            target_block_size: Estimated bytes per yielded table; ``None`` yields
                one table per file.
            schema: Dataset schema, used to type partition columns.
            decode_json: Whether ``data`` holds decoded JSON values (planned so
                because every selected channel of the sample was JSON-encoded)
                rather than the payload bytes.
            max_row_bytes: Largest payload a topic or file row may carry before
                the read fails rather than build it.
        """
        if granularity == WINDOW_GRANULARITY and window is None:
            raise ValueError("window granularity needs a WindowSpec")
        self._selection = selection
        self._decode_json = decode_json
        self._granularity = granularity
        self._window = window
        self._video = video
        self._include_metadata = include_metadata
        self._include_row_id = include_row_id
        self._log_time_order = log_time_order
        self._columns = list(columns) if columns is not None else None
        self._limit = limit
        self._filesystem = filesystem
        self._partition_parser = (
            PathPartitionParser(partitioning) if partitioning is not None else None
        )
        self._synthesized_columns = tuple(synthesized_columns)
        self._target_block_size = target_block_size
        self._schema = schema
        self._max_row_bytes = max_row_bytes

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read the files and chunks named by ``input_split``.

        Rows of one file are read together however many manifest rows name it;
        files come out in manifest order.
        """
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        retried_io_errors = DataContext.get_current().retried_io_errors
        remaining = self._limit
        for assignment in _assignments(input_split):
            tables = iterate_with_retry(
                partial(self._read_file, assignment),
                f"read MCAP file {assignment.path}",
                match=retried_io_errors,
            )
            for table in tables:
                if remaining is not None:
                    if remaining <= 0:
                        return
                    if table.num_rows > remaining:
                        table = table.slice(0, remaining)
                    remaining -= table.num_rows
                yield table

    # -- metadata ----------------------------------------------------------

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield one ``BlockMetadata`` per file with its selected message count.

        ``Statistics`` holds the count per channel, so a selection by topic or
        schema is summed from it without reading a payload. A file with no
        statistics is counted by scanning it, which is still exact.
        """
        from mcap.reader import SeekingReader
        from mcap.records import Attachment, Metadata

        filesystem = self._filesystem or LocalFileSystem()
        for path in dict.fromkeys(str(p) for p in file_manifest.paths):
            with filesystem.open_input_file(path) as f:
                summary = SeekingReader(f).get_summary()
                statistics = summary.statistics if summary is not None else None
                if self._granularity == ATTACHMENT_GRANULARITY:
                    if statistics is not None:
                        num_rows = statistics.attachment_count
                    elif summary is not None and summary.attachment_indexes:
                        num_rows = len(summary.attachment_indexes)
                    else:
                        num_rows = sum(1 for _ in iter_records(f, Attachment))
                elif self._granularity == METADATA_GRANULARITY:
                    if statistics is not None:
                        num_rows = statistics.metadata_count
                    elif summary is not None and summary.metadata_indexes:
                        num_rows = len(summary.metadata_indexes)
                    else:
                        num_rows = sum(1 for _ in iter_records(f, Metadata))
                elif summary is not None and statistics is not None:
                    selected = self._selection.selected_channel_ids(
                        summary.channels, summary.schemas
                    )
                    num_rows = sum(
                        statistics.channel_message_counts.get(cid, 0)
                        for cid in selected
                    )
                else:
                    num_rows = sum(1 for _ in self._iter_unindexed(f, path))
            yield BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                exec_stats=None,
                input_files=(path,),
            )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        # A time range cannot be counted from statistics; a coarse row is not a
        # message, so nothing counts it. Metadata records carry no time.
        if self._granularity == METADATA_GRANULARITY:
            return {MetadataType.NUM_ROWS}
        if (
            self._granularity not in (MESSAGE_GRANULARITY, ATTACHMENT_GRANULARITY)
            or self._selection.time_range is not None
        ):
            return set()
        return {MetadataType.NUM_ROWS}

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        return self._COUNT_ROWS_BATCH_SIZE

    # -- one file ----------------------------------------------------------

    def _read_file(self, assignment: _Assignment) -> Iterator[pa.Table]:
        """Yield the tables of one file, limited to the assigned chunks."""
        from mcap.reader import SeekingReader

        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(assignment.path) as f:
            if self._granularity in (ATTACHMENT_GRANULARITY, METADATA_GRANULARITY):
                yield from self._record_tables(f, assignment)
                return
            summary = SeekingReader(f).get_summary()
            if summary is not None and not summary.chunk_indexes:
                summary = None
            if self._granularity == MESSAGE_GRANULARITY:
                if summary is None:
                    messages = self._iter_unindexed(f, assignment.path)
                else:
                    messages = self._iter_chunks(
                        f, assignment.path, summary, assignment.offsets
                    )
                yield from self._tables(messages, assignment)
            elif self._granularity == WINDOW_GRANULARITY:
                yield from self._window_tables(f, assignment, summary)
            elif self._granularity == TOPIC_GRANULARITY:
                yield from self._topic_tables(f, assignment, summary)
            else:
                yield from self._file_tables(f, assignment, summary)

    def _candidate_chunks(
        self, summary: "Summary", selected: Set[int]
    ) -> List["ChunkIndex"]:
        """The file's chunks that may hold a selected message, in file order.

        The indexer listed exactly these, so a window's owner is computed over
        the same chunks on both sides.
        """
        return sorted(
            (
                c
                for c in summary.chunk_indexes
                if self._selection.chunk_may_match(c, selected)
            ),
            key=lambda c: c.chunk_start_offset,
        )

    def _iter_chunks(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        offsets: Optional[Set[int]],
        *,
        chunk_indexes: Optional[List["ChunkIndex"]] = None,
        selected: Optional[Set[int]] = None,
        time_bounds: Optional[Tuple[Optional[int], Optional[int]]] = None,
        log_time_order: Optional[bool] = None,
    ) -> Iterator[_Selected]:
        """Yield the selected messages of the owned chunks of an indexed file.

        With log-time order the chunks are merged through one heap holding
        chunk indexes (keyed by their first log time) and messages (keyed by
        theirs): a chunk is expanded when it reaches the top, so at most the
        chunks that overlap in time are decompressed at once, and a message is
        yielded only once every chunk that could precede it has been expanded.
        Without it the chunks are read in file order.

        ``time_bounds`` overrides the selection's time range; coarse rows use
        it to read a window's lead-in, which lies before the range.
        """
        if selected is None:
            selected = self._selection.selected_channel_ids(
                summary.channels, summary.schemas
            )
        if chunk_indexes is None:
            chunk_indexes = self._candidate_chunks(summary, selected)
        if offsets is not None:
            chunk_indexes = [
                c for c in chunk_indexes if c.chunk_start_offset in offsets
            ]
        if time_bounds is None:
            time_bounds = (self._selection.start_time, self._selection.end_time)
        if log_time_order is None:
            log_time_order = self._log_time_order
        if not log_time_order:
            for chunk_index in chunk_indexes:
                yield from self._read_chunk(
                    f, path, summary, chunk_index, selected, time_bounds
                )
            return

        # Heap entries: (log time, kind, chunk offset, index in chunk, item).
        # ``kind`` 0 is a chunk index, 1 a message, so on a tied log time a
        # chunk is expanded before a message is yielded.
        heap: List[Tuple[int, int, int, int, Any]] = [
            (c.message_start_time, 0, c.chunk_start_offset, 0, c) for c in chunk_indexes
        ]
        heapq.heapify(heap)
        while heap:
            _, kind, offset, _, item = heapq.heappop(heap)
            if kind == 0:
                for index, selected_message in self._read_chunk(
                    f, path, summary, item, selected, time_bounds, with_index=True
                ):
                    heapq.heappush(
                        heap,
                        (
                            selected_message[2].log_time,
                            1,
                            offset,
                            index,
                            selected_message,
                        ),
                    )
            else:
                yield item

    def _read_chunk(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        chunk_index: "ChunkIndex",
        selected: Set[int],
        time_bounds: Tuple[Optional[int], Optional[int]],
        with_index: bool = False,
    ) -> Iterator[Any]:
        """Decompress one chunk and yield its selected messages in file order.

        A chunk may carry its own ``Schema`` and ``Channel`` records (a writer
        that did not repeat them in the summary); they are honoured over the
        summary's. ``row_id`` counts every message record of the chunk, so it
        does not depend on the selection.
        """
        from mcap.data_stream import ReadDataStream
        from mcap.records import Channel, Chunk, Message, Schema
        from mcap.stream_reader import breakup_chunk

        start_time, end_time = time_bounds
        # Skip the record's opcode (1 byte) and length (8 bytes).
        f.seek(chunk_index.chunk_start_offset + 1 + 8)
        chunk = Chunk.read(ReadDataStream(f))
        schemas = summary.schemas
        channels = summary.channels
        index = -1
        for record in breakup_chunk(chunk):
            if isinstance(record, Message):
                index += 1
                channel = channels.get(record.channel_id)
                if channel is None:
                    raise ValueError(
                        f"MCAP file {path!r} has a message on channel "
                        f"{record.channel_id}, which neither the summary nor the "
                        "chunk declares."
                    )
                schema = schemas.get(channel.schema_id) if channel.schema_id else None
                if record.channel_id not in selected:
                    if channel.id in summary.channels:
                        continue
                    # A channel declared only inside this chunk was unknown to
                    # the listing; apply the filters to it now.
                    if not self._selection.accepts_channel(channel, schema):
                        continue
                if start_time is not None and record.log_time < start_time:
                    continue
                if end_time is not None and record.log_time >= end_time:
                    continue
                row_id = message_row_id(path, chunk_index.chunk_start_offset, index)
                item = (schema, channel, record, row_id)
                yield (index, item) if with_index else item
            elif isinstance(record, Channel):
                if channels is summary.channels:
                    channels = dict(channels)
                channels[record.id] = record
            elif isinstance(record, Schema):
                if schemas is summary.schemas:
                    schemas = dict(schemas)
                schemas[record.id] = record

    def _iter_unindexed(
        self,
        f: Any,
        path: str,
        time_bounds: Optional[Tuple[Optional[int], Optional[int]]] = None,
        *,
        on_message: Optional[Callable[["Message"], None]] = None,
    ) -> Iterator[_Selected]:
        """Scan a file without a chunk index from the start, in file order.

        Ordering by log time would mean holding the whole file, which is what
        the legacy datasource did for every file; a file without an index is
        read as written and ``log_time_order`` is not honoured for its message
        rows. Coarse rows sort what they collect. ``on_message`` sees every
        message record before any filter, selected or not: it stands in for
        the file-wide statistics an indexed file carries.
        """
        from mcap.records import Channel, Message, Schema
        from mcap.stream_reader import StreamReader

        if time_bounds is None:
            time_bounds = (self._selection.start_time, self._selection.end_time)
        start_time, end_time = time_bounds
        f.seek(0)
        schemas: Dict[int, "Schema"] = {}
        channels: Dict[int, "Channel"] = {}
        ordinal = -1
        for record in StreamReader(f).records:
            if isinstance(record, Schema):
                schemas[record.id] = record
            elif isinstance(record, Channel):
                channels[record.id] = record
            elif isinstance(record, Message):
                ordinal += 1
                if on_message is not None:
                    on_message(record)
                channel = channels.get(record.channel_id)
                if channel is None:
                    raise ValueError(
                        f"MCAP file {path!r} has a message on channel "
                        f"{record.channel_id} before that channel is declared."
                    )
                schema = schemas.get(channel.schema_id) if channel.schema_id else None
                if not self._selection.accepts_channel(channel, schema):
                    continue
                if start_time is not None and record.log_time < start_time:
                    continue
                if end_time is not None and record.log_time >= end_time:
                    continue
                yield schema, channel, record, unindexed_message_row_id(path, ordinal)

    # -- message rows ------------------------------------------------------

    def _tables(
        self, messages: Iterator[_Selected], assignment: _Assignment
    ) -> Iterator[pa.Table]:
        """Build tables of about ``target_block_size`` bytes from the messages."""
        wanted = set(self._columns) if self._columns is not None else None
        builder = _MessageTableBuilder(
            columns=wanted,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            decode_json=self._decode_json,
        )
        rows_before = 0
        for selected in messages:
            builder.add(selected)
            if (
                self._target_block_size is not None
                and builder.estimated_bytes >= self._target_block_size
            ):
                yield self._finish(builder.build(), assignment, rows_before)
                rows_before += builder.num_rows
                builder.reset()
        if builder.num_rows > 0:
            yield self._finish(builder.build(), assignment, rows_before)

    # -- coarse rows -------------------------------------------------------

    def _span(self, summary: "Summary") -> Tuple[int, int]:
        """The file's first and last log time, clipped to the time range.

        File-wide, not the selection's: the window grid of a recording must
        not move when a read selects different topics, so ``file_start``
        means the file's first message whatever is read from it.
        """
        statistics = summary.statistics
        if statistics is not None and statistics.message_count > 0:
            start, end = statistics.message_start_time, statistics.message_end_time
        else:
            start = min(c.message_start_time for c in summary.chunk_indexes)
            end = max(c.message_end_time for c in summary.chunk_indexes)
        return self._clip_span(start, end)

    def _clip_span(self, start: int, end: int) -> Tuple[int, int]:
        if self._selection.start_time is not None:
            start = max(start, self._selection.start_time)
        if self._selection.end_time is not None:
            end = min(end, self._selection.end_time - 1)
        return start, end

    def _entries(
        self, selected: Iterator[_Selected], what: Optional[str] = None
    ) -> List[_Entry]:
        """Collect entries; with ``what`` (a row's name), fail as soon as their
        payloads exceed the row limit rather than after holding them all."""
        entries: List[_Entry] = []
        payload_bytes = 0
        for schema, channel, message, _ in selected:
            if what is not None:
                payload_bytes += len(message.data)
                if payload_bytes > self._max_row_bytes:
                    self._raise_row_too_large(payload_bytes, what, partial=True)
            entries.append((schema, channel, message))
        return entries

    def _window_tables(
        self, f: Any, assignment: _Assignment, summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Emit the window rows this task owns for one file."""
        assert self._window is not None
        path = assignment.path
        if summary is None:
            # No chunk index: the whole file is one task, so every window is
            # ours. Everything is read; a recorder that wrote no index gives
            # nothing to plan with.
            if log_once(f"mcap_window_unindexed:{path}"):
                logger.warning(
                    "MCAP file %r has no chunk index; reading it whole to place "
                    "windows. Rewrite the file with an index to split it across "
                    "tasks.",
                    path,
                )
            bounds: List[int] = []

            def observe(message: "Message") -> None:
                # The file-wide span, as an indexed file's statistics give it.
                if not bounds:
                    bounds.extend((message.log_time, message.log_time))
                else:
                    bounds[0] = min(bounds[0], message.log_time)
                    bounds[1] = max(bounds[1], message.log_time)

            entries = self._entries(
                self._iter_unindexed(f, path, (None, None), on_message=observe)
            )
            entries.sort(key=lambda e: e[2].log_time)
            if not entries or not any(
                self._selection.in_time_range(e[2].log_time) for e in entries
            ):
                return
            start, end = self._clip_span(bounds[0], bounds[1])
            if start > end:
                return
            windows = place_windows(self._window, start, end)
        else:
            selected = self._selection.selected_channel_ids(
                summary.channels, summary.schemas
            )
            candidates = self._candidate_chunks(summary, selected)
            if not candidates:
                return
            windows = place_windows(self._window, *self._span(summary))
            owners = owner_offsets(candidates, [start for start, _ in windows])
            windows = [
                window
                for window, owner in zip(windows, owners)
                if assignment.offsets is None or owner in assignment.offsets
            ]
            if not windows:
                return
            lead_in = self._max_lead_in_ns(summary, selected)
            low = windows[0][0] - lead_in
            high = max(end for _, end in windows)
            if self._selection.end_time is not None:
                high = min(high, self._selection.end_time)
            # Every chunk of the file that may hold a selected message in the
            # span, not only the candidates: a window's lead-in can lie before
            # the time range, in chunks the listing never considered.
            to_read = [
                c
                for c in summary.chunk_indexes
                if c.message_end_time >= low
                and c.message_start_time < high
                and (
                    not c.message_index_offsets
                    or not selected.isdisjoint(c.message_index_offsets)
                )
            ]
            to_read.sort(key=lambda c: c.chunk_start_offset)
            entries = self._entries(
                self._iter_chunks(
                    f,
                    path,
                    summary,
                    None,
                    chunk_indexes=to_read,
                    selected=selected,
                    time_bounds=(low, high),
                    log_time_order=True,
                )
            )
        yield from self._window_rows(assignment, entries, windows)

    def _max_lead_in_ns(self, summary: "Summary", selected: Set[int]) -> int:
        """How far before a window this task reads: zero without video topics."""
        if not any(
            is_video_channel(
                summary.channels[cid],
                summary.schemas.get(summary.channels[cid].schema_id)
                if summary.channels[cid].schema_id
                else None,
                self._video,
            )
            for cid in selected
        ):
            return 0
        video = self._video if self._video is not None else VideoOptions()
        return video.max_lead_in_ns

    def _window_rows(
        self,
        assignment: _Assignment,
        entries: List[_Entry],
        windows: Sequence[Tuple[int, int]],
    ) -> Iterator[pa.Table]:
        """Cut ``entries`` (log-time ordered) into the given windows."""
        path = assignment.path
        digest = self._selection.digest()
        times = [e[2].log_time for e in entries]
        video_channels = self._video_channels(entries)
        range_start = self._selection.start_time
        batch = self._new_batch()
        for start, end in windows:
            first = bisect.bisect_left(times, start)
            last = bisect.bisect_left(times, end)
            in_window = [
                e
                for e in entries[first:last]
                if self._selection.in_time_range(e[2].log_time)
            ]
            if not in_window:
                continue
            # A window that opens before the time range holds frames from the
            # range on, so its lead-in must reach the keyframe before *those*.
            anchor = start if range_start is None else max(start, range_start)
            lead_in: List[_Entry] = []
            for channel_messages in video_channels.values():
                lead_in.extend(self._lead_in(channel_messages, anchor))
            lead_in.sort(key=lambda e: e[2].log_time)
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
                self._target_block_size is not None
                and batch.payload_bytes >= self._target_block_size
            ):
                yield self._finish(batch.build(), assignment, 0)
                batch = self._new_batch()
        if len(batch) > 0:
            yield self._finish(batch.build(), assignment, 0)

    def _video_channels(self, entries: List[_Entry]) -> Dict[int, _ChannelMessages]:
        """Index the entries of every video channel for lead-in lookups."""
        by_channel: Dict[int, _ChannelMessages] = {}
        for entry in entries:
            schema, channel, message = entry
            if channel.id not in by_channel:
                if not is_video_channel(channel, schema, self._video):
                    continue
                codec = None
                if self._video is None or self._video.lead_in_ns is None:
                    codec = detect_codec(message.data)
                    if codec is None and log_once(f"mcap_no_codec:{channel.topic}"):
                        logger.warning(
                            "Cannot detect keyframes on video topic %r; its windows "
                            "carry no lead-in. Set VideoOptions(lead_in_s=...) to "
                            "add a fixed one.",
                            channel.topic,
                        )
                by_channel[channel.id] = _ChannelMessages(
                    channel, schema, [], [], codec, []
                )
            messages = by_channel[channel.id]
            messages.times.append(message.log_time)
            messages.entries.append(entry)
            messages.keyframe.append(None)
        return by_channel

    def _lead_in(self, messages: _ChannelMessages, anchor: int) -> List[_Entry]:
        """The frames a decoder needs before ``anchor`` on one video channel.

        ``anchor`` is the first log time whose frames the row carries. With a
        fixed ``lead_in_s`` the lead-in is every frame in that span before it.
        Otherwise it is the frames from the last keyframe before ``anchor``,
        searched back at most ``max_lead_in_s``; none if no keyframe is found
        there, or if the codec is unknown.
        """
        video = self._video if self._video is not None else VideoOptions()
        end = bisect.bisect_left(messages.times, anchor)
        start = bisect.bisect_left(messages.times, anchor - video.max_lead_in_ns)
        if video.lead_in_ns is not None:
            return messages.entries[start:end]
        if messages.codec is None:
            return []
        if end < len(messages.entries) and messages.is_keyframe_at(end):
            # The window opens on a keyframe: nothing before it is needed.
            return []
        for index in range(end - 1, start - 1, -1):
            if messages.is_keyframe_at(index):
                return messages.entries[index:end]
        return []

    def _topic_tables(
        self, f: Any, assignment: _Assignment, summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Emit one row per topic this task was assigned (one, when indexed)."""
        path = assignment.path
        if summary is None:
            by_topic: Dict[str, List[_Entry]] = {}
            sizes: Dict[str, int] = {}
            for schema, channel, message, _ in self._iter_unindexed(f, path):
                topic = channel.topic
                if assignment.topic is not None and topic != assignment.topic:
                    continue
                sizes[topic] = sizes.get(topic, 0) + len(message.data)
                if sizes[topic] > self._max_row_bytes:
                    self._raise_row_too_large(
                        sizes[topic], f"topic {topic!r} of {path!r}", partial=True
                    )
                by_topic.setdefault(topic, []).append((schema, channel, message))
            for topic_entries in by_topic.values():
                topic_entries.sort(key=lambda e: e[2].log_time)
            topics = (
                [assignment.topic] if assignment.topic is not None else sorted(by_topic)
            )
            groups = [(topic, by_topic.get(topic, [])) for topic in topics]
        else:
            assert assignment.topic is not None, "an indexed topic row names its topic"
            selected = self._selection.selected_channel_ids(
                summary.channels, summary.schemas
            )
            topic_ids = {
                cid
                for cid in selected
                if summary.channels[cid].topic == assignment.topic
            }
            entries = self._entries(
                self._iter_chunks(
                    f,
                    path,
                    summary,
                    assignment.offsets,
                    chunk_indexes=self._candidate_chunks(summary, topic_ids),
                    selected=topic_ids,
                    log_time_order=True,
                ),
                what=f"topic {assignment.topic!r} of {path!r}",
            )
            groups = [(assignment.topic, entries)]
        digest = self._selection.digest()
        batch = self._new_batch()
        for topic, topic_entries in groups:
            topic_entries = self._from_first_keyframe(topic_entries)
            if not topic_entries:
                continue
            row = CoarseRow(
                path=path,
                row_id=f"{path}#{topic}@{digest}",
                messages=topic_entries,
                topic=topic,
            )
            self._check_row_size(row, f"topic {topic!r} of {path!r}")
            batch.add(row)
        if len(batch) > 0:
            yield self._finish(batch.build(), assignment, 0)

    def _from_first_keyframe(self, entries: List[_Entry]) -> List[_Entry]:
        """Drop a video topic's frames before its first keyframe."""
        if not entries:
            return entries
        schema, channel, message = entries[0]
        if not is_video_channel(channel, schema, self._video):
            return entries
        if self._video is not None and self._video.lead_in_ns is not None:
            return entries
        codec = detect_codec(message.data)
        if codec is None or codec.every_frame_is_a_keyframe:
            return entries
        for index, (_, _, candidate) in enumerate(entries):
            if is_keyframe(candidate.data, codec):
                return entries[index:]
        return []

    def _file_tables(
        self, f: Any, assignment: _Assignment, summary: Optional["Summary"]
    ) -> Iterator[pa.Table]:
        """Emit the one row holding every selected message of the file."""
        path = assignment.path
        what = f"file {path!r}"
        if summary is None:
            entries = self._entries(self._iter_unindexed(f, path), what=what)
            entries.sort(key=lambda e: e[2].log_time)
        else:
            entries = self._entries(
                self._iter_chunks(f, path, summary, None, log_time_order=True),
                what=what,
            )
        if not entries:
            return
        row = CoarseRow(
            path=path,
            row_id=f"{path}@{self._selection.digest()}",
            messages=entries,
        )
        self._check_row_size(row, f"file {path!r}")
        batch = self._new_batch()
        batch.add(row)
        yield self._finish(batch.build(), assignment, 0)

    def _new_batch(self) -> CoarseRowBatch:
        return CoarseRowBatch(
            granularity=self._granularity,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
        )

    def _check_row_size(self, row: CoarseRow, what: str) -> None:
        if row.payload_bytes > self._max_row_bytes:
            self._raise_row_too_large(row.payload_bytes, what, partial=False)

    def _raise_row_too_large(self, payload_bytes: int, what: str, partial: bool):
        raise ValueError(
            f"The row for {what} would carry "
            f"{'at least ' if partial else ''}{payload_bytes} bytes of payload, "
            f"over the {self._max_row_bytes}-byte limit for one row "
            "(RAY_DATA_MCAP_MAX_ROW_BYTES). Read this data at "
            "read_granularity='window' or 'message' instead."
        )

    # -- attachment and metadata rows --------------------------------------

    def _record_tables(self, f: Any, assignment: _Assignment) -> Iterator[pa.Table]:
        """Emit the Attachment or Metadata rows this task was assigned.

        An indexed file's rows name the records by byte offset, so each is
        read with one seek. A whole-file row means the records are not indexed;
        the file is scanned for them, and ``time_range`` is applied to
        attachments either way.
        """
        from mcap.records import Attachment, Metadata

        path = assignment.path
        attachments = self._granularity == ATTACHMENT_GRANULARITY
        batch = RecordRowBatch(
            granularity=self._granularity, include_row_id=self._include_row_id
        )
        if assignment.offsets is not None:
            located = [
                (
                    read_record_at(f, offset),
                    (attachment_unit_id if attachments else metadata_unit_id)(
                        path, offset
                    ),
                    offset,
                )
                for offset in sorted(assignment.offsets)
            ]
        else:
            kind = "a" if attachments else "md"
            scanned = iter_records(f, Attachment if attachments else Metadata)
            located = [
                (record, unindexed_record_row_id(path, kind, ordinal), None)
                for ordinal, record in enumerate(scanned)
            ]
        for record, row_id, offset in located:
            if attachments:
                if not isinstance(record, Attachment):
                    raise ValueError(
                        f"MCAP file {path!r}: expected an Attachment record at "
                        f"offset {offset}, found {type(record).__name__}"
                    )
                if not self._selection.in_time_range(record.log_time):
                    continue
                batch.add_attachment(path, row_id, record)
            else:
                if not isinstance(record, Metadata):
                    raise ValueError(
                        f"MCAP file {path!r}: expected a Metadata record at offset "
                        f"{offset}, found {type(record).__name__}"
                    )
                batch.add_metadata(path, row_id, record)
            if (
                self._target_block_size is not None
                and batch.payload_bytes >= self._target_block_size
            ):
                yield self._finish(batch.build(), assignment, 0)
                batch = RecordRowBatch(
                    granularity=self._granularity, include_row_id=self._include_row_id
                )
        if len(batch) > 0:
            yield self._finish(batch.build(), assignment, 0)

    # -- finishing a table -------------------------------------------------

    def _finish(
        self, table: pa.Table, assignment: _Assignment, rows_before: int
    ) -> pa.Table:
        """Append partition and synthesized columns, then apply the projection."""
        wanted = set(self._columns) if self._columns is not None else None
        num_rows = table.num_rows
        path = assignment.path
        if self._partition_parser is not None:
            for name, value in self._partition_parser(path).items():
                if wanted is not None and name not in wanted:
                    continue
                if name in table.column_names:
                    table = table.drop([name])
                table = table.append_column(
                    name, self._partition_value_array(name, value, num_rows)
                )
        position = ReadUnitPosition(unit=assignment.unit, rows_before=rows_before)
        for column in self._synthesized_columns:
            if wanted is not None and column.name not in wanted:
                continue
            if column.name in table.column_names:
                table = table.drop([column.name])
            table = table.append_column(column.name, column.compute(position, num_rows))
        if self._columns is not None:
            produced = set(table.column_names)
            table = table.select([c for c in self._columns if c in produced])
            if table.num_columns == 0 and num_rows > 0:
                table = table.append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.nulls(num_rows)
                )
        # A JSON payload Arrow cannot type falls back to Ray's pickled-object
        # extension. Unpickling runs arbitrary code, so, like every other
        # datasource, refuse such a column unless the user opted in.
        raise_on_pickle_object_columns(table)
        return table

    def _partition_value_array(self, name: str, value: Any, num_rows: int) -> pa.Array:
        """Broadcast one path-derived partition value, typed by the schema if known."""
        as_str = None if value is None else str(value)
        array = pa.repeat(pa.scalar(as_str, type=pa.string()), num_rows)
        if self._schema is not None:
            idx = self._schema.get_field_index(name)
            if idx != -1 and self._schema.field(idx).type != pa.string():
                array = array.cast(self._schema.field(idx).type)
        return array


def _assignments(manifest: FileManifest) -> List[_Assignment]:
    """Group a manifest's rows by file (and topic): what this task reads of each.

    A row with chunk metadata contributes its ``unit_ids`` (chunk byte offsets);
    a row without it means the whole file, which wins over any offsets listed
    for the same path. A topic-granularity row also names its topic.
    """
    owned: Dict[Tuple[str, Optional[str]], Optional[Set[int]]] = {}
    for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
        path = str(path)
        topic = None
        if metadata is not None and metadata.get("topic") is not None:
            topic = str(metadata["topic"])
        key = (path, topic)
        if metadata is None or "unit_ids" not in metadata:
            owned[key] = None
            continue
        offsets = {int(i) for i in metadata["unit_ids"]}
        current = owned.get(key, offsets)
        if current is not None:
            current.update(offsets)
        owned[key] = current
    return [
        _Assignment(path=path, offsets=offsets, topic=topic)
        for (path, topic), offsets in owned.items()
    ]
