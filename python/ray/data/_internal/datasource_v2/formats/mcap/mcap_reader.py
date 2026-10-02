"""Reads the chunks a listing row assigned to a task and builds message rows.

``MCAPReader.read`` receives a ``FileManifest`` whose rows name files and, per
file, the byte offsets of the chunks this task owns. For each file it seeks to
those chunks, decompresses them, keeps the selected messages and yields Arrow
tables of about ``target_block_size`` bytes. With ``log_time_order`` the owned
chunks are merged by log time as they are read (a heap of chunk indexes and
messages, as the mcap library does for a whole file), so memory holds only the
chunks that overlap in time.

A row carries the same columns the legacy datasource produces, plus
``channel_metadata`` (a ``map<string, string>`` of the channel's metadata) and,
on request, ``row_id``: a deterministic name for the message built from the
file path, the chunk's byte offset and the message's position in the chunk.
"""

import heapq
import json
import logging
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
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

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ROW_ID_COLUMN,
    MCAPSelection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    message_row_id,
    unindexed_message_row_id,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.tensor_extensions.arrow import convert_to_pyarrow_array
from ray.data._internal.util import iterate_with_retry
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from mcap.records import Channel, ChunkIndex, Message, Schema
    from mcap.summary import Summary

logger = logging.getLogger(__name__)

# Rough in-memory cost of one row beyond its payload: the timestamps, the
# sequence number, the topic string and the Arrow offsets around them. Only
# used to decide when a table is big enough to yield.
_ROW_OVERHEAD_BYTES = 96

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


@DeveloperAPI
class MCAPReader(Reader[FileManifest]):
    """Reads the chunks of MCAP files a manifest assigns to one task.

    Created by ``MCAPScanner.create_reader`` with every pushdown applied:
    the message selection, the projected columns and the per-task row limit.
    """

    def __init__(
        self,
        *,
        selection: MCAPSelection,
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
    ):
        """Initialize the reader.

        Args:
            selection: Which messages to keep.
            include_metadata: Whether to emit the channel and schema columns.
            include_row_id: Whether to emit ``row_id``.
            log_time_order: Whether a task's messages come out in ascending
                ``log_time`` order (its chunks merged by time) or in file order.
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
        """
        self._selection = selection
        self._decode_json = decode_json
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
        for path, offsets in _owned_chunks(input_split):
            tables = iterate_with_retry(
                partial(self._read_file, path, offsets),
                f"read MCAP file {path}",
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

    # -- one file ----------------------------------------------------------

    def _read_file(self, path: str, offsets: Optional[Set[int]]) -> Iterator[pa.Table]:
        """Yield the tables of one file, limited to the chunks at ``offsets``.

        ``offsets`` is ``None`` for a whole-file row, which the indexer emits
        when a file has no chunk index; such a file is scanned linearly.
        """
        from mcap.reader import SeekingReader

        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(path) as f:
            reader = SeekingReader(f)
            summary = reader.get_summary()
            if summary is None or not summary.chunk_indexes:
                messages = self._iter_unindexed(f, path)
            else:
                messages = self._iter_chunks(f, path, summary, offsets)
            yield from self._tables(messages, path)

    def _iter_chunks(
        self,
        f: Any,
        path: str,
        summary: "Summary",
        offsets: Optional[Set[int]],
    ) -> Iterator[_Selected]:
        """Yield the selected messages of the owned chunks of an indexed file.

        With ``log_time_order`` the chunks are merged through one heap holding
        chunk indexes (keyed by their first log time) and messages (keyed by
        theirs): a chunk is expanded when it reaches the top, so at most the
        chunks that overlap in time are decompressed at once, and a message is
        yielded only once every chunk that could precede it has been expanded.
        Without it the chunks are read in file order.
        """
        selected = self._selection.selected_channel_ids(
            summary.channels, summary.schemas
        )
        chunk_indexes = sorted(
            (
                c
                for c in summary.chunk_indexes
                if (offsets is None or c.chunk_start_offset in offsets)
                and self._selection.chunk_may_match(c, selected)
            ),
            key=lambda c: c.chunk_start_offset,
        )
        if not self._log_time_order:
            for chunk_index in chunk_indexes:
                yield from self._read_chunk(f, path, summary, chunk_index, selected)
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
                    f, path, summary, item, selected, with_index=True
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
                if not self._selection.in_time_range(record.log_time):
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

    def _iter_unindexed(self, f: Any, path: str) -> Iterator[_Selected]:
        """Scan a file without a chunk index from the start, in file order.

        Ordering by log time would mean holding the whole file, which is what
        the legacy datasource did for every file; a file without an index is
        read as written and ``log_time_order`` is not honoured for it.
        """
        from mcap.records import Channel, Message, Schema
        from mcap.stream_reader import StreamReader

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
                channel = channels.get(record.channel_id)
                if channel is None:
                    raise ValueError(
                        f"MCAP file {path!r} has a message on channel "
                        f"{record.channel_id} before that channel is declared."
                    )
                schema = schemas.get(channel.schema_id) if channel.schema_id else None
                if not self._selection.accepts_channel(channel, schema):
                    continue
                if not self._selection.in_time_range(record.log_time):
                    continue
                yield schema, channel, record, unindexed_message_row_id(path, ordinal)

    # -- rows to tables ----------------------------------------------------

    def _tables(self, messages: Iterator[_Selected], path: str) -> Iterator[pa.Table]:
        """Build tables of about ``target_block_size`` bytes from the messages."""
        wanted = set(self._columns) if self._columns is not None else None
        builder = _MessageTableBuilder(
            columns=wanted,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            decode_json=self._decode_json,
        )
        unit = ReadUnit(id=path, source=path, count=1)
        rows_before = 0
        for selected in messages:
            builder.add(selected)
            if (
                self._target_block_size is not None
                and builder.estimated_bytes >= self._target_block_size
            ):
                yield self._finish(builder.build(), path, unit, rows_before)
                rows_before += builder.num_rows
                builder.reset()
        if builder.num_rows > 0:
            yield self._finish(builder.build(), path, unit, rows_before)

    def _finish(
        self, table: pa.Table, path: str, unit: ReadUnit, rows_before: int
    ) -> pa.Table:
        """Append partition and synthesized columns, then apply the projection."""
        wanted = set(self._columns) if self._columns is not None else None
        num_rows = table.num_rows
        if self._partition_parser is not None:
            for name, value in self._partition_parser(path).items():
                if wanted is not None and name not in wanted:
                    continue
                if name in table.column_names:
                    table = table.drop([name])
                table = table.append_column(
                    name, self._partition_value_array(name, value, num_rows)
                )
        position = ReadUnitPosition(unit=unit, rows_before=rows_before)
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


def _owned_chunks(manifest: FileManifest) -> List[Tuple[str, Optional[Set[int]]]]:
    """Group a manifest's rows by file: ``(path, chunk offsets or None)``.

    A row with chunk metadata contributes its ``unit_ids`` (chunk byte offsets);
    a row without it means the whole file, which wins over any offsets listed
    for the same path.
    """
    owned: Dict[str, Optional[Set[int]]] = {}
    for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
        path = str(path)
        if metadata is None or "unit_ids" not in metadata:
            owned[path] = None
            continue
        offsets = {int(i) for i in metadata["unit_ids"]}
        current = owned.get(path, offsets)
        if current is not None:
            current.update(offsets)
        owned[path] = current
    return list(owned.items())
