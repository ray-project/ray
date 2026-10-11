"""Message rows: their Arrow schema and the builder that turns messages into tables."""

import json
from array import array
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple, Union

import pyarrow as pa

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import _Selected
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import ROW_ID_COLUMN
from ray.data._internal.tensor_extensions.arrow import convert_to_pyarrow_array

if TYPE_CHECKING:
    from mcap.records import Channel, Schema

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

# The ``int64`` columns a row takes from its message record.
_MESSAGE_INT_COLUMNS = ("log_time", "publish_time", "sequence")

# A ``binary`` or ``string`` array addresses its values with int32 offsets, so
# a buffer is sealed into its own chunk before it passes this many bytes.
_MAX_BUFFER_BYTES = 2**31 - 1

# Columns whose value a row takes from its channel and schema: the column type
# and how to read the value.
_CHANNEL_COLUMNS: Dict[
    str, Tuple[pa.DataType, Callable[["Channel", Optional["Schema"]], Any]]
] = {
    "topic": (pa.string(), lambda channel, schema: channel.topic),
    "channel_id": (pa.int64(), lambda channel, schema: channel.id),
    "message_encoding": (
        pa.string(),
        lambda channel, schema: channel.message_encoding,
    ),
    "schema_name": (
        pa.string(),
        lambda channel, schema: schema.name if schema else None,
    ),
    "schema_encoding": (
        pa.string(),
        lambda channel, schema: schema.encoding if schema else None,
    ),
    "channel_metadata": (
        pa.map_(pa.string(), pa.string()),
        lambda channel, schema: list(channel.metadata.items()),
    ),
}


def message_schema(
    *,
    include_metadata: bool,
    include_row_id: bool,
    data_type: Optional[pa.DataType] = None,
) -> pa.Schema:
    """The Arrow schema of message rows, before partition and synthesized columns.

    Text columns are ``string`` and integer columns ``int64``. ``schema_data``
    is dictionary-encoded: a schema definition is stored once per file but
    repeated on every row, and ROS 2 definitions run kilobytes each.
    ``channel_metadata`` is a ``map``, so its type does not depend on which
    keys a recorder wrote.

    Args:
        include_metadata: Whether the per-channel and per-schema columns are present.
        include_row_id: Whether ``row_id`` is present.
        data_type: Type of the ``data`` column when it holds decoded JSON
            values; ``None`` for ``binary``.

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

    The reader calls this only when ``data`` was planned as decoded JSON, and
    planning calls it to type that column. Arrow has no column type for a mix of
    bytes and decoded values, so a channel of another encoding, or a payload
    that is not valid JSON, raises a ``ValueError`` that names ``where``: a
    ``row_id`` or a file.
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


def _int_array(values: array, type_: pa.DataType) -> pa.Array:
    """Wrap an ``array.array`` of int32 or int64 values as an Arrow array, no copy."""
    return pa.Array.from_buffers(type_, len(values), [None, pa.py_buffer(values)])


class _VarWidthBuffer:
    """``binary`` or ``string`` values packed into one buffer as they arrive.

    A value costs its bytes plus an int32 offset, as in the finished array,
    rather than a Python object. The finished array wraps the buffer without a
    copy. A buffer is sealed into its own chunk before its offsets overflow.
    """

    def __init__(self, type_: pa.DataType):
        self._type = type_
        self._chunks: List[pa.Array] = []
        self._start_buffer()

    def append(self, value: bytes) -> None:
        if len(self._data) + len(value) > _MAX_BUFFER_BYTES:
            self._seal()
        self._data += value
        self._offsets.append(len(self._data))

    def finish(self) -> Union[pa.Array, pa.ChunkedArray]:
        """The values as one array, or as one chunk per 2 GiB buffer."""
        self._seal()
        chunks, self._chunks = self._chunks, []
        if len(chunks) == 1:
            return chunks[0]
        return pa.chunked_array(chunks, type=self._type)

    def _start_buffer(self) -> None:
        self._data = bytearray()
        self._offsets = array("i", [0])

    def _seal(self) -> None:
        count = len(self._offsets) - 1
        if count:
            buffers = [None, pa.py_buffer(self._offsets), pa.py_buffer(self._data)]
            self._chunks.append(pa.Array.from_buffers(self._type, count, buffers))
        self._start_buffer()


@dataclass(frozen=True)
class _ChannelSlot:
    """What a row takes from its channel and schema, stored once per table."""

    channel: "Channel"
    schema: Optional["Schema"]
    # Position of ``schema.data`` in the table's ``schema_data`` dictionary.
    schema_index: Optional[int]


class _MessageTableBuilder:
    """Accumulates selected messages column by column and builds Arrow tables.

    A row costs about what it costs in the finished table. Integers go into
    typed arrays, payloads and row ids into one buffer each, and what a row
    takes from its channel and schema is stored once per table, in a
    ``_ChannelSlot``, with one int32 per row pointing at it. ``schema_data`` is
    built as a dictionary from the slots, so a table holds each definition
    once.
    """

    def __init__(
        self,
        *,
        columns: Optional[Set[str]],
        include_metadata: bool,
        include_row_id: bool,
        decode_json: bool,
    ):
        """Initialize the builder.

        Args:
            columns: Columns to build; ``None`` for every column. A pruned read
                never decodes a JSON payload it will not return.
            include_metadata: Whether to build the channel and schema columns.
            include_row_id: Whether to build ``row_id``.
            decode_json: Whether ``data`` holds decoded JSON values rather than
                bytes. Fixed by the planned schema for every row of the dataset.
        """
        names = list(DATA_COLUMNS)
        if include_metadata:
            names += METADATA_COLUMNS
        if include_row_id:
            names.append(ROW_ID_COLUMN)
        # The columns this builder emits, in output order.
        self._names = [n for n in names if columns is None or n in columns]
        self._decode_json = decode_json
        self.reset()

    def reset(self) -> None:
        """Drop every row added so far."""
        self.num_rows = 0
        self.estimated_bytes = 0
        self._slots: List[_ChannelSlot] = []
        self._slot_by_records: Dict[Tuple[int, int], int] = {}
        self._schema_index: Dict[bytes, int] = {}
        self._slot_ids = array("i")
        self._ints = {n: array("q") for n in _MESSAGE_INT_COLUMNS if n in self._names}
        wants_data = "data" in self._names
        self._json_values: Optional[List[Any]] = (
            [] if wants_data and self._decode_json else None
        )
        self._payloads: Optional[_VarWidthBuffer] = (
            _VarWidthBuffer(pa.binary())
            if wants_data and not self._decode_json
            else None
        )
        self._row_ids: Optional[_VarWidthBuffer] = (
            _VarWidthBuffer(pa.string()) if ROW_ID_COLUMN in self._names else None
        )

    def add(self, selected: _Selected) -> None:
        schema, channel, message, row_id = selected
        self._slot_ids.append(self._slot(channel, schema))
        self.num_rows += 1
        self.estimated_bytes += len(message.data) + _ROW_OVERHEAD_BYTES
        for name, values in self._ints.items():
            values.append(getattr(message, name))
        if self._payloads is not None:
            self._payloads.append(message.data)
        elif self._json_values is not None:
            self._json_values.append(decode_payload(channel, message.data, row_id))
        if self._row_ids is not None:
            self._row_ids.append(row_id.encode())

    def build(self) -> pa.Table:
        """Build a table of the rows added so far, and empty the builder.

        The builder lets go of the rows before the table is returned, so a
        caller that yields the table does not also hold what it was built from.
        """
        num_rows = self.num_rows
        slot_ids = _int_array(self._slot_ids, pa.int32())
        arrays = {name: self._column(name, slot_ids) for name in self._names}
        self.reset()
        if not arrays:
            # Every column was pruned, as for ``count()``. A stub column keeps
            # the row count, as in ``FileReader``.
            return pa.table({_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(num_rows)})
        return pa.table(arrays)

    def _slot(self, channel: "Channel", schema: Optional["Schema"]) -> int:
        """The index of the slot for ``channel`` and ``schema``, added on first use."""
        # Keyed by identity: the slot holds both records, so neither id is
        # reused while the builder lives.
        key = (id(channel), id(schema))
        slot = self._slot_by_records.get(key)
        if slot is None:
            slot = len(self._slots)
            self._slots.append(self._new_slot(channel, schema))
            self._slot_by_records[key] = slot
        return slot

    def _new_slot(self, channel: "Channel", schema: Optional["Schema"]) -> _ChannelSlot:
        schema_index = None
        if "schema_data" in self._names and schema is not None:
            schema_index = self._schema_index.get(schema.data)
            if schema_index is None:
                schema_index = len(self._schema_index)
                self._schema_index[schema.data] = schema_index
        return _ChannelSlot(channel, schema, schema_index)

    def _column(
        self, name: str, slot_ids: pa.Array
    ) -> Union[pa.Array, pa.ChunkedArray]:
        """Build one column of the table."""
        if name in _CHANNEL_COLUMNS:
            type_, value_of = _CHANNEL_COLUMNS[name]
            values = [value_of(s.channel, s.schema) for s in self._slots]
            return pa.array(values, type=type_).take(slot_ids)
        if name == "schema_data":
            return self._schema_data(slot_ids)
        if name in self._ints:
            return _int_array(self._ints[name], pa.int64())
        if name == ROW_ID_COLUMN:
            assert self._row_ids is not None
            return self._row_ids.finish()
        if self._json_values is not None:
            # Ray's converter infers a struct from the decoded values.
            # Blocks whose structs differ are unified downstream with nulls.
            return convert_to_pyarrow_array(self._json_values, "data")
        assert self._payloads is not None
        return self._payloads.finish()

    def _schema_data(self, slot_ids: pa.Array) -> pa.DictionaryArray:
        """``schema_data`` as a dictionary holding each definition once.

        Slots are numbered in order of first use, so the dictionary lists the
        definitions in order of first appearance, as ``dictionary_encode`` does.
        """
        per_slot = [s.schema_index for s in self._slots]
        indices = pa.array(per_slot, type=pa.int32()).take(slot_ids)
        dictionary = pa.array(list(self._schema_index), type=pa.binary())
        return pa.DictionaryArray.from_arrays(indices, dictionary)
