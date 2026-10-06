"""Message rows: their Arrow schema and the builder that turns messages into tables."""

import json
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import pyarrow as pa

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import _Selected
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import ROW_ID_COLUMN
from ray.data._internal.tensor_extensions.arrow import convert_to_pyarrow_array

if TYPE_CHECKING:
    from mcap.records import Channel

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
                # Ray's converter infers a struct from the decoded values.
                # Blocks whose structs differ are unified downstream with nulls.
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
            # Every column was pruned, as for ``count()``. A stub column keeps
            # the row count, as in ``FileReader``.
            return pa.table({_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(n)})
        return pa.table(arrays)
