"""Attachment and Metadata records as rows.

Besides messages, an MCAP file can hold ``Attachment`` records (named blobs
with a media type and two timestamps: calibration files, maps, thumbnails) and
``Metadata`` records (a named string-to-string map: recorder version, vehicle
id, operator notes). Neither lives in a chunk; the summary indexes each by
byte offset. ``read_granularity="attachment"`` and ``"metadata"`` return one
row per record.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ATTACHMENT_GRANULARITY,
    METADATA_GRANULARITY,
    ROW_ID_COLUMN,
)

if TYPE_CHECKING:
    from mcap.records import Attachment, McapRecord, Metadata


def record_schema(granularity: str, *, include_row_id: bool) -> pa.Schema:
    """The schema of attachment or metadata rows.

    Args:
        granularity: ``attachment`` or ``metadata``.
        include_row_id: Whether ``row_id`` is present.

    Returns:
        The schema, columns in output order.
    """
    fields = [pa.field("path", pa.string())]
    if include_row_id:
        fields.append(pa.field(ROW_ID_COLUMN, pa.string()))
    if granularity == ATTACHMENT_GRANULARITY:
        fields += [
            pa.field("name", pa.string()),
            pa.field("media_type", pa.string()),
            pa.field("log_time", pa.int64()),
            pa.field("create_time", pa.int64()),
            pa.field("data", pa.binary()),
        ]
    elif granularity == METADATA_GRANULARITY:
        fields += [
            pa.field("name", pa.string()),
            pa.field("metadata", pa.map_(pa.string(), pa.string())),
        ]
    else:
        raise ValueError(f"not a record granularity: {granularity!r}")
    return pa.schema(fields)


def read_record_at(f: Any, offset: int) -> "McapRecord":
    """Read the one record starting at byte ``offset`` of an open file."""
    from mcap.stream_reader import StreamReader

    f.seek(offset)
    return next(StreamReader(f, skip_magic=True).records)


def iter_records(f: Any, record_type: type) -> Iterator["McapRecord"]:
    """Scan a file from the start, yielding every record of ``record_type``."""
    from mcap.stream_reader import StreamReader

    f.seek(0)
    for record in StreamReader(f).records:
        if isinstance(record, record_type):
            yield record


@dataclass
class RecordRowBatch:
    """Accumulates attachment or metadata rows and builds one Arrow table."""

    granularity: str
    include_row_id: bool
    rows: List[Dict[str, Any]] = field(default_factory=list)
    payload_bytes: int = 0

    def add_attachment(self, path: str, row_id: str, record: "Attachment") -> None:
        self.rows.append(
            {
                "path": path,
                ROW_ID_COLUMN: row_id,
                "name": record.name,
                "media_type": record.media_type,
                "log_time": record.log_time,
                "create_time": record.create_time,
                "data": record.data,
            }
        )
        self.payload_bytes += len(record.data)

    def add_metadata(self, path: str, row_id: str, record: "Metadata") -> None:
        self.rows.append(
            {
                "path": path,
                ROW_ID_COLUMN: row_id,
                "name": record.name,
                "metadata": list(record.metadata.items()),
            }
        )
        self.payload_bytes += sum(len(k) + len(v) for k, v in record.metadata.items())

    def __len__(self) -> int:
        return len(self.rows)

    def build(self) -> pa.Table:
        schema = record_schema(self.granularity, include_row_id=self.include_row_id)
        columns: Dict[str, pa.Array] = {}
        for column in schema:
            values: Optional[List[Any]] = [row[column.name] for row in self.rows]
            columns[column.name] = pa.array(values, type=column.type)
        return pa.table(columns, schema=schema)
