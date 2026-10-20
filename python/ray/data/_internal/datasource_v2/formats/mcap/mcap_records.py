"""Attachment and Metadata records as rows.

Besides messages, an MCAP file can hold ``Attachment`` and ``Metadata``
records. An attachment is a named blob with a media type, a log time and a
creation time, such as a calibration file. A metadata record is a named
string-to-string map, such as the recorder version. Neither lives in a chunk,
and the summary can index each by byte offset. ``read_granularity="attachment"``
and ``"metadata"`` return one row per record.
"""

import io
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Set

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_io import buffered
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    ATTACHMENT_GRANULARITY,
    METADATA_GRANULARITY,
    ROW_ID_COLUMN,
    MCAPSelection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    attachment_unit_id,
    metadata_unit_id,
    unindexed_record_row_id,
)

if TYPE_CHECKING:
    from mcap.records import Attachment, McapRecord, Metadata
    from mcap.summary import Summary


def record_schema(granularity: str, *, include_row_id: bool) -> pa.Schema:
    """Schema of ``attachment`` or ``metadata`` rows, columns in output order."""
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


def read_record_at(f: Any, offset: int, length: int) -> "McapRecord":
    """Read the record at byte ``offset`` of an open file, in one read.

    ``length`` counts the opcode and length prefix, as an ``AttachmentIndex``
    or ``MetadataIndex`` gives it.
    """
    from mcap.stream_reader import StreamReader

    f.seek(offset)
    data = f.read(length)
    return next(StreamReader(io.BytesIO(data), skip_magic=True).records)


def iter_records(f: Any, record_type: type) -> Iterator["McapRecord"]:
    """Scan a file from the start, yielding every record of ``record_type``.

    Attachment and Metadata records never sit inside a chunk, so the stream
    reader returns each chunk whole (``emit_chunks``) without decompressing it.
    The file is still read once from end to end, through a read buffer of
    ``DataContext.streaming_read_buffer_size`` bytes.
    """
    from mcap.stream_reader import StreamReader

    from ray.data.context import DataContext

    f.seek(0)
    stream = buffered(f, DataContext.get_current().streaming_read_buffer_size)
    for record in StreamReader(stream, emit_chunks=True).records:
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


@dataclass(frozen=True)
class _LocatedRecord:
    """An Attachment or Metadata record read for a task, with its ``row_id``."""

    record: Any
    row_id: str
    # Byte offset the record was read at, or ``None`` when a scan found it.
    offset: Optional[int]


@dataclass(frozen=True)
class RecordSettings:
    """What a read of attachment or metadata rows keeps and emits."""

    # ``attachment`` or ``metadata``.
    granularity: str
    # Its ``time_range`` applies to attachments.
    selection: MCAPSelection
    include_row_id: bool
    # Estimated payload bytes per table; ``None`` for one table per file.
    target_block_size: Optional[int]


class RecordRows:
    """Builds the attachment or metadata rows of one read task."""

    def __init__(self, settings: RecordSettings):
        self._settings = settings

    def tables(
        self,
        f: Any,
        path: str,
        offsets: Optional[Set[int]],
        summary: Optional["Summary"],
        finish: Callable[[pa.Table], pa.Table],
    ) -> Iterator[pa.Table]:
        """Emit the Attachment or Metadata rows this task was assigned.

        An indexed file's listing rows name the records by byte offset, and
        ``summary`` indexes their lengths, so each is read in one read. A
        whole-file row means the records are not indexed, so the file is
        scanned for them. ``time_range`` applies to attachments either way.
        Records are read one at a time as rows are built, so the task holds
        about one block of payloads.
        """
        if offsets is not None:
            records = self._read_records_at(f, path, offsets, summary)
        else:
            records = self._scan_records(f, path)
        batch = RecordRowBatch(
            granularity=self._settings.granularity,
            include_row_id=self._settings.include_row_id,
        )
        for located in records:
            if not self._add_record_row(batch, path, located):
                continue
            if (
                self._settings.target_block_size is not None
                and batch.payload_bytes >= self._settings.target_block_size
            ):
                yield finish(batch.build())
                batch = RecordRowBatch(
                    granularity=self._settings.granularity,
                    include_row_id=self._settings.include_row_id,
                )
        if len(batch) > 0:
            yield finish(batch.build())

    def _read_records_at(
        self, f: Any, path: str, offsets: Set[int], summary: Optional["Summary"]
    ) -> Iterator[_LocatedRecord]:
        """Read the records at ``offsets``, in file order, one read each."""
        attachments = self._settings.granularity == ATTACHMENT_GRANULARITY
        unit_id = attachment_unit_id if attachments else metadata_unit_id
        lengths = _indexed_record_lengths(summary, attachments)
        for offset in sorted(offsets):
            if offset not in lengths:
                kind = "Attachment" if attachments else "Metadata"
                raise ValueError(
                    f"The summary of MCAP file {path!r} indexes no {kind} record "
                    f"at offset {offset}. The file may have changed since listing."
                )
            record = read_record_at(f, offset, lengths[offset])
            yield _LocatedRecord(record, unit_id(path, offset), offset)

    def _scan_records(self, f: Any, path: str) -> Iterator[_LocatedRecord]:
        """Scan the whole file for the records of the granularity's kind.

        Without an index, a record's ``row_id`` is its ordinal among the file's
        records of its kind.
        """
        from mcap.records import Attachment, Metadata

        attachments = self._settings.granularity == ATTACHMENT_GRANULARITY
        kind = "a" if attachments else "md"
        scanned = iter_records(f, Attachment if attachments else Metadata)
        for ordinal, record in enumerate(scanned):
            row_id = unindexed_record_row_id(path, kind, ordinal)
            yield _LocatedRecord(record, row_id, None)

    def _add_record_row(
        self, batch: RecordRowBatch, path: str, located: _LocatedRecord
    ) -> bool:
        """Add a record's row to ``batch``; ``False`` if ``time_range`` drops it."""
        from mcap.records import Attachment, Metadata

        record = located.record
        if self._settings.granularity == ATTACHMENT_GRANULARITY:
            if not isinstance(record, Attachment):
                raise ValueError(
                    f"MCAP file {path!r}: expected an Attachment record at "
                    f"offset {located.offset}, found {type(record).__name__}"
                )
            if not self._settings.selection.in_time_range(record.log_time):
                return False
            batch.add_attachment(path, located.row_id, record)
        else:
            if not isinstance(record, Metadata):
                raise ValueError(
                    f"MCAP file {path!r}: expected a Metadata record at offset "
                    f"{located.offset}, found {type(record).__name__}"
                )
            batch.add_metadata(path, located.row_id, record)
        return True


def _indexed_record_lengths(
    summary: Optional["Summary"], attachments: bool
) -> Dict[int, int]:
    """Byte length of each indexed Attachment (or Metadata) record, by offset."""
    if summary is None:
        return {}
    indexes = summary.attachment_indexes if attachments else summary.metadata_indexes
    return {index.offset: index.length for index in indexes}
