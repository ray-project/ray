"""Attachment and Metadata records as rows.

Besides messages, an MCAP file can hold ``Attachment`` and ``Metadata``
records. An attachment is a named blob with a media type, a log time and a
creation time, such as a calibration file. A metadata record is a named
string-to-string map, such as the recorder version. Neither lives in a chunk,
and the summary can index each by byte offset. ``read_granularity="attachment"``
and ``"metadata"`` return one row per record.

Each of the two is a ``RecordKind``, which says where the summary indexes and
counts that kind. ``listed_records`` holds the rule for which records a file
holds, which listing, reading and ``count()`` share.
"""

import io
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Union,
)

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
    from mcap.records import Attachment, McapRecord, Metadata, Statistics
    from mcap.summary import Summary


@dataclass(frozen=True)
class RecordKind:
    """One kind of record a record granularity reads, and where a file's
    summary indexes and counts it."""

    # The ``read_granularity`` that reads this kind.
    granularity: str
    # The record's class in ``mcap.records``, such as ``Attachment``.
    record_name: str
    # The ``Summary`` attribute that holds the kind's index records.
    index_attribute: str
    # The ``Statistics`` attribute that counts the kind's records.
    statistics_attribute: str
    # The index-record attribute that sizes a record's listing row, in bytes.
    size_attribute: str
    # Names the read unit and ``row_id`` of an indexed record by byte offset.
    unit_id: Callable[[str, int], str]
    # Tags the ``row_id`` of a record that a scan finds.
    scan_tag: str
    # Whether the records carry a ``log_time``, which ``time_range`` filters.
    timed: bool

    def record_type(self) -> type:
        """The record's class in ``mcap.records``."""
        import mcap.records

        return getattr(mcap.records, self.record_name)

    def indexes(self, summary: "Summary") -> List[Any]:
        """The summary's index records of this kind, in file order."""
        return getattr(summary, self.index_attribute)

    def counted(self, statistics: "Statistics") -> int:
        """How many records of this kind the statistics count."""
        return getattr(statistics, self.statistics_attribute)

    def size_bytes(self, index: Any) -> int:
        """The bytes an index record gives its record's listing row."""
        return getattr(index, self.size_attribute)


ATTACHMENT_RECORDS = RecordKind(
    granularity=ATTACHMENT_GRANULARITY,
    record_name="Attachment",
    index_attribute="attachment_indexes",
    statistics_attribute="attachment_count",
    size_attribute="data_size",
    unit_id=attachment_unit_id,
    scan_tag="a",
    timed=True,
)
METADATA_RECORDS = RecordKind(
    granularity=METADATA_GRANULARITY,
    record_name="Metadata",
    index_attribute="metadata_indexes",
    statistics_attribute="metadata_count",
    size_attribute="length",
    unit_id=metadata_unit_id,
    scan_tag="md",
    timed=False,
)
# The kind each record granularity reads.
RECORD_KINDS: Dict[str, RecordKind] = {
    kind.granularity: kind for kind in (ATTACHMENT_RECORDS, METADATA_RECORDS)
}


def listed_records(
    summary: Optional["Summary"], kind: RecordKind
) -> Optional[List[Any]]:
    """The index records of the ``kind`` records a file holds, or ``None`` when
    the file must be scanned for them.

    A file whose summary indexes the kind holds the indexed records. Without an
    index, a file whose statistics count none holds none. Listing and
    ``count()`` both follow this rule.
    """
    if summary is None:
        return None
    indexes = kind.indexes(summary)
    if indexes:
        return list(indexes)
    statistics = summary.statistics
    if statistics is not None and kind.counted(statistics) == 0:
        return []
    return None


def count_records(f: Any, summary: Optional["Summary"], kind: RecordKind) -> int:
    """Count one open file's ``kind`` records as listing finds them
    (``listed_records``), scanning the file when no index lists them."""
    listed = listed_records(summary, kind)
    if listed is not None:
        return len(listed)
    return sum(1 for _ in iter_records(f, kind.record_type()))


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

    kind: RecordKind
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
        """One table of the rows added, with the columns of ``record_schema``.

        Every row holds a ``row_id``, which the table keeps only with
        ``include_row_id``.
        """
        schema = record_schema(
            self.kind.granularity, include_row_id=self.include_row_id
        )
        columns: Dict[str, pa.Array] = {}
        for column in schema:
            values: Optional[List[Any]] = [row[column.name] for row in self.rows]
            columns[column.name] = pa.array(values, type=column.type)
        return pa.table(columns, schema=schema)


@dataclass(frozen=True)
class _LocatedRecord:
    """An Attachment or Metadata record read for a task, with its ``row_id``."""

    record: Union["Attachment", "Metadata"]
    row_id: str


@dataclass(frozen=True)
class RecordSettings:
    """What a read of attachment or metadata rows keeps and emits."""

    # The kind of record each row is.
    kind: RecordKind
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
        batch = self._new_batch()
        for located in records:
            if not self._add_record_row(batch, path, located):
                continue
            if (
                self._settings.target_block_size is not None
                and batch.payload_bytes >= self._settings.target_block_size
            ):
                yield finish(batch.build())
                batch = self._new_batch()
        if len(batch) > 0:
            yield finish(batch.build())

    def _new_batch(self) -> RecordRowBatch:
        return RecordRowBatch(
            kind=self._settings.kind, include_row_id=self._settings.include_row_id
        )

    def _read_records_at(
        self, f: Any, path: str, offsets: Set[int], summary: Optional["Summary"]
    ) -> Iterator[_LocatedRecord]:
        """Read the records at ``offsets``, in file order, one read each."""
        kind = self._settings.kind
        lengths = _indexed_record_lengths(summary, kind)
        for offset in sorted(offsets):
            if offset not in lengths:
                raise ValueError(
                    f"The summary of MCAP file {path!r} indexes no "
                    f"{kind.record_name} record at offset {offset}. The file may "
                    "have changed since listing."
                )
            record = read_record_at(f, offset, lengths[offset])
            checked = _checked(record, kind, path, offset)
            yield _LocatedRecord(checked, kind.unit_id(path, offset))

    def _scan_records(self, f: Any, path: str) -> Iterator[_LocatedRecord]:
        """Scan the whole file for the records of the granularity's kind.

        Without an index, a record's ``row_id`` is its ordinal among the file's
        records of its kind.
        """
        kind = self._settings.kind
        for ordinal, record in enumerate(iter_records(f, kind.record_type())):
            row_id = unindexed_record_row_id(path, kind.scan_tag, ordinal)
            yield _LocatedRecord(_checked(record, kind, path), row_id)

    def _add_record_row(
        self, batch: RecordRowBatch, path: str, located: _LocatedRecord
    ) -> bool:
        """Add a record's row to ``batch``; ``False`` if ``time_range`` drops it."""
        from mcap.records import Attachment

        record = located.record
        if isinstance(record, Attachment):
            if not self._settings.selection.in_time_range(record.log_time):
                return False
            batch.add_attachment(path, located.row_id, record)
        else:
            batch.add_metadata(path, located.row_id, record)
        return True


def _checked(
    record: "McapRecord", kind: RecordKind, path: str, offset: Optional[int] = None
) -> Union["Attachment", "Metadata"]:
    """``record``, which must be of ``kind``: an index record can point
    elsewhere in a file that changed since listing. ``offset`` is where an
    indexed record was read."""
    from mcap.records import Attachment, Metadata

    if isinstance(record, (Attachment, Metadata)) and isinstance(
        record, kind.record_type()
    ):
        return record
    where = "" if offset is None else f" at offset {offset}"
    raise ValueError(
        f"MCAP file {path!r}: expected a record of type {kind.record_name}{where}, "
        f"found {type(record).__name__}. The file may have changed since listing."
    )


def _indexed_record_lengths(
    summary: Optional["Summary"], kind: RecordKind
) -> Dict[int, int]:
    """Byte length of each indexed record of ``kind``, by offset."""
    if summary is None:
        return {}
    return {index.offset: index.length for index in kind.indexes(summary)}
