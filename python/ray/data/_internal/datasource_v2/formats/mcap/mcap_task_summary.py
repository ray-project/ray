"""The summary records each read task needs, carried to it in its manifest.

A read task needs, per file, the schema and channel records and the index
records of the chunks it owns. The listing has read every summary already, so
it hands those records on, and a read task neither requests nor parses the
whole summary. They stay MCAP records, in two steps:

1. ``MCAPSummaryIndexer`` adds two columns to its listing rows
   (``with_listing_records``): each chunk row's ``ChunkIndex`` record, and the
   file's records, stored once per file. For a chunk row those are the file's
   schema and channel records. A whole-file row carries the raw summary section,
   or no records when the file has no summary. An unreadable file carries null.
2. ``TaskSummaryPartitioner`` groups the rows into read tasks with
   ``OrderedFileCutter`` and ``OnlineBinPacker`` (``mcap_file_cuts``), which
   keep only paths, sizes and chunk metadata. To each packed row it adds the
   file's records followed by the index records of the chunks the row names,
   compressed with zstd (``TASK_SUMMARY_COLUMN``).

``MCAPReader`` parses that column (``task_summaries``). A null, carried by a
file the listing could not read, makes the reader read the summary from the
file, which fails the task with the file's own error.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    _RECORD_PREFIX_BYTES,
    summary_records,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
)

# Listing columns: the file's records, once per file, and each row's chunk.
FILE_RECORDS_COLUMN = "__mcap_file_records"
CHUNK_INDEX_COLUMN = "__mcap_chunk_index"
# Packed column: the records one read task needs for one file.
TASK_SUMMARY_COLUMN = "__mcap_task_summary"

_FILE_RECORDS_TYPE = pa.dictionary(pa.int32(), pa.binary())

# A ``ChunkIndex`` record starts with the opcode and the record length, then the
# chunk's two log times (8 bytes each) and its start offset (8).
_LOG_TIMES_AT = _RECORD_PREFIX_BYTES
_CHUNK_START_OFFSET_AT = _RECORD_PREFIX_BYTES + 16


@dataclass(frozen=True)
class SectionRecords:
    """The raw records of a summary section that chunk rows carry."""

    # The schema and channel records.
    declarations: bytes
    # Each ``ChunkIndex`` record, by its chunk's start offset.
    chunk_indexes: Mapping[int, bytes]


def split_section(section: memoryview) -> SectionRecords:
    """Split a summary section into the records that chunk rows carry."""
    from mcap.opcode import Opcode

    declarations: List[memoryview] = []
    chunk_indexes: Dict[int, bytes] = {}
    for opcode, record in summary_records(section):
        if opcode in (Opcode.SCHEMA, Opcode.CHANNEL):
            declarations.append(record)
        elif opcode == Opcode.CHUNK_INDEX:
            at = _CHUNK_START_OFFSET_AT
            offset = int.from_bytes(record[at : at + 8], "little")
            chunk_indexes[offset] = bytes(record)
    return SectionRecords(
        declarations=b"".join(declarations),
        chunk_indexes=chunk_indexes,
    )


def chunk_log_times(record: Optional[bytes]) -> Optional[Tuple[int, int]]:
    """The first and last log time in a chunk, read from its ``ChunkIndex`` record.

    ``None`` when ``record`` is not a ``ChunkIndex`` record, such as the empty or
    missing record of a listing row that names no chunk.
    """
    from mcap.opcode import Opcode

    at = _LOG_TIMES_AT
    if not record or record[0] != Opcode.CHUNK_INDEX or len(record) < at + 16:
        return None
    return (
        int.from_bytes(record[at : at + 8], "little"),
        int.from_bytes(record[at + 8 : at + 16], "little"),
    )


def with_listing_records(
    manifest: FileManifest,
    file_records: Optional[bytes],
    chunk_index_records: Sequence[Optional[bytes]],
) -> FileManifest:
    """One file's listing rows, plus the columns that carry its records.

    ``file_records`` is stored once, however many rows the file has.
    ``chunk_index_records`` has one entry per row, ``None`` for a whole-file row.
    """
    num_rows = len(manifest)
    assert len(chunk_index_records) == num_rows
    if file_records is None:
        repeated = pa.nulls(num_rows, type=_FILE_RECORDS_TYPE)
    else:
        repeated = pa.DictionaryArray.from_arrays(
            pa.array([0] * num_rows, type=pa.int32()),
            pa.array([file_records], type=pa.binary()),
        )
    block = _table(manifest).append_column(FILE_RECORDS_COLUMN, repeated)
    block = block.append_column(
        CHUNK_INDEX_COLUMN, pa.array(chunk_index_records, type=pa.binary())
    )
    return FileManifest(block)


def task_summaries(manifest: FileManifest) -> List[Optional[bytes]]:
    """Per row of a packed manifest, the summary records its read task needs.

    ``None`` for a file whose summary the listing could not read.
    """
    import zstandard

    block = _table(manifest)
    assert TASK_SUMMARY_COLUMN in block.column_names, (
        f"An MCAP read task's manifest has no {TASK_SUMMARY_COLUMN!r} column. "
        "Pack the listing rows with TaskSummaryPartitioner."
    )
    return [
        None if records is None else zstandard.decompress(records)
        for records in block.column(TASK_SUMMARY_COLUMN).to_pylist()
    ]


def _table(manifest: FileManifest) -> pa.Table:
    """The Arrow table behind ``manifest``."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    return block


@dataclass
class _HeldFile:
    """The records of one file whose listing rows are not all packed yet."""

    file_records: Optional[bytes]
    chunk_indexes: Dict[int, bytes] = field(default_factory=dict)
    unpacked_rows: int = 0


class TaskSummaryPartitioner(FilePartitioner):
    """Packs listing rows with ``packer`` and adds each row's summary records.

    ``packer`` groups the rows into read tasks (``OrderedFileCutter``). It
    drops the listing's record columns, so this partitioner holds each file's
    records until its last row leaves in a packed task. What it holds follows
    the packer's open bins.
    """

    def __init__(self, packer: FilePartitioner):
        self._packer = packer
        self._held: Dict[str, _HeldFile] = {}

    @property
    def requires_global_input(self) -> bool:
        return self._packer.requires_global_input

    def add_input(self, input_manifest: FileManifest) -> None:
        self._hold_records(input_manifest)
        self._packer.add_input(input_manifest)

    def has_partition(self) -> bool:
        return self._packer.has_partition()

    def next_partition(self) -> FileManifest:
        manifest = self._packer.next_partition()
        records = [
            self._task_summary(str(path), metadata)
            for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas)
        ]
        block = _table(manifest).append_column(
            TASK_SUMMARY_COLUMN, pa.array(records, type=pa.binary())
        )
        return FileManifest(block)

    def finalize(self) -> None:
        self._packer.finalize()

    def _hold_records(self, manifest: FileManifest) -> None:
        """Keep the records of ``manifest``'s rows until they are packed."""
        block = _table(manifest)
        assert FILE_RECORDS_COLUMN in block.column_names, (
            f"MCAP listing rows have no {FILE_RECORDS_COLUMN!r} column. List "
            "the files with MCAPSummaryIndexer."
        )
        file_records = block.column(FILE_RECORDS_COLUMN)
        chunk_indexes = block.column(CHUNK_INDEX_COLUMN).to_pylist()
        rows = zip(manifest.paths, manifest.file_chunk_metadatas, chunk_indexes)
        for i, (path, metadata, chunk_index) in enumerate(rows):
            # A path listed twice shares one entry: its copies carry the same
            # records, and every row of both counts toward the release.
            held = self._held.get(str(path))
            if held is None:
                # A dictionary value: one copy per file, not per row.
                held = _HeldFile(file_records[i].as_py())
                self._held[str(path)] = held
            held.unpacked_rows += 1
            if chunk_index is not None:
                (offset,) = metadata["unit_ids"]
                held.chunk_indexes[int(offset)] = chunk_index

    def _task_summary(
        self, path: str, metadata: Optional[Mapping[str, Any]]
    ) -> Optional[bytes]:
        """The records of one packed row, releasing them once the file is packed.

        A packed row unites the listing rows of one file in one task: one per
        chunk it names, or one whole-file row.
        """
        held = self._held[path]
        units = list(metadata["unit_ids"]) if metadata is not None else []
        held.unpacked_rows -= max(1, len(units))
        if held.unpacked_rows <= 0:
            del self._held[path]
        if held.file_records is None:
            return None
        import zstandard

        offsets = sorted({int(u) for u in units})
        records = held.file_records + b"".join(held.chunk_indexes[o] for o in offsets)
        return zstandard.compress(records)
