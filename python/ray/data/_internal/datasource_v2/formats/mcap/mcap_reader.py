"""``MCAPReader``, which reads one read task's chunks into message rows.

``MCAPReader.read`` receives a ``FileManifest`` whose rows name files and, per
file, the byte offsets of the chunks the task owns and the summary records it
needs (``mcap_task_summary``). For each file it seeks to those chunks,
decompresses them, keeps the selected messages and yields Arrow tables of about
``target_block_size`` bytes. With ``log_time_order``, a file's chunks are merged
by log time as they are read, so memory holds only the chunks that overlap in
time.

A row has the columns of ``message_schema``. Its optional ``row_id`` is a
deterministic name for the message, built from the file path, the chunk's byte
offset and the message's position in the chunk.

This module groups the manifest by file and finishes every table with its
partition and synthesized columns. The other modules read and build the rows:

- ``mcap_chunks`` reads the selected messages of a file.
- ``mcap_message_rows`` builds message rows.
"""

import itertools
import logging
from dataclasses import dataclass
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
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    buffered,
    parse_summary_records,
    read_summary_from,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    _MessageTableBuilder,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import chunk_unit_id
from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    task_summaries,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import iterate_with_retry
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import ChunkIndex
    from mcap.summary import Summary

logger = logging.getLogger(__name__)


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
            log_time_order: Whether each file's messages come out in ascending
                ``log_time`` order rather than in file order.
            columns: Columns to produce, in order; ``None`` for all of them.
            limit: Stop after this many rows per manifest.
            filesystem: Filesystem the paths resolve against; local when ``None``.
            partitioning: Path partitioning whose values become columns.
            synthesized_columns: Columns appended to every table rather than
                read, e.g. ``PathColumn`` for ``include_paths``.
            target_block_size: Estimated bytes per yielded table; ``None`` yields
                one table per file.
            schema: Dataset schema, used to type partition columns.
            decode_json: Whether ``data`` holds decoded JSON values rather than
                the payload bytes.
        """
        self._message_reader = SelectedMessageReader(selection, log_time_order)
        self._decode_json = decode_json
        self._include_metadata = include_metadata
        self._include_row_id = include_row_id
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

        All rows that name one file are read together. Files come out in
        manifest order. With ``limit``, reading stops at that many rows: no
        later message is added to a table and no later chunk or file is read.

        When ``DataContext.max_errored_blocks`` lets errors be skipped, a
        corrupt chunk costs only itself: the reader skips it and reads the
        file's other chunks. A file that fails as a whole does not stop the
        task's other files. Once they are read, one error names every file and
        chunk that failed, so the task counts once against the budget.
        Otherwise the first failure fails the task at once.
        """
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        context = DataContext.get_current()
        retried_io_errors = context.retried_io_errors
        read_past_failures = context.max_errored_blocks != 0
        remaining = self._limit
        failures: List[Tuple[str, Exception]] = []
        for owned in _owned_files(input_split):
            if remaining is not None and remaining <= 0:
                break
            # The chunks skipped as corrupt, by offset. Each attempt starts it
            # afresh, so it holds the last attempt's.
            corrupt: Optional[Dict[int, Exception]] = {} if read_past_failures else None
            tables = iterate_with_retry(
                partial(self._read_file, owned, remaining, corrupt),
                f"read MCAP file {owned.path}",
                match=retried_io_errors,
            )
            try:
                for table in tables:
                    if remaining is not None:
                        remaining -= table.num_rows
                    yield table
            except Exception as e:  # noqa: BLE001 - raised once the others are read
                if not read_past_failures:
                    raise
                failures.append((owned.path, e))
            else:
                if corrupt:
                    failures.append(
                        (owned.path, _corrupt_chunks_error(owned.path, corrupt))
                    )
        if failures:
            raise _read_error(failures)

    # -- one file ----------------------------------------------------------

    def _read_file(
        self,
        owned: "_OwnedFile",
        limit: Optional[int],
        corrupt: Optional[Dict[int, Exception]] = None,
    ) -> Iterator[pa.Table]:
        """Yield the tables of one file, limited to the chunks it owns.

        ``owned.offsets`` is ``None`` for a whole-file row, which means every
        chunk. A file without a chunk index is scanned linearly, through a read
        buffer of ``DataContext.streaming_read_buffer_size`` bytes. At most
        ``limit`` rows are yielded. With ``corrupt``, a chunk that cannot be
        decoded is recorded there by offset and skipped; a scanned file has no
        chunk boundaries to skip to, so it still fails whole. A retry reads the
        file from its start, so ``corrupt`` is cleared first.
        """
        from ray.data.context import DataContext

        if corrupt is not None:
            corrupt.clear()

        path, offsets = owned.path, owned.offsets
        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(path) as f:
            if owned.summary_records is None:
                summary = read_summary_from(f, path)
                file_summary = None
            else:
                # The listing's copy, so the summary is not read. A chunk row's
                # copy indexes only the owned chunks.
                summary = parse_summary_records(owned.summary_records, path)
                file_summary = None if offsets is None else _FileSummary(f, path)
            if summary is None or not summary.chunk_indexes:
                buffer_size = DataContext.get_current().streaming_read_buffer_size
                messages = self._message_reader.iter_unindexed(
                    buffered(f, buffer_size), path, limit
                )
            else:
                messages = self._message_reader.iter_chunks(
                    f,
                    path,
                    summary,
                    offsets,
                    file_summary,
                    on_corrupt=None
                    if corrupt is None
                    else partial(_record_corrupt_chunk, corrupt),
                )
            yield from self._tables(itertools.islice(messages, limit), path, offsets)

    # -- rows to tables ----------------------------------------------------

    def _tables(
        self, messages: Iterator[_Selected], path: str, offsets: Optional[Set[int]]
    ) -> Iterator[pa.Table]:
        """Build tables of about ``target_block_size`` bytes from the messages.

        Messages are pulled one at a time, so a chunk is read only once its
        first message is the next one needed.
        """
        # Every table reports one read unit: the file for a whole-file read, else
        # the task's first chunk. Listing excludes read units chunk by chunk
        # (``excluded_read_unit_ids``), so handing this unit back would skip only
        # that first chunk. Nothing hands units back yet: a ``row_id`` checkpoint
        # filters rows by id instead.
        if offsets is None:
            unit = ReadUnit(id=path, source=path, count=1)
        else:
            unit = ReadUnit(id=chunk_unit_id(path, min(offsets)), source=path)
        wanted = set(self._columns) if self._columns is not None else None
        builder = _MessageTableBuilder(
            columns=wanted,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            decode_json=self._decode_json,
            extra_row_bytes=self._appended_row_bytes(path, unit, wanted),
        )
        rows_before = 0
        for selected in messages:
            builder.add(selected)
            if (
                self._target_block_size is not None
                and builder.estimated_bytes >= self._target_block_size
            ):
                num_rows = builder.num_rows
                # ``build`` empties the builder, so the yielded table is the
                # only copy of its rows.
                yield self._finish(builder.build(), path, unit, rows_before)
                rows_before += num_rows
        if builder.num_rows > 0:
            yield self._finish(builder.build(), path, unit, rows_before)

    def _appended_row_bytes(
        self, path: str, unit: ReadUnit, wanted: Optional[Set[str]]
    ) -> int:
        """Bytes per row of the partition and synthesized columns ``_finish`` adds."""
        total = 0
        if self._partition_parser is not None:
            for name, value in self._partition_parser(path).items():
                if wanted is None or name in wanted:
                    total += self._partition_value_array(name, value, 1).nbytes
        position = ReadUnitPosition(unit=unit, rows_before=0)
        for column in self._synthesized_columns:
            if wanted is None or column.name in wanted:
                total += column.compute(position, 1).nbytes
        return total

    def _finish(
        self, table: pa.Table, path: str, unit: ReadUnit, rows_before: int
    ) -> pa.Table:
        """Append partition and synthesized columns, then apply the projection."""
        wanted = set(self._columns) if self._columns is not None else None
        num_rows = table.num_rows
        table = self._append_partition_columns(table, path, wanted)
        table = self._append_synthesized_columns(table, unit, rows_before, wanted)
        if self._columns is not None:
            produced = set(table.column_names)
            table = table.select([c for c in self._columns if c in produced])
            if table.num_columns == 0 and num_rows > 0:
                table = table.append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.nulls(num_rows)
                )
        # A JSON payload Arrow cannot type falls back to Ray's pickled-object
        # extension. Unpickling runs arbitrary code, so such a column is refused
        # unless the user opted in, as in every other datasource.
        raise_on_pickle_object_columns(table)
        return table

    def _append_partition_columns(
        self, table: pa.Table, path: str, wanted: Optional[Set[str]]
    ) -> pa.Table:
        """Append the partition values parsed from ``path``, if wanted."""
        if self._partition_parser is None:
            return table
        num_rows = table.num_rows
        for name, value in self._partition_parser(path).items():
            if wanted is not None and name not in wanted:
                continue
            if name in table.column_names:
                # A partition key that names a message column, such as a
                # ``topic=camera/`` folder, does not replace the messages' own
                # values. Parquet also keeps the file's column.
                if log_once(f"mcap_partition_key_shadowed:{name}"):
                    logger.warning(
                        "The partition key %r in %r names a message column; "
                        "read_mcap keeps the messages' values and ignores the "
                        "one in the path.",
                        name,
                        path,
                    )
                continue
            table = table.append_column(
                name, self._partition_value_array(name, value, num_rows)
            )
        return table

    def _append_synthesized_columns(
        self,
        table: pa.Table,
        unit: ReadUnit,
        rows_before: int,
        wanted: Optional[Set[str]],
    ) -> pa.Table:
        """Append the wanted synthesized columns, replacing any of the same name."""
        num_rows = table.num_rows
        position = ReadUnitPosition(unit=unit, rows_before=rows_before)
        for column in self._synthesized_columns:
            if wanted is not None and column.name not in wanted:
                continue
            if column.name in table.column_names:
                table = table.drop([column.name])
            table = table.append_column(column.name, column.compute(position, num_rows))
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


def _record_corrupt_chunk(
    corrupt: Dict[int, Exception], chunk_index: "ChunkIndex", error: Exception
) -> None:
    """Keep the first error of each chunk a read skips as corrupt."""
    corrupt.setdefault(chunk_index.chunk_start_offset, error)


def _corrupt_chunks_error(path: str, corrupt: Dict[int, Exception]) -> ValueError:
    """The error for the chunks of ``path`` that a read skipped as corrupt."""
    offsets = sorted(corrupt)
    first = corrupt[offsets[0]]
    where = "chunk at byte offset" if len(offsets) == 1 else "chunks at byte offsets"
    error = ValueError(
        f"Skipped {len(offsets)} corrupt {where} "
        f"{', '.join(str(offset) for offset in offsets)} of MCAP file {path!r}: "
        f"{type(first).__name__}: {_first_line(first)}"
    )
    error.__cause__ = first
    return error


def _read_error(failures: List[Tuple[str, Exception]]) -> Exception:
    """The error for the files a task could not read in full.

    A single failure is its own error, whose message already names the file.
    Several become one ``ValueError`` naming each file, caused by the first.
    """
    if len(failures) == 1:
        return failures[0][1]
    lines = [f"{path}: {type(e).__name__}: {_first_line(e)}" for path, e in failures]
    error = ValueError(
        f"{len(failures)} MCAP files of this read task could not be read in full:\n"
        + "\n".join(lines)
    )
    error.__cause__ = failures[0][1]
    return error


def _first_line(error: Exception) -> str:
    """The first non-empty line of ``error``'s message."""
    return next((line for line in str(error).splitlines() if line.strip()), "")


@dataclass(frozen=True)
class _OwnedFile:
    """What one read task reads of one file."""

    path: str
    # Byte offsets of the owned chunks; ``None`` for the whole file.
    offsets: Optional[Set[int]]
    # The summary records the task needs (``mcap_task_summary``); ``None`` to
    # read the summary from the file.
    summary_records: Optional[bytes]


def _owned_files(manifest: FileManifest) -> List[_OwnedFile]:
    """Group a manifest's rows by file, in manifest order.

    A row with chunk metadata contributes its ``unit_ids``, the chunk byte
    offsets. A row that names no chunk means the whole file (see
    ``mcap_file_indexer``), which wins over any offsets listed for the same path.
    A file named by more than one row reads its summary from the file.
    """
    offsets_by_path: Dict[str, Optional[Set[int]]] = {}
    records_by_path: Dict[str, Optional[bytes]] = {}
    rows = zip(manifest.paths, manifest.file_chunk_metadatas, task_summaries(manifest))
    for path, metadata, records in rows:
        path = str(path)
        records_by_path[path] = None if path in records_by_path else records
        unit_ids = None if metadata is None else metadata.get("unit_ids")
        if unit_ids is None or len(unit_ids) == 0:
            offsets_by_path[path] = None
            continue
        offsets = {int(i) for i in unit_ids}
        current = offsets_by_path.get(path, offsets)
        if current is not None:
            current.update(offsets)
        offsets_by_path[path] = current
    return [
        _OwnedFile(path, offsets, records_by_path[path])
        for path, offsets in offsets_by_path.items()
    ]


class _FileSummary:
    """The whole summary of an open file, read on the first call."""

    def __init__(self, f: Any, path: str):
        self._f = f
        self._path = path
        self._summary: Optional["Summary"] = None

    def __call__(self) -> "Summary":
        if self._summary is None:
            self._summary = read_summary_from(self._f, self._path)
            assert self._summary is not None, "a file with chunk rows has a summary"
        return self._summary
