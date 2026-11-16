"""``MCAPReader``, which reads one read task's chunks into rows.

``MCAPReader.read`` receives a ``FileManifest`` whose rows name files and, per
file, the byte offsets of the chunks the task owns and the summary records it
needs (``mcap_task_summary``). For each file it seeks to those chunks,
decompresses them, keeps the selected messages and yields Arrow tables of about
``target_block_size`` bytes. With ``log_time_order``, a file's chunks are merged
by log time as they are read, so memory holds only the chunks that overlap in
time.

A message row has the columns of ``message_schema``. Its optional ``row_id``
is a deterministic name for the message, built from the file path, the chunk's
byte offset and the message's position in the chunk. A sample read's manifest
rows are samples instead, and each becomes one row of each topic's steps.

This module reads the manifest row by row and finishes every table with its
partition and synthesized columns. The other modules read and build the rows:

- ``mcap_chunks`` reads the selected messages of a file.
- ``mcap_message_rows`` builds message rows.
- ``mcap_sample_rows`` builds sample rows, with the step rules of
  ``mcap_samples``. With ``video``, ``mcap_decoded_samples`` decodes their
  camera frames, with ``mcap_decode``.
"""

import logging
from dataclasses import dataclass, field, replace
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
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
    ChunkReads,
    buffered,
    is_corrupt_data_error,
    parse_summary_records,
    read_summary_from,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    InvalidJsonPayload,
    _MessageTableBuilder,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    DEFAULT_MAX_LEAD_IN_NS,
    MCAPSelection,
    VideoOptions,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_sample_rows import (
    DEFAULT_MAX_ROW_BYTES,
    SampleFile,
    SamplePaths,
    SampleRows,
    group_samples_by_file,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import TopicPlan
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
from ray.data._internal.util import MiB, iterate_with_retry
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.records import ChunkIndex
    from mcap.summary import Summary

logger = logging.getLogger(__name__)

# Largest read of a decoding task: RAY_DATA_MCAP_CHUNK_READ_BYTES, at most 4 MiB.
# Larger reads leave more freed memory in glibc's heap next to the frame buffers.
# On S3, a decoding task peaked at 1,616 MiB with 16 MiB reads and at 1,156 MiB
# with 4 MiB reads, in the same time.
_DECODING_CHUNK_READ_BYTES = min(ChunkReads().max_bytes, 4 * MiB)


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
        max_row_bytes: int = DEFAULT_MAX_ROW_BYTES,
        topic_plans: Sequence[TopicPlan] = (),
        max_run_bytes: Optional[int] = None,
        video: Optional[VideoOptions] = None,
        decoded_topics: Sequence[str] = (),
        max_lead_in_ns: int = DEFAULT_MAX_LEAD_IN_NS,
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
            max_row_bytes: Largest payload a sample row may carry. A larger row
                fails the read.
            topic_plans: For a sample read, each topic's steps
                (``mcap_samples``), in the order the row's columns take. Empty
                for a read of message rows.
            max_run_bytes: Most bytes the chunks of a run of samples hold
                (``mcap_sample_rows``); ``None`` ends runs only at gaps.
            video: For a sample read, decode the frames that the steps of
                ``decoded_topics`` take, scaled to ``video.resize``.
            decoded_topics: The topics of sample rows that get frame columns,
                settled at planning.
            max_lead_in_ns: How far back a decoder looks for a keyframe
                (``RAY_DATA_MCAP_MAX_LEAD_IN_S``).
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
        self._sample_rows: Optional[SampleRows] = None
        if topic_plans:
            decodes = video is not None and bool(decoded_topics)
            sample_reader = self._message_reader
            if decodes:
                sample_reader = SelectedMessageReader(
                    selection,
                    log_time_order,
                    ChunkReads(max_bytes=_DECODING_CHUNK_READ_BYTES),
                )
            self._sample_rows = SampleRows(
                topic_plans,
                sample_reader,
                self._project,
                columns=self._columns,
                target_block_size=target_block_size,
                max_row_bytes=max_row_bytes,
                max_run_bytes=max_run_bytes,
                decoded_topics=decoded_topics if decodes else (),
                resize=video.resize if video is not None else None,
                max_lead_in_ns=max_lead_in_ns,
                decodes_video=video is not None,
            )
        self._sample_paths = SamplePaths(filesystem)

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read the files and chunks named by ``input_split``.

        Each row is read on its own, in manifest order, so a path listed twice
        is read twice. With ``limit``, reading stops at that many rows: no later
        message is added to a table and no later chunk or file is read. A sample
        read reads only as many samples as the limit leaves.

        When ``DataContext.max_errored_blocks`` lets errors be skipped, a
        corrupt chunk costs only itself: the reader skips it and reads the
        file's other chunks, but in a sample read it fails the file. A message
        whose payload is not valid JSON likewise costs only its own row. A file
        whose other data does not decode does not stop the task's other files,
        and a message read keeps the file's rows before the failure. Once they
        are read, one error names what was skipped (``_skipped_data_error``),
        so the task counts once against the budget. Any other error, such as a
        failed read, raises at once, so the task's retry settings see it.
        Without skipping, the first failure fails the task at once.
        """
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        skip_corrupt = DataContext.get_current().max_errored_blocks != 0
        remaining = self._limit
        failures: List[Tuple[str, Exception]] = []
        for path, read_file in self._file_reads(input_split):
            if remaining is not None and remaining <= 0:
                break
            for table in self._read_one_file(
                path, read_file, remaining, skip_corrupt, failures
            ):
                if remaining is not None:
                    remaining -= table.num_rows
                yield table
        if failures:
            raise _read_error(failures)

    def _read_one_file(
        self,
        path: str,
        read_file: Callable[..., Iterator[pa.Table]],
        limit: Optional[int],
        skip_corrupt: bool,
        failures: List[Tuple[str, Exception]],
    ) -> Iterator[pa.Table]:
        """Yield the tables of one file, retrying as ``retried_io_errors`` allows.

        With ``skip_corrupt``, the file's corrupt data is appended to
        ``failures`` rather than raised: its skipped chunks and messages, or
        its own error when it does not decode as a whole
        (``is_corrupt_data_error``).
        """
        from ray.data.context import DataContext

        # What the read skips. Each attempt starts it afresh, so it holds the
        # last attempt's.
        skipped = _SkippedData() if skip_corrupt else None
        tables = iterate_with_retry(
            partial(read_file, limit, skipped),
            f"read MCAP file {path}",
            match=DataContext.get_current().retried_io_errors,
        )
        try:
            yield from tables
        except Exception as e:
            if not skip_corrupt or not is_corrupt_data_error(e):
                raise
            failures.append((path, e))
            return
        if skipped is not None and (skipped.chunks.offsets or skipped.num_messages):
            failures.append((path, _skipped_data_error(path, skipped)))

    def _file_reads(
        self, manifest: FileManifest
    ) -> List[Tuple[str, Callable[..., Iterator[pa.Table]]]]:
        """Each file of the task, with the call that reads its tables."""
        if self._sample_rows is not None:
            return [
                (file.path, partial(self._read_samples, file))
                for file in group_samples_by_file(manifest, self._sample_paths)
            ]
        return [
            (owned.path, partial(self._read_file, owned))
            for owned in _owned_files(manifest)
        ]

    # -- one file ----------------------------------------------------------

    def _read_file(
        self,
        owned: "_OwnedFile",
        limit: Optional[int],
        skipped: Optional["_SkippedData"] = None,
    ) -> Iterator[pa.Table]:
        """Yield the tables of one file, limited to the chunks it owns.

        ``owned.offsets`` is ``None`` for a whole-file row, which means every
        chunk. A file without a chunk index is scanned linearly, through a read
        buffer of ``DataContext.streaming_read_buffer_size`` bytes. At most
        ``limit`` rows are built. With ``skipped``, a chunk that cannot be
        decoded and a message that is not valid JSON are recorded there and
        skipped, and a skipped message does not count against ``limit``. A
        scanned file has no chunk boundaries to skip to, so a corrupt chunk
        still fails it whole. A retry reads the file from its start, so
        ``skipped`` is cleared first.
        """
        from ray.data.context import DataContext

        if skipped is not None:
            skipped.clear()

        path, offsets = owned.path, owned.offsets
        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(path) as f:
            if owned.summary_records is None:
                # The listing could not read this summary. Reading it here fails
                # the task with the file's own error.
                summary = read_summary_from(f, path)
                file_summary = None
            else:
                # The listing's copy, so the summary is not read. A chunk row's
                # copy indexes only the owned chunks.
                summary = parse_summary_records(owned.summary_records, path)
                file_summary = None if offsets is None else _FileSummary(f, path)
            if summary is None or not summary.chunk_indexes:
                buffer_size = DataContext.get_current().streaming_read_buffer_size
                # A skipped message leaves its place in the limit to the next
                # one, so a read that can skip holds every selected message.
                messages = self._message_reader.iter_unindexed(
                    buffered(f, buffer_size), path, limit if skipped is None else None
                )
            else:
                messages = self._message_reader.iter_chunks(
                    f,
                    path,
                    summary,
                    offsets,
                    file_summary,
                    on_corrupt=None
                    if skipped is None
                    else partial(_record_corrupt_chunk, skipped.chunks),
                )
            yield from self._tables(messages, path, offsets, limit, skipped)

    def _read_samples(
        self,
        file: SampleFile,
        limit: Optional[int],
        skipped: Optional["_SkippedData"] = None,
    ) -> Iterator[pa.Table]:
        """Yield the sample rows of one file, at most ``limit`` of them.

        A chunk that cannot be decoded fails the file, so ``skipped`` stays
        empty. A read that keeps only the samples' own columns opens no file.
        """
        assert self._sample_rows is not None
        if limit is not None and limit < file.samples.num_rows:
            # Rows come out in the anchor order ``samples`` holds, so the
            # samples past the limit need no chunk read.
            file = replace(file, samples=file.samples.slice(0, limit))
        if not self._sample_rows.reads_messages:
            yield from self._sample_rows.tables_without_messages(file)
            return
        filesystem = file.filesystem or self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(file.path) as f:
            summary = read_summary_from(f, file.path)
            if summary is not None and not summary.chunk_indexes:
                summary = None
            yield from self._sample_rows.tables(f, file, summary)

    # -- rows to tables ----------------------------------------------------

    def _tables(
        self,
        messages: Iterator[_Selected],
        path: str,
        offsets: Optional[Set[int]],
        limit: Optional[int],
        skipped: Optional["_SkippedData"] = None,
    ) -> Iterator[pa.Table]:
        """Build tables of about ``target_block_size`` bytes from the messages.

        Messages are pulled one at a time, so a chunk is read only once its
        first message is the next one needed, and no message is pulled once
        ``limit`` rows are built. With ``skipped``, a message that is not valid
        JSON is recorded there instead of becoming a row. An error that fails
        the rest of the file (``is_corrupt_data_error``) first yields the rows
        built before it, so what the file keeps does not depend on the block
        size.
        """
        # Every table reports one read unit: the file for a whole-file read, else
        # the row's first chunk. Listing excludes read units chunk by chunk
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
        try:
            for selected in messages:
                if not _add_message(builder, selected, skipped):
                    continue
                if limit is not None and rows_before + builder.num_rows >= limit:
                    break
                if self._block_is_full(builder):
                    num_rows = builder.num_rows
                    # ``build`` empties the builder, so the yielded table is the
                    # only copy of its rows.
                    yield self._finish(builder.build(), path, unit, rows_before)
                    rows_before += num_rows
        except Exception as e:
            if skipped is None or not is_corrupt_data_error(e):
                raise
            # The file's bytes fail the same way on every read, so a retried
            # read yields these same tables and the retry skips them all.
            if builder.num_rows > 0:
                yield self._finish(builder.build(), path, unit, rows_before)
            raise
        if builder.num_rows > 0:
            yield self._finish(builder.build(), path, unit, rows_before)

    def _block_is_full(self, builder: _MessageTableBuilder) -> bool:
        """Whether ``builder`` holds ``target_block_size`` bytes or more."""
        return (
            self._target_block_size is not None
            and builder.estimated_bytes >= self._target_block_size
        )

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
        table = self._append_partition_columns(table, path, wanted)
        table = self._append_synthesized_columns(table, unit, rows_before, wanted)
        return self._project(table)

    def _project(self, table: pa.Table) -> pa.Table:
        """Apply the projection, keeping the row count of a table left without
        columns, and refuse pickled-object columns."""
        num_rows = table.num_rows
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


@dataclass(frozen=True)
class _SkippedMessage:
    """A message whose payload is not valid JSON, and the error it raised."""

    row_id: str
    error: Exception


@dataclass
class _CorruptOffsets:
    """The byte offsets of what a read skipped as corrupt, and one error.

    Only the error at the lowest offset is kept. An error's traceback holds the
    bytes that failed to parse, so keeping every error would hold every
    corrupt chunk until the file is done.
    """

    offsets: Set[int] = field(default_factory=set)
    lowest: Optional[int] = None
    error: Optional[Exception] = None

    def add(self, offset: int, error: Exception) -> None:
        """Record ``offset``, keeping ``error`` if ``offset`` is the lowest."""
        if self.lowest is None or offset < self.lowest:
            self.lowest = offset
            self.error = error
        self.offsets.add(offset)


@dataclass
class _SkippedData:
    """What a read of one file skipped.

    Every corrupt chunk is named by its offset, and one error is kept for them
    all. Messages that are not valid JSON are counted and only the first is
    kept.
    """

    chunks: _CorruptOffsets = field(default_factory=_CorruptOffsets)
    num_messages: int = 0
    first_message: Optional[_SkippedMessage] = None

    def record_message(self, row_id: str, error: Exception) -> None:
        """Count a skipped message, keeping it if it is the first."""
        if self.first_message is None:
            self.first_message = _SkippedMessage(row_id, error)
        self.num_messages += 1

    def clear(self) -> None:
        """Forget what was skipped, for a read that starts the file over."""
        self.chunks = _CorruptOffsets()
        self.num_messages = 0
        self.first_message = None


def _add_message(
    builder: _MessageTableBuilder,
    selected: _Selected,
    skipped: Optional[_SkippedData],
) -> bool:
    """Add one message to ``builder`` and return whether it became a row.

    With ``skipped``, a message whose payload is not valid JSON is recorded
    there rather than raised.
    """
    try:
        builder.add(selected)
    except InvalidJsonPayload as e:
        if skipped is None:
            raise
        _, _, _, row_id = selected
        skipped.record_message(row_id, e)
        return False
    return True


def _record_corrupt_chunk(
    corrupt: _CorruptOffsets, chunk_index: "ChunkIndex", error: Exception
) -> None:
    """Record a chunk a read skips as corrupt."""
    corrupt.add(chunk_index.chunk_start_offset, error)


def _skipped_data_error(path: str, skipped: _SkippedData) -> ValueError:
    """The error for the chunks and messages of ``path`` that a read skipped.

    It names every chunk and the first message, and quotes the first error:
    the first chunk's, or the first message's when no chunk was skipped.
    """
    offsets = sorted(skipped.chunks.offsets)
    parts = []
    if offsets:
        where = (
            "chunk at byte offset" if len(offsets) == 1 else "chunks at byte offsets"
        )
        listed = ", ".join(str(offset) for offset in offsets)
        parts.append(f"{len(offsets)} corrupt {where} {listed}")
    message = skipped.first_message
    if message is not None:
        count = skipped.num_messages
        what = "message that is" if count == 1 else "messages that are"
        which = "row id" if count == 1 else "first: row id"
        parts.append(f"{count} {what} not valid JSON ({which} {message.row_id})")
    if offsets:
        first = skipped.chunks.error
        assert first is not None
    else:
        assert message is not None
        first = message.error
    error = ValueError(
        f"Skipped {' and '.join(parts)} of MCAP file {path!r}: "
        f"{type(first).__name__}: {_first_line(first)}"
    )
    error.__cause__ = first
    return error


def _read_error(failures: List[Tuple[str, Exception]]) -> Exception:
    """The error for the files whose data a task found corrupt.

    A single failure is its own error, whose message already names the file.
    Several become one ``ValueError`` naming each file, caused by the first.
    Every failure is corruption, so no rerun would read any of them.
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
    """What one manifest row reads of one file."""

    path: str
    # Byte offsets of the owned chunks; ``None`` for the whole file.
    offsets: Optional[Set[int]]
    # The summary records the row needs (``mcap_task_summary``); ``None`` for a
    # file whose summary the listing could not read.
    summary_records: Optional[bytes]


def _owned_files(manifest: FileManifest) -> List[_OwnedFile]:
    """One ``_OwnedFile`` per manifest row, in manifest order.

    A row with chunk metadata owns its ``unit_ids``, the chunk byte offsets. A
    row that names no chunk means the whole file (see ``mcap_file_indexer``).
    """
    owned = []
    rows = zip(manifest.paths, manifest.file_chunk_metadatas, task_summaries(manifest))
    for path, metadata, records in rows:
        unit_ids = None if metadata is None else metadata.get("unit_ids")
        whole_file = unit_ids is None or len(unit_ids) == 0
        offsets = None if whole_file else {int(i) for i in unit_ids}
        owned.append(_OwnedFile(str(path), offsets, records))
    return owned


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
