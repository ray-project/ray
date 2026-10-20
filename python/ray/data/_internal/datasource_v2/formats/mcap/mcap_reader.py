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
byte offset and the message's position in the chunk. At ``attachment`` and
``metadata`` granularity a row is one record of that kind. A sample read's
manifest rows are samples instead, and each becomes one row of each topic's
steps.

This module reads the manifest row by row and finishes every table with its
partition and synthesized columns. The other modules read and build the rows:

- ``mcap_chunks`` reads the selected messages of a file.
- ``mcap_message_rows`` builds message rows.
- ``mcap_records`` builds attachment and metadata rows.
- ``mcap_sample_rows`` builds sample rows, with the step rules of
  ``mcap_samples``.
- With ``video``, ``mcap_decoded_messages`` builds one row per decoded frame,
  and ``mcap_decoded_samples`` decodes the frames of sample rows.
  ``mcap_video_source`` picks what a decoding task reads, and ``mcap_decode``
  decodes the frames.
"""

import contextlib
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    FrozenSet,
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

from ray._common.utils import env_integer
from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decoded_messages import (
    DecodedMessageRows,
    DecodedRowSettings,
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
    MESSAGE_GRANULARITY,
    MCAPSelection,
    VideoOptions,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_records import (
    RECORD_KINDS,
    RecordRows,
    RecordSettings,
    count_records,
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
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import (
    MiB,
    call_with_retry,
    iterate_with_retry,
    make_async_gen,
)
from ray.data.block import BlockMetadata
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

# Record files a read task reads at once. A file's records take small requests
# in a row (open, then a read per record), so a task of small files waits on
# request latency, not on the CPU.
_RECORD_FILES_IN_FLIGHT = env_integer("RAY_DATA_MCAP_RECORD_FILES_IN_FLIGHT", 16)


def read_skips_errors() -> bool:
    """Whether ``DataContext.max_errored_blocks`` lets a read skip what it
    cannot read and keep the rest of the file or task.

    A read then returns fewer rows than the summaries count, so ``count()``
    reads rather than summing them.
    """
    from ray.data.context import DataContext

    return DataContext.get_current().max_errored_blocks != 0


@DeveloperAPI
class MCAPReader(Reader[FileManifest], SupportsMetadata):
    """Reads the chunks of MCAP files a manifest assigns to one task.

    Created by ``MCAPScanner.create_reader`` with every pushdown applied:
    the message selection, the projected columns and the per-task row limit.
    Also answers ``count()`` from the summaries (:meth:`read_metadata`) when
    the selection can be counted there.
    """

    # Files per count task. A count reads little of each file and waits on
    # request latency, so a task counts several, reading them at the same time.
    _COUNT_ROWS_BATCH_SIZE = env_integer(
        "RAY_DATA_MCAP_READER_COUNT_ROWS_BATCH_SIZE", 16
    )

    def __init__(
        self,
        *,
        selection: MCAPSelection,
        granularity: str = MESSAGE_GRANULARITY,
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
        video_topics: FrozenSet[str] = frozenset(),
        decoded_topics: Sequence[str] = (),
        max_lead_in_ns: int = DEFAULT_MAX_LEAD_IN_NS,
    ):
        """Initialize the reader.

        Args:
            selection: Which messages to keep.
            granularity: What one row of a read of paths is: ``message``,
                ``attachment`` or ``metadata``.
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
            video: Decode the video topics in the task, scaled to
                ``video.resize``: in message rows one frame per row; in sample
                rows the frames that the steps of ``decoded_topics`` take.
            video_topics: Topics to read as video besides those with a known
                video schema name (``read_mcap(video_topics=...)``).
            decoded_topics: The topics of sample rows that get frame columns,
                settled at planning.
            max_lead_in_ns: How far back a decoder looks for a keyframe
                (``RAY_DATA_MCAP_MAX_LEAD_IN_S``).
        """
        self._message_reader = SelectedMessageReader(selection, log_time_order)
        self._selection = selection
        self._video = video
        self._record_kind = RECORD_KINDS.get(granularity)
        self._record_rows: Optional[RecordRows] = None
        if self._record_kind is not None:
            self._record_rows = RecordRows(
                RecordSettings(
                    kind=self._record_kind,
                    selection=selection,
                    include_row_id=include_row_id,
                    target_block_size=target_block_size,
                )
            )
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
        # Without ``video`` nothing is decoded, so the plain reader stands in.
        decoding_reader = self._message_reader
        if video is not None:
            decoding_reader = SelectedMessageReader(
                selection,
                log_time_order,
                ChunkReads(max_bytes=_DECODING_CHUNK_READ_BYTES),
            )
        self._decoded_messages: Optional[DecodedMessageRows] = None
        if video is not None and not topic_plans:
            settings = DecodedRowSettings(
                selection=selection,
                video=video,
                video_topics=frozenset(video_topics),
                include_metadata=include_metadata,
                include_row_id=include_row_id,
                columns=self._columns,
                target_block_size=target_block_size,
                max_lead_in_ns=max_lead_in_ns,
            )
            self._decoded_messages = DecodedMessageRows(settings, decoding_reader)
        self._sample_rows: Optional[SampleRows] = None
        if topic_plans:
            decodes = video is not None and bool(decoded_topics)
            sample_reader = decoding_reader if decodes else self._message_reader
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
        """Read the files and chunks (or records) named by ``input_split``.

        Each row is read on its own, in manifest order, so a path listed twice
        is read twice, though a task's record files are read several at a time
        (``_RECORD_FILES_IN_FLIGHT``). With ``limit``, reading stops at that
        many rows: no later message is added to a table and no later chunk or
        file is read. A sample read reads only as many samples as the limit
        leaves. A decoded frame can leave its decoder after later messages, and
        record rows come a table at a time, so for those rows the table that
        reaches the limit is cut.

        When ``DataContext.max_errored_blocks`` lets errors be skipped, a
        corrupt chunk costs only itself: the reader skips it and reads the
        file's other chunks, but in a sample read it fails the file. An indexed
        attachment or metadata record that does not decode, and a message whose
        payload is not valid JSON, likewise cost only their own rows. A file
        whose other data does not decode does not stop the task's other files,
        and a message read keeps the file's rows before the failure. Once they
        are read, one error names every file, chunk, record and message
        skipped, so the task counts once against the budget. Any other error,
        such as a failed read, raises at once, so the task's retry settings see
        it. Without skipping, the first failure fails the task at once.
        """
        if len(input_split) == 0:
            return
        file_reads = self._file_reads(input_split)
        failures: List[Tuple[str, Exception]] = []
        if self._reads_records_ahead(file_reads):
            yield from self._read_records_ahead(file_reads, failures)
        else:
            yield from self._read_files_in_turn(file_reads, failures)
        if failures:
            raise _read_error(failures)

    def _read_files_in_turn(
        self,
        file_reads: List[Tuple[str, Callable[..., Iterator[pa.Table]]]],
        failures: List[Tuple[str, Exception]],
    ) -> Iterator[pa.Table]:
        """Yield each file's tables, one file at a time, within ``limit``.

        Corrupt data that ``read_skips_errors`` lets pass goes to ``failures``.
        """
        skip_corrupt = read_skips_errors()
        remaining = self._limit
        for path, read_file in file_reads:
            if remaining is not None and remaining <= 0:
                break
            for table in self._read_one_file(
                path, read_file, remaining, skip_corrupt, failures
            ):
                if remaining is not None:
                    remaining -= table.num_rows
                yield table

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
        if skipped is not None and not skipped.is_empty():
            failures.append((path, _skipped_data_error(path, skipped)))

    # -- metadata ----------------------------------------------------------

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield one ``BlockMetadata`` per manifest row with its file's row
        count, in order. A path listed twice is counted twice, as ``read``
        reads it twice.

        ``Statistics`` counts messages per channel, and the record index the
        attachment and metadata records, so no payload is read. A file whose
        summary cannot answer is counted by reading it as ``read`` does. The
        files are counted at the same time, and a count that fails with an
        error in ``DataContext.retried_io_errors`` is retried.
        """
        from ray.data.context import DataContext

        retried_io_errors = DataContext.get_current().retried_io_errors

        def count_rows(path: str) -> int:
            return call_with_retry(
                partial(self._count_rows, path),
                description=f"count the rows of MCAP file {path}",
                match=retried_io_errors,
            )

        paths = [str(p) for p in file_manifest.paths]
        with ThreadPoolExecutor() as executor:
            for path, num_rows in zip(paths, executor.map(count_rows, paths)):
                yield BlockMetadata(
                    num_rows=num_rows,
                    size_bytes=None,
                    exec_stats=None,
                    input_files=(path,),
                )

    def _count_rows(self, path: str) -> int:
        """Count the rows a read of one whole file yields."""
        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(path) as f:
            summary = read_summary_from(f, path)
            if self._record_kind is not None:
                return count_records(f, summary, self._record_kind)
            if (
                summary is not None
                and summary.statistics is not None
                and self._channel_counts_complete(summary)
                and self._selection.schemas_settle(summary.channels, summary.schemas)
            ):
                selected = self._selection.selected_channel_ids(
                    summary.channels, summary.schemas
                )
                counts = summary.statistics.channel_message_counts
                return sum(counts.get(cid, 0) for cid in selected)
            # The summary cannot answer: no statistics, per-channel counts that
            # miss messages, or a channel or schema record declared only inside
            # a chunk. Count by reading.
            return self._count_by_reading(f, path, summary)

    def _count_by_reading(self, f: Any, path: str, summary: Optional["Summary"]) -> int:
        """Count the selected messages of one file by reading what ``read`` reads.

        An indexed file is counted over the chunks a read reads, so a chunk that
        names only channels the summary omits is skipped here too. A count needs
        no order, so the messages stream in file order, unsorted. A file without a chunk index is
        scanned through a read buffer of
        ``DataContext.streaming_read_buffer_size`` bytes, as ``read`` scans it.
        """
        from ray.data.context import DataContext

        if summary is None or not summary.chunk_indexes:
            buffer_size = DataContext.get_current().streaming_read_buffer_size
            messages = self._message_reader.iter_unindexed(
                buffered(f, buffer_size), path, log_time_order=False
            )
        else:
            messages = self._message_reader.iter_chunks(
                f, path, summary, None, log_time_order=False
            )
        return sum(1 for _ in messages)

    @staticmethod
    def _channel_counts_complete(summary: "Summary") -> bool:
        """Whether the per-channel counts cover every message of the file.

        An empty map means the counts are not available. A map that does not
        add up to ``message_count``, or that counts a channel the summary does
        not list, cannot be summed for a selection.
        """
        statistics = summary.statistics
        assert statistics is not None
        counts = statistics.channel_message_counts
        return set(counts) <= set(summary.channels) and (
            sum(counts.values()) == statistics.message_count
        )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        # A read that skips what it cannot read returns fewer rows than the
        # summaries count. Statistics count records, not the records in a time
        # range. Metadata records carry no time, so a time range does not change
        # their count. A sample row is not one record, so nothing counts it. A
        # decoded read emits frames, which a decoder may drop.
        if read_skips_errors():
            return set()
        if self._record_kind is not None and not self._record_kind.timed:
            return {MetadataType.NUM_ROWS}
        if (
            self._sample_rows is not None
            or self._selection.time_range is not None
            or self._video is not None
        ):
            return set()
        return {MetadataType.NUM_ROWS}

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        return self._COUNT_ROWS_BATCH_SIZE

    def _reads_records_ahead(
        self, file_reads: List[Tuple[str, Callable[..., Iterator[pa.Table]]]]
    ) -> bool:
        """Whether ``read`` reads several files of the task at a time.

        It does for record rows of several files without a limit. A read with
        a limit may need only its first file. The packer bounds the records of
        a task with several files, so a few files' tables held at once stay
        within its budget.
        """
        return (
            self._record_rows is not None
            and len(file_reads) > 1
            and self._limit is None
        )

    def _read_records_ahead(
        self,
        file_reads: List[Tuple[str, Callable[..., Iterator[pa.Table]]]],
        failures: List[Tuple[str, Exception]],
    ) -> Iterator[pa.Table]:
        """Yield each file's record tables in order, reading up to
        ``_RECORD_FILES_IN_FLIGHT`` files at a time.

        When ``read_skips_errors``, a corrupt record costs only itself and a
        file whose data is corrupt does not stop the others: their errors go
        to ``failures``. Any other error raises.
        Closing this generator stops the read-ahead threads once their running
        reads finish.
        """
        from ray.data.context import DataContext

        retried_io_errors = DataContext.get_current().retried_io_errors
        skips_errors = read_skips_errors()
        read_file = partial(
            _read_whole_file,
            retried_io_errors=retried_io_errors,
            skip_corrupt=skips_errors,
        )
        files = make_async_gen(
            iter(file_reads),
            lambda reads: map(read_file, reads),
            preserve_ordering=True,
            num_workers=max(1, min(_RECORD_FILES_IN_FLIGHT, len(file_reads))),
        )
        try:
            for file in files:
                if file.error is None:
                    yield from file.tables
                    if file.skipped is not None and not file.skipped.is_empty():
                        error = _skipped_data_error(file.path, file.skipped)
                        failures.append((file.path, error))
                elif skips_errors and is_corrupt_data_error(file.error):
                    failures.append((file.path, file.error))
                else:
                    raise file.error
        finally:
            files.close()

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
        decoded, an indexed record that does not decode and a message that is
        not valid JSON are recorded there and skipped, and a skipped message
        does not count against ``limit``. A scanned file has no chunk
        boundaries to skip to, so a corrupt chunk still fails it whole. A retry
        reads the file from its start, so ``skipped`` is cleared first.
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
            if self._record_rows is not None:
                assert self._record_kind is not None
                # Records need no chunk index. Their index records, carried or
                # read from the summary, give their lengths.
                unit = _read_unit(path, offsets, self._record_kind.unit_id)
                yield from _within_limit(
                    self._record_rows.tables(
                        f,
                        path,
                        offsets,
                        summary,
                        lambda table: self._finish(table, path, unit, 0),
                        on_corrupt=None if skipped is None else skipped.skip_record,
                    ),
                    limit,
                )
                return
            if self._decoded_messages is not None:
                yield from self._decoded_tables(
                    f, owned, summary, file_summary, limit, skipped
                )
                return
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

    def _decoded_tables(
        self,
        f: Any,
        owned: "_OwnedFile",
        summary: Optional["Summary"],
        file_summary: Optional["_FileSummary"],
        limit: Optional[int],
        skipped: Optional["_SkippedData"] = None,
    ) -> Iterator[pa.Table]:
        """Yield the decoded frame rows of one file, at most ``limit`` of them.

        The decoders look at chunks the task does not own, so they plan with
        the file's whole summary. With ``skipped``, a corrupt chunk is recorded
        there and skipped, and each camera it may hold has null frames until
        its next keyframe (``mcap_video_source``).
        """
        assert self._decoded_messages is not None
        path, offsets = owned.path, owned.offsets
        whole = None
        if summary is not None and summary.chunk_indexes:
            whole = summary if file_summary is None else file_summary()
        unit = _read_unit(path, offsets)
        wanted = set(self._columns) if self._columns is not None else None
        tables = self._decoded_messages.tables(
            f,
            path,
            offsets,
            whole,
            lambda table, rows_before: self._finish(table, path, unit, rows_before),
            self._appended_row_bytes(path, unit, wanted),
            limit,
            on_corrupt=None
            if skipped is None
            else partial(_record_corrupt_chunk, skipped.chunks),
        )
        # A limit can stop the rows mid-file: closing them then ends the
        # file's decoders while the file is still open.
        with contextlib.closing(tables):
            yield from _within_limit(tables, limit)

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
        unit = _read_unit(path, offsets)
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
    corrupt chunk or record until the file is done.
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

    Every corrupt chunk and attachment or metadata record is named by its
    offset, and one error is kept for each kind. Messages that are not valid
    JSON are counted and only the first is kept.
    """

    chunks: _CorruptOffsets = field(default_factory=_CorruptOffsets)
    # Corrupt attachment or metadata records.
    records: _CorruptOffsets = field(default_factory=_CorruptOffsets)
    num_messages: int = 0
    first_message: Optional[_SkippedMessage] = None

    def skip_record(self, offset: int, error: Exception) -> None:
        """Record an attachment or metadata record skipped as corrupt."""
        self.records.add(offset, error)

    def record_message(self, row_id: str, error: Exception) -> None:
        """Count a skipped message, keeping it if it is the first."""
        if self.first_message is None:
            self.first_message = _SkippedMessage(row_id, error)
        self.num_messages += 1

    def is_empty(self) -> bool:
        """Whether the read skipped nothing."""
        return not (self.chunks.offsets or self.records.offsets or self.num_messages)

    def clear(self) -> None:
        """Forget what was skipped, for a read that starts the file over."""
        self.chunks = _CorruptOffsets()
        self.records = _CorruptOffsets()
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


def _read_unit(
    path: str,
    offsets: Optional[Set[int]],
    unit_id: Callable[[str, int], str] = chunk_unit_id,
) -> ReadUnit:
    """The read unit every table of a task's file reports.

    It is the file for a whole-file read, else the row's first chunk or
    record, which ``unit_id`` names. Listing excludes read units one by one
    (``excluded_read_unit_ids``), so handing this unit back would skip only
    that first chunk or record. Only a ``generated_id_column`` checkpoint
    hands units back, and ``read_mcap`` refuses it. A ``row_id`` checkpoint
    filters rows by id instead.
    """
    if offsets is None:
        return ReadUnit(id=path, source=path, count=1)
    return ReadUnit(id=unit_id(path, min(offsets)), source=path)


@dataclass(frozen=True)
class _FileTables:
    """The record tables of one file read ahead, or the error that failed it."""

    path: str
    tables: List[pa.Table]
    error: Optional[Exception] = None
    # The corrupt records the read skipped; ``None`` when it skips nothing.
    skipped: Optional[_SkippedData] = None


def _read_whole_file(
    file_read: Tuple[str, Callable[..., Iterator[pa.Table]]],
    retried_io_errors: List[str],
    skip_corrupt: bool,
) -> _FileTables:
    """Read all of one file's tables, retrying ``retried_io_errors``.

    A failure is returned rather than raised, so the files after it are read.
    With ``skip_corrupt``, a corrupt record is skipped and kept in ``skipped``.
    """
    path, read = file_read
    skipped = _SkippedData() if skip_corrupt else None
    try:
        tables = list(
            iterate_with_retry(
                partial(read, None, skipped),
                f"read MCAP file {path}",
                match=retried_io_errors,
            )
        )
    except Exception as e:  # noqa: BLE001 - ``read`` decides whether to raise
        return _FileTables(path, [], e)
    return _FileTables(path, tables, skipped=skipped)


def _within_limit(
    tables: Iterator[pa.Table], limit: Optional[int]
) -> Iterator[pa.Table]:
    """``tables``, cut to hold at most ``limit`` rows in all.

    Decoded and record rows are built a table at a time, so the table that
    reaches the limit is cut here, and no later table is built.
    """
    if limit is None:
        yield from tables
        return
    for table in tables:
        if table.num_rows >= limit:
            yield table.slice(0, limit)
            return
        limit -= table.num_rows
        yield table


def _record_corrupt_chunk(
    corrupt: _CorruptOffsets, chunk_index: "ChunkIndex", error: Exception
) -> None:
    """Record a chunk a read skips as corrupt."""
    corrupt.add(chunk_index.chunk_start_offset, error)


def _skipped_data_error(path: str, skipped: _SkippedData) -> ValueError:
    """The error for the chunks, records and messages of ``path`` that a read
    skipped.

    It names every chunk and record and the first message, and quotes the
    first error: the first chunk's, else the first record's, else the first
    message's.
    """
    parts: List[str] = []
    errors: List[Exception] = []
    for what, corrupt in (("chunk", skipped.chunks), ("record", skipped.records)):
        if corrupt.error is not None:
            parts.append(_corrupt_offsets(what, corrupt))
            errors.append(corrupt.error)
    message = skipped.first_message
    if message is not None:
        count = skipped.num_messages
        what = "message that is" if count == 1 else "messages that are"
        which = "row id" if count == 1 else "first: row id"
        parts.append(f"{count} {what} not valid JSON ({which} {message.row_id})")
        errors.append(message.error)
    first = errors[0]
    error = ValueError(
        f"Skipped {' and '.join(parts)} of MCAP file {path!r}: "
        f"{type(first).__name__}: {_first_line(first)}"
    )
    error.__cause__ = first
    return error


def _corrupt_offsets(what: str, corrupt: _CorruptOffsets) -> str:
    """Name the corrupt ``what``, a chunk or a record, by their byte offsets."""
    offsets = sorted(corrupt.offsets)
    where = (
        f"{what} at byte offset" if len(offsets) == 1 else f"{what}s at byte offsets"
    )
    listed = ", ".join(str(offset) for offset in offsets)
    return f"{len(offsets)} corrupt {where} {listed}"


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
