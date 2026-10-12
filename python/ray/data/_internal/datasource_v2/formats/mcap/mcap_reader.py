"""``MCAPReader``, which reads one read task's chunks into rows.

``MCAPReader.read`` receives a ``FileManifest`` whose rows name files and, per
file, the byte offsets of the chunks the task owns. For each file it seeks to
those chunks, decompresses them, keeps the selected messages and yields Arrow
tables of about ``target_block_size`` bytes. With ``log_time_order``, a file's
chunks are merged by log time as they are read, so memory holds only the
chunks that overlap in time.

At ``message`` granularity a row has the columns of ``message_schema``. Its
optional ``row_id`` is a deterministic name for the message, built from the
file path, the chunk's byte offset and the message's position in the chunk. At
``window``, ``topic`` and ``file`` granularity a row packs many messages into
parallel list columns.

This module groups the manifest by file and finishes every table with its
partition and synthesized columns. The other modules read and build the rows:

- ``mcap_chunks`` reads the selected messages of a file.
- ``mcap_message_rows`` builds message rows.
- ``mcap_coarse_rows`` builds window, topic and file rows. ``mcap_windows``
  places the windows, ``mcap_lead_in`` picks the frames a decoder needs first,
  and ``mcap_coarse_layout`` lays the rows out in Arrow.
- With ``video``, ``mcap_decoded_messages`` builds one row per decoded frame,
  and ``mcap_decoded_windows`` adds each window's frames. ``mcap_video_source``
  picks what a decoding task reads, and ``mcap_decode`` decodes the frames.
"""

import logging
from dataclasses import dataclass
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
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

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_coarse_rows import (
    CoarseRows,
    RowSettings,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decoded_messages import (
    DecodedMessageRows,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decoded_windows import (
    DecodedWindowRows,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    _MessageTableBuilder,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    DEFAULT_MAX_LEAD_IN_NS,
    MESSAGE_GRANULARITY,
    TOPIC_GRANULARITY,
    WINDOW_GRANULARITY,
    MCAPSelection,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    chunk_unit_id,
    topic_unit_id,
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
from ray.data._internal.util import GiB, iterate_with_retry
from ray.data.block import BlockMetadata
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

if TYPE_CHECKING:
    from mcap.summary import Summary

logger = logging.getLogger(__name__)

# Largest payload a topic or file row may carry: a memory guardrail, not a
# format limit. A row is built in the worker, converted to Arrow and copied
# into the object store as one object, so a worker needs about three times its
# size. Payloads use 64-bit offsets, so the cap can be raised past 2 GiB.
DEFAULT_MAX_ROW_BYTES = GiB


@dataclass(frozen=True)
class _Assignment:
    """What one task reads of one file.

    ``offsets`` holds the byte offsets of the owned chunks, ``None`` for a
    whole-file listing row. ``topic`` is set at topic granularity.
    """

    path: str
    offsets: Optional[Set[int]]
    topic: Optional[str] = None

    @property
    def unit(self) -> ReadUnit:
        """The read unit every table of this assignment reports.

        A topic row reports its topic, a whole-file read the file, and a task
        owning some chunks its first one. Each chunk has one owner, so a
        checkpoint that hands the unit back skips only this task's rows. The
        unit is finished once every table of the file is written.
        """
        if self.topic is not None:
            return ReadUnit(id=topic_unit_id(self.path, self.topic), source=self.path)
        if self.offsets is None:
            return ReadUnit(id=self.path, source=self.path, count=1)
        first = min(self.offsets)
        return ReadUnit(id=chunk_unit_id(self.path, first), source=self.path)


@DeveloperAPI
class MCAPReader(Reader[FileManifest], SupportsMetadata):
    """Reads the chunks of MCAP files a manifest assigns to one task.

    Created by ``MCAPScanner.create_reader`` with every pushdown applied:
    the message selection, the projected columns and the per-task row limit.
    Also answers ``count()`` from the summaries (:meth:`read_metadata`) when
    the selection can be counted there.
    """

    # Files per count task. Reading a summary takes two small ranged requests,
    # so several files per task amortize the task overhead.
    _COUNT_ROWS_BATCH_SIZE = 16

    def __init__(
        self,
        *,
        selection: MCAPSelection,
        granularity: str = MESSAGE_GRANULARITY,
        window: Optional[WindowSpec] = None,
        video: Optional[VideoOptions] = None,
        video_topics: FrozenSet[str] = frozenset(),
        decoded_topics: Sequence[str] = (),
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
        max_lead_in_ns: int = DEFAULT_MAX_LEAD_IN_NS,
    ):
        """Initialize the reader.

        Args:
            selection: Which messages to keep.
            granularity: What one row is: ``message``, ``window``, ``topic`` or
                ``file``.
            window: Window placement, required at ``window`` granularity.
            video: Decode the video topics in the task: at ``message``
                granularity one frame per row, at ``window`` granularity the
                window's frames per topic, thinned to ``fps`` and scaled to
                ``resize``.
            video_topics: Topics to read as video besides those with a known
                video schema name (``read_mcap(video_topics=...)``).
            decoded_topics: With ``video`` at ``window`` granularity, the topics
                that get frame columns, settled at planning.
            include_metadata: Whether to emit the channel and schema columns.
            include_row_id: Whether to emit ``row_id``.
            log_time_order: Whether each file's message rows come out in
                ascending ``log_time`` order rather than in file order. Coarse
                rows are always in log-time order.
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
            max_row_bytes: Largest payload a topic or file row may carry. A
                larger row fails the read.
            max_lead_in_ns: How far back a video topic's lead-in may reach
                (``RAY_DATA_MCAP_MAX_LEAD_IN_S``).
        """
        if granularity == WINDOW_GRANULARITY and window is None:
            raise ValueError("window granularity needs a WindowSpec")
        self._selection = selection
        self._message_reader = SelectedMessageReader(selection, log_time_order)
        self._decode_json = decode_json
        self._granularity = granularity
        self._video = video
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
        settings = RowSettings(
            selection=selection,
            granularity=granularity,
            window=window,
            video=video,
            video_topics=frozenset(video_topics),
            decoded_topics=tuple(decoded_topics),
            include_metadata=include_metadata,
            include_row_id=include_row_id,
            columns=self._columns,
            target_block_size=target_block_size,
            max_row_bytes=max_row_bytes,
            max_lead_in_ns=max_lead_in_ns,
        )
        self._coarse_rows = CoarseRows(settings, self._message_reader, self._finish)
        self._decoded_messages = DecodedMessageRows(
            settings, self._message_reader, self._finish
        )
        self._decoded_windows = DecodedWindowRows(
            settings, self._coarse_rows, self._finish
        )

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read the files and chunks named by ``input_split``.

        All rows that name one file are read together. Files come out in
        manifest order.
        """
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        retried_io_errors = DataContext.get_current().retried_io_errors
        remaining = self._limit
        for assignment in _assignments(input_split):
            tables = iterate_with_retry(
                partial(self._read_file, assignment),
                f"read MCAP file {assignment.path}",
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

    # -- metadata ----------------------------------------------------------

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield one ``BlockMetadata`` per file with its selected message count.

        ``Statistics`` holds the count per channel, so a selection by topic or
        schema is summed from it without reading a payload. A file whose
        summary cannot answer is counted by reading it as ``read`` does.
        """
        from mcap.reader import SeekingReader

        filesystem = self._filesystem or LocalFileSystem()
        for path in dict.fromkeys(str(p) for p in file_manifest.paths):
            with filesystem.open_input_file(path) as f:
                summary = SeekingReader(f).get_summary()
                statistics = summary.statistics if summary is not None else None
                if (
                    summary is not None
                    and statistics is not None
                    and self._channel_counts_complete(summary)
                    and self._schemas_known_for_selection(summary)
                ):
                    selected = self._selection.selected_channel_ids(
                        summary.channels, summary.schemas
                    )
                    num_rows = sum(
                        statistics.channel_message_counts.get(cid, 0)
                        for cid in selected
                    )
                else:
                    # The summary cannot answer: no statistics, per-channel
                    # counts that miss messages, or a channel or schema record
                    # declared only inside a chunk. Count by reading.
                    num_rows = self._count_by_reading(f, path, summary)
            yield BlockMetadata(
                num_rows=num_rows,
                size_bytes=None,
                exec_stats=None,
                input_files=(path,),
            )

    def _count_by_reading(self, f: Any, path: str, summary: Optional["Summary"]) -> int:
        """Count the selected messages of one file by reading what ``read`` reads.

        An indexed file is counted over the chunks a read reads, so a chunk that
        names only channels the summary omits is skipped here too.
        """
        if summary is None or not summary.chunk_indexes:
            messages = self._message_reader.iter_unindexed(f, path)
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

    def _schemas_known_for_selection(self, summary: "Summary") -> bool:
        """Whether the summary carries every schema ``message_types`` needs.

        A channel whose schema record lives only inside a chunk passes the
        listing's schema filter unchecked. The reader filters its messages once
        the chunk declares the schema, so a count summed from the statistics
        would include them. Without ``message_types`` the schemas do not matter.
        """
        if self._selection.message_types is None:
            return True
        return all(
            not channel.schema_id or channel.schema_id in summary.schemas
            for channel in summary.channels.values()
        )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        # Statistics count messages per channel, not the messages in a time
        # range, and a coarse row is not a message. A decoded read emits
        # frames, which ``fps`` thins and a decoder may drop.
        if (
            self._granularity != MESSAGE_GRANULARITY
            or self._selection.time_range is not None
            or self._video is not None
        ):
            return set()
        return {MetadataType.NUM_ROWS}

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        return self._COUNT_ROWS_BATCH_SIZE

    # -- one file ----------------------------------------------------------

    def _read_file(self, assignment: _Assignment) -> Iterator[pa.Table]:
        """Yield the tables of one file, limited to the assigned chunks."""
        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(assignment.path) as f:
            summary = self._chunk_summary(f)
            if self._granularity == MESSAGE_GRANULARITY:
                if self._video is not None:
                    yield from self._decoded_messages.tables(f, assignment, summary)
                    return
                if summary is None:
                    messages = self._message_reader.iter_unindexed(f, assignment.path)
                else:
                    messages = self._message_reader.iter_chunks(
                        f, assignment.path, summary, assignment.offsets
                    )
                yield from self._tables(messages, assignment)
            elif self._granularity == WINDOW_GRANULARITY:
                if self._video is not None:
                    yield from self._decoded_windows.tables(f, assignment, summary)
                    return
                yield from self._coarse_rows.window_tables(f, assignment, summary)
            elif self._granularity == TOPIC_GRANULARITY:
                yield from self._coarse_rows.topic_tables(f, assignment, summary)
            else:
                yield from self._coarse_rows.file_tables(f, assignment, summary)

    def _chunk_summary(self, f: Any) -> Optional["Summary"]:
        """The file's summary, or ``None`` when it is read as if it had no index."""
        from mcap.reader import SeekingReader

        summary = SeekingReader(f).get_summary()
        if summary is not None and not summary.chunk_indexes:
            summary = None
        if (
            summary is not None
            and not summary.channels
            and self._granularity != MESSAGE_GRANULARITY
        ):
            # A summary without channel records cannot say which chunk
            # holds what, so the indexer listed this file whole. Coarse
            # rows scan it as if it had no index. Message rows keep the
            # chunk walk, which filters channels as their records appear.
            summary = None
        return summary

    # -- message rows ------------------------------------------------------

    def _tables(
        self, messages: Iterator[_Selected], assignment: _Assignment
    ) -> Iterator[pa.Table]:
        """Build tables of about ``target_block_size`` bytes from the messages."""
        wanted = set(self._columns) if self._columns is not None else None
        builder = _MessageTableBuilder(
            columns=wanted,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            decode_json=self._decode_json,
        )
        rows_before = 0
        for selected in messages:
            builder.add(selected)
            if (
                self._target_block_size is not None
                and builder.estimated_bytes >= self._target_block_size
            ):
                yield self._finish(builder.build(), assignment, rows_before)
                rows_before += builder.num_rows
                builder.reset()
        if builder.num_rows > 0:
            yield self._finish(builder.build(), assignment, rows_before)

    # -- finishing a table -------------------------------------------------

    def _finish(
        self, table: pa.Table, assignment: _Assignment, rows_before: int
    ) -> pa.Table:
        """Append partition and synthesized columns, then apply the projection."""
        wanted = set(self._columns) if self._columns is not None else None
        num_rows = table.num_rows
        table = self._append_partition_columns(table, assignment.path, wanted)
        table = self._append_synthesized_columns(
            table, assignment.unit, rows_before, wanted
        )
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


def _assignments(manifest: FileManifest) -> List[_Assignment]:
    """Group a manifest's rows by file (and topic): what this task reads of each.

    A row with chunk metadata contributes its ``unit_ids``, the chunk byte
    offsets. A row without it means the whole file, which wins over any offsets
    listed for the same path. A topic-granularity row also names its topic.
    """
    owned: Dict[Tuple[str, Optional[str]], Optional[Set[int]]] = {}
    for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
        path = str(path)
        topic = None
        if metadata is not None and metadata.get("topic") is not None:
            topic = str(metadata["topic"])
        key = (path, topic)
        if metadata is None or "unit_ids" not in metadata:
            owned[key] = None
            continue
        offsets = {int(i) for i in metadata["unit_ids"]}
        current = owned.get(key, offsets)
        if current is not None:
            current.update(offsets)
        owned[key] = current
    return [
        _Assignment(path=path, offsets=offsets, topic=topic)
        for (path, topic), offsets in owned.items()
    ]
