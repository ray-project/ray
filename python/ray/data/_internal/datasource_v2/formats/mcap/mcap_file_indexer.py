"""The MCAP file listing: each file split into chunk-sized listing rows.

``MCAPSummaryIndexer`` walks the paths like ``NonSamplingFileIndexer``, then
reads each file's summary and emits one manifest row per chunk that can hold a
selected message. The partitioner groups those rows into read tasks by the
memory each chunk is estimated to need (``chunk_runs``). A large recording is
then read by many tasks, and a small one shares a task with its neighbours
(``mcap_file_cuts``). A file whose summary shows it cannot match the selection
is dropped before any payload is read.

At ``attachment`` and ``metadata`` granularity there is one row per indexed
record of that kind instead, weighted by the memory reading it takes
(``_RECORD_WORKING_SET_RATIO``). A file's first row also weighs the requests
that opening the file takes, so a read task opens at most 16 files of small
records (``_RECORD_FILES_PER_READ_TASK``).

A file whose summary cannot place what the granularity needs is listed as one
whole-file row: no summary, chunk index or channel records for messages, and no
record index for records.

Each row also carries the summary records its read task will need, so read
tasks do not read summaries (``mcap_task_summary``).

A file whose summary cannot be read, such as a recording cut short, does not
fail the listing. It is listed as one row that names no chunk, which the reader
takes as the whole file, and that weighs a whole read task, so the packer puts
no other file in its task. Reading it then fails that task alone, where
``DataContext.max_errored_blocks`` can skip it.
"""

import logging
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    MessageRowLayout,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MESSAGE_GRANULARITY,
    MCAPSelection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_pushdown import (
    narrow_selection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_records import (
    RECORD_KINDS,
    RecordKind,
    listed_records,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    _DEFAULT_SUMMARY_IO_CONCURRENCY,
    ListedSummary,
    ReadMemory,
    chunk_runs,
    chunk_unit_id,
    parse_listed_summary,
    read_summaries,
    record_run,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_task_summary import (
    split_section,
    with_listing_records,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)

if TYPE_CHECKING:
    from concurrent.futures import Future

    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# A read task opens at most this many files of records. Opening a file is a
# request before its records' reads, so a file's first record row also weighs
# ``1 / (_RECORD_FILES_PER_READ_TASK + 1)`` of the task's budget. Every row
# weighs at least one byte, so one more file does not fit.
_RECORD_FILES_PER_READ_TASK = env_integer(
    "RAY_DATA_MCAP_RECORD_FILES_PER_READ_TASK", 16
)

# A record row weighs this many times its record's bytes. Reading a record
# holds three copies of it: the bytes read, the parsed record and its Arrow
# column. On S3, a read task of 24 MiB attachments took 3-4.5x its payload.
_RECORD_WORKING_SET_RATIO = 3


class MCAPSummaryIndexer(NonSamplingFileIndexer):
    """Lists MCAP files and emits one listing row per chunk.

    Inherits directory traversal, extension and partition pruning, file shuffle
    and whole-file checkpoint exclusion from :class:`NonSamplingFileIndexer`.
    Overrides :meth:`list_files` to read each listed file's summary and turn it
    into chunk rows (``mcap_summary.chunk_runs``). Grouping rows into read tasks
    is the partitioner's job.
    """

    def __init__(
        self,
        *,
        selection: MCAPSelection,
        ignore_missing_paths: bool,
        read_task_bytes: int,
        log_time_order: bool = True,
        row_layout: MessageRowLayout = MessageRowLayout(),
        granularity: str = MESSAGE_GRANULARITY,
        skip_paths: Optional[Iterable[str]] = None,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
        io_concurrency: Optional[int] = None,
    ):
        super().__init__(
            ignore_missing_paths=ignore_missing_paths,
            skip_paths=skip_paths,
            num_workers=num_workers,
            max_paths_per_output=max_paths_per_output,
        )
        self._selection = selection
        # The partitioner's budget per read task, which the row of an unreadable
        # file weighs (see the module docstring), and which bounds the summary
        # sections held at once.
        self._read_task_bytes = read_task_bytes
        self._read_memory = ReadMemory(read_task_bytes, log_time_order)
        # The columns a row carries, which size each chunk's listing row.
        self._row_layout = row_layout
        # The kind of record one row is, or ``None`` when a row is a message.
        self._record_kind: Optional[RecordKind] = RECORD_KINDS.get(granularity)
        # What opening a file of records weighs (``_RECORD_FILES_PER_READ_TASK``).
        self._record_open_bytes = read_task_bytes // (
            max(1, _RECORD_FILES_PER_READ_TASK) + 1
        )
        self._io_concurrency = (
            io_concurrency
            if io_concurrency is not None
            else env_integer(
                "RAY_DATA_MCAP_SUMMARY_IO_CONCURRENCY", _DEFAULT_SUMMARY_IO_CONCURRENCY
            )
        )

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"],
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        execution_idx: int = 0,
        excluded_read_unit_ids: Optional[AbstractSet[str]] = None,
    ) -> Iterable[FileManifest]:
        # ``predicate`` holds the ``topic`` and ``log_time`` conjuncts the
        # scanner folded into its selection. Folding them into the same base
        # selection lets listing prune with the selection the reader applies.
        # The scanner applies ``limit`` per task, and ``projected_columns`` is unused.
        selection = self._selection
        if predicate is not None:
            selection = narrow_selection(self._selection, predicate).selection
        file_infos = self._iter_file_infos_for_list(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
            shuffle_config=shuffle_config,
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        )
        assert filesystem is not None  # ``list_file_infos`` raised otherwise
        excluded = excluded_read_unit_ids or frozenset()
        for file_info, section_read in self._read_summaries(file_infos, filesystem):
            try:
                listed = parse_listed_summary(section_read.result(), file_info.path)
            except Exception as e:  # noqa: BLE001 - the file's own read task fails
                manifest = self._unreadable_file_manifest(file_info, e)
            else:
                manifest = self._manifest_for_file(
                    file_info, listed, excluded, selection
                )
            if manifest is not None:
                yield manifest

    def _read_summaries(
        self, file_infos: Iterable[FileInfo], filesystem: "FileSystem"
    ) -> Iterator[Tuple[FileInfo, "Future[Optional[memoryview]]"]]:
        """Fetch summary sections concurrently, yielding them in listing order.

        Listing order decides how the partitioner groups chunks into tasks, so
        it must not depend on which read finishes first.
        """
        return read_summaries(
            filesystem,
            file_infos,
            lambda info: info.path,
            self._read_task_bytes,
            self._io_concurrency,
        )

    def _record_manifest(
        self,
        path: str,
        size: int,
        listed: Optional[ListedSummary],
        excluded_read_unit_ids: AbstractSet[str],
        selection: MCAPSelection,
    ) -> Optional[FileManifest]:
        """Listing rows for a file's Attachment or Metadata records, one each.

        The summary indexes both kinds by byte offset. A row weighs
        ``_RECORD_WORKING_SET_RATIO`` times its record's bytes, the file's first
        row also ``_record_open_bytes``, and carries its record's index record,
        which gives the record's length. Excluded records are skipped, and so
        are attachments whose ``log_time`` is outside ``time_range``. A file
        that holds no records of the kind yields
        nothing, and one whose records are not indexed is one whole-file row
        that the reader scans (``listed_records``).
        """
        kind = self._record_kind
        assert kind is not None
        summary = listed.summary if listed is not None else None
        indexes = listed_records(summary, kind)
        if indexes is None:
            return _whole_file_manifest(path, size, listed)
        runs = []
        for index in indexes:
            if kind.timed and not selection.overlaps(index.log_time, index.log_time):
                continue
            if kind.unit_id(path, index.offset) in excluded_read_unit_ids:
                continue
            weight = _RECORD_WORKING_SET_RATIO * kind.size_bytes(index)
            if not runs:
                weight += self._record_open_bytes
            runs.append(record_run(index.offset, weight))
        if not runs:
            return None
        manifest = FileManifest.construct_manifest(
            paths=[path] * len(runs),
            sizes=[size] * len(runs),
            chunk_metadatas=[run.to_metadata() for run in runs],
        )
        assert listed is not None
        records = split_section(listed.section)
        return with_listing_records(
            manifest, b"", [records.record_indexes[run.unit_ids[0]] for run in runs]
        )

    def _unreadable_file_manifest(
        self, file_info: FileInfo, error: Exception
    ) -> FileManifest:
        """The one row of a file whose summary cannot be read (see module doc)."""
        logger.warning(
            "Cannot read the summary of MCAP file %s, so it is read whole in a "
            "read task of its own: %s",
            file_info.path,
            error,
        )
        assert file_info.size is not None
        whole_file = FileChunk(
            unit_ids=(),
            num_rows=0,
            size_bytes=self._read_task_bytes,
            fully_matched=False,
        )
        manifest = FileManifest.construct_manifest(
            paths=[file_info.path],
            sizes=[file_info.size],
            chunk_metadatas=[whole_file.to_metadata()],
        )
        # No records: the read task reads the summary itself and fails on it.
        return with_listing_records(manifest, None, [None])

    def _manifest_for_file(
        self,
        file_info: FileInfo,
        listed: Optional[ListedSummary],
        excluded_read_unit_ids: AbstractSet[str],
        selection: MCAPSelection,
    ) -> Optional[FileManifest]:
        """The listing rows of one file, or ``None`` when it cannot match.

        One row per chunk that may hold a selected message, carrying the chunk's
        byte offset as its unit id and its estimated read memory as its weight. A
        file whose summary is missing, has no chunk index or lists no channels
        is one whole-file row, since nothing can be pruned or split.
        """
        path = file_info.path
        assert file_info.size is not None
        summary = listed.summary if listed is not None else None

        if self._record_kind is not None:
            return self._record_manifest(
                path, file_info.size, listed, excluded_read_unit_ids, selection
            )
        if summary is None or not summary.chunk_indexes or not summary.channels:
            # Without a summary, a chunk index or channel records, nothing says
            # which chunk holds what.
            return _whole_file_manifest(path, file_info.size, listed)

        selected = selection.selected_channel_ids(summary.channels, summary.schemas)
        if not selection.file_may_match(
            summary.statistics, selected, has_channels=bool(summary.channels)
        ):
            logger.debug("Skipping %s: no chunk can match the selection", path)
            return None

        chunk_indexes = [
            chunk_index
            for chunk_index in summary.chunk_indexes
            if selection.chunk_may_match(chunk_index, selected)
            and chunk_unit_id(path, chunk_index.chunk_start_offset)
            not in excluded_read_unit_ids
        ]
        if not chunk_indexes:
            return None
        row_bytes = self._row_layout.row_bytes(
            path, (summary.channels[c] for c in selected), summary.schemas
        )
        runs = chunk_runs(path, summary, chunk_indexes, row_bytes, self._read_memory)
        manifest = FileManifest.construct_manifest(
            paths=[path] * len(runs),
            sizes=[file_info.size] * len(runs),
            chunk_metadatas=[run.to_metadata() for run in runs],
        )
        assert listed is not None
        records = split_section(listed.section)
        return with_listing_records(
            manifest,
            records.declarations,
            [records.chunk_indexes[run.unit_ids[0]] for run in runs],
        )


def _whole_file_manifest(
    path: str, size: int, listed: Optional[ListedSummary]
) -> FileManifest:
    """The one row of a file read whole, whose read task gets the whole summary."""
    manifest = FileManifest.construct_manifest(
        paths=[path], sizes=[size], chunk_metadatas=[None]
    )
    records = b"" if listed is None else bytes(listed.section)
    return with_listing_records(manifest, records, [None])
