"""The MCAP file listing: each file split into chunk-sized listing rows.

``MCAPSummaryIndexer`` walks the paths like ``NonSamplingFileIndexer``, then
reads each file's summary and emits one manifest row per chunk that can hold a
selected message. The partitioner groups those rows into read tasks by the
memory each chunk is estimated to need (``chunk_run``). A large recording is
then read by many tasks, and a small one shares a task with its neighbours
(``mcap_file_cuts``). A file whose summary shows it cannot match the selection
is dropped before any payload is read.

At ``attachment`` and ``metadata`` granularity there is one row per indexed
record of that kind instead, weighted by its bytes plus the requests that
reading it takes, so a read task takes at most 16 small records
(``_RECORDS_PER_READ_TASK``).

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
    ATTACHMENT_GRANULARITY,
    MESSAGE_GRANULARITY,
    RECORD_GRANULARITIES,
    MCAPSelection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_pushdown import (
    narrow_selection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    _DEFAULT_SUMMARY_IO_CONCURRENCY,
    ListedSummary,
    attachment_unit_id,
    chunk_run,
    chunk_unit_id,
    metadata_unit_id,
    read_listed_summaries,
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

    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# A read task takes at most this many small records. Reading a record also
# opens its file, a request before the read, so each record row also weighs
# this share of the task's budget.
_RECORDS_PER_READ_TASK = env_integer("RAY_DATA_MCAP_RECORDS_PER_READ_TASK", 16)


class MCAPSummaryIndexer(NonSamplingFileIndexer):
    """Lists MCAP files and emits one listing row per chunk.

    Inherits directory traversal, extension and partition pruning, file shuffle
    and whole-file checkpoint exclusion from :class:`NonSamplingFileIndexer`.
    Overrides :meth:`list_files` to read each listed file's summary and turn it
    into chunk rows (``mcap_summary.chunk_run``). Grouping rows into read tasks
    is the partitioner's job.
    """

    def __init__(
        self,
        *,
        selection: MCAPSelection,
        ignore_missing_paths: bool,
        read_task_bytes: int,
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
        # file weighs (see the module docstring).
        self._read_task_bytes = read_task_bytes
        # The columns a row carries, which size each chunk's listing row.
        self._row_layout = row_layout
        # What one row is: messages, or attachment or metadata records.
        self._granularity = granularity
        # What reading a record weighs besides its bytes.
        self._record_read_bytes = read_task_bytes // max(1, _RECORDS_PER_READ_TASK)
        self._io_concurrency = (
            io_concurrency
            if io_concurrency is not None
            else env_integer(
                "RAY_DATA_MCAP_SUMMARY_IO_CONCURRENCY", _DEFAULT_SUMMARY_IO_CONCURRENCY
            )
        )

    def as_whole_file_indexer(self) -> NonSamplingFileIndexer:
        """A plain indexer for ``count()``, with one file per listing block.

        ``PushdownCountFiles`` reads each block's summaries in one task, so a file
        whose summary cannot be read fails only its own count, as in a read.
        """
        return NonSamplingFileIndexer(
            ignore_missing_paths=self._ignore_missing_paths,
            skip_paths=self._skip_paths,
            num_workers=self._num_workers,
            max_paths_per_output=1,
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
        for file_info, summary_read in self._read_summaries(file_infos, filesystem):
            try:
                listed = summary_read.result()
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
    ) -> Iterator[Tuple[FileInfo, "Future[Optional[ListedSummary]]"]]:
        """Start summary reads concurrently, yielding them in listing order.

        Listing order decides how the partitioner groups chunks into tasks, so
        it must not depend on which read finishes first.
        """
        return read_listed_summaries(
            filesystem, file_infos, lambda info: info.path, self._io_concurrency
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

        The summary indexes both kinds by byte offset. A row weighs its record's
        bytes plus ``_record_read_bytes``, and carries its record's index record,
        which gives the record's length. Excluded records are skipped, and so
        are attachments whose ``log_time`` is outside ``time_range``. A file
        whose statistics count zero records of the kind yields nothing. A file
        without a summary, or whose records were written without an index, is
        one whole-file row that the reader scans.
        """
        summary = listed.summary if listed is not None else None
        attachments = self._granularity == ATTACHMENT_GRANULARITY
        if summary is None or not (
            summary.attachment_indexes if attachments else summary.metadata_indexes
        ):
            if self._statistics_rule_out_records(summary):
                return None
            # The read task gets the whole summary, as for messages.
            manifest = FileManifest.construct_manifest(
                paths=[path], sizes=[size], chunk_metadatas=[None]
            )
            records = (
                b"" if listed is None else split_section(listed.section).everything
            )
            return with_listing_records(manifest, records, [None])
        runs = []
        if attachments:
            for attachment in summary.attachment_indexes:
                if not selection.overlaps(attachment.log_time, attachment.log_time):
                    continue
                if (
                    attachment_unit_id(path, attachment.offset)
                    in excluded_read_unit_ids
                ):
                    continue
                weight = attachment.data_size + self._record_read_bytes
                runs.append(record_run(attachment.offset, weight))
        else:
            for metadata in summary.metadata_indexes:
                if metadata_unit_id(path, metadata.offset) in excluded_read_unit_ids:
                    continue
                weight = metadata.length + self._record_read_bytes
                runs.append(record_run(metadata.offset, weight))
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

    def _statistics_rule_out_records(self, summary: Optional["Summary"]) -> bool:
        """Whether the statistics count zero records of the granularity's kind."""
        statistics = summary.statistics if summary is not None else None
        if statistics is None:
            return False
        if self._granularity == ATTACHMENT_GRANULARITY:
            return statistics.attachment_count == 0
        return statistics.metadata_count == 0

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

        if self._granularity in RECORD_GRANULARITIES:
            return self._record_manifest(
                path, file_info.size, listed, excluded_read_unit_ids, selection
            )
        if summary is None or not summary.chunk_indexes or not summary.channels:
            # Without a summary, a chunk index or channel records, nothing says
            # which chunk holds what. The read task gets the whole summary.
            manifest = FileManifest.construct_manifest(
                paths=[path], sizes=[file_info.size], chunk_metadatas=[None]
            )
            records = (
                b"" if listed is None else split_section(listed.section).everything
            )
            return with_listing_records(manifest, records, [None])

        selected = selection.selected_channel_ids(summary.channels, summary.schemas)
        if not selection.file_may_match(
            summary.statistics, selected, has_channels=bool(summary.channels)
        ):
            logger.debug("Skipping %s: no chunk can match the selection", path)
            return None

        total_uncompressed = sum(c.uncompressed_size for c in summary.chunk_indexes)
        row_bytes = self._row_layout.row_bytes(
            path, (summary.channels[c] for c in selected), summary.schemas
        )
        runs = [
            chunk_run(chunk_index, summary.statistics, total_uncompressed, row_bytes)
            for chunk_index in summary.chunk_indexes
            if selection.chunk_may_match(chunk_index, selected)
            and chunk_unit_id(path, chunk_index.chunk_start_offset)
            not in excluded_read_unit_ids
        ]
        if not runs:
            return None
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
