"""The MCAP file listing: each file split into chunk-sized listing rows.

``MCAPSummaryIndexer`` walks the paths like ``NonSamplingFileIndexer``, then
reads each file's summary and emits one manifest row per chunk that can hold a
selected message. The partitioner (``OnlineBinPacker``) groups those rows into
read tasks by the memory each chunk is estimated to need (``chunk_run``). A
large recording is then read by many tasks, and a small one shares a task with
its neighbours. A file whose summary shows it cannot match the selection is
dropped before any payload is read.

A file whose summary is missing, has no chunk index or lists no channels is
listed as one whole-file row.

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
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    _DEFAULT_SUMMARY_IO_CONCURRENCY,
    ListedSummary,
    chunk_run,
    chunk_unit_id,
    read_listed_summaries,
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
        # ``predicate``, ``limit`` and ``projected_columns`` are not used here:
        # the scanner applies the limit per task, and the selection this indexer
        # prunes on is fixed at construction.
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
                manifest = self._manifest_for_file(file_info, listed, excluded)
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

        selected = self._selection.selected_channel_ids(
            summary.channels, summary.schemas
        )
        if not self._selection.file_may_match(
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
            if self._selection.chunk_may_match(chunk_index, selected)
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
