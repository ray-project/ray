"""The MCAP file listing: each file split into listing rows for its granularity.

``MCAPSummaryIndexer`` walks the paths like ``NonSamplingFileIndexer``, then
reads each file's summary and emits listing rows:

- ``message`` and ``window``: one row per chunk that can hold a selected
  message. The partitioner (``OnlineBinPacker``) groups these rows into read
  tasks by uncompressed bytes. A large recording is then read by many tasks,
  and a small one shares a task with its neighbours.
- ``topic``: one listing block per (file, topic), naming the chunks that hold
  the topic. Each block is one read task.
- ``file``: one whole-file block per file.

A file whose summary shows it cannot match the selection is dropped before any
payload is read. A file whose summary is missing, has no chunk index or lists
no channels is listed as one whole-file row.
"""

import logging
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    FILE_GRANULARITY,
    MESSAGE_GRANULARITY,
    TOPIC_GRANULARITY,
    MCAPSelection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_pushdown import (
    narrow_selection,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    _DEFAULT_SUMMARY_IO_CONCURRENCY,
    chunk_run,
    chunk_unit_id,
    read_summaries,
    topic_run_metadata,
    topic_unit_id,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest

if TYPE_CHECKING:
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


class MCAPSummaryIndexer(NonSamplingFileIndexer):
    """Lists MCAP files and emits listing rows from their summaries.

    Inherits directory traversal, extension and partition pruning, file shuffle
    and whole-file checkpoint exclusion from :class:`NonSamplingFileIndexer`.
    Overrides :meth:`list_files` to read each listed file's summary and turn it
    into the rows its granularity calls for. Grouping chunk rows into read tasks
    is the partitioner's job.
    """

    def __init__(
        self,
        *,
        selection: MCAPSelection,
        ignore_missing_paths: bool,
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
        self._granularity = granularity
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
        for file_info, summary in self._read_summaries(file_infos, filesystem):
            yield from self._manifests_for_file(file_info, summary, excluded, selection)

    def _read_summaries(
        self, file_infos: Iterable[FileInfo], filesystem: "FileSystem"
    ) -> Iterator[Tuple[FileInfo, Optional["Summary"]]]:
        """Read summaries concurrently, yielding them in listing order.

        Listing order decides how the partitioner groups chunks into tasks, so
        it must not depend on which read finishes first.
        """
        for file_info, summary_read in read_summaries(
            filesystem, file_infos, lambda info: info.path, self._io_concurrency
        ):
            yield file_info, summary_read.result()

    def _manifests_for_file(
        self,
        file_info: FileInfo,
        summary: Optional["Summary"],
        excluded_read_unit_ids: AbstractSet[str],
        selection: Optional[MCAPSelection] = None,
    ) -> Iterator[FileManifest]:
        """The listing blocks of one file; nothing when it cannot match.

        A file whose summary is missing, has no chunk index or lists no channels
        is one whole-file row at every granularity, and the reader finds its
        topics itself.
        """
        path = file_info.path
        assert file_info.size is not None
        size: int = file_info.size

        if summary is None or not summary.chunk_indexes or not summary.channels:
            # Without a summary, a chunk index or channel records, nothing says
            # which chunk holds what.
            yield _whole_file_manifest(path, size)
            return

        if selection is None:
            selection = self._selection
        selected = selection.selected_channel_ids(summary.channels, summary.schemas)
        if not selection.file_may_match(
            summary.statistics, selected, has_channels=bool(summary.channels)
        ):
            logger.debug("Skipping %s: no chunk can match the selection", path)
            return

        if self._granularity == FILE_GRANULARITY:
            yield _whole_file_manifest(path, size)
            return

        if self._granularity == TOPIC_GRANULARITY:
            yield from self._topic_manifests(
                file_info, summary, selected, excluded_read_unit_ids, selection
            )
            return

        yield from self._chunk_manifests(
            file_info, summary, selected, excluded_read_unit_ids, selection
        )

    def _topic_manifests(
        self,
        file_info: FileInfo,
        summary: "Summary",
        selected: AbstractSet[int],
        excluded_read_unit_ids: AbstractSet[str],
        selection: MCAPSelection,
    ) -> Iterator[FileManifest]:
        """One listing block per selected topic of the file: one read task each."""
        path = file_info.path
        assert file_info.size is not None
        channels_by_topic: Dict[str, List[int]] = {}
        for channel_id in selected:
            channels_by_topic.setdefault(summary.channels[channel_id].topic, []).append(
                channel_id
            )
        counts = summary.statistics.channel_message_counts if summary.statistics else {}
        for topic in sorted(channels_by_topic):
            if topic_unit_id(path, topic) in excluded_read_unit_ids:
                continue
            topic_ids = set(channels_by_topic[topic])
            chunk_indexes = [
                c
                for c in summary.chunk_indexes
                if selection.chunk_may_match(c, topic_ids)
            ]
            if not chunk_indexes:
                continue
            yield FileManifest.construct_manifest(
                paths=[path],
                sizes=[file_info.size],
                chunk_metadatas=[
                    topic_run_metadata(
                        chunk_indexes,
                        topic,
                        num_rows=sum(counts.get(cid, 0) for cid in topic_ids),
                    )
                ],
            )

    def _chunk_manifests(
        self,
        file_info: FileInfo,
        summary: "Summary",
        selected: Set[int],
        excluded_read_unit_ids: AbstractSet[str],
        selection: MCAPSelection,
    ) -> Iterator[FileManifest]:
        """One listing block with a row per chunk that may hold a selected message.

        Used at ``message`` and ``window`` granularity. A row carries the
        chunk's byte offset as its unit id and its uncompressed size as its
        weight.
        """
        path = file_info.path
        assert file_info.size is not None
        total_uncompressed = sum(c.uncompressed_size for c in summary.chunk_indexes)
        runs = [
            chunk_run(chunk_index, summary.statistics, total_uncompressed)
            for chunk_index in summary.chunk_indexes
            if selection.chunk_may_match(chunk_index, selected)
            and chunk_unit_id(path, chunk_index.chunk_start_offset)
            not in excluded_read_unit_ids
        ]
        if runs:
            yield FileManifest.construct_manifest(
                paths=[path] * len(runs),
                sizes=[file_info.size] * len(runs),
                chunk_metadatas=[run.to_metadata() for run in runs],
            )


def _whole_file_manifest(path: str, size: int) -> FileManifest:
    """One listing row for the whole file, which the reader scans."""
    return FileManifest.construct_manifest(
        paths=[path], sizes=[size], chunk_metadatas=[None]
    )
