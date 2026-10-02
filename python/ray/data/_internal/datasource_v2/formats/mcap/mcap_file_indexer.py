"""Lists MCAP files and splits each into listing rows for its granularity.

``MCAPSummaryIndexer`` walks the paths like ``NonSamplingFileIndexer``, then
reads each file's summary and emits listing rows:

- at ``message`` and ``window`` granularity, one row per chunk that can hold a
  selected message, which the partitioner (``OnlineBinPacker``) groups into
  read tasks by uncompressed bytes, so a large recording is read by many tasks
  and a small one shares a task with its neighbours;
- at ``topic`` granularity, one listing block per (file, topic) naming the
  chunks that hold the topic, each block becoming one read task;
- at ``file`` granularity, one whole-file block per file.

Files whose summary shows they cannot match the selection are dropped before
any payload is read. A file without a summary, or without a chunk index, is
listed as one whole-file row; the reader scans it linearly.
"""

import logging
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Deque,
    Dict,
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
    chunk_run,
    chunk_unit_id,
    read_summary,
    topic_run_metadata,
    topic_unit_id,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Summaries read concurrently within one listing task. A summary read is two
# small ranged requests, so it is latency-bound on remote storage.
_DEFAULT_SUMMARY_IO_CONCURRENCY = env_integer(
    "RAY_DATA_MCAP_SUMMARY_IO_CONCURRENCY", 16
)


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
        # ``predicate`` is the scanner's pushed predicate: the ``topic`` and
        # ``log_time`` conjuncts it folded into its selection. Fold them into
        # the same base selection here so listing prunes exactly what the
        # reader will skip. ``limit`` and ``projected_columns`` are not used:
        # the scanner applies the limit per task.
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
        it must not depend on which summary happened to land first. A bounded
        window of in-flight reads keeps memory flat for a long listing.
        """
        from ray.data.context import DataContext

        retried_io_errors = DataContext.get_current().retried_io_errors

        def read(file_info: FileInfo) -> Optional["Summary"]:
            return call_with_retry(
                lambda: read_summary(filesystem, file_info.path),
                description=f"read MCAP summary for {file_info.path}",
                match=retried_io_errors,
            )

        window = max(1, self._io_concurrency * 2)
        pending: Deque[Tuple[FileInfo, Future]] = deque()
        with ThreadPoolExecutor(max_workers=max(1, self._io_concurrency)) as pool:
            for file_info in file_infos:
                pending.append((file_info, pool.submit(read, file_info)))
                if len(pending) >= window:
                    done_info, future = pending.popleft()
                    yield done_info, future.result()
            while pending:
                done_info, future = pending.popleft()
                yield done_info, future.result()

    def _manifests_for_file(
        self,
        file_info: FileInfo,
        summary: Optional["Summary"],
        excluded_read_unit_ids: AbstractSet[str],
        selection: Optional[MCAPSelection] = None,
    ) -> Iterator[FileManifest]:
        """The listing blocks of one file; nothing when it cannot match.

        A file with no summary or no chunk index is one whole-file row at every
        granularity: nothing is known about it, so nothing can be pruned or
        split, and the reader discovers its topics itself.
        """
        path = file_info.path
        assert file_info.size is not None
        size: int = file_info.size

        def whole_file() -> FileManifest:
            return FileManifest.construct_manifest(
                paths=[path], sizes=[size], chunk_metadatas=[None]
            )

        if summary is None or not summary.chunk_indexes:
            yield whole_file()
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
            yield whole_file()
            return

        if self._granularity == TOPIC_GRANULARITY:
            yield from self._topic_manifests(
                file_info, summary, selected, excluded_read_unit_ids, selection
            )
            return

        # ``message`` and ``window``: one row per chunk that may hold a selected
        # message, carrying the chunk's byte offset as its unit id and its
        # uncompressed size as its weight.
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
                sizes=[size] * len(runs),
                chunk_metadatas=[run.to_metadata() for run in runs],
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
