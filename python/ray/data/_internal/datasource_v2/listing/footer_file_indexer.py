from __future__ import annotations

import logging
from collections import deque
from typing import TYPE_CHECKING, Deque, Iterable, Iterator, List, Optional, Tuple

import ray
from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ParquetRowGroupChunkMetadata,
    create_chunk_metadata,
)
from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import FileChunks
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import (
    FileManifest,
)
from ray.data._internal.datasource_v2.listing.footer_reader import FooterReaderActor

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.actor import ActorProxy
    from ray.data._internal.datasource_v2.listing.file_indexer import FileInfo
    from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
    from ray.data._internal.datasource_v2.listing.footer_reader import FooterReader
    from ray.data.block import BlockColumn
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# A pool of footer-reading actors spread across the cluster, provisioned once
# per ``list_files`` call. Footer reads are network-bound, so several actors each
# driving many concurrent reads keeps IO from bottlenecking on a single node.
_DEFAULT_NUM_ACTORS = env_integer("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", 32)
_DEFAULT_IO_CONCURRENCY = env_integer("RAY_DATA_PARQUET_FOOTER_IO_CONCURRENCY", 128)
# Files per ``read_footers`` call. Small footers -> batch several per task to
# amortize the per-task and per-result object-store overhead.
_DEFAULT_BATCH_SIZE = env_integer("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", 10)
# ``FileChunks`` per streamed result. The driver pays one object-store fetch per
# yielded list, so a large directory costs one fetch per file at ``1``. Raising
# it trades a little latency-to-first-chunk for far fewer driver-side fetches.
_DEFAULT_RESULT_BATCH_SIZE = env_integer("RAY_DATA_PARQUET_FOOTER_RESULT_BATCH_SIZE", 1)
# Max in-flight footer batches. ``0`` -> auto (``num_actors * 2``). A smaller
# window reads fewer footers before an early ``limit`` stop cancels the pool, at
# the cost of less pipelining on full reads.
_DEFAULT_MAX_INFLIGHT_BATCHES = env_integer(
    "RAY_DATA_PARQUET_FOOTER_MAX_INFLIGHT_BATCHES", 0
)
# Bin sizing now belongs to ``OnlineBinPacker``, which the datasource supplies
# as a ``FilePartitioner``; see ``ParquetDatasourceV2.get_file_partitioner``.


def _manifest_of_runs(file_chunks: FileChunks) -> FileManifest:
    """One listing row per row-group run of a file.

    The row carries the run's exact footer stats, so a downstream partitioner
    can group runs into read units -- and split them at row-group boundaries --
    without re-reading the footer. Grouping is deliberately *not* done here:
    listing discovers, the partitioner groups.
    """
    n = len(file_chunks.row_groups)
    return FileManifest.construct_manifest(
        [file_chunks.path] * n,
        [file_chunks.size] * n,
        [
            create_chunk_metadata(
                ParquetRowGroupChunkMetadata,
                row_group_ids=tuple(range(rg.rg_idx, rg.rg_idx + rg.rg_count)),
                num_rows=rg.num_rows,
                uncompressed_size=rg.uncompressed_size,
                fully_matched=rg.fully_matched,
                rg_sizes=rg.rg_sizes,
                rg_rows=rg.rg_rows,
            )
            for rg in file_chunks.row_groups
        ],
    )


class FooterFileIndexer(NonSamplingFileIndexer):
    """Lists files, then footer-reads them to emit one row per row-group run.

    Inherits directory traversal and ``list_file_infos`` from
    :class:`NonSamplingFileIndexer`; overrides :meth:`list_files` to attach exact
    footer stats to each run instead of emitting opaque per-file chunks. Grouping
    runs into read units is the partitioner's job -- see
    :class:`~ray.data._internal.datasource_v2.partitioners.online_bin_packer.OnlineBinPacker`.
    """

    def __init__(
        self,
        *,
        ignore_missing_paths: bool,
        skip_paths: Optional[Iterable[str]] = None,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
        coalesce_bytes: int = 0,
        io_concurrency: Optional[int] = None,
        footer_batch_size: Optional[int] = None,
        result_batch_size: Optional[int] = None,
        max_inflight_batches: Optional[int] = None,
    ):
        super().__init__(
            ignore_missing_paths=ignore_missing_paths,
            skip_paths=skip_paths,
            num_workers=num_workers,
            max_paths_per_output=max_paths_per_output,
        )
        self._coalesce_bytes = coalesce_bytes
        # Every knob is re-read here rather than taken from the module-level
        # constant, so ``monkeypatch.setenv`` and release-test env overrides work
        # after this module has already been imported. An explicit ctor argument
        # still wins over the environment.
        self._num_actors = env_integer(
            "RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", _DEFAULT_NUM_ACTORS
        )
        self._io_concurrency = (
            io_concurrency
            if io_concurrency is not None
            else env_integer(
                "RAY_DATA_PARQUET_FOOTER_IO_CONCURRENCY", _DEFAULT_IO_CONCURRENCY
            )
        )
        self._footer_batch_size = (
            footer_batch_size
            if footer_batch_size is not None
            else env_integer("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", _DEFAULT_BATCH_SIZE)
        )
        self._result_batch_size = (
            result_batch_size
            if result_batch_size is not None
            else env_integer(
                "RAY_DATA_PARQUET_FOOTER_RESULT_BATCH_SIZE",
                _DEFAULT_RESULT_BATCH_SIZE,
            )
        )
        # In-flight footer-batch window; 0/None -> auto (``num_actors * 2``).
        _inflight = (
            max_inflight_batches
            if max_inflight_batches is not None
            else env_integer(
                "RAY_DATA_PARQUET_FOOTER_MAX_INFLIGHT_BATCHES",
                _DEFAULT_MAX_INFLIGHT_BATCHES,
            )
        )
        self._max_inflight_batches = _inflight if _inflight else self._num_actors * 2

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
    ) -> Iterable[FileManifest]:
        file_infos = self.list_file_infos(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
        )
        actors: List[ActorProxy[FooterReader]] = [
            FooterReaderActor.options(scheduling_strategy="SPREAD").remote(
                filesystem,
                self._io_concurrency,
                predicate,
                projected_columns,
                self._coalesce_bytes,
            )
            for _ in range(self._num_actors)
        ]
        logger.debug(
            "Provisioned %d FooterReader actors (io_concurrency=%d)",
            self._num_actors,
            self._io_concurrency,
        )
        try:
            yield from self._read_footers(actors, file_infos, limit)
        finally:
            for actor in actors:
                # ``ActorProxy`` is ``ActorHandle | type[T]``; kill wants a handle.
                # pyrefly: ignore[bad-argument-type]
                ray.kill(actor)

    def _read_footers(
        self,
        actors: List["ActorProxy[FooterReader]"],
        file_infos: "Iterable[FileInfo]",
        limit: Optional[int],
    ) -> Iterator[FileManifest]:
        # Bound the number of in-flight footer batches so listing stays roughly
        # demand-driven (matters under a limit) and memory stays flat.
        window = max(1, self._max_inflight_batches)
        batches = self._batches(file_infos)
        # FIFO of in-flight streaming generators, one per dispatched footer batch.
        pending: Deque[ray.ObjectRefGenerator] = deque()
        batch_no = 0
        delivered_fully_matched_rows = 0

        def dispatch_next() -> bool:
            nonlocal batch_no
            batch = next(batches, None)
            if batch is None:
                return False
            actor: ActorProxy[FooterReader] = actors[batch_no % len(actors)]
            # Streaming ``@ray.method``; stub types ``.remote`` as ``ObjectRef``
            # and don't preserve the method's keyword args.
            # pyrefly: ignore[bad-assignment]
            gen: ray.ObjectRefGenerator = actor.read_footers.remote(
                batch,
                result_batch_size=self._result_batch_size,  # pyrefly: ignore[unexpected-keyword]
            )
            pending.append(gen)
            batch_no += 1
            return True

        # Prime the window.
        for _ in range(window):
            if not dispatch_next():
                break

        while pending:
            gen = pending.popleft()
            for ref in gen:  # blocks until this generator's next result lands
                for file_chunks in ray.get(ref):
                    yield _manifest_of_runs(file_chunks)
                    if limit is not None:
                        # Count only fully-matched (exact-survivor) rows so
                        # stopping can never under-deliver under a filter.
                        delivered_fully_matched_rows += sum(
                            rg.num_rows
                            for rg in file_chunks.row_groups
                            if rg.fully_matched
                        )
                if limit is not None and delivered_fully_matched_rows >= limit:
                    # Stop listing; abandon in-flight generators (the actor
                    # teardown cancels them).
                    return
            # This generator drained; keep the window full.
            dispatch_next()

    def _batches(
        self, file_infos: "Iterable[FileInfo]"
    ) -> Iterator[List[Tuple[str, int]]]:
        batch: List[Tuple[str, int]] = []
        for file_info in file_infos:
            if file_info.size is None:
                continue
            batch.append((file_info.path, file_info.size))
            if len(batch) >= self._footer_batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
