from __future__ import annotations

import logging
from collections import deque
from typing import TYPE_CHECKING, AbstractSet, Deque, Iterable, Iterator, List, Optional

import numpy as np

import ray
from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
    ChunkedFile,
    FooterReaderActor,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.actor import ActorProxy
    from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
        FooterReader,
    )
    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
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
# ``ChunkedFile`` per streamed result. The driver pays one object-store fetch per
# yielded list, so a large directory costs one fetch per file at ``1``. Raising
# it trades a little latency-to-first-chunk for far fewer driver-side fetches.
_DEFAULT_RESULT_BATCH_SIZE = env_integer("RAY_DATA_PARQUET_FOOTER_RESULT_BATCH_SIZE", 1)
# Max in-flight footer batches. ``None`` -> auto (``num_actors * 2``). A smaller
# window reads fewer footers before an early ``limit`` stop cancels the pool, at
# the cost of less pipelining on full reads.
_DEFAULT_MAX_INFLIGHT_BATCHES: Optional[int] = env_integer(
    "RAY_DATA_PARQUET_FOOTER_MAX_INFLIGHT_BATCHES", None
)
# Bin sizing now belongs to ``OnlineBinPacker``, which the datasource supplies
# as a ``FilePartitioner``; see ``ParquetDatasourceV2.get_file_partitioner``.


def _chunked_file_to_manifest(chunked_file: ChunkedFile) -> FileManifest:
    """One listing row per row-group run of a file.

    The row is the run's ``FileChunk`` with its exact footer stats (``size_bytes``
    is the projection-scoped uncompressed size), so a downstream partitioner
    can group runs into read tasks -- and split them at row-group boundaries --
    without re-reading the footer. Grouping is deliberately *not* done here:
    listing discovers, the partitioner groups.
    """
    n = len(chunked_file.row_groups)
    assert chunked_file.file.size is not None
    return FileManifest.construct_manifest(
        paths=[chunked_file.file.path] * n,
        sizes=[chunked_file.file.size] * n,
        chunk_metadatas=[run.to_metadata() for run in chunked_file.row_groups],
    )


def _shuffle_row_group_runs(
    manifests: Iterable[FileManifest],
    *,
    seed: Optional[int],
    max_rows_per_output: int,
) -> Iterator[FileManifest]:
    """Permute every row-group run across all files, then re-batch.

    Backs ``FileShuffleConfig(small_chunks_shuffle=True)``. It drains the whole
    listing because a global permutation needs every row. When ``seed`` is set,
    rows are first sorted by ``(path, first row group)`` so the permutation does
    not depend on the order footer reads finished in.

    Rows are rebuilt from Python values rather than concatenated as Arrow
    tables: per-file manifests infer their own chunk-metadata types (an empty
    ``unit_sizes`` is ``list<null>``, a coalesced one ``list<int64>``), so their
    schemas need not match.
    """
    rows = [row for manifest in manifests for row in manifest.as_block().to_pylist()]
    n = len(rows)
    if n == 0:
        return
    if seed is not None:
        rows.sort(
            key=lambda row: (
                row[PATH_COLUMN_NAME],
                row[FILE_CHUNK_METADATA_COLUMN_NAME]["unit_ids"][0],
            )
        )
    rows = [rows[i] for i in np.random.default_rng(seed).permutation(n)]
    for start in range(0, n, max_rows_per_output):
        batch = rows[start : start + max_rows_per_output]
        yield FileManifest.construct_manifest(
            paths=[row[PATH_COLUMN_NAME] for row in batch],
            sizes=[row[FILE_SIZE_COLUMN_NAME] for row in batch],
            chunk_metadatas=[row[FILE_CHUNK_METADATA_COLUMN_NAME] for row in batch],
        )


class FooterFileIndexer(NonSamplingFileIndexer):
    """Lists files, then footer-reads them to emit one row per row-group run.

    Inherits directory traversal and ``list_file_infos`` from
    :class:`NonSamplingFileIndexer`; overrides :meth:`list_files` to attach exact
    footer stats to each run instead of emitting opaque per-file chunks. File
    shuffle, when requested, runs after path discovery and before footer reads.
    Row-group shuffle (``FileShuffleConfig.small_chunks_shuffle``) coalesces row
    groups into ~``small_shuffle_chunk_size`` runs and then permutes those runs
    across all files.
    Grouping runs into read units is the partitioner's job -- see
    :class:`~ray.data._internal.datasource_v2.common.online_bin_packer.OnlineBinPacker`.
    """

    def __init__(
        self,
        *,
        ignore_missing_paths: bool,
        skip_paths: Optional[Iterable[str]] = None,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
        coalesce_bytes: int = 0,
        small_shuffle_chunk_size: int = 0,
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
        # Coalesce target used instead of ``coalesce_bytes`` (when larger) for a
        # row-group shuffle, so the shuffle unit is a ~chunk-sized run rather
        # than a single, possibly tiny, row group.
        self._small_shuffle_chunk_size = small_shuffle_chunk_size
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
        # In-flight footer-batch window; ``None`` -> auto (``num_actors * 2``).
        _inflight = (
            max_inflight_batches
            if max_inflight_batches is not None
            else env_integer(
                "RAY_DATA_PARQUET_FOOTER_MAX_INFLIGHT_BATCHES",
                _DEFAULT_MAX_INFLIGHT_BATCHES,
            )
        )
        self._max_inflight_batches = (
            _inflight if _inflight is not None else self._num_actors * 2
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
        shuffle_row_groups = (
            shuffle_config is not None and shuffle_config.small_chunks_shuffle
        )
        coalesce_bytes = self._coalesce_bytes
        if shuffle_row_groups:
            coalesce_bytes = max(coalesce_bytes, self._small_shuffle_chunk_size)
        file_infos = self._iter_file_infos_for_list(
            paths,
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
            shuffle_config=shuffle_config,
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        )
        # Whole files were dropped above; row-group ids are applied by the
        # footer reader once it knows each file's row groups.
        actors: List[ActorProxy[FooterReader]] = [
            FooterReaderActor.options(scheduling_strategy="SPREAD").remote(
                filesystem,
                self._io_concurrency,
                predicate,
                projected_columns,
                coalesce_bytes,
                excluded_read_unit_ids=excluded_read_unit_ids,
            )
            for _ in range(self._num_actors)
        ]
        logger.debug(
            "Provisioned %d FooterReader actors (io_concurrency=%d)",
            self._num_actors,
            self._io_concurrency,
        )
        try:
            manifests = self._read_footers(
                actors, file_infos, limit, preserve_order=preserve_order
            )
            if shuffle_row_groups:
                assert shuffle_config is not None
                manifests = _shuffle_row_group_runs(
                    manifests,
                    seed=shuffle_config.get_seed(execution_idx),
                    max_rows_per_output=self._max_paths_per_output,
                )
            yield from manifests
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
        *,
        preserve_order: bool = False,
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
                preserve_order=preserve_order,  # pyrefly: ignore[unexpected-keyword]
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
                for chunked_file in ray.get(ref):
                    yield _chunked_file_to_manifest(chunked_file)
                    if limit is not None:
                        # Count only fully-matched (exact-survivor) rows so
                        # stopping can never under-deliver under a filter.
                        delivered_fully_matched_rows += sum(
                            rg.num_rows
                            for rg in chunked_file.row_groups
                            if rg.fully_matched
                        )
                if limit is not None and delivered_fully_matched_rows >= limit:
                    # Stop listing; abandon in-flight generators (the actor
                    # teardown cancels them).
                    return
            # This generator drained; keep the window full.
            dispatch_next()

    def _batches(self, file_infos: "Iterable[FileInfo]") -> Iterator[List[FileInfo]]:
        batch: List[FileInfo] = []
        for file_info in file_infos:
            if file_info.size is None:
                continue
            batch.append(file_info)
            if len(batch) >= self._footer_batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
