from typing import TYPE_CHECKING, Iterable, List, Optional

import pyarrow as pa

from ray.data._internal.datasource_v2.common.file_pruners import (
    FileExtensionPruner,
    PartitionPruner,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import FilePartitioner
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data._internal.datasource_v2.interfaces.read_units import (
    EXCLUDED_READ_UNIT_IDS_KWARG_NAME,
)
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import Block

DEFAULT_SCHEMA_FILE_INFO_BLOCK_SIZE = 1000


if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.datasource.partitioning import PathPartitionFilter
    from ray.data.expressions import Expr


def partition_files(
    blocks: Iterable[Block],
    _: TaskContext,
    partitioner: FilePartitioner,
) -> Iterable[Block]:
    for block in blocks:
        partitioner.add_input(FileManifest(block))
        while partitioner.has_partition():
            yield partitioner.next_partition().as_block()

    partitioner.finalize()
    while partitioner.has_partition():
        yield partitioner.next_partition().as_block()


def _build_pruners(
    file_extensions: Optional[List[str]],
    partition_filter: Optional["PathPartitionFilter"],
    partition_pruner: Optional[FilePruner] = None,
) -> List[FilePruner]:
    pruners: List[FilePruner] = []
    if file_extensions is not None:
        pruners.append(FileExtensionPruner(file_extensions))
    if partition_filter is not None:
        pruners.append(PartitionPruner(partition_filter))
    if partition_pruner is not None:
        # Stacks with, rather than replaces, any user ``partition_filter``.
        pruners.append(partition_pruner)
    return pruners


def list_files_for_each_block(
    blocks: Iterable[Block],
    ctx: TaskContext,
    *,
    indexer: "FileIndexer",
    filesystem: Optional["FileSystem"],
    file_extensions: Optional[List[str]] = None,
    partition_filter: Optional["PathPartitionFilter"] = None,
    partition_pruner: Optional[FilePruner] = None,
    preserve_order: bool = False,
    predicate: Optional["Expr"] = None,
    limit: Optional[int] = None,
    projected_columns: Optional[List[str]] = None,
    shuffle_config: Optional["FileShuffleConfig"] = None,
    execution_idx: int = 0,
    prelisted_file_infos: bool = False,
) -> Iterable[Block]:
    """Expand path blocks into ``FileManifest`` blocks.

    Root-path input blocks carry a ``__path`` column. Prelisted blocks carry
    ``__path`` and ``__file_size`` from planning-time discovery. The indexer
    turns either input into ``FileManifest`` blocks.
    For root paths, construct the initial ``file_extensions`` and
    ``partition_filter`` pruners once per task. Prelisted files already passed
    those filters; only the later optimizer-derived partition pruner runs.

    Pushed-down ``predicate`` / ``limit`` / ``projected_columns`` are forwarded
    to the indexer; metadata-aware indexers (e.g. the footer-based Parquet
    indexer) use them to skip row groups, stop early, and size projected
    columns, while the plain indexer ignores them.

    ``shuffle_config`` is also forwarded: the indexer shuffles listed files
    after path discovery and before metadata fetch. Listing still runs as a
    single task when shuffle is requested so the indexer sees the full file
    set.

    ``ctx.kwargs[EXCLUDED_READ_UNIT_IDS_KWARG_NAME]``, when a resumed job sets
    it on the ``ListFiles`` operator, is the set of read unit ids already
    finished; the indexer leaves them out, so the partitioner packs only the
    remaining work.
    """
    # The excluded ids arrive through TaskContext only at execution time.
    # Both root-path listing and the prelisted route must honor them.
    excluded_ids = ctx.kwargs.get(EXCLUDED_READ_UNIT_IDS_KWARG_NAME)
    excluded_read_unit_ids = frozenset(excluded_ids) if excluded_ids else None
    if prelisted_file_infos:
        # Initial path filters already ran during schema inference. Reapplying a
        # user callback here could change the candidate set. Only apply the
        # partition pruner derived later from the optimized scanner.
        def _file_infos() -> Iterable[FileInfo]:
            for block in blocks:
                assert isinstance(block, pa.Table)
                for path, size in zip(
                    block[PATH_COLUMN_NAME].to_pylist(),
                    block[FILE_SIZE_COLUMN_NAME].to_pylist(),
                ):
                    if partition_pruner is None or partition_pruner.should_include(
                        path
                    ):
                        yield FileInfo(path=path, size=size)

        for manifest in indexer.list_files_from_file_infos(
            _file_infos(),
            filesystem=filesystem,
            preserve_order=preserve_order,
            predicate=predicate,
            limit=limit,
            projected_columns=projected_columns,
            shuffle_config=shuffle_config,
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        ):
            if len(manifest) > 0:
                yield manifest.as_block()
        return

    pruners = _build_pruners(file_extensions, partition_filter, partition_pruner)
    for block in blocks:
        for manifest in indexer.list_files(
            block[PATH_COLUMN_NAME],
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
            predicate=predicate,
            limit=limit,
            projected_columns=projected_columns,
            shuffle_config=shuffle_config,
            execution_idx=execution_idx,
            excluded_read_unit_ids=excluded_read_unit_ids,
        ):
            if len(manifest) > 0:
                yield manifest.as_block()


def sample_files(
    indexer: "FileIndexer",
    paths: List[str],
    filesystem: Optional["FileSystem"],
    pruners: Optional[List[FilePruner]] = None,
    max_files: Optional[int] = 16,
) -> FileManifest:
    """List up to ``max_files`` files, or all files when it is ``None``.

    Used for driver-side schema inference in ``_read_datasource_v2``. Sampling
    more than one file lets callers unify schemas (e.g., if the first file has an
    all-null column, later files' non-null types can promote it). In full mode,
    the paths and sizes are retained as ``ListFiles`` input so execution does
    not discover a wider file set.

    Uses ``list_file_infos`` (raw path + size), not ``list_files``, so that
    metadata-heavy indexers (e.g. the footer-based Parquet indexer) don't do
    their footer-read + bin-pack work on the driver just to sample a schema.
    Full mode also retains file sizes for execution.
    """
    assert max_files is None or max_files >= 1
    paths_column = pa.array(paths, type=pa.string())
    sampled_paths: List[str] = []
    sampled_sizes: List[int] = []
    for file_info in indexer.list_file_infos(
        paths_column,
        filesystem=filesystem,
        pruners=pruners or [],
        preserve_order=True,
    ):
        if file_info.size is None:
            continue
        sampled_paths.append(file_info.path)
        sampled_sizes.append(file_info.size)
        if max_files is not None and len(sampled_paths) >= max_files:
            break
    return FileManifest.construct_manifest(
        paths=sampled_paths,
        sizes=sampled_sizes,
        chunk_metadatas=[None] * len(sampled_paths),
    )
