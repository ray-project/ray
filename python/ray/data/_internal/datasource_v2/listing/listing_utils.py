from typing import TYPE_CHECKING, Iterable, List, Optional

import pyarrow as pa

from ray.data._internal.datasource_v2.listing.file_manifest import (
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.listing.file_pruners import (
    FileExtensionPruner,
    FilePruner,
    PartitionPruner,
)
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.block import Block

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.datasource.partitioning import PathPartitionFilter


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
) -> List[FilePruner]:
    pruners: List[FilePruner] = []
    if file_extensions:
        pruners.append(FileExtensionPruner(file_extensions))
    if partition_filter is not None:
        pruners.append(PartitionPruner(partition_filter))
    return pruners


def list_files_for_each_block(
    blocks: Iterable[Block],
    _: TaskContext,
    *,
    indexer: "FileIndexer",
    filesystem: "FileSystem",
    file_extensions: Optional[List[str]] = None,
    partition_filter: Optional["PathPartitionFilter"] = None,
    preserve_order: bool = False,
) -> Iterable[Block]:
    """Expand path blocks into ``FileManifest`` blocks.

    Each input block carries a single ``__path`` column of path strings.
    For every path, the indexer is invoked to produce a stream of
    ``FileManifest`` objects; each manifest's backing block is yielded.
    Pruners are constructed once per task from ``file_extensions`` /
    ``partition_filter`` — keeps pruner construction out of the
    ``_read_datasource_v2`` entry point.
    """
    pruners = _build_pruners(file_extensions, partition_filter)
    for block in blocks:
        for manifest in indexer.list_files(
            block[PATH_COLUMN_NAME],
            filesystem=filesystem,
            pruners=pruners,
            preserve_order=preserve_order,
        ):
            if len(manifest) > 0:
                yield manifest.as_block()


def shuffle_files(
    blocks: Iterable[Block],
    _: TaskContext,
    *,
    shuffle_config: "FileShuffleConfig",
    execution_idx: int,
) -> Iterable[Block]:
    """Concatenate manifest blocks and shuffle rows with the seeded RNG.

    Runs in a single task (`plan_list_files_op` sets `should_parallelize=False`
    when shuffle is requested) so we have the full manifest before
    shuffling. Emits one merged manifest block. Determinism comes from
    ``FileManifest.shuffle`` which sorts by path before applying the
    permutation — this protects against non-deterministic upstream
    indexer yield order.
    """
    builder = DelegatingBlockBuilder()
    for block in blocks:
        if len(block) > 0:
            builder.add_block(block)

    combined = builder.build()
    if len(combined) == 0:
        return

    seed = shuffle_config.get_seed(execution_idx)
    yield FileManifest(combined).shuffle(seed).as_block()


def sample_first_file(
    indexer: "FileIndexer",
    paths: List[str],
    filesystem: "FileSystem",
    pruners: Optional[List[FilePruner]] = None,
    max_files: int = 16,
) -> FileManifest:
    """Drive the indexer until up to ``max_files`` files arrive; return them.

    Used for driver-side schema inference in ``_read_datasource_v2``.
    Sampling more than one file lets callers unify schemas (e.g., if the
    first file has an all-null column, later files' non-null types can
    promote it). No caching — the returned manifest is discarded after
    schema inference, and the ``ListFiles`` op lists the same paths
    again on workers at execution time.
    """
    assert max_files >= 1
    paths_column = pa.array(paths, type=pa.string())
    collected: List[FileManifest] = []
    collected_rows = 0
    for manifest in indexer.list_files(
        paths_column,
        filesystem=filesystem,
        pruners=pruners or [],
        preserve_order=True,
    ):
        if len(manifest) == 0:
            continue
        remaining = max_files - collected_rows
        if len(manifest) <= remaining:
            collected.append(manifest)
            collected_rows += len(manifest)
        else:
            collected.append(FileManifest(manifest.as_block().slice(0, remaining)))
            collected_rows = max_files
        if collected_rows >= max_files:
            break
    if not collected:
        raise ValueError(f"sample_first_file: no files found under {paths!r}")
    return FileManifest.concat(collected)
