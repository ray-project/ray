"""Physical planner for the V2 ``ListFiles`` source operator.

Emits ``FileManifest`` blocks by (a) sharding user-supplied paths into
parallel listing tasks, (b) invoking the configured ``FileIndexer``, and
optionally (c) globally shuffling + size-balanced bucketing before the
downstream ``ReadFiles`` physical op consumes them.

Checkpoint filtering is not attached here — it's wrapped around the
downstream ``ReadFiles`` physical op by
:func:`plan_read_files_op_with_checkpoint_filter`, matching V1's
dispatch pattern.
"""
from __future__ import annotations

import logging
from functools import partial
from typing import Iterator, List

import numpy as np
import pyarrow as pa

import ray
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import (
    PATH_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.listing.indexing_utils import _get_file_infos
from ray.data._internal.datasource_v2.listing.listing_utils import (
    list_files_for_each_block,
    partition_files,
    shuffle_files,
)
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data._internal.logical.operators import ListFiles
from ray.data._internal.util import make_async_gen
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)

# Cap on the number of parallel listing tasks. In practice most reads
# pass a single directory (one task); this matters when users hand in
# thousands of explicit paths.
DEFAULT_MAX_NUM_LIST_FILES_TASKS = 200


def plan_list_files_op(
    op: ListFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 0

    # NOTE: Avoid capturing ``op`` inside closures — only its field values.
    file_extensions = op.file_extensions
    partition_filter = op.partition_filter
    filesystem = op.filesystem
    indexer = op.file_indexer
    partitioner = op.file_partitioner

    shuffle_config = op.shuffle_config_factory()

    transform_fns: List[MapTransformFn] = [
        BlockMapTransformFn(
            partial(
                list_files_for_each_block,
                indexer=indexer,
                filesystem=filesystem,
                file_extensions=file_extensions,
                partition_filter=partition_filter,
                preserve_order=data_context.execution_options.preserve_order,
            ),
            # Disable block-shaping: produce manifest blocks as-is.
            disable_block_shaping=True,
        ),
    ]

    if shuffle_config is not None:
        transform_fns.append(
            BlockMapTransformFn(
                partial(
                    shuffle_files,
                    shuffle_config=shuffle_config,
                    execution_idx=data_context._execution_idx,
                ),
                disable_block_shaping=True,
            )
        )

    if partitioner is not None:
        transform_fns.append(
            BlockMapTransformFn(
                partial(partition_files, partitioner=partitioner),
                disable_block_shaping=True,
            )
        )

    map_transformer = MapTransformer(transform_fns)

    map_op = MapOperator.create(
        map_transformer,
        _create_input_data_buffer(
            op,
            data_context,
            # Shuffle needs every manifest on a single task to compute one
            # global RNG over the full listing.
            should_parallelize=shuffle_config is None,
        ),
        data_context,
        name="ListFiles",
        # Listing is extremely fast; default backpressure would starve the
        # downstream reader of inputs.
        ray_remote_args={"_generator_backpressure_num_objects": -1},
        # Don't fuse into the downstream ``ReadFiles`` — listing and reading
        # have different resource profiles.
        supports_fusion=False,
    )
    map_op.throttling_disabled = lambda: True
    return map_op


def _expand_paths_to_files(
    paths: List[str],
    filesystem: "pa.fs.FileSystem",
    ignore_missing_paths: bool,
    *,
    num_workers: int,
) -> List[str]:
    """Expand user paths/prefixes into concrete file paths at plan time.

    Metadata-only listing (``get_file_info`` + directory walk) -- it does NOT
    read Parquet footers. Reuses the same :func:`_get_file_infos` path the
    indexer uses inside the listing task, so directory pruning (``.``/``_``
    prefixes), missing-path handling, and per-directory sorting are identical.
    The LIST walks are fanned across ``num_workers`` threads over DISTINCT
    input paths (a single prefix's recursion is not itself parallelized). The
    returned list is path-sorted for deterministic shard boundaries.
    """
    if not paths:
        return []

    def _expand(path_iter: Iterator[str]) -> Iterator[str]:
        for input_path in path_iter:
            resolved_paths, _ = _resolve_paths_and_filesystem(input_path, filesystem)
            assert len(resolved_paths) == 1
            for file_path, _size in _get_file_infos(
                resolved_paths[0], filesystem, ignore_missing_paths
            ):
                yield file_path

    workers = min(num_workers, len(paths))
    if workers > 1:
        files = list(
            make_async_gen(
                base_iterator=iter(paths),
                fn=_expand,
                preserve_ordering=False,
                num_workers=workers,
                buffer_size=1024,
            )
        )
    else:
        files = list(_expand(iter(paths)))

    files.sort()
    return files


def _create_input_data_buffer(
    op: ListFiles,
    data_context: DataContext,
    *,
    should_parallelize: bool,
) -> InputDataBuffer:
    """Wrap the listing inputs into listing-input RefBundles.

    Each bundle's block is a 1-column arrow table ``{"__path": [paths...]}``
    that :func:`list_files_for_each_block` expands into manifest blocks.

    By default the user's paths/prefixes are expanded into concrete files at
    plan time (a cheap metadata-only LIST, no footer reads) and THOSE are
    sharded across listing tasks -- so the expensive per-file footer reads
    (row-group-aware Parquet chunking) fan out across many Ray tasks instead
    of concentrating in one task for a single-prefix input. Gated on
    ``ctx.list_files_expand_paths`` and the standard
    :class:`NonSamplingFileIndexer` (custom indexers fall back to sharding raw
    input paths, the pre-expansion behavior).
    """
    indexer = op.file_indexer
    expand = (
        data_context.list_files_expand_paths
        and isinstance(indexer, NonSamplingFileIndexer)
        and bool(op.paths)
    )
    if expand:
        shard_items = _expand_paths_to_files(
            list(op.paths),
            op.filesystem,
            indexer.ignore_missing_paths,
            num_workers=data_context.list_files_expand_num_workers,
        )
    else:
        shard_items = list(op.paths)

    if should_parallelize and shard_items:
        path_splits = np.array_split(
            shard_items,
            min(DEFAULT_MAX_NUM_LIST_FILES_TASKS, len(shard_items)),
        )
    else:
        path_splits = [shard_items]

    input_data: List[RefBundle] = []
    for path_split in path_splits:
        paths = list(path_split)
        if not paths:
            continue
        block = pa.Table.from_pydict({PATH_COLUMN_NAME: paths})
        metadata = BlockAccessor.for_block(block).get_metadata(
            input_files=None, block_exec_stats=None
        )
        block_ref: ray.ObjectRef[Block] = ray.put(block)
        ref_bundle = RefBundle(
            (
                # pyrefly: ignore[bad-argument-type]
                BlockEntry(block_ref, metadata),
            ),
            # ``owns_blocks=False``: these are the root of the DAG and
            # must not be freed eagerly, or the DAG can't be reconstructed.
            owns_blocks=False,
            schema=BlockAccessor.for_block(block).schema(),
        )
        input_data.append(ref_bundle)

    return InputDataBuffer(data_context, input_data=input_data)
