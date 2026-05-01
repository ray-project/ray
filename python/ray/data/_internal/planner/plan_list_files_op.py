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
from typing import List

import numpy as np
import pyarrow as pa

import ray
from ray.data._internal.datasource_v2.listing.file_manifest import (
    PATH_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.listing.listing_utils import (
    list_files_for_each_block,
    partition_files,
    shuffle_files,
)
from ray.data._internal.datasource_v2.readers.read_files_task_memory import (
    MAP_TASK_KWARG_ENRICH_MANIFEST_TASK_MEMORY,
)
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
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
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext

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
    size_estimator = op.size_estimator
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
                size_estimator=size_estimator,
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
        map_task_kwargs={MAP_TASK_KWARG_ENRICH_MANIFEST_TASK_MEMORY: True},
    )
    map_op.throttling_disabled = lambda: True
    return map_op


def _create_input_data_buffer(
    op: ListFiles,
    data_context: DataContext,
    *,
    should_parallelize: bool,
) -> InputDataBuffer:
    """Wrap ``op.paths`` into listing-input RefBundles.

    Each bundle's block is a 1-column arrow table ``{"__path": [paths...]}``
    that :func:`list_files_for_each_block` expands into manifest blocks.
    """
    if should_parallelize and op.paths:
        path_splits = np.array_split(
            list(op.paths),
            min(DEFAULT_MAX_NUM_LIST_FILES_TASKS, len(op.paths)),
        )
    else:
        path_splits = [list(op.paths)]

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
            ((block_ref, metadata),),  # pyrefly: ignore[bad-argument-type]
            # ``owns_blocks=False``: these are the root of the DAG and
            # must not be freed eagerly, or the DAG can't be reconstructed.
            owns_blocks=False,
            schema=BlockAccessor.for_block(block).schema(),
        )
        input_data.append(ref_bundle)

    return InputDataBuffer(data_context, input_data=input_data)
