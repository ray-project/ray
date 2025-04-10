import logging
from functools import partial
from typing import Iterable, List, Optional

import numpy as np
import pyarrow as pa

import ray
from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    ListFiles,
)
from ray.anyscale.data._internal.readers import InMemorySizeEstimator
from ray.data import FileShuffleConfig
from ray.data._internal.arrow_block import ArrowBlockAccessor, ArrowBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlocksToRowsMapTransformFn,
    MapTransformer,
    Row,
    RowMapTransformFn,
    RowToBlockMapTransformFn,
)
from ray.data.block import Block, BlockAccessor
from ray.data.context import DEFAULT_READ_OP_MIN_NUM_BLOCKS, DataContext
from ray.data.datasource.path_util import (
    _has_file_extension,
    _resolve_paths_and_filesystem,
)

logger = logging.getLogger(__name__)

# TODO(@bveeramani): 200 is arbitrary.
# This is the maximum total number of list files task that we launch. In practice, we'll
# usually only launch one list files task (i.e., in the case the user provides a single
# directory).
DEFAULT_MAX_NUM_LIST_FILES_TASKS = 200


NUM_BLOCKS_PER_READ_TASK = 8


class Bucket:
    """A bucket of paths."""

    def __init__(self):
        self._paths = []
        self._in_memory_size = 0

    @property
    def paths(self):
        return self._paths

    @property
    def in_memory_size(self):
        return self._in_memory_size

    def add(self, path: str, in_memory_size: int):
        self._paths.append(path)
        self._in_memory_size += in_memory_size

    def clear(self):
        self._paths.clear()
        self._in_memory_size = 0


def plan_list_files_op(
    op: ListFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 0

    #
    # NOTE: Avoid capturing operators in closures!
    #
    file_extensions = op.file_extensions
    partition_filter = op.partition_filter

    # Instantiate shuffle configuration (if any)
    shuffle_config = op.shuffle_config_factory()

    fs = op.filesystem
    indexer = op.file_indexer
    in_memory_size_estimator = op.in_memory_size_estimator

    def list_files(rows: Iterable[Row], _: TaskContext) -> Iterable[Row]:
        for row in rows:
            resolved_paths, _ = _resolve_paths_and_filesystem(row[PATH_COLUMN_NAME], fs)
            assert len(resolved_paths) == 1
            for file_path, file_size in indexer.list_files(resolved_paths[0], fs):
                if not _has_file_extension(file_path, file_extensions):
                    logger.debug(
                        f"Skipping file '{file_path}' because it does not have one "
                        f"of the required extensions: {file_extensions}"
                    )
                    continue

                if partition_filter is not None:
                    if not partition_filter([file_path]):
                        logger.debug(
                            f"Skipping file '{file_path}' because it does not "
                            "match the partition filter."
                        )
                        continue

                yield {
                    PATH_COLUMN_NAME: file_path,
                    FILE_SIZE_COLUMN_NAME: file_size,
                }

    transform_fns = [
        BlocksToRowsMapTransformFn.instance(),
        RowMapTransformFn(list_files, is_udf=False),
        # NOTE: This transformation yields blocks as soon as they become reach
        #       min_bucket_size, and have to be maintained this way to prevent
        #       unnecessary buffering at this layer (and enable faster reading)
        RowToBlockMapTransformFn(
            partial(
                partition_files,
                # We default to `DEFAULT_READ_OP_MIN_NUM_BLOCKS` buckets for consistency
                # with the OSS implementation.
                # TODO: Replace `DEFAULT_READ_OP_MIN_NUM_BLOCKS` with the maximum number
                # of CPUs in the cluster once we have access to that information.
                num_buckets=DEFAULT_READ_OP_MIN_NUM_BLOCKS,
                min_bucket_size=data_context.target_min_block_size,
                max_bucket_size=(
                    data_context.target_max_block_size
                    # To amortize overheads associated with launching Ray tasks and
                    # using multi-threading, produce multiple blocks in each read task.
                    # This doesn't change the size of the blocks, but it does change the
                    # number of blocks produced by each task.
                    * NUM_BLOCKS_PER_READ_TASK
                ),
                in_memory_size_estimator=in_memory_size_estimator,
                filesystem=fs,
                shuffle_config=shuffle_config,
            ),
        ),
    ]

    map_transformer = MapTransformer(transform_fns)

    return MapOperator.create(
        map_transformer,
        create_input_data_buffer(
            op,
            data_context,
            # NOTE: If shuffling is requested we can't parallelize the listing
            #       as we need to collect all files in a single task for subsequent
            #       global shuffling
            should_parallelize=shuffle_config is None,
        ),
        data_context,
        name="ListFiles",
        # This will push the blocks to the next operator faster.
        target_max_block_size=data_context.target_min_block_size,
        ray_remote_args={
            # This is operator is extremely fast. If we don't unblock backpressure, this
            # operator gets bottlenecked by the Ray Data scheduler. This can prevent Ray
            # Data from launching enough read tasks.
            "_generator_backpressure_num_objects": -1,
        },
    )


def create_input_data_buffer(
    logical_op: ListFiles, data_context: DataContext, *, should_parallelize: bool
) -> InputDataBuffer:

    if should_parallelize:
        max_num_list_files_tasks = data_context.get_config(
            "max_num_list_files_tasks", DEFAULT_MAX_NUM_LIST_FILES_TASKS
        )
        path_splits = np.array_split(
            logical_op.paths, min(max_num_list_files_tasks, len(logical_op.paths))
        )
    else:
        path_splits = [logical_op.paths]

    input_data = []
    for path_split in path_splits:
        block = pa.Table.from_pydict({PATH_COLUMN_NAME: path_split})
        metadata = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=None
        )
        ref_bundle = RefBundle(
            [(ray.put(block), metadata)],
            # `owns_blocks` is False, because these refs are the root of the
            # DAG. We shouldn't eagerly free them. Otherwise, the DAG cannot
            # be reconstructed.
            owns_blocks=False,
        )
        input_data.append(ref_bundle)
    return InputDataBuffer(data_context, input_data=input_data)


def partition_files(
    rows_iter: Iterable[Row],
    ctx: TaskContext,
    *,
    num_buckets: int,
    min_bucket_size: int,
    max_bucket_size: int,
    in_memory_size_estimator: InMemorySizeEstimator,
    filesystem: pa.fs.FileSystem,
    shuffle_config: Optional[FileShuffleConfig],
) -> Iterable[Block]:
    """Partitions input paths into blocks based on the in-memory size of files.

    This partitioning ensures read tasks effectively utilize the cluster and
    produce appropriately-sized blocks

    **Steps:**
        1. Initialize empty buckets.
        2. Iterate through input blocks and add paths to buckets. For each path:
            - If the current bucket falls below `min_bucket_size`, add the path and don't move
              to the next bucket.
            - If the current bucket exceeds `min_bucket_size` but not `max_bucket_size`,
              add the path and move to the next bucket.
            - If the current bucket exceeds `max_bucket_size`, yield the paths as a block, clear
              the bucket, and move to the next bucket.
        3. Yield any remaining paths in the buckets as blocks.

    This algorithm ensures that each block contains [target_min_block_size,
    target_max_block_size * NUM_BLOCKS_PER_READ_TASK] worth of files. It's a
    deterministic algorithm, but it doesn't maintain the order of the input paths.

    TODO elaborate on shuffling

    """

    if shuffle_config:
        from ray.data._internal.arrow_ops import transform_pyarrow

        builder = ArrowBlockBuilder()

        # NOTE: This will block until file listing is complete!
        for row in rows_iter:
            builder.add(row)

        block = builder.build()

        shuffled_block = transform_pyarrow.shuffle(block, shuffle_config.seed)

        rows_iter = ArrowBlockAccessor(shuffled_block).iter_rows(
            public_row_format=False
        )

    # This function might yield less than `num_buckets` blocks because it gives priority
    # to producing blocks larger than `target_min_block_size`.
    buckets = [Bucket() for _ in range(num_buckets)]
    current_bucket_index = 0

    for row in rows_iter:
        file_path, file_size = row[PATH_COLUMN_NAME], row[FILE_SIZE_COLUMN_NAME]

        current_bucket = buckets[current_bucket_index]

        if file_size is not None:
            in_memory_size_estimate = in_memory_size_estimator.estimate_in_memory_size(
                file_path, file_size, filesystem=filesystem
            )
        else:
            # `HTTPFileSystem` returns `None` for `file_size`. In this case, we
            # place all paths in the same block.
            in_memory_size_estimate = 0

        current_bucket.add(file_path, in_memory_size_estimate)
        if current_bucket.in_memory_size >= max_bucket_size:
            block = pa.Table.from_pydict({PATH_COLUMN_NAME: current_bucket.paths})
            yield block
            current_bucket_index = (current_bucket_index + 1) % num_buckets
            current_bucket.clear()
        elif current_bucket.in_memory_size >= min_bucket_size:
            current_bucket_index = (current_bucket_index + 1) % num_buckets

    for bucket in buckets:
        if bucket.paths:
            block = pa.Table.from_pydict({PATH_COLUMN_NAME: bucket.paths})
            yield block
