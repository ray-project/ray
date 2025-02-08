import logging
from functools import partial
from typing import Iterable, List, Optional

import pyarrow as pa

from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
from ray.anyscale.data._internal.logical.operators.partition_files_operator import (
    PartitionFiles,
)
from ray.anyscale.data._internal.readers import FileReader
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data.block import Block
from ray.data.context import DEFAULT_READ_OP_MIN_NUM_BLOCKS, DataContext

logger = logging.getLogger(__name__)

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


def plan_partition_files_op(
    logical_op: PartitionFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1

    transform_fns: List[MapTransformFn] = [
        BlockMapTransformFn(
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
                reader=logical_op.reader,
                filesystem=logical_op.filesystem,
            )
        )
    ]
    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        physical_children[0],
        data_context,
        name="PartitionFiles",
        ray_remote_args={
            # This is operator is extremely fast. If we don't unblock backpressure, this
            # operator gets bottlenecked by the Ray Data scheduler. This can prevent Ray
            # Data from launching enough read tasks.
            "_generator_backpressure_num_objects": -1,
        },
    )


def partition_files(
    blocks: Iterable[Block],
    _: TaskContext,
    num_buckets: int,
    min_bucket_size: int,
    max_bucket_size: int,
    reader: FileReader,
    filesystem: pa.fs.FileSystem,
) -> Iterable[Block]:
    """Partition paths for reading.

    The `partition_files` function partitions input paths into blocks based on the
    in-memory size of files. This partitioning ensures read tasks effectively utilize
    the cluster and produce appropriately-sized blocks

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

    This algorithms ensures that each block contains [target_min_block_size,
    target_max_block_size * NUM_BLOCKS_PER_READ_TASK] worth of files. It's a
    deterministic algorithm, but it doesn't maintain the order of the input paths.
    """
    # This function might yield less than `num_buckets` blocks because it gives priority
    # to producing blocks larger than `target_min_block_size`.
    buckets = [Bucket() for _ in range(num_buckets)]
    current_bucket_index = 0
    encoding_ratio = None  # Ratio of in-memory size to file size.
    for block in blocks:
        if len(block) == 0:
            continue

        assert isinstance(block, pa.Table), type(block)
        assert (
            PATH_COLUMN_NAME in block.column_names
            and FILE_SIZE_COLUMN_NAME in block.column_names
        ), block.column_names

        for file_path, file_size in zip(
            block[PATH_COLUMN_NAME].to_pylist(),
            block[FILE_SIZE_COLUMN_NAME].to_pylist(),
        ):
            current_bucket = buckets[current_bucket_index]

            # Estimating the encoding ratio can be expensive (e.g., if the
            # estimation requires reading the file). So, we only estimate the
            # encoding ratio if we don't already have one.
            if encoding_ratio is None:
                encoding_ratio = _estimate_encoding_ratio(
                    file_path, file_size, reader, filesystem
                )

            # `HTTPFileSystem` returns `None` for `file_size`. In this case, we
            # place all paths in the same block.
            if file_size is not None:
                in_memory_size_estimate = file_size * encoding_ratio
            else:
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


def _estimate_encoding_ratio(
    path: str,
    file_size: Optional[int],
    reader: FileReader,
    filesystem: pa.fs.FileSystem,
) -> float:
    if file_size is not None and file_size > 0:
        in_memory_size = reader.estimate_in_memory_size(
            path, file_size, filesystem=filesystem
        )
        encoding_ratio = in_memory_size / file_size
    else:
        encoding_ratio = 1.0
    return encoding_ratio
