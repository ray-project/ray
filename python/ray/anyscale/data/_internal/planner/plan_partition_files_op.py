import logging
from typing import Iterable, List, Optional

import pyarrow as pa

from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
from ray.anyscale.data._internal.logical.operators.partition_files_operator import (
    PartitionFiles,
)
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data.block import Block
from ray.data.context import DataContext

logger = logging.getLogger(__name__)

NUM_BLOCKS_PER_READ_TASK = 8


def plan_partition_files_op(
    logical_op: PartitionFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 1

    def partition_files(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        file_paths = []
        running_in_memory_size = 0
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
                # Estimating the encoding ratio can be expensive (e.g., if the
                # estimation requires reading the file). So, we only estimate the
                # encoding ratio if we don't already have one.
                if encoding_ratio is None:
                    encoding_ratio = _estimate_encoding_ratio(
                        file_path, file_size, logical_op
                    )

                file_paths.append(file_path)
                # `HTTPFileSystem` returns `None` for `file_size`. In this case, we
                # place all paths in the same block.
                if file_size is not None:
                    in_memory_size = file_size * encoding_ratio
                    running_in_memory_size += in_memory_size

                if (
                    running_in_memory_size
                    > data_context.target_max_block_size
                    # To amortize overheads associated with launching Ray tasks and
                    # using multi-threading, produce multiple blocks in each read task.
                    # This doesn't change the size of the blocks, but it does change the
                    # number of blocks produced by each task.
                    * NUM_BLOCKS_PER_READ_TASK
                ):
                    block = pa.Table.from_pydict({PATH_COLUMN_NAME: file_paths})
                    yield block

                    file_paths = []
                    running_in_memory_size = 0

        if file_paths:
            block = pa.Table.from_pydict({PATH_COLUMN_NAME: file_paths})
            yield block

    transform_fns: List[MapTransformFn] = [BlockMapTransformFn(partition_files)]
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


def _estimate_encoding_ratio(
    path: str, file_size: Optional[int], logical_op: PartitionFiles
) -> float:
    if file_size is not None and file_size > 0:
        in_memory_size = logical_op.reader.estimate_in_memory_size(
            path, file_size, filesystem=logical_op.filesystem
        )
        encoding_ratio = in_memory_size / file_size
    else:
        encoding_ratio = 1.0
    return encoding_ratio
