import logging
from typing import Iterable, List

import numpy as np

import ray
from ray.anyscale.data._internal.logical.operators.partition_parquet_fragments_operator import (  # noqa: E501
    PartitionParquetFragments,
)
from ray.data._internal.compute import TaskPoolStrategy
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data._internal.util import call_with_retry
from ray.data.block import Block, BlockAccessor

logger = logging.getLogger(__name__)

# TODO(@bveeramani): I'm setting a limit on the number of partioning tasks to prevent
# rate limiting errors. 8 is arbitrary. Figure out a better way to determine this.
MAX_NUM_PARTITION_TASKS = 8

NUM_FRAGMENTS_PER_PARTITION_TASK = 128


def plan_partition_parquet_fragments_op(
    op: PartitionParquetFragments, physical_children: List[PhysicalOperator]
) -> PhysicalOperator:
    assert len(physical_children) == 0
    input_data_buffer = create_input_data_buffer(op)
    partition_parquet_fragments_operator = create_partition_parquet_fragments_operator(
        input_data_buffer, op
    )
    return partition_parquet_fragments_operator


def create_input_data_buffer(logical_op: PartitionParquetFragments) -> InputDataBuffer:
    def input_data_factory(_):
        serialized_fragments = logical_op.serialized_fragments

        if logical_op.shuffle == "files":
            serialized_fragments = np.random.permutation(serialized_fragments)

        serialized_fragment_splits = np.array_split(
            serialized_fragments,
            max(len(serialized_fragments) // NUM_FRAGMENTS_PER_PARTITION_TASK, 1),
        )

        input_data = []
        for serialized_fragment_split in serialized_fragment_splits:
            block_builder = DelegatingBlockBuilder()
            block_builder.add_batch({"fragment": serialized_fragment_split})
            block = block_builder.build()
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
        return input_data

    return InputDataBuffer(input_data_factory=input_data_factory)


def create_partition_parquet_fragments_operator(
    input_op: PhysicalOperator,
    logical_op: PartitionParquetFragments,
) -> PhysicalOperator:
    ctx = ray.data.DataContext.get_current()

    def partition_fragments(blocks: Iterable[Block], _: TaskContext) -> Iterable[Block]:
        running_serialized_fragments = []
        running_in_memory_size = 0
        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            serialized_fragments = [
                row["fragment"]
                for row in block_accessor.iter_rows(public_row_format=False)
            ]
            for serialized_fragment in serialized_fragments:
                fragment = serialized_fragment.deserialize()

                if logical_op.partition_filter is not None:
                    if not logical_op.partition_filter([fragment.path]):
                        logger.debug(
                            f"Skipping file '{fragment.path}' because it does not "
                            "match the partition filter."
                        )
                        continue

                running_serialized_fragments.append(serialized_fragment)

                def get_file_size() -> int:
                    file_size = 0
                    for idx in range(fragment.metadata.num_row_groups):
                        file_size += fragment.metadata.row_group(idx).total_byte_size
                    return file_size

                # Getting the file size requires network calls, so it might fail with
                # transient errors.
                file_size = call_with_retry(
                    get_file_size,
                    description="get fragment size",
                    match=ctx.retried_io_errors,
                )
                in_memory_size = file_size * logical_op.encoding_ratio
                running_in_memory_size += in_memory_size

                if running_in_memory_size > ctx.target_max_block_size:
                    block_builder = DelegatingBlockBuilder()
                    block_builder.add_batch(
                        {
                            "fragment": running_serialized_fragments,
                        }
                    )
                    yield block_builder.build()

                    running_serialized_fragments = []
                    running_in_memory_size = 0

        if running_serialized_fragments:
            block_builder = DelegatingBlockBuilder()
            block_builder.add_batch({"fragment": running_serialized_fragments})
            yield block_builder.build()

    transform_fns: List[MapTransformFn] = [BlockMapTransformFn(partition_fragments)]
    map_transformer = MapTransformer(transform_fns)
    return MapOperator.create(
        map_transformer,
        input_op,
        name="PartitionParquetFragments",
        target_max_block_size=None,
        compute_strategy=TaskPoolStrategy(MAX_NUM_PARTITION_TASKS),
        ray_remote_args={
            "num_cpus": 0.5,
            # This is operator is extremely fast. If we don't unblock backpressure, this
            # operator gets bottlenecked by the Ray Data scheduler. This can prevent Ray
            # Data from launching enough read tasks.
            "_generator_backpressure_num_objects": -1,
        },
    )
