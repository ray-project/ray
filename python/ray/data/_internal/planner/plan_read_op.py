import functools
from typing import Iterable, List, Optional

import ray
import ray.cloudpickle as cloudpickle
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    MapTransformer,
    MapTransformFn,
    MapTransformFnDataType,
    blocks_to_output_blocks,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasource import ReadTask

TASK_SIZE_WARN_THRESHOLD_BYTES = 100000


# Defensively compute the size of the block as the max size reported by the
# datasource and the actual read task size. This is to guard against issues
# with bad metadata reporting.
def cleaned_metadata(read_task):
    block_meta = read_task.get_metadata()
    task_size = len(cloudpickle.dumps(read_task))
    if block_meta.size_bytes is None or task_size > block_meta.size_bytes:
        if task_size > TASK_SIZE_WARN_THRESHOLD_BYTES:
            print(
                f"WARNING: the read task size ({task_size} bytes) is larger "
                "than the reported output size of the task "
                f"({block_meta.size_bytes} bytes). This may be a size "
                "reporting bug in the datasource being read from."
            )
        block_meta.size_bytes = task_size
    return block_meta


def _splitrange(n, k):
    """Calculates array lens of np.array_split().

    This is the equivalent of
    `[len(x) for x in np.array_split(range(n), k)]`.
    """
    base = n // k
    output = [base] * k
    rem = n - sum(output)
    for i in range(len(output)):
        if rem > 0:
            output[i] += 1
            rem -= 1
    assert rem == 0, (rem, output, n, k)
    assert sum(output) == n, (output, n, k)
    return output


def _do_additional_splits(
    blocks: Iterable[Block], _: TaskContext, additional_output_splits: int
) -> Iterable[Block]:
    """Do additional splits to the output blocks of a ReadTask.

    Args:
      blocks: The input blocks.
      additional_output_splits: The number of additional splits, must be greater than 1.
    """
    assert additional_output_splits > 1
    for block in blocks:
        block = BlockAccessor.for_block(block)
        offset = 0
        split_sizes = _splitrange(block.num_rows(), additional_output_splits)
        for size in split_sizes:
            yield block.slice(offset, offset + size, copy=True)
            offset += size


def plan_read_op(op: Read) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for Read.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        read_tasks = op._reader.get_read_tasks(op._parallelism)
        return [
            RefBundle(
                [
                    (
                        # TODO(chengsu): figure out a better way to pass read
                        # tasks other than ray.put().
                        ray.put(read_task),
                        cleaned_metadata(read_task),
                    )
                ],
                owns_blocks=True,
            )
            for read_task in read_tasks
        ]

    inputs = InputDataBuffer(
        input_data_factory=get_input_data, num_output_blocks=op._estimated_num_blocks
    )

    def do_read(blocks: Iterable[ReadTask], _: TaskContext) -> Iterable[Block]:
        for read_task in blocks:
            yield from read_task()

    # Create a MapTransformer for a read operator
    transform_fns = [
        MapTransformFn(
            do_read, MapTransformFnDataType.Block, MapTransformFnDataType.Block
        ),
    ]
    if op._additional_split_factor is None:
        # If additional_split_factor is None, we build output blocks from the
        # output blocks of the read tasks.
        transform_fns.append(blocks_to_output_blocks)
    else:
        # If additional_split_factor is not None, we do additional splits to the
        # output blocks of the read tasks.
        transform_fns.append(
            MapTransformFn(
                functools.partial(
                    _do_additional_splits,
                    additional_output_splits=op._additional_split_factor,
                ),
                MapTransformFnDataType.Block,
                MapTransformFnDataType.Block,
            )
        )

    map_transformer = MapTransformer(transform_fns)

    return MapOperator.create(
        map_transformer,
        inputs,
        name=op.name,
        ray_remote_args=op._ray_remote_args,
    )


def apply_output_block_building_and_additional_splitting_to_read_tasks(
    read_tasks: List[ReadTask],
    additional_split_factor: Optional[int],
):
    """Apply output block building and additional splitting logic to read tasks.

    This function is only used for compability with the legacy LazyBlockList code path.
    """
    transform_fns = [
        blocks_to_output_blocks,
    ]
    if additional_split_factor is not None:
        transform_fns.append(
            MapTransformFn(
                functools.partial(
                    _do_additional_splits,
                    additional_output_splits=additional_split_factor,
                ),
                MapTransformFnDataType.Block,
                MapTransformFnDataType.Block,
            )
        )
    map_transformer = MapTransformer(transform_fns)

    for read_task in read_tasks:
        original_read_fn = read_task._read_fn

        def new_read_fn():
            blocks = original_read_fn()
            # We pass None as the TaskContext because we don't have access to it here.
            # This is okay because the transform functions don't use the TaskContext.
            return map_transformer.apply_transform(blocks, None)  # type: ignore

        read_task._read_fn = new_read_fn
