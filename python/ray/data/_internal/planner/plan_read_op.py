from typing import Iterable, List, Optional

import ray
import ray.cloudpickle as cloudpickle
from ray.data._internal.execution.interfaces import PhysicalOperator, RefBundle
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    BuildOutputBlocksMapTransformFn,
    MapTransformer,
    MapTransformFn,
    MapTransformFnDataType,
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


class BuildOutputBlocksWithAdditionalSplit(MapTransformFn):
    """Build output blocks and do additional splits."""

    def __init__(self, additional_split_factor: int):
        """
        Args:
          additional_output_splits: The number of additional splits, must be
          greater than 1.
        """
        assert additional_split_factor > 1
        self._additional_split_factor = additional_split_factor
        super().__init__(MapTransformFnDataType.Block, MapTransformFnDataType.Block)

    def __call__(self, blocks: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        blocks = BuildOutputBlocksMapTransformFn.for_blocks()(blocks, ctx)
        return self._do_additional_splits(blocks)

    def _do_additional_splits(
        self,
        blocks: Iterable[Block],
    ) -> Iterable[Block]:
        for block in blocks:
            block = BlockAccessor.for_block(block)
            offset = 0
            split_sizes = _splitrange(block.num_rows(), self._additional_split_factor)
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
                # `owns_blocks` is False, because these refs are the root of the
                # DAG. We shouldn't eagerly free them. Otherwise, the DAG cannot
                # be reconstructed.
                owns_blocks=False,
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
    transform_fns: List[MapTransformFn] = [
        # First, execute the read tasks.
        BlockMapTransformFn(do_read),
    ]
    if op._additional_split_factor is None:
        # Then build the output blocks.
        transform_fns.append(BuildOutputBlocksMapTransformFn.for_blocks())
    else:
        # Build the output blocks and do additional splits.
        # NOTE, we explictly do these two steps in one MapTransformFn to avoid
        # `BuildOutputBlocksMapTransformFn` getting dropped by the optimizer.
        transform_fns.append(
            BuildOutputBlocksWithAdditionalSplit(op._additional_split_factor)
        )

    map_transformer = MapTransformer(transform_fns)

    return MapOperator.create(
        map_transformer,
        inputs,
        name=op.name,
        ray_remote_args=op._ray_remote_args,
    )


def apply_output_blocks_handling_to_read_task(
    read_task: ReadTask,
    additional_split_factor: Optional[int],
):
    """Patch the read task and apply output blocks handling logic.

    This function is only used for compability with the legacy LazyBlockList code path.
    """
    transform_fns: List[MapTransformFn] = []

    if additional_split_factor is None:
        transform_fns.append(BuildOutputBlocksMapTransformFn.for_blocks())
    else:
        transform_fns.append(
            BuildOutputBlocksWithAdditionalSplit(additional_split_factor)
        )
    map_transformer = MapTransformer(transform_fns)

    original_read_fn = read_task._read_fn

    def new_read_fn():
        blocks = original_read_fn()
        # We pass None as the TaskContext because we don't have access to it here.
        # This is okay because the transform functions don't use the TaskContext.
        return map_transformer.apply_transform(blocks, None)  # type: ignore

    read_task._read_fn = new_read_fn
