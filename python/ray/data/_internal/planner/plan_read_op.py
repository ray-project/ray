from typing import Iterable, List

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
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.util import _autodetect_parallelism, _warn_on_high_parallelism
from ray.data.block import Block
from ray.data.context import DataContext
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


def plan_read_op(op: Read) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for Read.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data(target_max_block_size) -> List[RefBundle]:
        parallelism, min_safe_parallelism, _ = _autodetect_parallelism(
            op._parallelism,
            target_max_block_size,
            DataContext.get_current(),
            op._reader,
            op._mem_size,
        )

        read_tasks = op._reader.get_read_tasks(parallelism)
        _warn_on_high_parallelism(parallelism, len(read_tasks))

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
        input_data_factory=get_input_data,
    )

    def do_read(blocks: Iterable[ReadTask], _: TaskContext) -> Iterable[Block]:
        for read_task in blocks:
            yield from read_task()

    # Create a MapTransformer for a read operator
    transform_fns: List[MapTransformFn] = [
        # First, execute the read tasks.
        BlockMapTransformFn(do_read),
    ]
    transform_fns.append(BuildOutputBlocksMapTransformFn.for_blocks())
    map_transformer = MapTransformer(transform_fns)

    return MapOperator.create(
        map_transformer,
        inputs,
        name=op.name,
        target_max_block_size=None,
        ray_remote_args=op._ray_remote_args,
    )
