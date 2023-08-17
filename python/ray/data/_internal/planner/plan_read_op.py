from typing import Iterable, Iterator, List

import ray
import ray.cloudpickle as cloudpickle
from ray.data._internal.execution.interfaces import (
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_data_processor import (
    create_map_data_processor_for_read_op,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.read_operator import Read
from ray.data.block import Block
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


def _plan_read_op(op: Read) -> PhysicalOperator:
    """Get the corresponding DAG of physical operators for Read.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """

    def get_input_data() -> List[RefBundle]:
        read_tasks = op._reader.get_read_tasks(op._parallelism)
        if op._additional_split_factor is not None:
            for r in read_tasks:
                r._set_additional_split_factor(op._additional_split_factor)
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

    def do_read(blocks: Iterable[ReadTask], _) -> Iterable[Block]:
        for read_task in blocks:
            yield from read_task()

    map_data_processor = create_map_data_processor_for_read_op(do_read)

    return MapOperator.create(
        map_data_processor,
        inputs,
        name=op.name,
        ray_remote_args=op._ray_remote_args,
    )
