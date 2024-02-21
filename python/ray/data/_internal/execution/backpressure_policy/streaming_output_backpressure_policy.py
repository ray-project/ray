import time
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Tuple

import ray

from ray._private.worker import wait
from .backpressure_policy import BackpressurePolicy
from ray.data._internal.dataset_logger import DatasetLogger

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


logger = DatasetLogger(__name__)


class StreamingOutputBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy that throttles the streaming outputs of the `DataOpTask`s.

    The are 2 levels of configs to control the behavior:
    - At the Ray Core level, we use
      `MAX_BLOCKS_IN_GENERATOR_BUFFER` to limit the number of blocks buffered in
      the streaming generator of each OpDataTask. When it's reached, the task will
      be blocked at `yield` until the caller reads another `ObjectRef.
    - At the Ray Data level, we use
      `MAX_BLOCKS_IN_OP_OUTPUT_QUEUE` to limit the number of blocks buffered in the
      output queue of each operator. When it's reached, we'll stop reading from the
      streaming generators of the op's tasks, and thus trigger backpressure at the
      Ray Core level.

    Thus, total number of buffered blocks for each operator can be
    `MAX_BLOCKS_IN_GENERATOR_BUFFER * num_running_tasks +
    MAX_BLOCKS_IN_OP_OUTPUT_QUEUE`.
    """

    def __init__(self, topology: "Topology"):
        pass

    def calculate_max_bytes_to_read_per_op(
        self, topology: "Topology", resource_manager: "ResourceManager"
    ) -> Dict["OpState", int]:
        if not resource_manager.op_resource_allocator_enabled():
            return {}

        max_blocks_to_read_per_op: Dict["OpState", int] = {}
        for op, state in reversed(topology.items()):
            max_bytes_to_read = resource_manager._op_resource_alloator.max_task_outputs_to_fetch(op)
            # if max_bytes_to_read == 0:
            #     if (
            #         resource_manager._obj_store_output_buffers[op]
            #         + resource_manager._obj_store_next_op_input_buffers[op]
            #         == 0
            #     ):
            #         max_bytes_to_read = 1
            max_blocks_to_read_per_op[state] = max_bytes_to_read

        return max_blocks_to_read_per_op
