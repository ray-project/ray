from typing import TYPE_CHECKING, Dict

import ray
from .backpressure_policy import BackpressurePolicy

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


class StreamingOutputBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy that throttles the streaming outputs of the `DataOpTask`s.

    The are 2 levels of configs to control the behavior:
    - At the Ray Core level, we use
      `MAX_BLOCKS_IN_GENERATOR_BUFFER` to limit the number of blocks buffered in
      the streaming generator of each OpDataTask. When it's reached, the task will
      be blocked at `yield` until the caller reads another `ObjectRef.
    - At the Ray Data level, we use
      `MAX_BLOCKS_IN_GENERATOR_BUFFER` to limit the number of blocks buffered in the
      output queue of each operator. When it's reached, we'll stop reading from the
      streaming generators of the op's tasks, and thus trigger backpressure at the
      Ray Core level.

    Thus, total number of buffered blocks for each operator can be
    `MAX_BLOCKS_IN_GENERATOR_BUFFER * num_running_tasks +
    MAX_BLOCKS_IN_OP_OUTPUT_QUEUE`.
    """

    # The max number of blocks that can be buffered at the streaming generator
    # of each `DataOpTask`.
    MAX_BLOCKS_IN_GENERATOR_BUFFER = 10
    MAX_BLOCKS_IN_GENERATOR_BUFFER_CONFIG_KEY = (
        "backpressure_policies.streaming_output.max_blocks_in_generator_buffer"
    )
    # The max number of blocks that can be buffered at the operator output queue
    # (`OpState.outqueue`).
    MAX_BLOCKS_IN_OP_OUTPUT_QUEUE = 20
    MAX_BLOCKS_IN_OP_OUTPUT_QUEUE_CONFIG_KEY = (
        "backpressure_policies.streaming_output.max_blocks_in_op_output_queue"
    )

    def __init__(self, topology: "Topology"):
        data_context = ray.data.DataContext.get_current()
        self._max_num_blocks_in_streaming_gen_buffer = data_context.get_config(
            self.MAX_BLOCKS_IN_GENERATOR_BUFFER_CONFIG_KEY,
            self.MAX_BLOCKS_IN_GENERATOR_BUFFER,
        )
        assert self._max_num_blocks_in_streaming_gen_buffer > 0
        # The `_generator_backpressure_num_objects` parameter should be
        # `2 * self._max_num_blocks_in_streaming_gen_buffer` because we yield
        # 2 objects for each block: the block and the block metadata.
        data_context._task_pool_data_task_remote_args[
            "_generator_backpressure_num_objects"
        ] = (2 * self._max_num_blocks_in_streaming_gen_buffer)

        self._max_num_blocks_in_op_output_queue = data_context.get_config(
            self.MAX_BLOCKS_IN_OP_OUTPUT_QUEUE_CONFIG_KEY,
            self.MAX_BLOCKS_IN_OP_OUTPUT_QUEUE,
        )
        assert self._max_num_blocks_in_op_output_queue > 0

    def calculate_max_blocks_to_read_per_op(
        self, topology: "Topology"
    ) -> Dict["OpState", int]:
        max_blocks_to_read_per_op: Dict["OpState", int] = {}
        downstream_num_active_tasks = 0
        for op, state in reversed(topology.items()):
            max_blocks_to_read_per_op[state] = (
                self._max_num_blocks_in_op_output_queue - state.outqueue_num_blocks()
            )
            if downstream_num_active_tasks == 0:
                # If all downstream operators are idle, it could be because no resources
                # are available. In this case, we'll make sure to read at least one
                # block to avoid deadlock.
                # TODO(hchen): `downstream_num_active_tasks == 0` doesn't necessarily
                # mean no enough resources. One false positive case is when the upstream
                # op hasn't produced any blocks for the downstream op to consume.
                # In this case, at least reading one block is fine.
                # If there are other false positive cases, we may want to make this
                # deadlock check more accurate by directly checking resources.
                max_blocks_to_read_per_op[state] = max(
                    max_blocks_to_read_per_op[state],
                    1,
                )
            downstream_num_active_tasks += len(op.get_active_tasks())
        return max_blocks_to_read_per_op
