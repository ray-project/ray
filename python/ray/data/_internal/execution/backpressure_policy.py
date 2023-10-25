import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Dict

import ray

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology

logger = logging.getLogger(__name__)


# Default enabled backpressure policies and its config key.
# Use `DataContext.set_config` to config it.
# TODO(hchen): Enable ConcurrencyCapBackpressurePolicy by default.
ENABLED_BACKPRESSURE_POLICIES = []
ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY = "backpressure_policies.enabled"


def get_backpressure_policies(topology: "Topology"):
    data_context = ray.data.DataContext.get_current()
    policies = data_context.get_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY, ENABLED_BACKPRESSURE_POLICIES
    )

    return [policy(topology) for policy in policies]


class BackpressurePolicy(ABC):
    """Interface for back pressure policies."""

    @abstractmethod
    def __init__(self, topology: "Topology"):
        ...

    def calculate_max_blocks_to_read_per_op(
        self, topology: "Topology"
    ) -> Dict["OpState", int]:
        """Determine how many blocks of data we can read from each operator.
        The `DataOpTask`s of the operators will stop reading blocks when the limit is
        reached. Then the execution of these tasks will be paused when the streaming
        generator backpressure threshold is reached.
        Used in `streaming_executor_state.py::process_completed_tasks()`.

        Returns: A dict mapping from each operator's OpState to the desired number of
            blocks to read. For operators that are not in the dict, all available blocks
            will be read.

        Note: Only one backpressure policy that implements this method can be enabled
            at a time.
        """
        return {}

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        """Determine if we can add a new input to the operator. If returns False, the
        operator will be backpressured and will not be able to run new tasks.
        Used in `streaming_executor_state.py::select_operator_to_run()`.

        Returns: True if we can add a new input to the operator, False otherwise.

        Note, if multiple backpressure policies are enabled, the operator will be
        backpressured if any of the policies returns False.
        """
        return True


class ConcurrencyCapBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy that caps the concurrency of each operator.

    The concurrency cap limits the number of concurrently running tasks.
    It will be set to an intial value, and will ramp up exponentially.

    The concrete stategy is as follows:
    - Each PhysicalOperator is assigned an initial concurrency cap.
    - An PhysicalOperator can run new tasks if the number of running tasks is less
      than the cap.
    - When the number of finished tasks reaches a threshold, the concurrency cap will
      increase.
    """

    # Following are the default values followed by the config keys of the
    # available configs.
    # Use `DataContext.set_config` to config them.

    # The intial concurrency cap for each operator.
    INIT_CAP = 4
    INIT_CAP_CONFIG_KEY = "backpressure_policies.concurrency_cap.init_cap"
    # When the number of finished tasks reaches this threshold, the concurrency cap
    # will be multiplied by the multiplier.
    CAP_MULTIPLY_THRESHOLD = 0.5
    CAP_MULTIPLY_THRESHOLD_CONFIG_KEY = (
        "backpressure_policies.concurrency_cap.cap_multiply_threshold"
    )
    # The multiplier to multiply the concurrency cap by.
    CAP_MULTIPLIER = 2.0
    CAP_MULTIPLIER_CONFIG_KEY = "backpressure_policies.concurrency_cap.cap_multiplier"

    def __init__(self, topology: "Topology"):
        self._concurrency_caps: dict["PhysicalOperator", float] = {}

        data_context = ray.data.DataContext.get_current()
        self._init_cap = data_context.get_config(
            self.INIT_CAP_CONFIG_KEY, self.INIT_CAP
        )
        self._cap_multiplier = data_context.get_config(
            self.CAP_MULTIPLIER_CONFIG_KEY, self.CAP_MULTIPLIER
        )
        self._cap_multiply_threshold = data_context.get_config(
            self.CAP_MULTIPLY_THRESHOLD_CONFIG_KEY, self.CAP_MULTIPLY_THRESHOLD
        )

        assert self._init_cap > 0
        assert 0 < self._cap_multiply_threshold <= 1
        assert self._cap_multiplier > 1

        logger.debug(
            "ConcurrencyCapBackpressurePolicy initialized with config: "
            f"{self._init_cap}, {self._cap_multiply_threshold}, {self._cap_multiplier}"
        )

        for op, _ in topology.items():
            self._concurrency_caps[op] = self._init_cap

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        metrics = op.metrics
        while metrics.num_tasks_finished >= (
            self._concurrency_caps[op] * self._cap_multiply_threshold
        ):
            self._concurrency_caps[op] *= self._cap_multiplier
            logger.debug(
                f"Concurrency cap for {op} increased to {self._concurrency_caps[op]}"
            )
        return metrics.num_tasks_running < self._concurrency_caps[op]


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
        for op, state in list(topology.items())[::-1]:
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
