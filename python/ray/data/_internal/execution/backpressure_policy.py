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

    def calcuate_max_bytes_to_read_per_op(
        self, topology: "Topology"
    ) -> Dict["OpState", int]:
        """Determine how many bytes of data we can read from each operator's
        OpDataTask. See `streaming_executor_state.process_completed_tasks`.

        Returns: A dict mapping from each operator's OpState to the desired bytes to
            read. For operators that are not in the dict, all available blocks will be
            read.

        Note: Only one backpressure policy that implements this method can be enabled
            at a time.
        """
        return {}

    def can_run(self, op: "PhysicalOperator") -> bool:
        """Called when StreamingExecutor selects an operator to run in
        `streaming_executor_state.select_operator_to_run()`.

        Returns: True if the operator can run, False otherwise.
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

    def can_run(self, op: "PhysicalOperator") -> bool:
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
      `MAX_STREAMING_GEN_BUFFER_SIZE_BYTES` to limit the output buffer size
      of streaming generators. When it's reached, the task will be blocked at `yield`.
    - At the Ray Data level, we use
      `MAX_OP_OUTPUT_BUFFER_SIZE_BYTES` to limit the output buffer size of the
      operators. When it's reached, we'll stop reading from the tasks' output streaming
      generators, and thus trigger backpressure at the Ray Core level.
    """

    # TODO(hchen): Can we merge the following two configs? To do so,
    # we may need to make the Ray core-level streaming generators aware of
    # the app-level buffer size.

    # The max size of the output buffer at the Ray Core streaming generator level.
    # This will be used to set the `_streaming_generator_backpressure_size_bytes`
    # parameter.
    MAX_STREAMING_GEN_BUFFER_SIZE_BYTES = 1 * 1024 * 1024 * 1024
    MAX_STREAMING_GEN_BUFFER_SIZE_BYTES_CONFIG_KEY = (
        "backpressure_policies.streaming_output.max_streaming_gen_buffer_size_bytes"
    )
    # The max size of the output buffer at the Ray Data operator level.
    # I.e., the max size of `OpState.outqueue`.
    MAX_OP_OUTPUT_BUFFER_SIZE_BYTES = 1 * 1024 * 1024 * 1024
    MAX_OP_OUTPUT_BUFFER_SIZE_BYTES_CONFIG_KEY = (
        "backpressure_policies.streaming_output.max_op_output_buffer_size_bytes"
    )

    def __init__(self, topology: "Topology"):
        data_context = ray.data.DataContext.get_current()
        self._max_streaming_gen_output_buffer_size_bytes = data_context.get_config(
            self.MAX_STREAMING_GEN_BUFFER_SIZE_BYTES_CONFIG_KEY,
            self.MAX_STREAMING_GEN_BUFFER_SIZE_BYTES,
        )
        data_context._task_pool_data_task_remote_args[
            "_streaming_generator_backpressure_size_bytes"
        ] = self._max_streaming_gen_output_buffer_size_bytes

        self._max_op_output_buffer_size_bytes = data_context.get_config(
            self.MAX_OP_OUTPUT_BUFFER_SIZE_BYTES_CONFIG_KEY,
            self.MAX_OP_OUTPUT_BUFFER_SIZE_BYTES,
        )

    def calcuate_max_bytes_to_read_per_op(
        self, topology: "Topology"
    ) -> Dict["OpState", int]:
        max_bytes_to_read_per_op: Dict["OpState", int] = {}
        downstream_num_active_tasks = 0
        for op, state in list(topology.items())[::-1]:
            max_bytes_to_read_per_op[state] = (
                self._max_op_output_buffer_size_bytes - state.outqueue_memory_usage()
            )
            if downstream_num_active_tasks == 0:
                # If all downstream operators are idle, it could be because no resources
                # are available. In this case, we'll make sure to read at least one block
                # to avoid deadlock.
                max_bytes_to_read_per_op[state] = max(
                    max_bytes_to_read_per_op[state],
                    1,
                )
            downstream_num_active_tasks += len(op.get_active_tasks())
        return max_bytes_to_read_per_op
