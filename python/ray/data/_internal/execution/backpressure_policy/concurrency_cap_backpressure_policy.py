import logging
from typing import TYPE_CHECKING

import ray
from .backpressure_policy import BackpressurePolicy

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)


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
        assert self._cap_multiplier >= 1

        logger.debug(
            "ConcurrencyCapBackpressurePolicy initialized with config: "
            f"{self._init_cap}, {self._cap_multiply_threshold}, {self._cap_multiplier}"
        )

        for op, _ in topology.items():
            self._concurrency_caps[op] = self._init_cap

    def can_add_input(self, op: "PhysicalOperator") -> bool:
        metrics = op.metrics
        while self._cap_multiplier > 1 and metrics.num_tasks_finished >= (
            self._concurrency_caps[op] * self._cap_multiply_threshold
        ):
            self._concurrency_caps[op] *= self._cap_multiplier
            logger.debug(
                f"Concurrency cap for {op} increased to {self._concurrency_caps[op]}"
            )
        return metrics.num_tasks_running < self._concurrency_caps[op]
