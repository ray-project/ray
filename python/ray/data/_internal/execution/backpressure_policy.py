import logging
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor_state import Topology

logger = logging.getLogger(__name__)


# TODO(hchen): Enable ConcurrencyCapBackpressurePolicy by default.
DEFAULT_BACKPRESSURE_PLOCIES = []


def get_backpressure_policies(topology: "Topology"):
    return [policy(topology) for policy in DEFAULT_BACKPRESSURE_PLOCIES]


class BackpressurePolicy(ABC):
    """Interface for back pressure policies."""

    @abstractmethod
    def __init__(self, topology: "Topology"):
        ...

    @abstractmethod
    def can_run(self, op: "PhysicalOperator") -> bool:
        """Called when StreamingExecutor is about to select an operator to run.
        Returns True if the operator can run, False otherwise.
        """
        ...


class ConcurrencyCapBackpressurePolicy(BackpressurePolicy):
    """A backpressure policy that caps the concurrency of each operator.

    The concurrency cap limits the number of concurrently running tasks.
    It will be set to an intial value, and will ramp up exponentially.

    The concrete stategy is as follows:
    - Each op is assigned an initial concurrency cap.
    - An op can run new tasks if the number of running tasks is less than the cap.
    - When the number of finished tasks reaches a threshold, the concurrency cap will
      increase.
    """

    # Environment variable to configure this policy.
    # The format is: "<init_cap>,<cap_multiply_threshold>,<cap_multiplier>"
    CONFIG_ENV_VAR = "RAY_DATA_CONCURRENCY_CAP_CONFIG"

    # The intial concurrency cap for each operator.
    INIT_CAP = 4
    # When the number of finished tasks reaches this threshold, the concurrency cap
    # will be multiplied by the multiplier.
    CAP_MULTIPLY_THRESHOLD = 0.5
    # The multiplier to multiply the concurrency cap by.
    CAP_MULTIPLIER = 2.0

    def __init__(self, topology: "Topology"):
        self._concurrency_caps: dict["PhysicalOperator", float] = {}

        self._init_cap = self.INIT_CAP
        self._cap_multiplier = self.CAP_MULTIPLIER
        self._cap_multiply_threshold = self.CAP_MULTIPLY_THRESHOLD

        env_config = os.environ.get(self.CONFIG_ENV_VAR, "")
        if env_config:
            try:
                configs = env_config.split(",")
                self._init_cap = int(configs[0])
                self._cap_multiply_threshold = float(configs[1])
                self._cap_multiplier = float(configs[2])
                assert self._init_cap > 0
                assert 0 < self._cap_multiply_threshold <= 1
                assert self._cap_multiplier > 1
            except Exception as e:
                raise ValueError("Invalid concurrency cap config", env_config) from e

        logger.debug(
            "Concurrency cap config: "
            f"{self._init_cap}, {self._cap_multiply_threshold}, {self._cap_multiplier}"
        )

        for op, _ in topology.items():
            self._concurrency_caps[op] = self._init_cap

    def can_run(self, op: "PhysicalOperator") -> bool:
        metrics = op.metrics
        if metrics.num_tasks_finished >= (
            self._concurrency_caps[op] * self._cap_multiply_threshold
        ):
            self._concurrency_caps[op] *= self._cap_multiplier
            logger.debug(
                f"Concurrency cap for {op} increased to {self._concurrency_caps[op]}"
            )
        return metrics.num_tasks_running < self._concurrency_caps[op]
