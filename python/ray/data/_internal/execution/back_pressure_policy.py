from abc import ABC, abstractmethod
from pyarrow.hdfs import os
from ray.data._internal.execution.streaming_executor_state import Topology

from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator


def get_back_pressure_policies(topology: Topology):
    return [ConcurrencyCapBackPressurePolicy(topology)]


class BackPressurePolicy(ABC):

    @abstractmethod
    def __init__(self, topology: Topology):
        ...

    @abstractmethod
    def can_run(self, op: PhysicalOperator) -> bool:
        ...


class ConcurrencyCapBackPressurePolicy(BackPressurePolicy):

    INITIAL_CONCURRENCY_CAP = 4
    CONCURRENCY_CAP_MULTIPLIER = 2
    CONCURRENCY_CAP_MULTIPLY_THRESHOLD = 0.5

    def __init__(self, topology: Topology):
        self._concurrency_cap: dict[PhysicalOperator, int] = {}

        self._initial_concurrency_cap = self.INITIAL_CONCURRENCY_CAP
        self._concurrency_cap_multiplier = self.CONCURRENCY_CAP_MULTIPLIER
        self._concurrency_cap_multiply_threshold = self.CONCURRENCY_CAP_MULTIPLY_THRESHOLD

        env_config = os.environ.get("RAY_DATA_CONCURRENCY_CAP_CONFIG", "")
        if env_config:
            env_config = env_config.split(",")
            assert len(env_config) == 3, "Invalid concurrency cap config", env_config
            self._initial_concurrency_cap = int(env_config[0])
            self._concurrency_cap_multiplier = int(env_config[1])
            self._concurrency_cap_multiply_threshold = float(env_config[2])

        for op, _ in topology.items():
            self._concurrency_cap[op] = self._initial_concurrency_cap

    def can_run(self, op: PhysicalOperator) -> bool:
        metrics = op.metrics
        if metrics.num_tasks_finished >= (
                self._concurrency_cap[op] *
                self._concurrency_cap_multiply_threshold):
            self._concurrency_cap[op] *= self._concurrency_cap_multiplier
        return metrics.num_tasks_running < self._concurrency_cap[op]

