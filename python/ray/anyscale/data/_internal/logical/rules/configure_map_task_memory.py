from typing import Optional

from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.rules.configure_map_task_memory import (
    ConfigureMapTaskMemoryRule,
)


class ConfigureMapTaskMemoryWithProfiling(ConfigureMapTaskMemoryRule):
    def estimate_per_task_memory_requirement(self, op: MapOperator) -> Optional[int]:
        return (
            op.metrics.average_max_uss_per_task or op.metrics.average_bytes_per_output
        )
