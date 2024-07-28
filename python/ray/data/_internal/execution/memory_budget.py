import logging
import time

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager

logger = logging.getLogger(__name__)


def humanize(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


# @lsf
class GlobalMemoryBudget:
    def __init__(self, initial_budget: float):
        self._budget = initial_budget
        self._last_replenish_time = -1
        logger.debug("@lsf Initial global memory budget is", humanize(self._budget))

    def get(self) -> float:
        return self._budget

    def can_spend(self, amount: float) -> bool:
        return self._budget >= amount

    def spend(self, amount: float):
        if not self.can_spend(amount):
            raise ValueError("Insufficient global memory budget")
        self._budget -= amount
        return self._budget

    def spend_if_available(self, amount: float) -> bool:
        if self.can_spend(amount):
            self.spend(amount)
            return True
        return False

    def replenish(self):
        self._budget = self._budget
        logger.debug("@lsf Replenished memory budget to", humanize(self._budget))


# @mzm
class PerOpMemoryBudget:
    def __init__(self):
        self._budget = -1
        self._last_replenish_time = -1

    def get(self) -> float:
        return self._budget

    def can_spend(self, amount: float) -> bool:
        return self._budget >= amount

    def spend(self, amount: float):
        if not self.can_spend(amount):
            raise ValueError("Insufficient memory budget")
        self._budget -= amount
        return self._budget

    def spend_if_available(self, amount: float) -> bool:
        if self.can_spend(amount):
            self.spend(amount)
            return True
        return False

    def replenish(self, op, resource_manager: "ResourceManager") -> float:
        # Initialize output_budget to object_store_memory.
        INITIAL_BUDGET = resource_manager.get_global_limits().object_store_memory
        if self._budget == -1:
            self._budget = INITIAL_BUDGET
            self._last_replenish_time = time.time()
            return

        grow_rate = self._get_grow_rate(op, resource_manager)
        now = time.time()
        time_elapsed = now - self._last_replenish_time

        self._budget += time_elapsed * grow_rate
        # Cap output_budget to object_store_memory
        self._budget = min(INITIAL_BUDGET, self._budget)
        logger.debug(
            f"@mzm INITIAL_BUDGET: {humanize(INITIAL_BUDGET)}, "
            f"self.output_budget: {self._budget}, "
            f"time elapsed: {time_elapsed} "
            f"grow_rate: {grow_rate}"
        )
        self._last_replenish_time = now

    def _get_grow_rate(self, op, resource_manager: "ResourceManager") -> float:
        time_for_pipeline_to_process_one_data = 0
        next_op = op
        output_input_multipler = 1
        time_for_op = 0

        while len(next_op.output_dependencies) > 0:
            assert len(next_op.output_dependencies) == 1

            next_op = next_op.output_dependencies[0]

            # Initialize grow rate to be 0.
            if (
                not next_op._metrics.average_task_duration
                or not next_op._metrics.average_bytes_inputs_per_task
                or not next_op._metrics.average_bytes_outputs_per_task
            ):
                continue

            time_for_op += (
                output_input_multipler
                * next_op._metrics.average_task_duration
                / next_op._metrics.average_bytes_inputs_per_task
            )

            output_input_multipler *= (
                next_op._metrics.average_bytes_outputs_per_task
                / next_op._metrics.average_bytes_inputs_per_task
            )

            if next_op.incremental_resource_usage().cpu == 0:
                # @MaoZiming: if it is on GPU.
                # However, time_for_op still accumulates.
                # If the last stage is on GPU, then you don't have to care.
                continue
            time_for_pipeline_to_process_one_data += time_for_op
            time_for_op = 0

        num_executors_not_running_op = (
            resource_manager.get_global_limits().cpu
            - op.num_active_tasks() * op.incremental_resource_usage().cpu
        )

        if time_for_pipeline_to_process_one_data == 0:
            return 0

        return (
            1 / time_for_pipeline_to_process_one_data
        ) * num_executors_not_running_op
