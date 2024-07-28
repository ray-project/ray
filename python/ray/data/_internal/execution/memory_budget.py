import logging
import time
from typing import TYPE_CHECKING

import ray
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager

logger = logging.getLogger(__name__)


def humanize(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.2f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


# @lsf
class GlobalMemoryBudget:
    def __init__(self, initial_budget: float):
        self._initial_budget = initial_budget
        self._budget = initial_budget
        self._last_replenish_time = -1
        logger.debug(f"@lsf Initial global memory budget is {humanize(self._budget)}")

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

    def global_replenish(self, resource_manager: "ResourceManager", topology):
        grow_rate = _get_global_growth_rate(resource_manager, topology)
        now = time.time()
        time_elapsed = now - self._last_replenish_time

        self._budget += time_elapsed * grow_rate
        # Cap output_budget to object_store_memory
        self._budget = min(self._initial_budget, self._budget)
        logger.debug(
            f"@lsf INITIAL_BUDGET: {humanize(self._initial_budget)}, "
            f"self.output_budget: {humanize(self._budget)}, "
            f"time elapsed: {time_elapsed:.2f}s, "
            f"replenish rate: {humanize(grow_rate)}/s"
        )
        self._last_replenish_time = now


def tasks_recently_completed(metrics, duration) -> tuple[int, float]:
    """Return the number of tasks that started running recently, and the time span
    between the first task started running and now."""
    now = time.time()
    time_start = now - duration
    first_task_start = now

    num_tasks = 0
    for t in metrics._running_tasks_start_time.values():
        if t >= time_start:
            num_tasks += 1
            first_task_start = min(first_task_start, t)

    return num_tasks, now - first_task_start


def is_first_op(op) -> bool:
    return len(op.input_dependencies) == 1 and isinstance(
        op.input_dependencies[0], InputDataBuffer
    )


def _get_global_growth_rate(resource_manager: "ResourceManager", topology):
    DURATION = 30  # consider tasks in the last 30 seconds

    ret = 0
    for op, _state in topology.items():
        task_input_size = op._metrics.average_bytes_inputs_per_task
        if task_input_size is None:
            if is_first_op(op):
                task_input_size = 0
            else:
                task_input_size = (
                    ray.data.DataContext.get_current().target_max_block_size
                )

        num_completed, timespan = tasks_recently_completed(op._metrics, DURATION)
        if num_completed > 0:
            conservative_rate = num_completed / timespan
        else:
            conservative_rate = 0

        optimistic_rate = 0
        task_duration = op._metrics.average_task_duration
        num_slots = resource_manager.get_global_limits().cpu
        if task_duration is not None:
            optimistic_rate = num_slots / task_duration
        else:
            optimistic_rate = 0

        conservative_rate = (
            conservative_rate if conservative_rate > 0 else optimistic_rate
        )
        optimistic_rate = optimistic_rate if optimistic_rate > 0 else conservative_rate
        avg_rate = (optimistic_rate + conservative_rate) / 2
        logger.debug(
            f"@lsf {op.name} input size {humanize(task_input_size)} "
            f"optimistic {optimistic_rate:.2f}/s conservative {conservative_rate:.2f}/s avg {avg_rate:.2f}/s"
        )

        ret += task_input_size * avg_rate

    return ret


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

    def replenish(self, op, resource_manager: "ResourceManager"):
        # Initialize output_budget to object_store_memory.
        INITIAL_BUDGET = resource_manager.get_global_limits().object_store_memory
        if self._budget == -1:
            self._budget = INITIAL_BUDGET
            self._last_replenish_time = time.time()
            return

        grow_rate = _get_per_op_grow_rate(op, resource_manager)
        now = time.time()
        time_elapsed = now - self._last_replenish_time

        self._budget += time_elapsed * grow_rate
        # Cap output_budget to object_store_memory
        self._budget = min(INITIAL_BUDGET, self._budget)
        logger.debug(
            f"@mzm INITIAL_BUDGET: {humanize(INITIAL_BUDGET)}, "
            f"self.output_budget: {humanize(self._budget)}, "
            f"time elapsed: {time_elapsed:.2f}s, "
            f"grow_rate: {humanize(grow_rate)}/s"
        )
        self._last_replenish_time = now


def _get_per_op_grow_rate(op, resource_manager: "ResourceManager") -> float:
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

    return (1 / time_for_pipeline_to_process_one_data) * num_executors_not_running_op
