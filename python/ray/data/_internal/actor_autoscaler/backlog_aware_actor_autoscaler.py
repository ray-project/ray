import logging
import math
from typing import TYPE_CHECKING

from .autoscaling_actor_pool import AutoscalingActorPool
from .default_actor_autoscaler import DefaultActorAutoscaler

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import OpState

logger = logging.getLogger(__name__)


class BacklogAwareActorAutoscaler(DefaultActorAutoscaler):
    """Actor autoscaler sizing the pool off the enqueued input backlog.

    ``DefaultActorAutoscaler`` derives the upscaling delta purely from current
    utilization, so a pool with a large queue of unprocessed inputs still ramps
    up proportionally to its current size. This variant instead sizes the pool
    to the work it can already see (in-flight tasks plus the tasks the enqueued
    blocks will turn into), which lets it reach the required size in one step.

    NOTE: ``AutoscalingConfig.actor_pool_max_upscaling_delta`` defaults to 1 and
    truncates the delta computed here, so enabling this autoscaler alone is a
    no-op. Raise (or unset) that cap alongside it.
    """

    def _compute_upscale_delta(
        self, actor_pool: AutoscalingActorPool, op_state: "OpState"
    ) -> int:
        # Total tasks includes both in-flight tasks and tasks needed to
        # process enqueued backlog blocks
        total_tasks = actor_pool.num_tasks_in_flight() + _estimate_expected_tasks(
            op_state
        )
        max_concurrency = actor_pool.max_actor_concurrency()
        threshold = self._actor_pool_scaling_up_threshold
        current_size = actor_pool.current_size()

        # Pick delta such that after scale-up completes, pool utilization
        # drops to or below the upscaling threshold (assuming no new bundles):
        #   total_tasks / (max_concurrency * (current_size + delta)) <= threshold
        #   current_size + delta >= total_tasks / (max_concurrency * threshold)
        min_required_size = math.ceil(total_tasks / (max_concurrency * threshold))

        return min_required_size - current_size


def _estimate_expected_tasks(
    op_state: "OpState",
) -> int:
    # Each task consumes `average_num_inputs_per_task` input blocks on average,
    # so the total expected number of tasks:
    #
    #   ceil(num enqueued blocks / avg_inputs_per_task)
    #
    avg_input_blocks_per_task = op_state.op.metrics.average_num_inputs_per_task or 1
    return math.ceil(op_state.total_enqueued_input_blocks() / avg_input_blocks_per_task)
