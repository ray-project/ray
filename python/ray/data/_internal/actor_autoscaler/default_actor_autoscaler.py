import logging
import math
from typing import TYPE_CHECKING, Optional

from .actor_pool_resizing_policy import (
    ActorPoolResizingPolicy,
    DefaultResizingPolicy,
)
from .autoscaling_actor_pool import ActorPoolScalingRequest, AutoscalingActorPool
from .base_actor_autoscaler import ActorAutoscaler
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data.context import WARN_PREFIX, AutoscalingConfig

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology

logger = logging.getLogger(__name__)


class DefaultActorAutoscaler(ActorAutoscaler):
    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        *,
        config: AutoscalingConfig,
        actor_pool_resizing_policy: Optional[ActorPoolResizingPolicy] = None,
    ):
        super().__init__(topology, resource_manager)

        self._actor_pool_scaling_up_threshold = (
            config.actor_pool_util_upscaling_threshold
        )
        self._actor_pool_scaling_down_threshold = (
            config.actor_pool_util_downscaling_threshold
        )
        self._actor_pool_max_upscaling_delta = config.actor_pool_max_upscaling_delta

        self._actor_pool_resizing_policy = (
            actor_pool_resizing_policy
            or DefaultResizingPolicy(
                upscaling_threshold=config.actor_pool_util_upscaling_threshold,
                max_upscaling_delta=config.actor_pool_max_upscaling_delta,
            )
        )

        self._validate_autoscaling_config()

    def try_trigger_scaling(self):
        for op, state in self._topology.items():
            actor_pools = op.get_autoscaling_actor_pools()
            for actor_pool in actor_pools:
                # Trigger auto-scaling
                actor_pool.scale(
                    self._derive_target_scaling_config(actor_pool, op, state)
                )

    def _compute_utilization(self, actor_pool: AutoscalingActorPool) -> float:
        """Compute the utilization of the actor pool.

        Args:
            actor_pool: The actor pool to compute utilization for.

        Returns:
            The utilization value. Can be infinite if the actor pool has no running actors.
        """
        return actor_pool.get_pool_util()

    def _compute_upscale_delta(
        self,
        actor_pool: AutoscalingActorPool,
        util: float,
    ) -> int:
        """Compute how many actors to add when scaling up.

        Args:
            actor_pool: The actor pool to scale.
            util: The current utilization of the actor pool.

        Returns:
            The number of actors to add (must be >= 1).
        """
        return self._actor_pool_resizing_policy.compute_upscale_delta(actor_pool, util)

    def _compute_downscale_delta(self, actor_pool: AutoscalingActorPool) -> int:
        """Compute how many actors to remove when scaling down.

        Args:
            actor_pool: The actor pool to scale down.

        Returns:
            The number of actors to remove (must be >= 1).
        """
        return self._actor_pool_resizing_policy.compute_downscale_delta(actor_pool)

    def _derive_target_scaling_config(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
        op_state: "OpState",
    ) -> ActorPoolScalingRequest:
        # If all inputs have been consumed, short-circuit
        if op.completed() or (
            op._inputs_complete and op_state.total_enqueued_input_blocks() == 0
        ):
            num_to_scale_down = self._compute_downscale_delta(actor_pool)
            return ActorPoolScalingRequest.downscale(
                delta=-num_to_scale_down, force=True, reason="consumed all inputs"
            )

        if actor_pool.current_size() < actor_pool.min_size():
            # Scale up, if the actor pool is below min size.
            return ActorPoolScalingRequest.upscale(
                delta=actor_pool.min_size() - actor_pool.current_size(),
                reason="pool below min size",
            )
        elif actor_pool.current_size() > actor_pool.max_size():
            return ActorPoolScalingRequest.downscale(
                delta=-(actor_pool.current_size() - actor_pool.max_size()),
                reason="pool exceeding max size",
            )

        # Determine whether to scale up based on the actor pool utilization.
        util = self._compute_utilization(actor_pool)

        if util >= self._actor_pool_scaling_up_threshold:
            average_num_inputs_per_task = op.metrics.average_num_inputs_per_task or 1
            # Skip scaling up if the current free task slots can handle all pending work.
            # Each task consumes `average_num_inputs_per_task` input blocks on average,
            # so the total input capacity is: free_slots × avg_inputs_per_task.
            # If enqueued blocks ≤ capacity, we have enough resources already.
            if (
                op_state.total_enqueued_input_blocks()
                <= actor_pool.num_free_task_slots() * average_num_inputs_per_task
            ):
                return ActorPoolScalingRequest.no_op(
                    reason="enough free task slots to consume the existing inputs"
                )

            # Do not scale up if either
            #   - Previous scale up has not finished yet
            #   - Actor Pool is at max size already
            #   - Op is throttled (ie exceeding allocated resource quota)
            if actor_pool.num_pending_actors() > 0:
                return ActorPoolScalingRequest.no_op(reason="pending actors")
            elif actor_pool.current_size() >= actor_pool.max_size():
                return ActorPoolScalingRequest.no_op(reason="reached max size")
            if not op_state._scheduling_status.under_resource_limits:
                return ActorPoolScalingRequest.no_op(
                    reason="operator exceeding resource quota"
                )
            budget = self._resource_manager.get_budget(op)
            max_scale_up = _get_max_scale_up(actor_pool, budget)
            if max_scale_up == 0:
                return ActorPoolScalingRequest.no_op(reason="exceeded resource limits")
            if util == float("inf"):
                return ActorPoolScalingRequest.upscale(
                    delta=1, reason="no running actors, scale up immediately"
                )
            delta = self._compute_upscale_delta(actor_pool, util)
            if max_scale_up is not None:
                delta = min(delta, max_scale_up)
            delta = max(1, delta)  # At least scale up by 1

            return ActorPoolScalingRequest.upscale(
                delta=delta,
                reason=(
                    f"utilization of {util} >= "
                    f"{self._actor_pool_scaling_up_threshold}"
                ),
            )
        elif util <= self._actor_pool_scaling_down_threshold:
            if actor_pool.current_size() <= actor_pool.min_size():
                return ActorPoolScalingRequest.no_op(reason="reached min size")

            max_can_release = actor_pool.current_size() - actor_pool.min_size()
            num_to_scale_down = min(
                self._compute_downscale_delta(actor_pool), max_can_release
            )

            return ActorPoolScalingRequest.downscale(
                delta=-num_to_scale_down,
                reason=(
                    f"utilization of {util} <= "
                    f"{self._actor_pool_scaling_down_threshold}"
                ),
            )
        else:
            return ActorPoolScalingRequest.no_op(
                reason=(
                    f"utilization of {util} w/in limits "
                    f"[{self._actor_pool_scaling_down_threshold}, "
                    f"{self._actor_pool_scaling_up_threshold}]"
                )
            )

    def _validate_autoscaling_config(self):
        # Validate that max upscaling delta is positive to prevent override by safeguard
        if self._actor_pool_max_upscaling_delta <= 0:
            raise ValueError(
                f"actor_pool_max_upscaling_delta must be positive, "
                f"got {self._actor_pool_max_upscaling_delta}"
            )
        # Validate that upscaling threshold is positive to prevent division by zero
        # and incorrect scaling calculations
        if self._actor_pool_scaling_up_threshold <= 0:
            raise ValueError(
                f"actor_pool_util_upscaling_threshold must be positive, "
                f"got {self._actor_pool_scaling_up_threshold}"
            )

        for op, state in self._topology.items():
            for actor_pool in op.get_autoscaling_actor_pools():
                self._validate_actor_pool_autoscaling_config(actor_pool, op)

    def _validate_actor_pool_autoscaling_config(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
    ) -> None:
        """Validate autoscaling configuration.

        Args:
            actor_pool: Actor pool to validate configuration thereof.
            op: ``PhysicalOperator`` using target actor pool.
        """
        max_tasks_in_flight_per_actor = actor_pool.max_tasks_in_flight_per_actor()
        max_concurrency = actor_pool.max_actor_concurrency()

        if (
            max_tasks_in_flight_per_actor / max_concurrency
            < self._actor_pool_scaling_up_threshold
        ):
            logger.warning(
                f"{WARN_PREFIX} Actor Pool configuration of the {op} will not allow it to scale up: "
                f"configured utilization threshold ({self._actor_pool_scaling_up_threshold * 100}%) "
                f"couldn't be reached with configured max_concurrency={max_concurrency} "
                f"and max_tasks_in_flight_per_actor={max_tasks_in_flight_per_actor} "
                f"(max utilization will be max_tasks_in_flight_per_actor / max_concurrency = {(max_tasks_in_flight_per_actor / max_concurrency) * 100:g}%)"
            )


def _get_max_scale_up(
    actor_pool: AutoscalingActorPool,
    budget: Optional[ExecutionResources],
) -> Optional[int]:
    """Get the maximum number of actors that can be scaled up.

    Args:
        actor_pool: The actor pool to scale up.
        budget: The budget to scale up.

    Returns:
        The maximum number of actors that can be scaled up, or `None` if you can
        scale up infinitely.
    """
    if budget is None:
        return None

    assert budget.cpu >= 0 and budget.gpu >= 0

    num_cpus_per_actor = actor_pool.per_actor_resource_usage().cpu
    num_gpus_per_actor = actor_pool.per_actor_resource_usage().gpu
    assert num_cpus_per_actor >= 0 and num_gpus_per_actor >= 0

    max_cpu_scale_up: float = float("inf")
    if num_cpus_per_actor > 0 and not math.isinf(budget.cpu):
        max_cpu_scale_up = budget.cpu // num_cpus_per_actor

    max_gpu_scale_up: float = float("inf")
    if num_gpus_per_actor > 0 and not math.isinf(budget.gpu):
        max_gpu_scale_up = budget.gpu // num_gpus_per_actor

    max_scale_up = min(max_cpu_scale_up, max_gpu_scale_up)
    if math.isinf(max_scale_up):
        return None
    else:
        assert not math.isnan(max_scale_up), (
            budget,
            num_cpus_per_actor,
            num_gpus_per_actor,
        )
        return int(max_scale_up)
