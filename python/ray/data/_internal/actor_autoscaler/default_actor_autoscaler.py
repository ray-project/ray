import logging
import math
from typing import TYPE_CHECKING, Optional

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
    ):
        super().__init__(topology, resource_manager)

        self._actor_pool_scaling_up_threshold = (
            config.actor_pool_util_upscaling_threshold
        )
        self._actor_pool_scaling_down_threshold = (
            config.actor_pool_util_downscaling_threshold
        )
        self._actor_pool_max_upscaling_delta = config.actor_pool_max_upscaling_delta

        self._validate_autoscaling_config()

    def try_trigger_scaling(self):
        for op, state in self._topology.items():
            actor_pools = op.get_autoscaling_actor_pools()
            for actor_pool in actor_pools:
                # Trigger auto-scaling
                actor_pool.scale(
                    self._derive_target_scaling_config(actor_pool, op, state)
                )

    def _derive_target_scaling_config(
        self,
        actor_pool: "AutoscalingActorPool",
        op: "PhysicalOperator",
        op_state: "OpState",
    ) -> ActorPoolScalingRequest:
        # If all inputs have been consumed, short-circuit
        if op.completed() or (
            op._inputs_complete and op_state.total_enqueued_input_blocks() == 0
        ):
            return ActorPoolScalingRequest.downscale(
                delta=-1, force=True, reason="consumed all inputs"
            )

        if actor_pool.current_size() < actor_pool.min_size():
            # Scale up, if the actor pool is below min size.
            return ActorPoolScalingRequest.upscale(
                delta=actor_pool.min_size() - actor_pool.current_size(),
                reason="pool below min size",
            )
        elif actor_pool.current_size() > actor_pool.max_size():
            # Do not scale up, if the actor pool is already at max size.
            return ActorPoolScalingRequest.downscale(
                # NOTE: For scale down delta has to be negative
                delta=-(actor_pool.current_size() - actor_pool.max_size()),
                reason="pool exceeding max size",
            )

        # Determine whether to scale up based on the actor pool utilization.
        util = actor_pool.get_pool_util()
        if util >= self._actor_pool_scaling_up_threshold:
            # Do not scale up if either
            #   - Previous scale up has not finished yet
            #   - Actor Pool is at max size already
            #   - Op is throttled (ie exceeding allocated resource quota)
            #   - Actor Pool has sufficient amount of slots available to handle
            #   pending tasks
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

            # Calculate desired delta based on utilization
            plan_delta = math.ceil(
                actor_pool.current_size()
                * (util / self._actor_pool_scaling_up_threshold - 1)
            )

            upscale_capacities = self._get_upscale_capacities(actor_pool, max_scale_up)
            delta = min(
                plan_delta,
                *upscale_capacities,
            )
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

            return ActorPoolScalingRequest.downscale(
                delta=-1,
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

    def _get_upscale_capacities(
        self,
        actor_pool: "AutoscalingActorPool",
        max_scale_up: Optional[int],
    ):
        limits = []
        if max_scale_up is not None:
            limits.append(max_scale_up)
        limits.append(self._actor_pool_max_upscaling_delta)
        limits.append(actor_pool.max_size() - actor_pool.current_size())
        return limits

    def _validate_actor_pool_autoscaling_config(
        self,
        actor_pool: "AutoscalingActorPool",
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
