from __future__ import annotations

import logging
import math
import sys
from typing import TYPE_CHECKING, Optional

from .autoscaling_actor_pool import (
    ActorPoolScalingRequest,
    AutoscalingActorPool,
    drain_floor,
)
from .base_actor_autoscaler import ActorAutoscaler
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data.context import WARN_PREFIX, AutoscalingConfig, DataContext

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology

logger = logging.getLogger(__name__)


def _resolve_upscale_gate(data_context: DataContext) -> bool:
    """Whether growth requires the pool's output to be consumable.

    Growing a pool whose output is not being taken only re-creates the backlog
    that parked its tasks (see `ResourceManager.downstream_can_absorb`), so the
    gate is part of the drain feature: it follows ``actor_pool_drain_window_s``,
    meaning a pipeline that never opted into draining keeps its previous
    autoscaling behaviour.

    ``data_context`` is the one the *executor* was built with, and has to be passed
    in rather than read from ``DataContext.get_current()`` -- which
    ``DataContext`` itself tells internal components not to do. It also matters
    beyond style: the executor's context is a snapshot copy taken when the Dataset
    was created (see the read APIs), while ``get_current()`` is the live ambient
    one. Reading the latter would let the gate be on for a pipeline whose drain
    policy -- built from the snapshot -- is inert, which is exactly the coupling
    this default is meant to express.
    """
    return data_context.actor_pool_drain_window_s is not None


class DefaultActorAutoscaler(ActorAutoscaler):
    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        *,
        config: AutoscalingConfig,
        data_context: DataContext,
    ):
        super().__init__(topology, resource_manager)

        self._actor_pool_scaling_up_threshold: float = (
            config.actor_pool_util_upscaling_threshold
        )
        self._actor_pool_scaling_down_threshold: float = (
            config.actor_pool_util_downscaling_threshold
        )
        self._actor_pool_max_upscaling_delta: Optional[
            int
        ] = config.actor_pool_max_upscaling_delta
        self._upscale_requires_downstream_capacity: bool = _resolve_upscale_gate(
            data_context
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

    def _derive_target_scaling_config(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
        op_state: "OpState",
    ) -> ActorPoolScalingRequest:
        # If all inputs have been consumed, short-circuit. While a drain is in
        # progress, leave the decision to the drain branch below: by the time the
        # stall window fills, all inputs are commonly already dispatched, and the
        # teardown shortcut would otherwise preempt the drain every time. The
        # shortcut takes over again once the policy releases.
        if not op.actor_pool_drain_in_progress and (
            op.has_completed()
            or (op._inputs_complete and op_state.total_enqueued_input_blocks() == 0)
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

        allocation = self._resource_manager.get_allocation(op)
        op_usage = self._resource_manager.get_op_usage(op)
        if allocation is not None and op_usage is not None:
            over_budget_scale_down = _get_required_scale_down(
                actor_pool, allocation.subtract(op_usage)
            )
            if over_budget_scale_down > 0:
                max_can_release = actor_pool.current_size() - actor_pool.min_size()
                num_to_scale_down = min(over_budget_scale_down, max_can_release)
                if num_to_scale_down > 0:
                    return ActorPoolScalingRequest.downscale(
                        delta=-num_to_scale_down,
                        reason="actor pool exceeds resource allocation",
                    )
                return ActorPoolScalingRequest.no_op(
                    reason="actor pool exceeds resource allocation "
                    "but cannot scale below min size",
                )

        if op.actor_pool_drain_in_progress:
            # The operator is withholding inputs so its tasks can drain and its
            # actors can be reclaimed. Its utilization is useless here -- the tasks
            # that keep it high are parked on the output budget, not doing work --
            # so hand actors back without waiting for utilization to drop. This is
            # checked after the allocation case: an operator that is over its
            # resource allocation has to release down to that allocation first, even
            # while draining (the allocation path is a hard resource constraint).
            #
            # The pool is only walked down to its *drain floor*: `min_size` as
            # before, or the adaptive ceiling maintained by `ActorPoolDrainPolicy`
            # (see `AutoscalingActorPool.set_dynamic_max_size`) once it has been
            # stepped down -- shrinking past the ceiling would hand back actors the
            # operator needs again as soon as the blockage clears.
            drain_target = drain_floor(actor_pool)
            if actor_pool.current_size() <= drain_target:
                reason = (
                    "reached min size"
                    if actor_pool.dynamic_max_size() is None
                    else "reached drain target"
                )
                return ActorPoolScalingRequest.no_op(reason=reason)
            max_can_release = actor_pool.current_size() - drain_target
            num_to_scale_down = min(
                self._compute_downscale_delta(actor_pool), max_can_release
            )
            # Forced: the cool-off exists so a freshly scaled-up pool gets a chance
            # to be utilized, but a drain already established that more actors
            # cannot help this operator.
            return ActorPoolScalingRequest.downscale(
                delta=-num_to_scale_down,
                force=True,
                reason="draining a stalled actor pool",
            )

        # To prevent unexpected downscaling from the initial size, short-circuit if
        # the operator hasn't received any inputs.
        if op.metrics.num_inputs_received == 0:
            return ActorPoolScalingRequest.no_op(reason="no inputs received")

        # Determine whether to scale up based on the actor pool utilization.
        util = actor_pool.get_pool_util()

        if util >= self._actor_pool_scaling_up_threshold:
            # Do not scale up if either
            #   - Actor Pool is at max size already
            #   - Op is throttled (ie exceeding allocated resource quota)
            if actor_pool.current_size() >= actor_pool.max_size():
                return ActorPoolScalingRequest.no_op(reason="reached max size")
            if not op_state._scheduling_status.under_resource_limits:
                return ActorPoolScalingRequest.no_op(
                    reason="operator exceeding resource quota"
                )

            budget = self._resource_manager.get_budget(op)
            budget_max_scale_up = (
                _get_max_scale_up(actor_pool, budget) if budget else sys.maxsize
            )

            # Adaptive ceiling maintained by `ActorPoolDrainPolicy`: while the
            # operator keeps producing output its downstream cannot read, the
            # ceiling is stepped down; it is stepped back up as the blockage
            # clears. Growing past it would immediately re-create the backlog
            # that triggered the drain in the first place.
            effective_max_size = actor_pool.effective_max_size()
            if actor_pool.current_size() >= effective_max_size:
                return ActorPoolScalingRequest.no_op(
                    reason="reached dynamic max size (drain cap)"
                )

            # Determine maximum available scale up based on
            #   - Maximum available resource budget
            #   - Configured max scale-up delta (or "+inf" if not configured)
            #   - Total # of actors needed to reach the effective `max_size`
            max_scale_up: int = min(
                budget_max_scale_up,
                self._get_actor_pool_max_upscaling_delta(),
                effective_max_size - actor_pool.current_size(),
            )

            if max_scale_up == 0:
                return ActorPoolScalingRequest.no_op(reason="exceeded resource limits")

            if util == float("inf"):
                return ActorPoolScalingRequest.upscale(
                    delta=1, reason="no running actors, scale up immediately"
                )

            # Growth is only useful if the pool's output can actually be taken: an
            # operator whose tasks are parked on the output budget produces nothing
            # by adding actors, it only re-creates the backlog. Liveness is
            # preserved -- a downstream that has run dry always allows growth.
            if self._upscale_requires_downstream_capacity and not (
                self._resource_manager.downstream_can_absorb(op)
            ):
                return ActorPoolScalingRequest.no_op(
                    reason="downstream cannot absorb more output"
                )

            delta = self._compute_upscale_delta(actor_pool, op_state)
            # At least scale up by 1
            delta = max(1, delta)
            # Cap delta
            delta = min(delta, max_scale_up)

            return ActorPoolScalingRequest.upscale(
                delta=delta,
                reason=(
                    f"utilization of {util} >= "
                    f"{self._actor_pool_scaling_up_threshold}"
                ),
            )
        elif util <= self._actor_pool_scaling_down_threshold:
            if actor_pool.num_pending_actors() > 0:
                return ActorPoolScalingRequest.no_op(
                    reason="no downscaling while actors are pending"
                )
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

    def _get_actor_pool_max_upscaling_delta(self) -> int:
        return (
            self._actor_pool_max_upscaling_delta
            if self._actor_pool_max_upscaling_delta is not None
            else sys.maxsize
        )

    def _validate_autoscaling_config(self):
        # Validate that max upscaling delta is positive to prevent override by safeguard
        if (
            self._actor_pool_max_upscaling_delta is not None
            and self._actor_pool_max_upscaling_delta <= 0
        ):
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
        # Fixed-size pools don't autoscale by design
        if actor_pool.min_size() == actor_pool.max_size():
            return

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

    def _compute_upscale_delta(
        self, actor_pool: AutoscalingActorPool, op_state: OpState
    ) -> int:
        # Calculate desired delta based on utilization
        return math.ceil(
            actor_pool.current_size()
            * (actor_pool.get_pool_util() / self._actor_pool_scaling_up_threshold - 1)
        )

    def _compute_downscale_delta(self, actor_pool: "AutoscalingActorPool") -> int:
        return 1


def _estimate_total_available_task_slots(actor_pool: "AutoscalingActorPool") -> int:
    # Estimates number of available task slots to schedule new tasks
    #
    # NOTE: This must include pending actors for estimation to make sure
    #       autoscaler appropriately accounts task slots that will be available
    #       once pending actors become running.
    return (
        actor_pool.max_tasks_in_flight_per_actor() * actor_pool.current_size()
        - actor_pool.num_tasks_in_flight()
    )


def _get_max_scale_up(
    actor_pool: AutoscalingActorPool,
    budget: ExecutionResources,
) -> int:
    """Get the maximum number of actors that can be scaled up.

    Args:
        actor_pool: The actor pool to scale up.
        budget: The budget to scale up.

    Returns:
        The maximum number of actors that can be scaled up, or `None` if you can
        scale up infinitely.
    """
    assert budget.cpu >= 0 and budget.gpu >= 0 and budget.memory >= 0

    per_actor = actor_pool.per_actor_resource_usage()
    assert per_actor.cpu >= 0 and per_actor.gpu >= 0 and per_actor.memory >= 0

    # floordiv handles per_actor.x == 0 → inf (no constraint from that resource)
    # and budget.x == inf → inf. We ignore object_store_memory since it is not
    # a per-actor declared resource.
    divisions = budget.floordiv(per_actor)
    max_scale_up = min(divisions.cpu, divisions.gpu, divisions.memory)
    if math.isinf(max_scale_up):
        return sys.maxsize
    return int(max_scale_up)


def _get_required_scale_down(
    actor_pool: AutoscalingActorPool,
    budget: ExecutionResources,
) -> int:
    """Get the number of actors that must be removed to fit within budget.

    Args:
        actor_pool: The actor pool to scale down.
        budget: The net remaining budget (allocation - usage). Can be negative
            if the operator is over its allocation.

    Returns:
        The number of actors that need to be removed, or 0 if the pool
        is within budget.
    """
    per_actor = actor_pool.per_actor_resource_usage()

    required_cpu_scale_down = 0
    if per_actor.cpu > 0 and budget.cpu < 0:
        required_cpu_scale_down = math.ceil(abs(budget.cpu) / per_actor.cpu)

    required_gpu_scale_down = 0
    if per_actor.gpu > 0 and budget.gpu < 0:
        required_gpu_scale_down = math.ceil(abs(budget.gpu) / per_actor.gpu)

    required_memory_scale_down = 0
    if per_actor.memory > 0 and budget.memory < 0:
        required_memory_scale_down = math.ceil(abs(budget.memory) / per_actor.memory)

    return max(
        required_cpu_scale_down, required_gpu_scale_down, required_memory_scale_down
    )
