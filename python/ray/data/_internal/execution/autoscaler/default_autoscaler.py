import math
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, Optional

import ray
from .autoscaler import Autoscaler
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources

if TYPE_CHECKING:
    from .autoscaling_actor_pool import AutoscalingActorPool
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


@dataclass
class _AutoscalingAction:

    delta: int
    reason: Optional[str] = field(default=None)

    @classmethod
    def no_op(cls, *, reason: Optional[str] = None) -> "_AutoscalingAction":
        return _AutoscalingAction(delta=0, reason=reason)

    @classmethod
    def up(cls, *, delta: int, reason: Optional[str] = None):
        assert delta > 0
        return _AutoscalingAction(delta=delta, reason=reason)

    @classmethod
    def down(cls, *, delta: int, reason: Optional[str] = None):
        assert delta < 0, "For scale down delta is expected to be negative!"
        return _AutoscalingAction(delta=delta, reason=reason)


class DefaultAutoscaler(Autoscaler):

    # Default threshold of actor pool utilization to trigger scaling up.
    # TODO make configurable
    DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD: float = 0.95
    # Default threshold of actor pool utilization to trigger scaling down.
    DEFAULT_ACTOR_POOL_SCALING_DOWN_THRESHOLD: float = 0.5

    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS = 20

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        execution_id: str,
        actor_pool_scaling_up_threshold: float = DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD,  # noqa: E501
        actor_pool_scaling_down_threshold: float = DEFAULT_ACTOR_POOL_SCALING_DOWN_THRESHOLD,  # noqa: E501
    ):
        self._actor_pool_scaling_up_threshold = actor_pool_scaling_up_threshold
        self._actor_pool_scaling_down_threshold = actor_pool_scaling_down_threshold
        # Last time when a request was sent to Ray's autoscaler.
        self._last_request_time = 0
        super().__init__(topology, resource_manager, execution_id)

    def try_trigger_scaling(self):
        self._try_scale_up_cluster()
        self._try_scale_up_or_down_actor_pool()

    def _derive_scaling_action(
        self,
        actor_pool: "AutoscalingActorPool",
        op: "PhysicalOperator",
        op_state: "OpState",
    ) -> _AutoscalingAction:
        # Do not scale up, if the op is completed or no more inputs are coming.
        if op.completed() or (
            op._inputs_complete and op_state.total_enqueued_input_bundles() == 0
        ):
            return _AutoscalingAction.down(delta=-1, reason="consumed all inputs")

        if actor_pool.current_size() < actor_pool.min_size():
            # Scale up, if the actor pool is below min size.
            return _AutoscalingAction.up(
                delta=actor_pool.min_size() - actor_pool.current_size(),
                reason="pool below min size",
            )
        elif actor_pool.current_size() > actor_pool.max_size():
            # Do not scale up, if the actor pool is already at max size.
            return _AutoscalingAction.down(
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
                return _AutoscalingAction.no_op(reason="pending actors")
            elif actor_pool.current_size() >= actor_pool.max_size():
                return _AutoscalingAction.no_op(reason="reached max size")
            if not op_state._scheduling_status.under_resource_limits:
                return _AutoscalingAction.no_op(
                    reason="operator exceeding resource quota"
                )

            return _AutoscalingAction.up(
                delta=1,
                reason=(
                    f"utilization of {util} >= "
                    f"{self._actor_pool_scaling_up_threshold}"
                ),
            )
        elif util <= self._actor_pool_scaling_down_threshold:
            if not actor_pool.can_scale_down():
                return _AutoscalingAction.no_op(reason="not allowed")
            elif actor_pool.current_size() <= actor_pool.min_size():
                return _AutoscalingAction.no_op(reason="reached min size")

            return _AutoscalingAction.down(
                delta=-1,
                reason=(
                    f"utilization of {util} <= "
                    f"{self._actor_pool_scaling_down_threshold}"
                ),
            )
        else:
            return _AutoscalingAction.no_op(
                reason=(
                    f"{self._actor_pool_scaling_down_threshold} < "
                    f"{util} < {self._actor_pool_scaling_up_threshold}"
                )
            )

    def _try_scale_up_or_down_actor_pool(self):
        for op, state in self._topology.items():
            actor_pools = op.get_autoscaling_actor_pools()
            for actor_pool in actor_pools:
                # Try to scale up or down the actor pool.
                recommended_action = self._derive_scaling_action(actor_pool, op, state)

                actor_pool.apply(recommended_action)

    def _try_scale_up_cluster(self):
        """Try to scale up the cluster to accomodate the provided in-progress workload.

        This makes a resource request to Ray's autoscaler consisting of the current,
        aggregate usage of all operators in the DAG + the incremental usage of all
        operators that are ready for dispatch (i.e. that have inputs queued). If the
        autoscaler were to grant this resource request, it would allow us to dispatch
        one task for every ready operator.

        Note that this resource request does not take the global resource limits or the
        liveness policy into account; it only tries to make the existing resource usage
        + one more task per ready operator feasible in the cluster.
        """
        # Limit the frequency of autoscaling requests.
        now = time.time()
        if now - self._last_request_time < self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS:
            return

        # Scale up the cluster, if no ops are allowed to run, but there are still data
        # in the input queues.
        no_runnable_op = all(
            not op_state._scheduling_status.runnable
            for _, op_state in self._topology.items()
        )
        any_has_input = any(
            op_state._pending_dispatch_input_bundles_count() > 0
            for _, op_state in self._topology.items()
        )
        if not (no_runnable_op and any_has_input):
            return

        self._last_request_time = now

        # Get resource usage for all ops + additional resources needed to launch one
        # more task for each ready op.
        resource_request = []

        def to_bundle(resource: ExecutionResources) -> Dict:
            req = {}
            if resource.cpu:
                req["CPU"] = math.ceil(resource.cpu)
            if resource.gpu:
                req["GPU"] = math.ceil(resource.gpu)
            return req

        for op, state in self._topology.items():
            per_task_resource = op.incremental_resource_usage()
            task_bundle = to_bundle(per_task_resource)
            resource_request.extend([task_bundle] * op.num_active_tasks())
            # Only include incremental resource usage for ops that are ready for
            # dispatch.
            if state._pending_dispatch_input_bundles_count() > 0:
                # TODO(Clark): Scale up more aggressively by adding incremental resource
                # usage for more than one bundle in the queue for this op?
                resource_request.append(task_bundle)

        self._send_resource_request(resource_request)

    def _send_resource_request(self, resource_request):
        # Make autoscaler resource request.
        actor = get_or_create_autoscaling_requester_actor()
        actor.request_resources.remote(resource_request, self._execution_id)

    def on_executor_shutdown(self):
        # Make request for zero resources to autoscaler for this execution.
        actor = get_or_create_autoscaling_requester_actor()
        actor.request_resources.remote({}, self._execution_id)

    def get_total_resources(self) -> ExecutionResources:
        return ExecutionResources.from_resource_dict(ray.cluster_resources())
