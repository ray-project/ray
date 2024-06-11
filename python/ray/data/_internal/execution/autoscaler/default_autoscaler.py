import math
import time
from typing import TYPE_CHECKING, Dict

from .autoscaler import Autoscaler
from .autoscaling_actor_pool import AutoscalingActorPool
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


class DefaultAutoscaler(Autoscaler):

    # Default threshold of actor pool utilization to trigger scaling up.
    DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD: float = 0.8
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

    def _calculate_actor_pool_util(self, actor_pool: AutoscalingActorPool):
        """Calculate the utilization of the given actor pool."""
        if actor_pool.current_size() == 0:
            return 0
        else:
            return actor_pool.num_active_actors() / actor_pool.current_size()

    def _actor_pool_should_scale_up(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
        op_state: "OpState",
    ):
        # Do not scale up, if the op is completed or no more inputs are coming.
        if op.completed() or (op._inputs_complete and op.internal_queue_size() == 0):
            return False
        if actor_pool.current_size() < actor_pool.min_size():
            # Scale up, if the actor pool is below min size.
            return True
        elif actor_pool.current_size() >= actor_pool.max_size():
            # Do not scale up, if the actor pool is already at max size.
            return False
        # Do not scale up, if the op does not have more resources.
        if not op_state._scheduling_status.under_resource_limits:
            return False
        # Do not scale up, if the op has enough free slots for the existing inputs.
        if op_state.num_queued() <= actor_pool.num_free_task_slots():
            return False
        # Determine whether to scale up based on the actor pool utilization.
        util = self._calculate_actor_pool_util(actor_pool)
        return util > self._actor_pool_scaling_up_threshold

    def _actor_pool_should_scale_down(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
    ):
        # Scale down, if the op is completed or no more inputs are coming.
        if op.completed() or (op._inputs_complete and op.internal_queue_size() == 0):
            return True
        if actor_pool.current_size() > actor_pool.max_size():
            # Scale down, if the actor pool is above max size.
            return True
        elif actor_pool.current_size() <= actor_pool.min_size():
            # Do not scale down, if the actor pool is already at min size.
            return False
        # Determine whether to scale down based on the actor pool utilization.
        util = self._calculate_actor_pool_util(actor_pool)
        return util < self._actor_pool_scaling_down_threshold

    def _try_scale_up_or_down_actor_pool(self):
        for op, state in self._topology.items():
            actor_pools = op.get_autoscaling_actor_pools()
            for actor_pool in actor_pools:
                while True:
                    # Try to scale up or down the actor pool.
                    should_scale_up = self._actor_pool_should_scale_up(
                        actor_pool,
                        op,
                        state,
                    )
                    should_scale_down = self._actor_pool_should_scale_down(
                        actor_pool, op
                    )
                    if should_scale_up and not should_scale_down:
                        if actor_pool.scale_up(1) == 0:
                            break
                    elif should_scale_down and not should_scale_up:
                        if actor_pool.scale_down(1) == 0:
                            break
                    else:
                        break

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
            op_state._scheduling_status.runnable is False
            for _, op_state in self._topology.items()
        )
        any_has_input = any(
            op_state.num_queued() > 0 for _, op_state in self._topology.items()
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
            if state.num_queued() > 0:
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
