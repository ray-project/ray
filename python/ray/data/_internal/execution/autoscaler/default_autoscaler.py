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
    from ray.data._internal.execution.streaming_executor_state import (
        OpSchedulingStatus,
        OpState,
        SchedulingDecision,
        Topology,
    )


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
    ):
        super().__init__(topology, resource_manager, execution_id)
        # Last time when a request was sent to Ray's autoscaler.
        self._last_request_time = 0

    def try_trigger_scaling(self, scheduling_decision: "SchedulingDecision"):
        self._try_scale_up_cluster(scheduling_decision)
        self._try_scale_up_or_down_actor_pool(scheduling_decision)

    def _actor_pool_util(self, actor_pool: AutoscalingActorPool):
        if actor_pool.current_size() == 0:
            return 0
        else:
            return actor_pool.num_running_actors() / actor_pool.current_size()

    def _actor_pool_should_scale_up(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
        op_state: "OpState",
        op_scheduling_status: "OpSchedulingStatus",
    ):
        # Do not scale up, if the op is completed or no more inputs are coming.
        if op.completed() or (op._inputs_complete and op.internal_queue_size() == 0):
            return False
        # Do not scale up, if the actor pool is already at max size.
        if actor_pool.current_size() >= actor_pool.max_size():
            return False
        # Do not scale up, if the op still has enough resources to run.
        if op_scheduling_status.under_resource_limits:
            return False
        # Do not scale up, if the op has enough free slots for the existing inputs.
        free_slots = (
            actor_pool.max_tasks_in_flight_per_actor() * actor_pool.current_size()
            - actor_pool.current_in_flight_tasks()
        )
        if op_state.num_queued() <= free_slots:
            return False
        # Determine whether to scale up based on the actor pool utilization.
        util = self._actor_pool_util(actor_pool)
        return util > self.DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD

    def _actor_pool_should_scale_down(
        self,
        actor_pool: AutoscalingActorPool,
        op: "PhysicalOperator",
    ):
        # Scale down, if the op is completed or no more inputs are coming.
        if op.completed() or (op._inputs_complete and op.internal_queue_size()) == 0:
            return True
        # Do not scale down, if the actor pool is already at min size.
        if actor_pool.current_size() <= actor_pool.min_size():
            return False
        # Determine whether to scale down based on the actor pool utilization.
        util = self._actor_pool_util(actor_pool)
        return util < self.DEFAULT_ACTOR_POOL_SCALING_DOWN_THRESHOLD

    def _try_scale_up_or_down_actor_pool(
        self, scheduling_decision: "SchedulingDecision"
    ):
        for op, state in self._topology.items():
            actor_pools = op.get_autoscaling_actor_pools()
            for actor_pool in actor_pools:
                while True:
                    # Try to scale up or down the actor pool.
                    should_scale_up = self._actor_pool_should_scale_up(
                        actor_pool,
                        op,
                        state,
                        scheduling_decision.op_scheduling_status[op],
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

    def _try_scale_up_cluster(self, scheduling_decision: "SchedulingDecision"):
        """Try to scale up the cluster to accomodate the provided in-progress workload.

        This makes a resource request to Ray's autoscaler consisting of the current,
        aggregate usage of all operators in the DAG + the incremental usage of all operators
        that are ready for dispatch (i.e. that have inputs queued). If the autoscaler were
        to grant this resource request, it would allow us to dispatch one task for every
        ready operator.

        Note that this resource request does not take the global resource limits or the
        liveness policy into account; it only tries to make the existing resource usage +
        one more task per ready operator feasible in the cluster.
        """
        # Limit the frequency of autoscaling requests.
        now = time.time()
        if now - self._last_request_time < self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS:
            return

        # Scale up the cluster, if no ops are allowed to run, but there are still data
        # in the input queues.
        no_runnable_op = all(
            status.runnable is False
            for _, status in scheduling_decision.op_scheduling_status.items()
        )
        any_has_input = any(
            op_state.num_queued() > 0 for _, op_state in self._topology.items()
        )
        if not (no_runnable_op and any_has_input):
            return

        self._last_request_time = now

        # Get resource usage for all ops + additional resources needed to launch one more
        # task for each ready op.
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

        # Make autoscaler resource request.
        actor = get_or_create_autoscaling_requester_actor()
        actor.request_resources.remote(resource_request, self._execution_id)

    def on_executor_shutdown(self):
        # Make request for zero resources to autoscaler for this execution.
        actor = get_or_create_autoscaling_requester_actor()
        actor.request_resources.remote({}, self._execution_id)
