import math
import time
from typing import TYPE_CHECKING, Dict

from .autoscaler import ActorPoolAutoscalingHandler, Autoscaler
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import PhysicalOperator
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


class DefaultAutoscaler(Autoscaler):

    DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD: float = 0.8
    DEFAULT_ACTOR_POOL_SCALING_DOWN_THRESHOLD: float = 0.5

    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS = 20

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        execution_id: str,
    ):
        self._last_request_time = 0
        super().__init__(topology, resource_manager, execution_id)

    def try_trigger_scaling(self):
        self._try_scale_up_cluster()
        self._try_scale_up_or_down_actor_pool()

    def _actor_pool_should_scale_up(self, op: "PhysicalOperator", op_state: "OpState"):
        if op._inputs_complete and op.internal_queue_size() == 0:
            return False
        assert isinstance(op, ActorPoolAutoscalingHandler)
        actor_pool = op
        if actor_pool.actor_pool_current_size() >= actor_pool.actor_pool_max_size():
            return False
        free_slots = (
            actor_pool.max_tasks_in_flight_per_actor()
            * actor_pool.actor_pool_current_size()
            - actor_pool.num_in_flight_tasks()
        )
        # TODO: under_resource_limits
        if op_state.num_queued() <= free_slots:
            return False
        util = actor_pool.num_running_actors() / actor_pool.actor_pool_current_size()
        return util > self.DEFAULT_ACTOR_POOL_SCALING_UP_THRESHOLD

    def _actor_pool_should_scale_down(
        self, op: "PhysicalOperator", op_state: "OpState"
    ):
        if op._inputs_complete and op.internal_queue_size() == 0:
            return True
        assert isinstance(op, ActorPoolAutoscalingHandler)
        actor_pool = op
        if actor_pool.actor_pool_current_size() <= actor_pool.actor_pool_min_size():
            return False
        util = actor_pool.num_running_actors() / actor_pool.actor_pool_current_size()
        return util < self.DEFAULT_ACTOR_POOL_SCALING_DOWN_THRESHOLD

    def _try_scale_up_or_down_actor_pool(self):
        for op, state in self._topology.items():
            if not isinstance(op, ActorPoolAutoscalingHandler):
                continue
            while self._actor_pool_should_scale_up(
                op, state
            ) and not self._actor_pool_should_scale_down(op, state):
                if op.scale_up_actor_pool(1) == 0:
                    break

            # self._kill_inactive_workers_if_done()
            while self._actor_pool_should_scale_down(
                op, state
            ) and not self._actor_pool_should_scale_up(op, state):
                if op.scale_down_actor_pool(1) == 0:
                    break

    def _try_scale_up_cluster(self):
        """Try to scale up the cluster to accomodate the provided in-progress workload.

        This makes a resource request to Ray's autoscaler consisting of the current,
        aggregate usage of all operators in the DAG + the incremental usage of all operators
        that are ready for dispatch (i.e. that have inputs queued). If the autoscaler were
        to grant this resource request, it would allow us to dispatch one task for every
        ready operator.

        Note that this resource request does not take the global resource limits or the
        liveness policy into account; it only tries to make the existing resource usage +
        one more task per ready operator feasible in the cluster.

        Args:
            topology: The execution state of the in-progress workload for which we wish to
                request more resources.
        """
        now = time.time()
        if now - self._last_request_time < self.MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS:
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
