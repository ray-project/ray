import math
from abc import ABCMeta, abstractmethod
from typing import Dict, List

from ray.actor import ActorHandle
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import Topology


class ActorPoolAutoscalingHandler(metaclass=ABCMeta):

    @abstractmethod
    def actor_pool_min_size(self) -> int: ...

    @abstractmethod
    def actor_pool_max_size(self) -> int: ...

    @abstractmethod
    def actor_pool_current_size(self) -> int: ...

    @abstractmethod
    def actor_pool_current_utilization(self) -> float: ...

    @abstractmethod
    def scale_up_actor_pool(self, num_actors: int) -> int: ...

    @abstractmethod
    def scale_down_actor_pool(self, num_actors: int) -> int: ...


class Autoscaler(metaclass=ABCMeta):

    def __init__(
        self,
        topology: Topology,
        resource_manager: ResourceManager,
        execution_id: str,
    ):
        self._topology = topology
        self._resource_manager = resource_manager
        self._execution_id = execution_id

    @abstractmethod
    def try_trigger_scaling(self):
        pass


class DefaultAutoscaler(Autoscaler):

    DEFAULT_SCALING_UP_THRESHOLD: float = 0.8
    DEFAULT_SCALING_DOWN_THRESHOLD: float = 0.5

    def __init__(
        self,
        topology: Topology,
        resource_manager: ResourceManager,
        execution_id: str,
    ):
        super().__init__(topology, resource_manager, execution_id)

    def try_trigger_scaling(self):
        self._try_scale_up_cluster()

    def _try_scale_up_or_down_actor_pool(self):
        for op, _ in self._topology.items():
            if not isinstance(op, ActorPoolAutoscalingHandler):
                continue
            # Scale up if the current utilization is above the threshold and we have not
            # reached the max size.
            while (
                op.actor_pool_current_utilization() > self.DEFAULT_SCALING_UP_THRESHOLD
                and op.actor_pool_current_size() < op.actor_pool_max_size()
            ):
                op.scale_up_actor_pool(1)

            # self._kill_inactive_workers_if_done()
            # Scale down if the current utilization is below the threshold and we have not
            # reached the min size.
            while (
                op.actor_pool_current_utilization()
                < self.DEFAULT_SCALING_DOWN_THRESHOLD
                and op.actor_pool_current_size() > op.actor_pool_min_size()
            ):
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
        actor.request_resources.remote(resource_request, execution_id)

    def
