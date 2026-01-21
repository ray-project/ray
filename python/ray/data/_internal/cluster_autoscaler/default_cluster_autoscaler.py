import math
import time
from typing import TYPE_CHECKING, Dict

import ray
from .base_cluster_autoscaler import ClusterAutoscaler
from .util import cap_resource_request_to_limits
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.interfaces import ExecutionResources

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology


class DefaultClusterAutoscaler(ClusterAutoscaler):
    # Min number of seconds between two autoscaling requests.
    MIN_GAP_BETWEEN_AUTOSCALING_REQUESTS = 20

    def __init__(
        self,
        topology: "Topology",
        resource_limits: ExecutionResources,
        *,
        execution_id: str,
    ):
        self._topology = topology
        self._resource_limits = resource_limits
        self._execution_id = execution_id

        # Last time when a request was sent to Ray's autoscaler.
        self._last_request_time = 0

    def try_trigger_scaling(self):
        """Try to scale up the cluster to accommodate the provided in-progress workload.

        This makes a resource request to Ray's autoscaler consisting of the current,
        aggregate usage of all operators in the DAG + the incremental usage of all
        operators that are ready for dispatch (i.e. that have inputs queued). If the
        autoscaler were to grant this resource request, it would allow us to dispatch
        one task for every ready operator.

        The resource request is capped to not exceed user-configured resource limits
        set via ExecutionOptions.resource_limits.
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
            op_state.has_pending_bundles() for _, op_state in self._topology.items()
        )
        if not (no_runnable_op and any_has_input):
            return

        self._last_request_time = now

        # Get resource usage for all ops + additional resources needed to launch one
        # more task for each ready op.
        # We separate active bundles (running tasks) from pending bundles (future work)
        # to ensure running tasks' resources are never crowded out by pending work.
        active_bundles = []
        pending_bundles = []

        def to_bundle(resource: ExecutionResources) -> Dict:
            req = {}
            if resource.cpu:
                req["CPU"] = math.ceil(resource.cpu)
            if resource.gpu:
                req["GPU"] = math.ceil(resource.gpu)
            if resource.memory:
                req["memory"] = math.ceil(resource.memory)
            return req

        for op, state in self._topology.items():
            per_task_resource = op.incremental_resource_usage()
            task_bundle = to_bundle(per_task_resource)
            # Bundles for running tasks -> active (must include)
            active_bundles.extend([task_bundle] * op.num_active_tasks())
            # Only include incremental resource usage for ops that are ready for
            # dispatch.
            if state.has_pending_bundles():
                # TODO(Clark): Scale up more aggressively by adding incremental resource
                # usage for more than one bundle in the queue for this op?
                # Bundle for pending work -> pending (best-effort)
                pending_bundles.append(task_bundle)

        # Cap the resource request to respect user-configured limits.
        # Active bundles are always included; pending bundles are best-effort.
        resource_request = cap_resource_request_to_limits(
            active_bundles, pending_bundles, self._resource_limits
        )

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
        """Get total resources available in the cluster."""
        return ExecutionResources.from_resource_dict(ray.cluster_resources())
