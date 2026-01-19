import logging
import math
import time
from typing import TYPE_CHECKING, Dict, List

import ray
from .base_cluster_autoscaler import ClusterAutoscaler
from ray.data._internal.execution.autoscaling_requester import (
    get_or_create_autoscaling_requester_actor,
)
from ray.data._internal.execution.interfaces import ExecutionResources

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology


logger = logging.getLogger(__name__)


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

    def _cap_resource_request_to_limits(
        self, resource_request: List[Dict]
    ) -> List[Dict]:
        """Cap the resource request to not exceed user-configured resource limits.

        If the user has set explicit (non-infinite) resource limits, this method
        filters the resource request to ensure the total requested resources do not
        exceed those limits.

        Args:
            resource_request: List of resource bundles to request.

        Returns:
            A filtered list of resource bundles that respects user limits.
        """
        limits = self._resource_limits

        # If no explicit limits are set (all infinite), return the original request
        if (
            limits.cpu == float("inf")
            and limits.gpu == float("inf")
            and limits.memory == float("inf")
        ):
            return resource_request

        # Calculate totals from the request
        total_cpu = sum(bundle.get("CPU", 0) for bundle in resource_request)
        total_gpu = sum(bundle.get("GPU", 0) for bundle in resource_request)
        total_memory = sum(bundle.get("memory", 0) for bundle in resource_request)

        # Check if the request already respects limits
        cpu_within_limit = limits.cpu == float("inf") or total_cpu <= limits.cpu
        gpu_within_limit = limits.gpu == float("inf") or total_gpu <= limits.gpu
        memory_within_limit = (
            limits.memory == float("inf") or total_memory <= limits.memory
        )

        if cpu_within_limit and gpu_within_limit and memory_within_limit:
            return resource_request

        # Cap the request by filtering bundles until we're within limits
        capped_request = []
        current_cpu = 0.0
        current_gpu = 0.0
        current_memory = 0.0

        for bundle in resource_request:
            bundle_cpu = bundle.get("CPU", 0)
            bundle_gpu = bundle.get("GPU", 0)
            bundle_memory = bundle.get("memory", 0)

            # Check if adding this bundle would exceed limits
            new_cpu = current_cpu + bundle_cpu
            new_gpu = current_gpu + bundle_gpu
            new_memory = current_memory + bundle_memory

            cpu_ok = limits.cpu == float("inf") or new_cpu <= limits.cpu
            gpu_ok = limits.gpu == float("inf") or new_gpu <= limits.gpu
            memory_ok = limits.memory == float("inf") or new_memory <= limits.memory

            if cpu_ok and gpu_ok and memory_ok:
                capped_request.append(bundle)
                current_cpu = new_cpu
                current_gpu = new_gpu
                current_memory = new_memory
            else:
                # Stop adding bundles once we hit the limit
                break

        if len(capped_request) < len(resource_request):
            logger.debug(
                f"Capped autoscaling resource request from {len(resource_request)} "
                f"bundles to {len(capped_request)} bundles to respect user-configured "
                f"resource limits (CPU={limits.cpu}, GPU={limits.gpu}, "
                f"memory={limits.memory})."
            )

        return capped_request

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
        resource_request = []

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
            resource_request.extend([task_bundle] * op.num_active_tasks())
            # Only include incremental resource usage for ops that are ready for
            # dispatch.
            if state.has_pending_bundles():
                # TODO(Clark): Scale up more aggressively by adding incremental resource
                # usage for more than one bundle in the queue for this op?
                resource_request.append(task_bundle)

        # Cap the resource request to respect user-configured limits
        resource_request = self._cap_resource_request_to_limits(resource_request)

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
        """Get total resources available, respecting user-configured limits."""
        cluster_resources = ExecutionResources.from_resource_dict(
            ray.cluster_resources()
        )
        # Respect user-configured resource limits
        user_limits = self._resource_limits
        return cluster_resources.min(user_limits)
