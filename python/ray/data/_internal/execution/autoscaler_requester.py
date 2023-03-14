from typing import Dict

import ray
from ray.data.context import DatasetContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@ray.remote(num_cpus=0)
class AutoscalingRequester:
    def __init__(self):
        # Mapping execution_uuid to high-watermark of resource request.
        self._resource_requests = {}

    def request_resources(self, req, execution_uuid):
        # For the same execution_uuid, we track the high watermark of the resource
        # requested.
        self._resource_requests[execution_uuid] = self._get_high_watermark(
            req, execution_uuid
        )
        # We aggregate the resource requests across all execution_uuid's to Ray
        # autoscaler.
        ray.autoscaler.sdk.request_resources(bundles=[self._aggregate_request()])

    def _get_high_watermark(self, req, execution_uuid) -> Dict:
        if execution_uuid in self._resource_requests:
            reqs = [req, self._resource_requests[execution_uuid]]
        else:
            reqs = [req]
        req = {}
        req["CPU"] = max(r["CPU"] if "CPU" in r else 0 for r in reqs)
        req["GPU"] = max(r["GPU"] if "GPU" in r else 0 for r in reqs)
        return req

    def _aggregate_request(self) -> Dict:
        req = {}
        req["CPU"] = (
            sum(self._resource_requests["CPU"])
            if "CPU" in self._resource_requests
            else 0
        )
        req["GPU"] = (
            sum(self._resource_requests["GPU"])
            if "GPU" in self._resource_requests
            else 0
        )
        return req


def get_or_create_autoscaler_requester_actor():
    ctx = DatasetContext.get_current()
    scheduling_strategy = ctx.scheduling_strategy
    # Pin the stats actor to the local node so it fate-shares with the driver.
    # Note: for Ray Client, the ray.get_runtime_context().get_node_id() should
    # point to the head node.
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
    return AutoscalingRequester.options(
        name="AutoscalerRequester",
        namespace="AutoscalingRequester",
        get_if_exists=True,
        lifetime="detached",
        scheduling_strategy=scheduling_strategy,
    ).remote()
