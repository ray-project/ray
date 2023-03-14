import time
from typing import Dict

import ray
from ray.data.context import DatasetContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@ray.remote(num_cpus=0)
class AutoscalingRequester:
    """Actor to make resource requests to autoscaler for the datasets.

    The resource requests are set to timeout for 60s.
    For those live requests (i.e. those made in last 60s), we keep track of the
    highest amount requested for each resource dimension for each execution; then
    sum the requested amounts across all executions as the final request to the
    autoscaler.
    """

    def __init__(self):
        # Mapping execution_uuid to high-watermark of resource request.
        self._resource_requests = {}
        # Mapping execution_uuid to expiration timestamp of resource request from
        # this execution.
        self._expiration_timestamp = {}
        # TTL for requests.
        self._timeout = 60

    def request_resources(self, req: Dict, execution_uuid: str):
        # Purge expired requests before making request to autoscaler.
        self._purge()
        self._expiration_timestamp[execution_uuid] = time.time() + self._timeout
        # For the same execution_uuid, we track the high watermark of the resource
        # requested.
        self._resource_requests[execution_uuid] = self._get_high_watermark(
            req, execution_uuid
        )
        # We aggregate the resource requests across all execution_uuid's to Ray
        # autoscaler.
        ray.autoscaler.sdk.request_resources(bundles=[self._aggregate_requests()])

    def _purge(self):
        # Purge requests that are stale.
        now = time.time()
        for k, v in list(self._expiration_timestamp.items()):
            if v < now:
                self._expiration_timestamp.pop(k)
                self._resource_requests.pop(k)

    def _get_high_watermark(self, req: Dict, execution_uuid: str) -> Dict:
        if execution_uuid in self._resource_requests:
            reqs = [req, self._resource_requests[execution_uuid]]
        else:
            reqs = [req]
        req = {}
        req["CPU"] = max(r["CPU"] if "CPU" in r else 0 for r in reqs)
        req["GPU"] = max(r["GPU"] if "GPU" in r else 0 for r in reqs)
        return req

    def _aggregate_requests(self) -> Dict:
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


def get_or_create_autoscaling_requester_actor():
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
        name="AutoscalingRequester",
        namespace="AutoscalingRequester",
        get_if_exists=True,
        lifetime="detached",
        scheduling_strategy=scheduling_strategy,
    ).remote()
