import math
import threading
import time
from typing import Dict, List

import ray
from ray.data.context import DataContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# Resource requests are considered stale after this number of seconds, and
# will be purged.
RESOURCE_REQUEST_TIMEOUT = 60
PURGE_INTERVAL = RESOURCE_REQUEST_TIMEOUT * 2

# When the autoscaling is driven by memory pressure and there are abundant
# CPUs to support incremental CPUs needed to launch more tasks, we'll translate
# memory pressure into an artificial request of CPUs. The amount of CPUs we'll
# request is ARTIFICIAL_CPU_SCALING_FACTOR * ray.cluster_resources()["CPU"].
ARTIFICIAL_CPU_SCALING_FACTOR = 1.2


@ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=-1)
class AutoscalingRequester:
    """Actor to make resource requests to autoscaler for the datasets.

    The resource requests are set to timeout after RESOURCE_REQUEST_TIMEOUT seconds.
    For those live requests, we keep track of the last request made for each execution,
    which overrides all previous requests it made; then sum the requested amounts
    across all executions as the final request to the autoscaler.
    """

    def __init__(self):
        # execution_id -> (List[Dict], expiration timestamp)
        self._resource_requests = {}
        # TTL for requests.
        self._timeout = RESOURCE_REQUEST_TIMEOUT

        self._self_handle = ray.get_runtime_context().current_actor

        # Start a thread to purge expired requests periodically.
        def purge_thread_run():
            while True:
                time.sleep(PURGE_INTERVAL)
                # Call purge_expired_requests() as an actor task,
                # so we don't need to handle multi-threading.
                ray.get(self._self_handle.purge_expired_requests.remote())

        self._purge_thread = threading.Thread(target=purge_thread_run, daemon=True)
        self._purge_thread.start()

    def purge_expired_requests(self):
        self._purge()
        ray.autoscaler.sdk.request_resources(bundles=self._aggregate_requests())

    def request_resources(self, req: List[Dict], execution_id: str):
        # Purge expired requests before making request to autoscaler.
        self._purge()
        # For the same execution_id, we track the latest resource request and
        # the its expiration timestamp.
        self._resource_requests[execution_id] = (
            req,
            time.time() + self._timeout,
        )
        # We aggregate the resource requests across all execution_id's to Ray
        # autoscaler.
        ray.autoscaler.sdk.request_resources(bundles=self._aggregate_requests())

    def _purge(self):
        # Purge requests that are stale.
        now = time.time()
        for k, (_, t) in list(self._resource_requests.items()):
            if t < now:
                self._resource_requests.pop(k)

    def _aggregate_requests(self) -> List[Dict]:
        req = []
        for _, (r, _) in self._resource_requests.items():
            req.extend(r)

        def get_cpus(req):
            num_cpus = 0
            for r in req:
                if "CPU" in r:
                    num_cpus += r["CPU"]
            return num_cpus

        # Round up CPUs to exceed total cluster CPUs so it can actually upscale.
        # This is to handle the issue where the autoscaling is driven by memory
        # pressure (rather than CPUs) from streaming executor. In such case, simply
        # asking for incremental CPUs (e.g. 1 CPU for each ready operator) may not
        # actually be able to trigger autoscaling if existing CPUs in cluster can
        # already satisfy the incremental CPUs request.
        num_cpus = get_cpus(req)
        if num_cpus > 0:
            total = ray.cluster_resources()
            if "CPU" in total and num_cpus <= total["CPU"]:
                delta = (
                    math.ceil(ARTIFICIAL_CPU_SCALING_FACTOR * total["CPU"]) - num_cpus
                )
                req.extend([{"CPU": 1}] * delta)

        return req

    def _test_set_timeout(self, ttl):
        """Set the timeout. This is for test only"""
        self._timeout = ttl


# Creating/getting an actor from multiple threads is not safe.
# https://github.com/ray-project/ray/issues/41324
_autoscaling_requester_lock: threading.RLock = threading.RLock()


def get_or_create_autoscaling_requester_actor():
    ctx = DataContext.get_current()
    scheduling_strategy = ctx.scheduling_strategy
    # Pin the autoscaling requester actor to the local node so it fate-shares with the driver.
    # Note: for Ray Client, the ray.get_runtime_context().get_node_id() should
    # point to the head node.
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
    with _autoscaling_requester_lock:
        return AutoscalingRequester.options(
            name="AutoscalingRequester",
            namespace="AutoscalingRequester",
            get_if_exists=True,
            lifetime="detached",
            scheduling_strategy=scheduling_strategy,
        ).remote()
