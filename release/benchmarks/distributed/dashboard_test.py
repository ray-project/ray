import asyncio
import time
from typing import Dict, Optional
from pprint import pprint

import requests
import ray
import logging

from collections import defaultdict
from ray._private.test_utils import format_web_url, fetch_prometheus_metrics
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from pydantic import BaseModel
from ray.dashboard.consts import DASHBOARD_METRIC_PORT

logger = logging.getLogger(__name__)


class Result(BaseModel):
    success: bool
    # endpoints -> P95 latency
    p95_ms: Optional[Dict[str, float]]
    # endpoints -> P95 latency
    p50_ms: Optional[Dict[str, float]]
    # Dashboard memory usage in MB.
    memory_mb: Optional[float]


# Currently every endpoint is GET endpoints.
endpoints = [
    "/logical/actors",
    "/nodes?view=summary",
    "/",
    "/api/cluster_status",
    "/events",
    "/api/jobs/",
    "/log_index",
    "/api/prometheus_health",
]


@ray.remote(num_cpus=0)
class DashboardTester:
    def __init__(self, addr: ray._private.worker.RayContext, interval_s: int = 1):
        self.dashboard_url = format_web_url(addr.dashboard_url)
        # Ping interval for all endpoints.
        self.interval_s = interval_s
        # endpoint -> a list of latencies
        self.result = defaultdict(list)

    async def run(self):
        await asyncio.gather(*[self.ping(endpoint) for endpoint in endpoints])

    async def ping(self, endpoint):
        """Synchronously call an endpoint."""
        while True:
            start = time.monotonic()
            resp = requests.get(self.dashboard_url + endpoint)
            elapsed = time.monotonic() - start

            if resp.status_code == 200:
                self.result[endpoint].append(time.monotonic() - start)
            else:
                try:
                    resp.raise_for_status()
                except Exception as e:
                    logger.exception(e)
            await asyncio.sleep(max(0, self.interval_s, elapsed))

    def get_result(self):
        return self.result


class DashboardTestAtScale:
    """This is piggybacked into existing scalability tests."""

    def __init__(self, addr: ray._private.worker.RayContext):
        self.addr = addr

        # Schedule the actor on the current node (which is a head node).
        current_node_ip = ray._private.worker.global_worker.node_ip_address
        nodes = ray.experimental.state.api.list_nodes()
        node = list(filter(lambda n: n["node_ip"] == current_node_ip, nodes))[0]
        # Schedule on a head node.
        self.tester = DashboardTester.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node["node_id"], soft=False
            )
        ).remote(addr)

        self.tester.run.remote()

    def get_result(self):
        """Get the result from the test.

        Returns:
            A tuple of success, and the result (Result object).
        """
        try:
            result = ray.get(self.tester.get_result.remote(), timeout=60)
        except ray.exceptions.GetTimeoutError:
            return Result(success=False)

        def calc_p(percent):
            return {
                # sort -> get PX -> convert second to ms -> round up.
                endpoint: round(
                    sorted(latencies)[int(len(latencies) / 100 * percent)] * 1000, 3
                )  # noqa
                for endpoint, latencies in result.items()
            }

        # Get the memory usage.
        dashboard_export_addr = "{}:{}".format(
            self.addr["raylet_ip_address"], DASHBOARD_METRIC_PORT
        )
        metrics = fetch_prometheus_metrics([dashboard_export_addr])
        memories = []
        for name, samples in metrics.items():
            if name == "ray_component_uss_mb":
                for sample in samples:
                    if sample.labels["Component"] == "dashboard":
                        memories.append(sample.value)

        return Result(success=True, p95_ms=calc_p(95), memory_mb=max(memories))

    def update_release_test_result(self, release_result: dict):
        test_result = self.get_result()
        pprint(test_result)

        if "perf_metrics" not in release_result:
            release_result["perf_metrics"] = []

        release_result["perf_metrics"].extend(
            [
                {
                    "perf_metric_name": f"dashboard_{endpoint}_p95_latency",
                    "perf_metric_value": p95,
                    "perf_metric_type": "LATENCY",
                }
                for endpoint, p95 in test_result.p95_ms.items()
            ]
        )
        release_result["_dashboard_memory_usage_mb"] = test_result.memory_mb
