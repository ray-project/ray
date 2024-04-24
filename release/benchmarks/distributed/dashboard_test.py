import asyncio
import time
import urllib
from typing import Dict, Optional, List, Tuple
from pprint import pprint
import os

import requests
import ray
import logging

from collections import defaultdict
from ray.util.state import list_nodes
from ray._private.test_utils import fetch_prometheus_metrics, query_prometheus
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from pydantic import BaseModel
from ray.dashboard.consts import DASHBOARD_METRIC_PORT
from ray.dashboard.utils import get_address_for_submission_client

logger = logging.getLogger(__name__)


def calc_p(latencies, percent):
    if len(latencies) == 0:
        return 0

    return round(sorted(latencies)[int(len(latencies) / 100 * percent)] * 1000, 3)


class Result(BaseModel):
    success: bool
    # endpoints -> list of latencies
    result: Dict[str, List[float]]
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
    "/api/v0/logs",
    "/api/prometheus_health",
]


@ray.remote(num_cpus=0)
class DashboardTester:
    def __init__(self, interval_s: int = 1):
        self.dashboard_url = get_address_for_submission_client(None)
        # Ping interval for all endpoints.
        self.interval_s = interval_s
        # endpoint -> a list of latencies
        self.result = defaultdict(list)

    async def run(self):
        await asyncio.gather(*[self.ping(endpoint) for endpoint in endpoints])

    async def ping(self, endpoint):
        """Synchronously call an endpoint."""
        node_id = ray.get_runtime_context().get_node_id()
        while True:
            start = time.monotonic()
            # for logs API, we should append node ID and glob.
            if "/api/v0/logs" in endpoint:
                glob_filter = "*"

                options_dict = {"node_id": node_id, "glob": glob_filter}
                url = (
                    f"{self.dashboard_url}{endpoint}?"
                    f"{urllib.parse.urlencode(options_dict)}"
                )
            else:
                url = f"{self.dashboard_url}{endpoint}"

            resp = requests.get(url, timeout=30)
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
        nodes = list_nodes(filters=[("node_ip", "=", current_node_ip)])
        assert len(nodes) > 0, f"{current_node_ip} not found in the cluster"
        node = nodes[0]
        # Schedule on a head node.
        self.tester = DashboardTester.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=node["node_id"], soft=False
            )
        ).remote()

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

        return Result(
            success=True, result=result, memory_mb=max(memories) if memories else None
        )

    def update_release_test_result(self, release_result: dict):
        test_result = self.get_result()

        def calc_endpoints_p(result, percent):
            return {
                # sort -> get PX -> convert second to ms -> round up.
                endpoint: calc_p(latencies, percent)
                for endpoint, latencies in result.items()
            }

        print("======Print per dashboard endpoint latencies======")
        print("=====================P50==========================")
        pprint(calc_endpoints_p(test_result.result, 50))
        print("=====================P95==========================")
        pprint(calc_endpoints_p(test_result.result, 95))
        print("=====================P99==========================")
        pprint(calc_endpoints_p(test_result.result, 99))

        latencies = []
        for per_endpoint_latencies in test_result.result.values():
            latencies.extend(per_endpoint_latencies)
        aggregated_metrics = {
            "p50": calc_p(latencies, 50),
            "p95": calc_p(latencies, 95),
            "p99": calc_p(latencies, 99),
        }

        print("=====================Aggregated====================")
        pprint(aggregated_metrics)

        release_result["_dashboard_test_success"] = test_result.success
        if test_result.success:
            if "perf_metrics" not in release_result:
                release_result["perf_metrics"] = []

            release_result["perf_metrics"].extend(
                [
                    {
                        "perf_metric_name": f"dashboard_{p}_latency_ms",
                        "perf_metric_value": value,
                        "perf_metric_type": "LATENCY",
                    }
                    for p, value in aggregated_metrics.items()
                ]
            )
            release_result["_dashboard_memory_usage_mb"] = test_result.memory_mb


def query_promthesus_dashboard_api_requests_duration_seconds(
    quantiles: Tuple[int] = (50, 95, 99), window: str = "5m"
):
    """Query promethesus for the metric with quantiles.

    Returns: {quantile_str: {endpoint: value}}
    Raises: Exception on failure to get, or ImportError in minimal tests.
    """
    promethesus_host = os.environ.get("RAY_PROMETHEUS_HOST", "http://localhost:9090")
    metric = "ray_dashboard_api_requests_duration_seconds_bucket"

    ret = {}
    for quantile in quantiles:
        assert 0 < quantile <= 100

        quantile_str = str(quantile)
        ret[quantile_str] = {}

        query = f"histogram_quantile(0.{quantile}, sum(rate({metric}[{window}])) by (le, endpoint))"
        results = query_prometheus(promethesus_host, query)
        for result in results["data"]["result"]:
            endpoint = result["metric"]["endpoint"]
            value = result["value"][1]
            if value != "NaN":
                ret[quantile_str][endpoint] = value
    return ret


def update_release_test_result_for_dashboard_api_requests_duration_seconds(
    release_result: dict,
):
    try:
        test_result = query_promthesus_dashboard_api_requests_duration_seconds()
        release_result["_dashboard_api_requests_duration_seconds_test_success"] = True
    except Exception as e:
        print(f"Failed to get prometheus metrics: {e}")
        release_result["_dashboard_api_requests_duration_seconds_test_success"] = False
        return

    print(f"====== Print ray_dashboard_api_requests_duration_seconds_bucket ======")
    pprint(test_result)

    if "perf_metrics" not in release_result:
        release_result["perf_metrics"] = []

    # Add the list of results, sorted by (quantile, endpoint).
    for quantile_str, endpoint_values in sorted(test_result.items()):
        p_str = f"p{quantile_str}"  # "95" -> "p95"
        p_results = [
            {
                "perf_metric_name": f"dashboard_api_requests_{endpoint}_{p_str}_latency_s",
                "perf_metric_value": value,
                "perf_metric_type": "LATENCY",
            }
            for endpoint, value in sorted(endpoint_values.items())
        ]
        release_result["perf_metrics"].extend(p_results)
