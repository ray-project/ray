# isort: skip_file
# ruff: noqa: E402
import os
import sys
import requests

import pytest

import ray
from ray._private.test_utils import (
    fetch_prometheus_metrics,
    wait_for_condition,
)
from ray._private.metrics_agent import WORKER_ID_TAG_KEY


try:
    import prometheus_client
except ImportError:
    prometheus_client = None


_WORKER_METRICS = ["actors"]
_NODE_METRICS = ["tasks"]
os.environ["RAY_prometheus_metric_to_aggregate"] = ",".join(_NODE_METRICS)


@pytest.fixture
def _setup_cluster_for_test(request, ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(
        _system_config={
            "metrics_report_interval_ms": 1000,
            "enable_metrics_collection": True,
        }
    )
    cluster.wait_for_nodes()
    ray_context = ray.init(address=cluster.address)

    @ray.remote
    def t():
        print("task")

    @ray.remote
    class A:
        async def run(self):
            print("actor")

    a = A.remote()
    obj_refs = [t.remote(), a.run.remote()]

    # Make a request to the dashboard to produce some dashboard metrics
    requests.get(f"http://{ray_context.dashboard_url}/nodes")

    node_info_list = ray.nodes()
    prom_addresses = []
    for node_info in node_info_list:
        prom_addresses.append(
            f"{node_info['NodeManagerAddress']}:{node_info['MetricsExportPort']}"
        )
    yield prom_addresses

    ray.get(obj_refs)
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
def test_worker_id_labels(_setup_cluster_for_test):
    TEST_TIMEOUT_S = 30
    prom_addresses = _setup_cluster_for_test

    def _validate():
        metric_samples = fetch_prometheus_metrics(prom_addresses)
        node_metric_check = False
        worker_metric_check = False

        # Check that all node metrics are aggregated and do not have worker id labels
        for metric in _NODE_METRICS:
            samples = metric_samples.get(f"ray_{metric}")
            node_metric_check = samples and all(
                sample.labels.get(WORKER_ID_TAG_KEY) is None for sample in samples
            )

        # Check that all worker metrics are not aggregated and have worker id labels
        for metric in _WORKER_METRICS:
            samples = metric_samples.get(f"ray_{metric}")
            worker_metric_check = samples and all(
                sample.labels.get(WORKER_ID_TAG_KEY) is not None for sample in samples
            )

        return node_metric_check and worker_metric_check

    wait_for_condition(
        _validate,
        timeout=TEST_TIMEOUT_S,
        retry_interval_ms=1000,  # Yield resource for other processes
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
