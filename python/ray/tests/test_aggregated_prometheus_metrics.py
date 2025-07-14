# isort: skip_file
# ruff: noqa: E402
import sys
import requests
import os

import pytest

import ray
from ray._private.test_utils import (
    fetch_prometheus_metrics,
    wait_for_assertion,
)
from ray._private.metrics_agent import WORKER_ID_TAG_KEY


try:
    import prometheus_client
except ImportError:
    prometheus_client = None


_TO_TEST_METRICS = ["ray_tasks", "ray_actors"]


@pytest.fixture
def _setup_cluster_for_test(request, ray_start_cluster):
    core_metric_cardinality_level = request.param
    os.environ["RAY_metric_cardinality_level"] = core_metric_cardinality_level
    cluster = ray_start_cluster
    cluster.add_node(
        _system_config={
            "metrics_report_interval_ms": 1000,
            "enable_metrics_collection": True,
            "metric_cardinality_level": core_metric_cardinality_level,
        }
    )
    cluster.wait_for_nodes()
    ray_context = ray.init(
        address=cluster.address,
    )

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


@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
@pytest.mark.parametrize(
    "_setup_cluster_for_test,cardinality_level",
    [("recommended", "recommended"), ("legacy", "legacy")],
    indirect=["_setup_cluster_for_test"],
)
def test_cardinality_levels(_setup_cluster_for_test, cardinality_level):
    """
    Test that the ray_tasks and ray_actors metric are reported with the expected cardinality level
    """
    TEST_TIMEOUT_S = 30
    prom_addresses = _setup_cluster_for_test

    def _validate():
        metric_samples = fetch_prometheus_metrics(prom_addresses)
        for metric in _TO_TEST_METRICS:
            samples = metric_samples.get(metric)
            assert samples, f"Metric {metric} not found in samples"
            for sample in samples:
                if cardinality_level == "recommended":
                    # If the cardinality level is recommended, the WorkerId tag should
                    # be removed
                    assert (
                        sample.labels.get(WORKER_ID_TAG_KEY) is None
                    ), f"Sample {sample} contains WorkerId tag"
                elif cardinality_level == "legacy":
                    # If the cardinality level is legacy, the WorkerId tag should be
                    # present
                    assert (
                        sample.labels.get(WORKER_ID_TAG_KEY) is not None
                    ), f"Sample {sample} does not contain WorkerId tag"
                else:
                    raise ValueError(f"Unknown cardinality level: {cardinality_level}")

    wait_for_assertion(
        _validate,
        timeout=TEST_TIMEOUT_S,
        retry_interval_ms=1000,  # Yield resource for other processes
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
