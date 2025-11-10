# isort: skip_file
# ruff: noqa: E402
import sys
import requests
import os

import pytest

import ray
from ray._private.test_utils import (
    PrometheusTimeseries,
    fetch_prometheus_metric_timeseries,
    wait_for_assertion,
)
from ray._common.network_utils import build_address
from ray._private.telemetry.metric_cardinality import (
    WORKER_ID_TAG_KEY,
    TASK_OR_ACTOR_NAME_TAG_KEY,
)

from ray._private.ray_constants import RAY_ENABLE_OPEN_TELEMETRY

try:
    import prometheus_client
except ImportError:
    prometheus_client = None


_TO_TEST_METRICS = ["ray_tasks", "ray_actors", "ray_running_jobs"]
_COMPONENT_TAG_KEY = "Component"


@pytest.fixture
def _setup_cluster_for_test(request, ray_start_cluster):
    global _CARDINALITY_LEVEL
    _CARDINALITY_LEVEL = None
    core_metric_cardinality_level = request.param
    os.environ["RAY_metric_cardinality_level"] = core_metric_cardinality_level
    cluster = ray_start_cluster
    cluster.add_node(
        _system_config={
            "metrics_report_interval_ms": 1000,
            "enable_metrics_collection": True,
            "metric_cardinality_level": core_metric_cardinality_level,
            "enable_open_telemetry": RAY_ENABLE_OPEN_TELEMETRY,
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
            build_address(
                node_info["NodeManagerAddress"], node_info["MetricsExportPort"]
            )
        )
    yield prom_addresses

    ray.get(obj_refs)


def _cardinality_level_test(_setup_cluster_for_test, cardinality_level, metric):
    """
    Test that the ray_tasks and ray_actors metric are reported with the expected cardinality level
    """
    TEST_TIMEOUT_S = 30
    prom_addresses = _setup_cluster_for_test

    def _validate():
        timeseries = PrometheusTimeseries()
        metric_samples = fetch_prometheus_metric_timeseries(prom_addresses, timeseries)
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
                if metric == "ray_tasks" or metric == "ray_actors":
                    assert (
                        sample.labels.get(TASK_OR_ACTOR_NAME_TAG_KEY) is not None
                    ), f"Sample {sample} does not contain Name tag"
            elif cardinality_level == "low":
                # If the cardinality level is low, the WorkerId and Name tags should
                # be removed
                assert (
                    sample.labels.get(WORKER_ID_TAG_KEY) is None
                ), f"Sample {sample} contains WorkerId tag"
                if metric == "ray_tasks" or metric == "ray_actors":
                    assert (
                        sample.labels.get(TASK_OR_ACTOR_NAME_TAG_KEY) is None
                    ), f"Sample {sample} contains Name tag"
            else:
                raise ValueError(f"Unknown cardinality level: {cardinality_level}")

            # The Component tag should be present on all cardinality levels
            assert (
                sample.labels.get(_COMPONENT_TAG_KEY) is not None
            ), f"Sample {sample} does not contain Component tag"

    wait_for_assertion(
        _validate,
        timeout=TEST_TIMEOUT_S,
        retry_interval_ms=1000,  # Yield resource for other processes
    )


@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
@pytest.mark.parametrize(
    "_setup_cluster_for_test,cardinality_level,metric",
    [
        (cardinality, cardinality, metric)
        for cardinality in ["recommended", "legacy"]
        for metric in _TO_TEST_METRICS
    ],
    indirect=["_setup_cluster_for_test"],
)
def test_cardinality_recommended_and_legacy_levels(
    _setup_cluster_for_test, cardinality_level, metric
):
    _cardinality_level_test(_setup_cluster_for_test, cardinality_level, metric)


# We only enable low cardinality test for open telemetry because the legacy opencensus
# implementation doesn't support low cardinality.
@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
@pytest.mark.skipif(
    not RAY_ENABLE_OPEN_TELEMETRY,
    reason="OpenTelemetry is not enabled",
)
@pytest.mark.parametrize(
    "_setup_cluster_for_test,cardinality_level,metric",
    [("low", "low", metric) for metric in _TO_TEST_METRICS],
    indirect=["_setup_cluster_for_test"],
)
def test_cardinality_low_levels(_setup_cluster_for_test, cardinality_level, metric):
    _cardinality_level_test(_setup_cluster_for_test, cardinality_level, metric)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
