# isort: skip_file
# ruff: noqa: E402
import sys
import requests
import os

import pytest

import ray
from ray._private.test_utils import wait_for_assertion
from ray._common.test_utils import (
    fetch_prometheus_metric_timeseries,
    PrometheusTimeseries,
)
from ray._common.network_utils import build_address
import ray._private.telemetry.metric_cardinality as mc
from ray._private.telemetry.metric_cardinality import (
    WORKER_ID_TAG_KEY,
    REPLICA_ID_TAG_KEY,
    TASK_OR_ACTOR_NAME_TAG_KEY,
    MetricCardinality,
)
from ray._private.telemetry.metric_types import MetricType

try:
    import prometheus_client
except ImportError:
    prometheus_client = None


_TO_TEST_METRICS = ["tasks", "actors", "running_jobs"]
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
        samples = metric_samples.get(f"ray_{metric}")
        assert samples, f"Metric {metric} not found in samples"
        for sample in samples:
            if (
                cardinality_level == "legacy"
                or metric not in MetricCardinality.get_high_cardinality_metrics()
            ):
                # If the cardinality level is legacy, the WorkerId tag should be
                # present
                assert (
                    sample.labels.get(WORKER_ID_TAG_KEY) is not None
                ), f"Sample {sample} does not contain WorkerId tag"
                if metric == "tasks" or metric == "actors":
                    assert (
                        sample.labels.get(TASK_OR_ACTOR_NAME_TAG_KEY) is not None
                    ), f"Sample {sample} does not contain Name tag"
            elif cardinality_level == "recommended":
                # If the cardinality level is recommended, the WorkerId tag should
                # be removed
                assert (
                    sample.labels.get(WORKER_ID_TAG_KEY) is None
                ), f"Sample {sample} contains WorkerId tag"
            elif cardinality_level == "low":
                # If the cardinality level is low, the WorkerId and Name tags should
                # be removed
                assert (
                    sample.labels.get(WORKER_ID_TAG_KEY) is None
                ), f"Sample {sample} contains WorkerId tag"
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
        for cardinality in ["low", "recommended", "legacy"]
        for metric in _TO_TEST_METRICS
    ],
    indirect=["_setup_cluster_for_test"],
)
def test_cardinality_recommended_and_legacy_levels(
    _setup_cluster_for_test, cardinality_level, metric
):
    _cardinality_level_test(_setup_cluster_for_test, cardinality_level, metric)


@pytest.fixture
def _setup_serve_metric_cluster(request, ray_start_cluster):
    """Bring up a node and emit a Serve-style gauge (carries ReplicaId) from two
    replicas, each setting 3.0 with a distinct ReplicaId."""
    global _CARDINALITY_LEVEL
    _CARDINALITY_LEVEL = None
    level = request.param
    os.environ["RAY_metric_cardinality_level"] = level
    cluster = ray_start_cluster
    cluster.add_node(
        _system_config={
            "metrics_report_interval_ms": 1000,
            "enable_metrics_collection": True,
            "metric_cardinality_level": level,
        }
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    @ray.remote
    class Emitter:
        def __init__(self, replica_id):
            from ray.util.metrics import Gauge

            self.replica_id = replica_id
            self.gauge = Gauge(
                "test_serve_replica_running",
                description="test serve metric",
                tag_keys=("model_name", REPLICA_ID_TAG_KEY),
            )

        def emit(self):
            self.gauge.set(
                3.0, tags={"model_name": "m", REPLICA_ID_TAG_KEY: self.replica_id}
            )

    emitters = [Emitter.remote(f"replica_{i}") for i in range(2)]
    for _ in range(5):
        ray.get([e.emit.remote() for e in emitters])

    node_info = ray.nodes()[0]
    yield build_address(node_info["NodeManagerAddress"], node_info["MetricsExportPort"])


@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
@pytest.mark.parametrize(
    "_setup_serve_metric_cluster,cardinality_level",
    [(level, level) for level in ["legacy", "recommended", "low"]],
    indirect=["_setup_serve_metric_cluster"],
)
def test_serve_metric_cardinality(_setup_serve_metric_cluster, cardinality_level):
    """A metric carrying ReplicaId collapses to one node-level series with both
    WorkerId and ReplicaId dropped and the values summed, at every level except
    legacy."""
    prom_address = _setup_serve_metric_cluster

    def _validate():
        timeseries = PrometheusTimeseries()
        samples = fetch_prometheus_metric_timeseries([prom_address], timeseries).get(
            "ray_test_serve_replica_running"
        )
        assert samples, "Serve metric not found in samples"
        if cardinality_level == "legacy":
            assert len(samples) == 2, f"Expected 2 per-replica series, got {samples}"
            for sample in samples:
                assert sample.labels.get(WORKER_ID_TAG_KEY) is not None
                assert sample.labels.get(REPLICA_ID_TAG_KEY) is not None
        else:
            assert len(samples) == 1, f"Expected 1 node-level series, got {samples}"
            sample = samples[0]
            assert sample.labels.get(WORKER_ID_TAG_KEY) is None
            assert sample.labels.get(REPLICA_ID_TAG_KEY) is None
            assert sample.value == 6.0, f"Expected summed 6.0, got {sample.value}"

    wait_for_assertion(_validate, timeout=30, retry_interval_ms=1000)


# A Serve metric carries ReplicaId; a plain user metric does not.
_SERVE_TAGS = ["model_name", REPLICA_ID_TAG_KEY, WORKER_ID_TAG_KEY]
_USER_TAGS = ["model_name", WORKER_ID_TAG_KEY]


@pytest.fixture
def set_level():
    """Set the cardinality level and clear the per-name label cache."""

    def _set(level: str):
        mc._CARDINALITY_LEVEL = MetricCardinality(level)
        mc._HIGH_CARDINALITY_LABELS.clear()

    original = mc._CARDINALITY_LEVEL
    yield _set
    mc._CARDINALITY_LEVEL = original
    mc._HIGH_CARDINALITY_LABELS.clear()


def _drop(name, tag_keys):
    return MetricCardinality.get_high_cardinality_labels_to_drop(name, tag_keys)


def test_unit_legacy_drops_nothing(set_level):
    set_level("legacy")
    assert _drop("tasks", _USER_TAGS) == []
    assert _drop("vllm_foo", _SERVE_TAGS) == []


@pytest.mark.parametrize("level", ["recommended", "low"])
def test_unit_serve_metric_drops_worker_and_replica_id(set_level, level):
    set_level(level)
    assert _drop("vllm_foo", _SERVE_TAGS) == [WORKER_ID_TAG_KEY, REPLICA_ID_TAG_KEY]


def test_unit_plain_user_metric_is_untouched(set_level):
    # No ReplicaId and not a Ray high-cardinality metric -> nothing dropped.
    for level in ("recommended", "low"):
        set_level(level)
        assert _drop("my_app_metric", _USER_TAGS) == []


def test_unit_tasks_actors_follow_existing_rules(set_level):
    set_level("recommended")
    assert _drop("tasks", _USER_TAGS) == [WORKER_ID_TAG_KEY]
    set_level("low")
    assert _drop("actors", _USER_TAGS) == [
        WORKER_ID_TAG_KEY,
        TASK_OR_ACTOR_NAME_TAG_KEY,
    ]


def test_unit_name_only_call_is_not_cached(set_level):
    # A call without tag_keys must not poison the cache for a Serve metric.
    set_level("recommended")
    assert _drop("vllm_foo", None) == []
    assert _drop("vllm_foo", _SERVE_TAGS) == [WORKER_ID_TAG_KEY, REPLICA_ID_TAG_KEY]


def test_unit_gauge_defaults_to_sum(set_level):
    agg = MetricCardinality.get_aggregation_function("vllm_foo", MetricType.GAUGE)
    assert agg([3.0, 3.0]) == 6.0


def test_unit_counter_and_sum_always_sum(set_level):
    for metric_type in (MetricType.COUNTER, MetricType.SUM):
        agg = MetricCardinality.get_aggregation_function("vllm_foo", metric_type)
        assert agg([1.0, 2.0, 3.0]) == 6.0


def test_unit_histogram_has_no_aggregation_function(set_level):
    with pytest.raises(ValueError):
        MetricCardinality.get_aggregation_function("vllm_foo", MetricType.HISTOGRAM)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
