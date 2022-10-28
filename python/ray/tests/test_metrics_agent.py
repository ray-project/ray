import json
import os
import pathlib
from pprint import pformat
import requests
from unittest.mock import MagicMock

import pytest

import ray
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter
from ray._private.ray_constants import PROMETHEUS_SERVICE_DISCOVERY_FILE
from ray._private.test_utils import (
    SignalActor,
    fetch_prometheus,
    get_log_batch,
    wait_for_condition,
    raw_metrics,
)
from ray.autoscaler._private.constants import AUTOSCALER_METRIC_PORT
from ray.dashboard.consts import DASHBOARD_METRIC_PORT
from ray.util.metrics import Counter, Gauge, Histogram

os.environ["RAY_event_stats"] = "1"

try:
    import prometheus_client
except ImportError:
    prometheus_client = None

# This list of metrics should be kept in sync with src/ray/stats/metric_defs.h
# NOTE: Commented out metrics are not available in this test.
# TODO(Clark): Find ways to trigger commented out metrics in cluster setup.
_METRICS = [
    "ray_node_disk_usage",
    "ray_node_mem_used",
    "ray_node_mem_total",
    "ray_node_cpu_utilization",
    # TODO(rickyx): refactoring the below 3 metric seem to be a bit involved
    # , e.g. need to see how users currently depend on them.
    "ray_object_store_available_memory",
    "ray_object_store_used_memory",
    "ray_object_store_num_local_objects",
    "ray_object_store_memory",
    "ray_object_manager_num_pull_requests",
    "ray_object_directory_subscriptions",
    "ray_object_directory_updates",
    "ray_object_directory_lookups",
    "ray_object_directory_added_locations",
    "ray_object_directory_removed_locations",
    "ray_heartbeat_report_ms_sum",
    "ray_process_startup_time_ms_sum",
    "ray_internal_num_processes_started",
    "ray_internal_num_spilled_tasks",
    # "ray_unintentional_worker_failures_total",
    # "ray_node_failure_total",
    "ray_grpc_server_req_process_time_ms",
    "ray_grpc_server_req_new_total",
    "ray_grpc_server_req_handling_total",
    "ray_grpc_server_req_finished_total",
    "ray_object_manager_received_chunks",
    "ray_pull_manager_usage_bytes",
    "ray_pull_manager_requested_bundles",
    "ray_pull_manager_requests",
    "ray_pull_manager_active_bundles",
    "ray_pull_manager_retries_total",
    "ray_push_manager_in_flight_pushes",
    "ray_push_manager_chunks",
    "ray_scheduler_failed_worker_startup_total",
    "ray_scheduler_tasks",
    "ray_scheduler_unscheduleable_tasks",
    "ray_spill_manager_objects",
    "ray_spill_manager_objects_bytes",
    "ray_spill_manager_request_total",
    # "ray_spill_manager_throughput_mb",
    "ray_gcs_placement_group_creation_latency_ms_sum",
    "ray_gcs_placement_group_scheduling_latency_ms_sum",
    "ray_gcs_placement_group_count",
    "ray_gcs_actors_count",
]

if not ray._raylet.Config.use_ray_syncer():
    _METRICS.append("ray_outbound_heartbeat_size_kb_sum")

# This list of metrics should be kept in sync with
# ray/python/ray/autoscaler/_private/prom_metrics.py
_AUTOSCALER_METRICS = [
    "autoscaler_config_validation_exceptions",
    "autoscaler_node_launch_exceptions",
    "autoscaler_pending_nodes",
    "autoscaler_reset_exceptions",
    "autoscaler_running_workers",
    "autoscaler_started_nodes",
    "autoscaler_stopped_nodes",
    "autoscaler_update_loop_exceptions",
    "autoscaler_worker_create_node_time",
    "autoscaler_worker_update_time",
    "autoscaler_updating_nodes",
    "autoscaler_successful_updates",
    "autoscaler_failed_updates",
    "autoscaler_failed_create_nodes",
    "autoscaler_recovering_nodes",
    "autoscaler_successful_recoveries",
    "autoscaler_failed_recoveries",
    "autoscaler_drain_node_exceptions",
    "autoscaler_update_time",
]


# This list of metrics should be kept in sync with
# ray/python/ray/autoscaler/_private/prom_metrics.py
_DASHBOARD_METRICS = [
    "dashboard_api_requests_duration_seconds",
    "dashboard_api_requests_count",
]


@pytest.fixture
def _setup_cluster_for_test(request, ray_start_cluster):
    enable_metrics_collection = request.param
    NUM_NODES = 2
    cluster = ray_start_cluster
    # Add a head node.
    cluster.add_node(
        _system_config={
            "metrics_report_interval_ms": 1000,
            "event_stats_print_interval_ms": 500,
            "event_stats": True,
            "enable_metrics_collection": enable_metrics_collection,
        }
    )
    # Add worker nodes.
    [cluster.add_node() for _ in range(NUM_NODES - 1)]
    cluster.wait_for_nodes()
    ray_context = ray.init(address=cluster.address)

    worker_should_exit = SignalActor.remote()

    # Generate a metric in the driver.
    counter = Counter("test_driver_counter", description="desc")
    counter.inc()

    # Generate some metrics from actor & tasks.
    @ray.remote
    def f():
        counter = Counter("test_counter", description="desc")
        counter.inc()
        counter = ray.get(ray.put(counter))  # Test serialization.
        counter.inc()
        counter.inc(2)
        ray.get(worker_should_exit.wait.remote())

    # Generate some metrics for the placement group.
    pg = ray.util.placement_group(bundles=[{"CPU": 1}])
    ray.get(pg.ready())
    ray.util.remove_placement_group(pg)

    @ray.remote
    class A:
        async def ping(self):
            histogram = Histogram(
                "test_histogram", description="desc", boundaries=[0.1, 1.6]
            )
            histogram = ray.get(ray.put(histogram))  # Test serialization.
            histogram.observe(1.5)
            ray.get(worker_should_exit.wait.remote())

    a = A.remote()
    obj_refs = [f.remote(), a.ping.remote()]
    # Infeasible task
    b = f.options(resources={"a": 1})  # noqa

    # Make a request to the dashboard to produce some dashboard metrics
    requests.get(f"http://{ray_context.dashboard_url}/nodes")

    node_info_list = ray.nodes()
    prom_addresses = []
    for node_info in node_info_list:
        metrics_export_port = node_info["MetricsExportPort"]
        addr = node_info["NodeManagerAddress"]
        prom_addresses.append(f"{addr}:{metrics_export_port}")
    autoscaler_export_addr = "{}:{}".format(
        cluster.head_node.node_ip_address, AUTOSCALER_METRIC_PORT
    )
    dashboard_export_addr = "{}:{}".format(
        cluster.head_node.node_ip_address, DASHBOARD_METRIC_PORT
    )
    yield prom_addresses, autoscaler_export_addr, dashboard_export_addr

    ray.get(worker_should_exit.send.remote())
    ray.get(obj_refs)
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skipif(prometheus_client is None, reason="Prometheus not installed")
@pytest.mark.parametrize("_setup_cluster_for_test", [True], indirect=True)
def test_metrics_export_end_to_end(_setup_cluster_for_test):
    TEST_TIMEOUT_S = 30
    (
        prom_addresses,
        autoscaler_export_addr,
        dashboard_export_addr,
    ) = _setup_cluster_for_test

    def test_cases():
        components_dict, metric_names, metric_samples = fetch_prometheus(prom_addresses)
        session_name = ray._private.worker.global_worker.node.session_name

        # Raylet should be on every node
        assert all("raylet" in components for components in components_dict.values())

        # GCS server should be on one node
        assert any(
            "gcs_server" in components for components in components_dict.values()
        )

        # Core worker should be on at least on node
        assert any(
            "core_worker" in components for components in components_dict.values()
        )

        # Make sure our user defined metrics exist
        for metric_name in ["test_counter", "test_histogram", "test_driver_counter"]:
            assert any(metric_name in full_name for full_name in metric_names)

        # Make sure metrics are recorded.
        for metric in _METRICS:
            assert metric in metric_names, f"metric {metric} not in {metric_names}"

        for sample in metric_samples:
            if sample.name in _METRICS:
                assert sample.labels["SessionName"] == session_name

        # Make sure the numeric values are correct
        test_counter_sample = [m for m in metric_samples if "test_counter" in m.name][0]
        assert test_counter_sample.value == 4.0

        test_driver_counter_sample = [
            m for m in metric_samples if "test_driver_counter" in m.name
        ][0]
        assert test_driver_counter_sample.value == 1.0

        test_histogram_samples = [
            m for m in metric_samples if "test_histogram" in m.name
        ]
        buckets = {
            m.labels["le"]: m.value
            for m in test_histogram_samples
            if "_bucket" in m.name
        }
        # We recorded value 1.5 for the histogram. In Prometheus data model
        # the histogram is cumulative. So we expect the count to appear in
        # <1.1 and <+Inf buckets.
        assert buckets == {"0.1": 0.0, "1.6": 1.0, "+Inf": 1.0}
        hist_count = [m for m in test_histogram_samples if "_count" in m.name][0].value
        hist_sum = [m for m in test_histogram_samples if "_sum" in m.name][0].value
        assert hist_count == 1
        assert hist_sum == 1.5

        # Make sure the gRPC stats are not reported from workers. We disabled
        # it there because it has too high cardinality.
        grpc_metrics = [
            "ray_grpc_server_req_process_time_ms",
            "ray_grpc_server_req_new_total",
            "ray_grpc_server_req_handling_total",
            "ray_grpc_server_req_finished_total",
        ]
        for grpc_metric in grpc_metrics:
            grpc_samples = [m for m in metric_samples if grpc_metric in m.name]
            for grpc_sample in grpc_samples:
                assert grpc_sample.labels["Component"] != "core_worker"

        # Autoscaler metrics
        _, autoscaler_metric_names, _ = fetch_prometheus([autoscaler_export_addr])
        for metric in _AUTOSCALER_METRICS:
            # Metric name should appear with some suffix (_count, _total,
            # etc...) in the list of all names
            assert any(
                name.startswith(metric) for name in autoscaler_metric_names
            ), f"{metric} not in {autoscaler_metric_names}"

        # Dashboard metrics
        _, dashboard_metric_names, _ = fetch_prometheus([dashboard_export_addr])
        for metric in _DASHBOARD_METRICS:
            # Metric name should appear with some suffix (_count, _total,
            # etc...) in the list of all names
            assert any(
                name.startswith(metric) for name in dashboard_metric_names
            ), f"{metric} not in {dashboard_metric_names}"

    def wrap_test_case_for_retry():
        try:
            test_cases()
            return True
        except AssertionError:
            return False

    try:
        wait_for_condition(
            wrap_test_case_for_retry,
            timeout=TEST_TIMEOUT_S,
            retry_interval_ms=1000,  # Yield resource for other processes
        )
    except RuntimeError:
        print(f"The components are {pformat(fetch_prometheus(prom_addresses))}")
        test_cases()  # Should fail assert


def test_operation_stats(monkeypatch, shutdown_only):
    # Test operation stats are available when flag is on.
    operation_metrics = [
        "ray_operation_count",
        "ray_operation_run_time_ms",
        "ray_operation_queue_time_ms",
        "ray_operation_active_count",
    ]
    with monkeypatch.context() as m:
        m.setenv("RAY_event_stats_metrics", "1")
        addr = ray.init()

        @ray.remote
        def f():
            pass

        ray.get(f.remote())

        def verify():
            metrics = raw_metrics(addr)
            metric_names = set(metrics.keys())
            for op_metric in operation_metrics:
                assert op_metric in metric_names
                samples = metrics[op_metric]
                components = set()
                for sample in samples:
                    components.add(sample.labels["Component"])
            assert {"raylet", "gcs_server", "core_worker"} == components
            return True

        wait_for_condition(verify, timeout=30)


def test_prometheus_file_based_service_discovery(ray_start_cluster):
    # Make sure Prometheus service discovery file is correctly written
    # when number of nodes are dynamically changed.
    NUM_NODES = 5
    cluster = ray_start_cluster
    nodes = [cluster.add_node() for _ in range(NUM_NODES)]
    cluster.wait_for_nodes()
    addr = ray.init(address=cluster.address)
    writer = PrometheusServiceDiscoveryWriter(
        addr["gcs_address"],
        "/tmp/ray",
    )

    def get_metrics_export_address_from_node(nodes):
        node_export_addrs = [
            "{}:{}".format(node.node_ip_address, node.metrics_export_port)
            for node in nodes
        ]
        # monitor should be run on head node for `ray_start_cluster` fixture
        autoscaler_export_addr = "{}:{}".format(
            cluster.head_node.node_ip_address, AUTOSCALER_METRIC_PORT
        )
        dashboard_export_addr = "{}:{}".format(
            cluster.head_node.node_ip_address, DASHBOARD_METRIC_PORT
        )
        return node_export_addrs + [autoscaler_export_addr, dashboard_export_addr]

    loaded_json_data = json.loads(writer.get_file_discovery_content())[0]
    assert set(get_metrics_export_address_from_node(nodes)) == set(
        loaded_json_data["targets"]
    )

    # Let's update nodes.
    for _ in range(3):
        nodes.append(cluster.add_node())

    # Make sure service discovery file content is correctly updated.
    loaded_json_data = json.loads(writer.get_file_discovery_content())[0]
    assert set(get_metrics_export_address_from_node(nodes)) == set(
        loaded_json_data["targets"]
    )


def test_prome_file_discovery_run_by_dashboard(shutdown_only):
    ray.init(num_cpus=0)
    global_node = ray._private.worker._global_node
    temp_dir = global_node.get_temp_dir_path()

    def is_service_discovery_exist():
        for path in pathlib.Path(temp_dir).iterdir():
            if PROMETHEUS_SERVICE_DISCOVERY_FILE in str(path):
                return True
        return False

    wait_for_condition(is_service_discovery_exist)


@pytest.fixture
def metric_mock():
    mock = MagicMock()
    mock.record.return_value = "haha"
    yield mock


"""
Unit test custom metrics.
"""


def test_basic_custom_metrics(metric_mock):
    # Make sure each of metric works as expected.
    # -- Counter --
    count = Counter("count", tag_keys=("a",))
    with pytest.raises(TypeError):
        count.inc("hi")
    with pytest.raises(ValueError):
        count.inc(0)
    with pytest.raises(ValueError):
        count.inc(-1)
    count._metric = metric_mock
    count.inc(1, {"a": "1"})
    metric_mock.record.assert_called_with(1, tags={"a": "1"})

    # -- Gauge --
    gauge = Gauge("gauge", description="gauge")
    gauge._metric = metric_mock
    gauge.record(4)
    metric_mock.record.assert_called_with(4, tags={})

    # -- Histogram
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 3.0], tag_keys=("a", "b")
    )
    histogram._metric = metric_mock
    tags = {"a": "10", "b": "b"}
    histogram.observe(8, tags=tags)
    metric_mock.record.assert_called_with(8, tags=tags)


def test_custom_metrics_info(metric_mock):
    # Make sure .info public method works.
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 2.0], tag_keys=("a", "b")
    )
    assert histogram.info["name"] == "hist"
    assert histogram.info["description"] == "hist"
    assert histogram.info["boundaries"] == [1.0, 2.0]
    assert histogram.info["tag_keys"] == ("a", "b")
    assert histogram.info["default_tags"] == {}
    histogram.set_default_tags({"a": "a"})
    assert histogram.info["default_tags"] == {"a": "a"}


def test_custom_metrics_default_tags(metric_mock):
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 2.0], tag_keys=("a", "b")
    ).set_default_tags({"b": "b"})
    histogram._metric = metric_mock

    # Check specifying non-default tags.
    histogram.observe(10, tags={"a": "a"})
    metric_mock.record.assert_called_with(10, tags={"a": "a", "b": "b"})

    # Check overriding default tags.
    tags = {"a": "10", "b": "c"}
    histogram.observe(8, tags=tags)
    metric_mock.record.assert_called_with(8, tags=tags)


def test_custom_metrics_edge_cases(metric_mock):
    # None or empty boundaries are not allowed.
    with pytest.raises(ValueError):
        Histogram("hist")

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[])

    # Empty name is not allowed.
    with pytest.raises(ValueError):
        Counter("")

    # The tag keys must be a tuple type.
    with pytest.raises(TypeError):
        Counter("name", tag_keys=("a"))

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[-1, 1, 2])

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[0, 1, 2])

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[-1, -0.5, -0.1])


def test_metrics_override_shouldnt_warn(ray_start_regular, log_pubsub):
    # https://github.com/ray-project/ray/issues/12859

    @ray.remote
    def override():
        a = Counter("num_count", description="")
        b = Counter("num_count", description="")
        a.inc(1)
        b.inc(1)

    ray.get(override.remote())

    # Check the stderr from the worker.
    def matcher(log_batch):
        return any("Attempt to register measure" in line for line in log_batch["lines"])

    match = get_log_batch(log_pubsub, 1, timeout=5, matcher=matcher)
    assert len(match) == 0, match


def test_custom_metrics_validation(shutdown_only):
    ray.init()
    # Missing tag(s) from tag_keys.
    metric = Counter("name", tag_keys=("a", "b"))
    metric.set_default_tags({"a": "1"})

    metric.inc(1.0, {"b": "2"})
    metric.inc(1.0, {"a": "1", "b": "2"})

    with pytest.raises(ValueError):
        metric.inc(1.0)

    with pytest.raises(ValueError):
        metric.inc(1.0, {"a": "2"})

    # Extra tag not in tag_keys.
    metric = Counter("name", tag_keys=("a",))
    with pytest.raises(ValueError):
        metric.inc(1.0, {"a": "1", "b": "2"})

    # tag_keys must be tuple.
    with pytest.raises(TypeError):
        Counter("name", tag_keys="a")
    # tag_keys must be strs.
    with pytest.raises(TypeError):
        Counter("name", tag_keys=(1,))

    metric = Counter("name", tag_keys=("a",))
    # Set default tag that isn't in tag_keys.
    with pytest.raises(ValueError):
        metric.set_default_tags({"a": "1", "c": "2"})
    # Default tag value must be str.
    with pytest.raises(TypeError):
        metric.set_default_tags({"a": 1})
    # Tag value must be str.
    with pytest.raises(TypeError):
        metric.inc(1.0, {"a": 1})


@pytest.mark.parametrize("_setup_cluster_for_test", [False], indirect=True)
def test_metrics_disablement(_setup_cluster_for_test):
    """Make sure the metrics are not exported when it is disabled."""
    prom_addresses, autoscaler_export_addr, _ = _setup_cluster_for_test

    def verify_metrics_not_collected():
        components_dict, metric_names, _ = fetch_prometheus(prom_addresses)
        # Make sure no component is reported.
        for _, comp in components_dict.items():
            if len(comp) > 0:
                print(f"metrics from a component {comp} exists although it should not.")
                return False

        # Make sure metrics are not there.
        for metric in _METRICS + _AUTOSCALER_METRICS + _DASHBOARD_METRICS:
            if metric in metric_names:
                print("f{metric} exists although it should not.")
                return False
        return True

    # Make sure metrics are not collected for more than 10 seconds.
    for _ in range(10):
        assert verify_metrics_not_collected()
        import time

        time.sleep(1)


if __name__ == "__main__":
    import sys

    # Test suite is timing out. Disable on windows for now.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
