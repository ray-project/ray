import json
import pathlib
import platform
from pprint import pformat
import sys
import time
from unittest.mock import MagicMock

import pytest

import ray
from ray.ray_constants import PROMETHEUS_SERVICE_DISCOVERY_FILE
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter
from ray.util.metrics import Count, Histogram, Gauge
from ray.test_utils import wait_for_condition, SignalActor, fetch_prometheus


@pytest.fixture
def _setup_cluster_for_test(ray_start_cluster):
    NUM_NODES = 2
    cluster = ray_start_cluster
    # Add a head node.
    cluster.add_node(_system_config={"metrics_report_interval_ms": 1000})
    # Add worker nodes.
    [cluster.add_node() for _ in range(NUM_NODES - 1)]
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    worker_should_exit = SignalActor.remote()

    # Generate a metric in the driver.
    counter = Count("test_driver_counter", description="desc")
    counter.inc()

    # Generate some metrics from actor & tasks.
    @ray.remote
    def f():
        counter = Count("test_counter", description="desc")
        counter.inc()
        counter = ray.get(ray.put(counter))  # Test serialization.
        counter.inc()
        ray.get(worker_should_exit.wait.remote())

    @ray.remote
    class A:
        async def ping(self):
            histogram = Histogram(
                "test_histogram", description="desc", boundaries=[0.1, 1.6])
            histogram = ray.get(ray.put(histogram))  # Test serialization.
            histogram.record(1.5)
            ray.get(worker_should_exit.wait.remote())

    a = A.remote()
    obj_refs = [f.remote(), a.ping.remote()]

    node_info_list = ray.nodes()
    prom_addresses = []
    for node_info in node_info_list:
        metrics_export_port = node_info["MetricsExportPort"]
        addr = node_info["NodeManagerAddress"]
        prom_addresses.append(f"{addr}:{metrics_export_port}")

    yield prom_addresses

    ray.get(worker_should_exit.send.remote())
    ray.get(obj_refs)
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_metrics_export_end_to_end(_setup_cluster_for_test):
    TEST_TIMEOUT_S = 20

    prom_addresses = _setup_cluster_for_test

    def test_cases():
        components_dict, metric_names, metric_samples = fetch_prometheus(
            prom_addresses)

        # Raylet should be on every node
        assert all(
            "raylet" in components for components in components_dict.values())

        # GCS server should be on one node
        assert any("gcs_server" in components
                   for components in components_dict.values())

        # Core worker should be on at least on node
        assert any("core_worker" in components
                   for components in components_dict.values())

        # Make sure our user defined metrics exist
        for metric_name in [
                "test_counter", "test_histogram", "test_driver_counter"
        ]:
            assert any(metric_name in full_name for full_name in metric_names)

        # Make sure GCS server metrics are recorded.
        assert "ray_outbound_heartbeat_size_kb_sum" in metric_names

        # Make sure the numeric values are correct
        test_counter_sample = [
            m for m in metric_samples if "test_counter" in m.name
        ][0]
        assert test_counter_sample.value == 2.0

        test_driver_counter_sample = [
            m for m in metric_samples if "test_driver_counter" in m.name
        ][0]
        assert test_driver_counter_sample.value == 1.0

        test_histogram_samples = [
            m for m in metric_samples if "test_histogram" in m.name
        ]
        buckets = {
            m.labels["le"]: m.value
            for m in test_histogram_samples if "_bucket" in m.name
        }
        # We recorded value 1.5 for the histogram. In Prometheus data model
        # the histogram is cumulative. So we expect the count to appear in
        # <1.1 and <+Inf buckets.
        assert buckets == {"0.1": 0.0, "1.6": 1.0, "+Inf": 1.0}
        hist_count = [m for m in test_histogram_samples
                      if "_count" in m.name][0].value
        hist_sum = [m for m in test_histogram_samples
                    if "_sum" in m.name][0].value
        assert hist_count == 1
        assert hist_sum == 1.5

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
        print(
            f"The components are {pformat(fetch_prometheus(prom_addresses))}")
        test_cases()  # Should fail assert


def test_prometheus_file_based_service_discovery(ray_start_cluster):
    # Make sure Prometheus service discovery file is correctly written
    # when number of nodes are dynamically changed.
    NUM_NODES = 5
    cluster = ray_start_cluster
    nodes = [cluster.add_node() for _ in range(NUM_NODES)]
    cluster.wait_for_nodes()
    addr = ray.init(address=cluster.address)
    redis_address = addr["redis_address"]
    writer = PrometheusServiceDiscoveryWriter(
        redis_address, ray.ray_constants.REDIS_DEFAULT_PASSWORD, "/tmp/ray")

    def get_metrics_export_address_from_node(nodes):
        return [
            "{}:{}".format(node.node_ip_address, node.metrics_export_port)
            for node in nodes
        ]

    loaded_json_data = json.loads(writer.get_file_discovery_content())[0]
    assert (set(get_metrics_export_address_from_node(nodes)) == set(
        loaded_json_data["targets"]))

    # Let's update nodes.
    for _ in range(3):
        nodes.append(cluster.add_node())

    # Make sure service discovery file content is correctly updated.
    loaded_json_data = json.loads(writer.get_file_discovery_content())[0]
    assert (set(get_metrics_export_address_from_node(nodes)) == set(
        loaded_json_data["targets"]))


@pytest.mark.skipif(
    platform.system() == "Windows", reason="Failing on Windows.")
def test_prome_file_discovery_run_by_dashboard(shutdown_only):
    ray.init(num_cpus=0)
    global_node = ray.worker._global_node
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
    # -- Count --
    count = Count("count", tag_keys=("a", ))
    with pytest.raises(TypeError):
        count.inc("hi")
    with pytest.raises(ValueError):
        count.inc(0)
        count.inc(-1)
    count._metric = metric_mock
    count.record(1, {"a": "1"})
    metric_mock.record.assert_called_with(1, tags={"a": "1"})

    # -- Gauge --
    gauge = Gauge("gauge", description="gauge")
    gauge._metric = metric_mock
    gauge.record(4)
    metric_mock.record.assert_called_with(4, tags={})

    # -- Histogram
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 3.0], tag_keys=("a", "b"))
    histogram._metric = metric_mock
    tags = {"a": "10", "b": "b"}
    histogram.record(8, tags=tags)
    metric_mock.record.assert_called_with(8, tags=tags)


def test_custom_metrics_info(metric_mock):
    # Make sure .info public method works.
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 2.0], tag_keys=("a", "b"))
    assert histogram.info["name"] == "hist"
    assert histogram.info["description"] == "hist"
    assert histogram.info["boundaries"] == [1.0, 2.0]
    assert histogram.info["tag_keys"] == ("a", "b")
    assert histogram.info["default_tags"] == {}
    histogram.set_default_tags({"a": "a"})
    assert histogram.info["default_tags"] == {"a": "a"}


def test_custom_metrics_default_tags(metric_mock):
    histogram = Histogram(
        "hist", description="hist", boundaries=[1.0, 2.0],
        tag_keys=("a", "b")).set_default_tags({
            "b": "b"
        })
    histogram._metric = metric_mock

    # Check specifying non-default tags.
    histogram.record(10, tags={"a": "a"})
    metric_mock.record.assert_called_with(10, tags={"a": "a", "b": "b"})

    # Check overriding default tags.
    tags = {"a": "10", "b": "c"}
    histogram.record(8, tags=tags)
    metric_mock.record.assert_called_with(8, tags=tags)


def test_custom_metrics_edge_cases(metric_mock):
    # None or empty boundaries are not allowed.
    with pytest.raises(ValueError):
        Histogram("hist")

    with pytest.raises(ValueError):
        Histogram("hist", boundaries=[])

    # Empty name is not allowed.
    with pytest.raises(ValueError):
        Count("")

    # The tag keys must be a tuple type.
    with pytest.raises(TypeError):
        Count("name", tag_keys=("a"))


def test_metrics_override_shouldnt_warn(ray_start_regular, log_pubsub):
    # https://github.com/ray-project/ray/issues/12859

    @ray.remote
    def override():
        a = Count("num_count", description="")
        b = Count("num_count", description="")
        a.record(1)
        b.record(1)

    ray.get(override.remote())

    # Check the stderr from the worker.
    start = time.time()
    while True:
        if (time.time() - start) > 5:
            break
        msg = log_pubsub.get_message()
        if msg is None:
            time.sleep(0.01)
            continue

        log_lines = json.loads(ray._private.utils.decode(msg["data"]))["lines"]
        for line in log_lines:
            assert "Attempt to register measure" not in line


def test_custom_metrics_validation(ray_start_regular_shared):
    # Missing tag(s) from tag_keys.
    metric = Count("name", tag_keys=("a", "b"))
    metric.set_default_tags({"a": "1"})

    metric.record(1.0, {"b": "2"})
    metric.record(1.0, {"a": "1", "b": "2"})

    with pytest.raises(ValueError):
        metric.record(1.0)

    with pytest.raises(ValueError):
        metric.record(1.0, {"a": "2"})

    # Extra tag not in tag_keys.
    metric = Count("name", tag_keys=("a", ))
    with pytest.raises(ValueError):
        metric.record(1.0, {"a": "1", "b": "2"})

    # tag_keys must be tuple.
    with pytest.raises(TypeError):
        Count("name", tag_keys="a")
    # tag_keys must be strs.
    with pytest.raises(TypeError):
        Count("name", tag_keys=(1, ))

    metric = Count("name", tag_keys=("a", ))
    # Set default tag that isn't in tag_keys.
    with pytest.raises(ValueError):
        metric.set_default_tags({"a": "1", "c": "2"})
    # Default tag value must be str.
    with pytest.raises(TypeError):
        metric.set_default_tags({"a": 1})
    # Tag value must be str.
    with pytest.raises(TypeError):
        metric.record(1.0, {"a": 1})


if __name__ == "__main__":
    import sys
    # Test suite is timing out. Disable on windows for now.
    sys.exit(pytest.main(["-v", __file__]))
