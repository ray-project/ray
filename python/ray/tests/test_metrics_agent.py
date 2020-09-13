import json
from pprint import pformat

import requests
import pytest
from prometheus_client.parser import text_string_to_metric_families

import ray
from ray.metrics_agent import PrometheusServiceDiscoveryWriter
from ray.experimental.metrics import Count, Histogram
from ray.test_utils import wait_for_condition, SignalActor


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

    # Generate some metrics from actor & tasks.
    @ray.remote
    def f():
        counter = Count(f"test_counter", "desc", "unit", [])
        counter.record(1, {})
        ray.get(worker_should_exit.wait.remote())

    @ray.remote
    class A:
        async def ping(self):
            histogram = Histogram("test_histogram", "desc", "unit", [0.1, 1.6],
                                  [])
            histogram.record(1.5, {})
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


def test_metrics_export_end_to_end(_setup_cluster_for_test):
    TEST_TIMEOUT_S = 20

    prom_addresses = _setup_cluster_for_test

    # Make sure we can ping Prometheus endpoints.
    def fetch_prometheus(prom_addresses):
        components_dict = {}
        metric_names = set()
        metric_samples = []
        for address in prom_addresses:
            if address not in components_dict:
                components_dict[address] = set()
            try:
                response = requests.get(f"http://{address}/metrics")
            except requests.exceptions.ConnectionError:
                continue

            for line in response.text.split("\n"):
                for family in text_string_to_metric_families(line):
                    for sample in family.samples:
                        metric_names.add(sample.name)
                        metric_samples.append(sample)
                        if "Component" in sample.labels:
                            components_dict[address].add(
                                sample.labels["Component"])
        return components_dict, metric_names, metric_samples

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
        for metric_name in ["test_counter", "test_histogram"]:
            assert any(metric_name in full_name for full_name in metric_names)

        # Make sure the numeric value is correct
        test_counter_sample = [
            m for m in metric_samples if "test_counter" in m.name
        ][0]
        assert test_counter_sample.value == 1.0

        # Make sure the numeric value is correct
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
            f"The compoenents are {pformat(fetch_prometheus(prom_addresses))}")
        test_cases()  # Should fail assert


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
