import json
import pytest

from collections import defaultdict

import requests

from opencensus.tags import tag_key as tag_key_module
from prometheus_client.parser import text_string_to_metric_families

import ray

from ray.core.generated.common_pb2 import MetricPoint
from ray.dashboard.util import get_unused_port
from ray.metrics_agent import (Gauge, MetricsAgent,
                               PrometheusServiceDiscoveryWriter)
from ray.test_utils import wait_for_condition


def generate_metrics_point(name: str,
                           value: float,
                           timestamp: int,
                           tags: dict,
                           description: str = None,
                           units: str = None):
    return MetricPoint(
        metric_name=name,
        timestamp=timestamp,
        value=value,
        tags=tags,
        description=description,
        units=units)


# NOTE: Opencensus metrics is a singleton per process.
# That says, we should re-use the same agent for all tests.
# Please be careful when you add new tests here. If each
# test doesn't use different metrics, it can have some confliction.
metrics_agent = None


@pytest.fixture
def cleanup_agent():
    global metrics_agent
    if not metrics_agent:
        metrics_agent = MetricsAgent(get_unused_port())
    yield
    metrics_agent._registry = defaultdict(lambda: None)


def test_gauge():
    tags = [tag_key_module.TagKey(str(i)) for i in range(10)]
    name = "name"
    description = "description"
    units = "units"
    gauge = Gauge(name, description, units, tags)
    assert gauge.__dict__()["name"] == name
    assert gauge.__dict__()["description"] == description
    assert gauge.__dict__()["units"] == units
    assert gauge.__dict__()["tags"] == tags


def test_basic_e2e(cleanup_agent):
    # Test the basic end to end workflow. This includes.
    # - Metrics are reported.
    # - Metrics are dynamically registered to registry.
    # - Metrics are accessbiel from Prometheus.
    POINTS_DEF = [0, 1, 2]
    tag = {"TAG_KEY": "TAG_VALUE"}
    metrics_points = [
        generate_metrics_point(
            str(i), float(i), i, tag, description=str(i), units=str(i))
        for i in POINTS_DEF
    ]
    metrics_points_dict = {
        metric_point.metric_name: metric_point
        for metric_point in metrics_points
    }
    assert metrics_agent.record_metrics_points(metrics_points) is False
    # Make sure all metrics are registered.
    for i, metric_entry in zip(POINTS_DEF, metrics_agent.registry.items()):
        metric_name, metric_entry = metric_entry
        assert metric_name == metric_entry.name
        assert metric_entry.name == str(i)
        assert metric_entry.description == str(i)
        assert metric_entry.units == str(i)
        assert metric_entry.tags == [tag_key_module.TagKey(key) for key in tag]

    # Make sure all metrics are available through a port.
    response = requests.get("http://localhost:{}".format(
        metrics_agent.metrics_export_port))
    response.raise_for_status()
    for line in response.text.split("\n"):
        for family in text_string_to_metric_families(line):
            metric_name = family.name

            if metric_name not in metrics_points_dict:
                continue

            if line.startswith("# HELP"):
                # description
                assert (family.documentation == metrics_points_dict[
                    metric_name].description)
            else:
                for sample in family.samples:
                    metrics_points_dict[metric_name].value == sample.value


def test_missing_def(cleanup_agent):
    # Make sure when metrics with description and units are reported,
    # agent updates its registry to include them.
    POINTS_DEF = [4, 5, 6]
    tag = {"TAG_KEY": "TAG_VALUE"}
    metrics_points = [
        generate_metrics_point(
            str(i),
            float(i),
            i,
            tag,
        ) for i in POINTS_DEF
    ]

    # At first, metrics shouldn't have description and units.
    assert metrics_agent.record_metrics_points(metrics_points) is True
    for i, metric_entry in zip(POINTS_DEF, metrics_agent.registry.items()):
        metric_name, metric_entry = metric_entry
        assert metric_name == metric_entry.name
        assert metric_entry.name == str(i)
        assert metric_entry.description == ""
        assert metric_entry.units == ""
        assert metric_entry.tags == [tag_key_module.TagKey(key) for key in tag]

    # The points are coming again with description and units.
    # Make sure they are updated.
    metrics_points = [
        generate_metrics_point(
            str(i), float(i), i, tag, description=str(i), units=str(i))
        for i in POINTS_DEF
    ]
    assert metrics_agent.record_metrics_points(metrics_points) is False
    for i, metric_entry in zip(POINTS_DEF, metrics_agent.registry.items()):
        metric_name, metric_entry = metric_entry
        assert metric_name == metric_entry.name
        assert metric_entry.name == str(i)
        assert metric_entry.description == str(i)
        assert metric_entry.units == str(i)
        assert metric_entry.tags == [tag_key_module.TagKey(key) for key in tag]


def test_multiple_record(cleanup_agent):
    # Make sure prometheus export data properly when multiple points with
    # the same name is reported.
    TOTAL_POINTS = 10
    NAME = "TEST"
    values = list(range(TOTAL_POINTS))
    tags = [{"TAG_KEY": str(i)} for i in range(TOTAL_POINTS)]
    timestamps = list(range(TOTAL_POINTS))
    points = []

    for i in range(TOTAL_POINTS):
        points.append(
            generate_metrics_point(
                name=NAME,
                value=values[i],
                timestamp=timestamps[i],
                tags=tags[i]))
    for point in points:
        metrics_agent.record_metrics_points([point])

    # Make sure data is available at prometheus.
    response = requests.get("http://localhost:{}".format(
        metrics_agent.metrics_export_port))
    response.raise_for_status()

    sample_values = []
    for line in response.text.split("\n"):
        for family in text_string_to_metric_families(line):
            metric_name = family.name
            name_without_prefix = metric_name.split("_")[1]
            if name_without_prefix != NAME:
                continue
            # Lines for recorded metrics values.
            for sample in family.samples:
                sample_values.append(sample.value)
    assert sample_values == [point.value for point in points]


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


def test_metrics_export_end_to_end(ray_start_cluster):
    NUM_NODES = 2
    cluster = ray_start_cluster
    # Add a head node.
    cluster.add_node(
        _internal_config=json.dumps({
            "metrics_report_interval_ms": 1000
        }))
    # Add worker nodes.
    [cluster.add_node() for _ in range(NUM_NODES - 1)]
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Generate some metrics around actor & tasks.
    @ray.remote
    def f():
        return 3

    @ray.remote
    class A:
        def ping(self):
            return 3

    ray.get([f.remote() for _ in range(30)])
    a = A.remote()
    ray.get(a.ping.remote())

    node_info_list = ray.nodes()
    prom_addresses = []
    for node_info in node_info_list:
        metrics_export_port = node_info["MetricsExportPort"]
        addr = node_info["NodeManagerAddress"]
        prom_addresses.append(f"{addr}:{metrics_export_port}")

    # Make sure we can ping Prometheus endpoints.
    def get_component_information(prom_addresses):
        # TODO(sang): Add a core worker & gcs_server after adding metrics.
        components_dict = {}
        for address in prom_addresses:
            if address not in components_dict:
                components_dict[address] = set()
            try:
                response = requests.get(
                    "http://localhost:{}".format(metrics_export_port))
            except requests.exceptions.ConnectionError:
                return components_dict

            for line in response.text.split("\n"):
                for family in text_string_to_metric_families(line):
                    for sample in family.samples:
                        # print(sample)
                        if "Component" in sample.labels:
                            components_dict[address].add(
                                sample.labels["Component"])
        return components_dict

    def test_prometheus_endpoint():
        # TODO(sang): Add a core worker & gcs_server after adding metrics.
        components_dict = get_component_information(prom_addresses)
        COMPONENTS_CANDIDATES = {"raylet"}
        return all(
            COMPONENTS_CANDIDATES.issubset(components)
            for components in components_dict.values())

    try:
        wait_for_condition(test_prometheus_endpoint, timeout=3)
    except RuntimeError:
        # This is for debugging when test failed.
        print(get_component_information(prom_addresses))
        raise RuntimeError("All components were not visible to "
                           "prometheus endpoints on time.")
    ray.shutdown()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
