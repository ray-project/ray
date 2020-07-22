import pytest
import random
import socket

import requests
from opencensus.tags import tag_key as tag_key_module
from prometheus_client.parser import text_string_to_metric_families

from ray.core.generated.common_pb2 import MetricPoint
from ray.metrics_agent import Gauge, MetricsAgent


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


def get_unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]

    # Try to generate a port that is far above the 'next available' one.
    # This solves issue #8254 where GRPC fails because the port assigned
    # from this method has been used by a different process.
    for _ in range(30):
        new_port = random.randint(port, 65535)
        new_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            new_s.bind(("", new_port))
        except OSError:
            new_s.close()
            continue
        s.close()
        new_s.close()
        return new_port
    print("Unable to succeed in selecting a random port.")
    s.close()
    return port


@pytest.fixture
def metrics_agent():
    agent = MetricsAgent(get_unused_port())
    yield agent


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


def test_basic_e2e(metrics_agent):
    tag = {"TAG_KEY": "TAG_VALUE"}
    metrics_points = [
        generate_metrics_point(
            str(i), float(i), i, tag, description=str(i), units=str(i))
        for i in range(3)
    ]
    metrics_points_dict = {
        metric_point.metric_name: metric_point
        for metric_point in metrics_points
    }
    assert metrics_agent.record_metrics_points(metrics_points) is False
    # Make sure all metrics are registered.
    for i, metric_entry in enumerate(metrics_agent.registry.items()):
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


def test_missing_def(metrics_agent):
    # Make sure the description and units are properly updated when
    # they are reported later.
    tag = {"TAG_KEY": "TAG_VALUE"}
    metrics_points = [
        generate_metrics_point(
            str(i),
            float(i),
            i,
            tag,
        ) for i in range(3)
    ]

    # At first, metrics shouldn't have description and units.
    assert metrics_agent.record_metrics_points(metrics_points) is True
    for i, metric_entry in enumerate(metrics_agent.registry.items()):
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
        for i in range(3)
    ]
    assert metrics_agent.record_metrics_points(metrics_points) is False
    for i, metric_entry in enumerate(metrics_agent.registry.items()):
        metric_name, metric_entry = metric_entry
        assert metric_name == metric_entry.name
        assert metric_entry.name == str(i)
        assert metric_entry.description == str(i)
        assert metric_entry.units == str(i)
        assert metric_entry.tags == [tag_key_module.TagKey(key) for key in tag]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
