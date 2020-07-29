import pytest

from collections import defaultdict

import requests

from opencensus.tags import tag_key as tag_key_module
from prometheus_client.parser import text_string_to_metric_families

from ray.core.generated.common_pb2 import MetricPoint
from ray.dashboard.util import get_unused_port
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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
