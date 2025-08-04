import random
import sys
import time
from unittest.mock import patch

import pytest

from ray._common.test_utils import wait_for_condition
import ray._private.prometheus_exporter as prometheus_exporter

from typing import List

from opencensus.metrics.export.metric_descriptor import MetricDescriptorType
from opencensus.stats.view_manager import ViewManager
from opencensus.stats.stats_recorder import StatsRecorder
from opencensus.stats import execution_context
from prometheus_client.core import REGISTRY


from ray._private.metrics_agent import Gauge, MetricsAgent, Record, RAY_WORKER_TIMEOUT_S
from opencensus.stats.aggregation_data import (
    LastValueAggregationData,
    SumAggregationData,
    CountAggregationData,
    DistributionAggregationData,
)
from opencensus.metrics.export.value import ValueDouble
from ray._private.metrics_agent import (
    MetricCardinalityLevel,
    OpenCensusProxyCollector,
    OpencensusProxyMetric,
    WORKER_ID_TAG_KEY,
)
from ray.core.generated.metrics_pb2 import (
    Metric,
    MetricDescriptor,
    Point,
    LabelKey,
    TimeSeries,
    LabelValue,
)
from ray._raylet import WorkerID
from ray._private.test_utils import (
    fetch_prometheus_metrics,
    fetch_raw_prometheus,
)


def raw_metrics(export_port):
    metrics_page = "localhost:{}".format(export_port)
    res = fetch_prometheus_metrics([metrics_page])
    return res


def get_metric(metric_name, export_port):
    res = raw_metrics(export_port)
    for name, samples in res.items():
        if name == metric_name:
            return name, samples

    return None


def get_prom_metric_name(namespace, metric_name):
    return f"{namespace}_{metric_name}"


def generate_timeseries(label_values: List[str], points: List[float]):
    return TimeSeries(
        label_values=[LabelValue(value=val) for val in label_values],
        points=[Point(double_value=val) for val in points],
    )


def generate_protobuf_metric(
    name: str,
    desc: str,
    unit: str,
    type: MetricDescriptorType,
    label_keys: List[str] = None,
    timeseries: List[TimeSeries] = None,
):
    if not label_keys:
        label_keys = []
    if not timeseries:
        timeseries = []

    return Metric(
        metric_descriptor=MetricDescriptor(
            name=name,
            description=desc,
            unit=unit,
            type=type,
            label_keys=[LabelKey(key="a"), LabelKey(key="b")],
        ),
        timeseries=timeseries,
    )


@pytest.fixture
def get_agent(request, monkeypatch):
    with monkeypatch.context() as m:
        if hasattr(request, "param"):
            delay = request.param
        else:
            delay = 0

        m.setenv(RAY_WORKER_TIMEOUT_S, delay)
        agent_port = random.randint(10000, 65535)
        stats_recorder = StatsRecorder()
        view_manager = ViewManager()
        stats_exporter = prometheus_exporter.new_stats_exporter(
            prometheus_exporter.Options(
                namespace="test",
                port=agent_port,
                address="127.0.0.1",
            )
        )
        agent = MetricsAgent(view_manager, stats_recorder, stats_exporter)
        REGISTRY.register(agent.proxy_exporter_collector)
        yield agent, agent_port
        REGISTRY.unregister(agent.stats_exporter.collector)
        REGISTRY.unregister(agent.proxy_exporter_collector)
        execution_context.set_measure_to_view_map({})


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_metrics_agent_record_and_export(get_agent):
    namespace = "test"
    agent, agent_port = get_agent

    # Record a new gauge.
    metric_name = "test"
    test_gauge = Gauge(metric_name, "desc", "unit", ["tag"])
    record_a = Record(
        gauge=test_gauge,
        value=3,
        tags={"tag": "a"},
    )
    agent.record_and_export([record_a])
    name, samples = get_metric(get_prom_metric_name(namespace, metric_name), agent_port)
    assert name == get_prom_metric_name(namespace, metric_name)
    assert len(samples) == 1
    assert samples[0].value == 3
    assert samples[0].labels == {"tag": "a"}

    # Record the same gauge.
    record_b = Record(
        gauge=test_gauge,
        value=4,
        tags={"tag": "a"},
    )
    record_c = Record(
        gauge=test_gauge,
        value=4,
        tags={"tag": "a"},
    )
    agent.record_and_export([record_b, record_c])
    name, samples = get_metric(get_prom_metric_name(namespace, metric_name), agent_port)
    assert name == get_prom_metric_name(namespace, metric_name)
    assert len(samples) == 1
    assert samples[0].value == 4
    assert samples[0].labels == {"tag": "a"}

    # Record the same gauge with different ag.
    record_d = Record(
        gauge=test_gauge,
        value=6,
        tags={"tag": "aa"},
    )
    agent.record_and_export(
        [
            record_d,
        ]
    )
    name, samples = get_metric(get_prom_metric_name(namespace, metric_name), agent_port)
    assert name == get_prom_metric_name(namespace, metric_name)
    assert len(samples) == 2
    assert samples[0].value == 4
    assert samples[0].labels == {"tag": "a"}
    assert samples[1].value == 6
    assert samples[1].labels == {"tag": "aa"}

    # Record more than 1 gauge.
    metric_name_2 = "test2"
    test_gauge_2 = Gauge(metric_name_2, "desc", "unit", ["tag"])
    record_e = Record(
        gauge=test_gauge_2,
        value=1,
        tags={"tag": "b"},
    )
    agent.record_and_export([record_e])
    name, samples = get_metric(
        get_prom_metric_name(namespace, metric_name_2), agent_port
    )

    assert name == get_prom_metric_name(namespace, metric_name_2)
    assert samples[0].value == 1
    assert samples[0].labels == {"tag": "b"}

    # Make sure the previous record is still there.
    name, samples = get_metric(get_prom_metric_name(namespace, metric_name), agent_port)
    assert name == get_prom_metric_name(namespace, metric_name)
    assert len(samples) == 2
    assert samples[0].value == 4
    assert samples[0].labels == {"tag": "a"}
    assert samples[1].value == 6
    assert samples[1].labels == {"tag": "aa"}


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_metrics_agent_record_and_export_failed_records_dont_block_other_records(
    get_agent,
    capsys,
):
    namespace = "test"
    agent, agent_port = get_agent

    metric_name = "test"
    test_gauge = Gauge(metric_name, "desc", "unit", ["tag"])
    record_a = Record(
        gauge=test_gauge,
        value=1,
        tags={"tag": "a"},
    )
    record_b = Record(
        gauge=test_gauge,
        value=1,
        # this tag is much too long (>255 characters), so recording this metric will fail
        tags={"tag": "b" * 1000},
    )
    record_c = Record(
        gauge=test_gauge,
        value=1,
        tags={"tag": "c"},
    )
    agent.record_and_export([record_a, record_b, record_c])

    name, samples = get_metric(get_prom_metric_name(namespace, metric_name), agent_port)
    assert name == get_prom_metric_name(namespace, metric_name)

    # a and c should be recorded, b's failure should be ignored
    assert {sample.labels["tag"] for sample in samples} == {"a", "c"}


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_metrics_agent_proxy_record_and_export_basic(get_agent):
    """Test the case the metrics are exported without worker_id."""
    namespace = "test"
    agent, agent_port = get_agent

    # Test the basic case.
    m = generate_protobuf_metric(
        "test",
        "desc",
        "",
        MetricDescriptorType.GAUGE_DOUBLE,
        label_keys=["a", "b"],
        timeseries=[],
    )
    m.timeseries.append(generate_timeseries(["a", "b"], [1, 2, 3]))
    agent.proxy_export_metrics([m])
    name, samples = get_metric(f"{namespace}_test", agent_port)
    assert name == f"{namespace}_test"
    assert len(samples) == 1
    assert samples[0].labels == {"a": "a", "b": "b"}
    assert samples[0].value == 3

    # Test new metric has proxyed.
    m = generate_protobuf_metric(
        "test",
        "desc",
        "",
        MetricDescriptorType.GAUGE_DOUBLE,
        label_keys=["a", "b"],
        timeseries=[],
    )
    m.timeseries.append(generate_timeseries(["a", "b"], [4]))
    agent.proxy_export_metrics([m])
    name, samples = get_metric(f"{namespace}_test", agent_port)
    assert name == f"{namespace}_test"
    assert len(samples) == 1
    assert samples[0].labels == {"a": "a", "b": "b"}
    assert samples[0].value == 4

    # Test new metric with different tag is reported.
    m = generate_protobuf_metric(
        "test",
        "desc",
        "",
        MetricDescriptorType.GAUGE_DOUBLE,
        label_keys=["a", "b"],
        timeseries=[],
    )
    m.timeseries.append(generate_timeseries(["a", "c"], [5]))
    agent.proxy_export_metrics([m])
    name, samples = get_metric(f"{namespace}_test", agent_port)
    assert name == f"{namespace}_test"
    assert len(samples) == 2
    assert samples[0].labels == {"a": "a", "b": "b"}
    assert samples[0].value == 4
    # Newly added metric has different tags and values.
    assert samples[1].labels == {"a": "a", "b": "c"}
    assert samples[1].value == 5


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_metrics_agent_proxy_record_and_export_from_workers(get_agent):
    """
    Test the basic worker death case.
    """
    namespace = "test"
    agent, agent_port = get_agent
    worker_id = WorkerID.from_random()

    m = generate_protobuf_metric(
        "test",
        "desc",
        "",
        MetricDescriptorType.GAUGE_DOUBLE,
        label_keys=["a", "b"],
        timeseries=[],
    )
    m.timeseries.append(generate_timeseries(["a", "b"], [1, 2, 3]))
    agent.proxy_export_metrics([m], worker_id_hex=worker_id.hex())
    # Metrics should be exposed.
    assert get_metric(f"{namespace}_test", agent_port) is not None
    agent.clean_all_dead_worker_metrics()
    # Once the worker is dead, metrics should be unavailble.
    assert get_metric(f"{namespace}_test", agent_port) is None
    # Once the worker metrics is re-reported, it is treated as alive again.
    agent.proxy_export_metrics([m], worker_id_hex=worker_id.hex())
    assert get_metric(f"{namespace}_test", agent_port) is not None
    # Clean it again and the worker metrics is cleaned again.
    agent.clean_all_dead_worker_metrics()
    assert get_metric(f"{namespace}_test", agent_port) is None


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_metrics_agent_proxy_record_and_export_from_workers_complicated(
    get_agent,
):  # noqa
    """
    Test the complicated worker death case.
    """
    namespace = "test"
    agent, agent_port = get_agent

    # Each worker will report 2 metrics.
    # i.e.,
    # worker 1 => test_1, test_2.
    # worker 2 => test_3, test_4.
    # ...
    worker_ids = [WorkerID.from_random() for _ in range(4)]

    metrics = []
    for i in range(8):
        m = generate_protobuf_metric(
            f"test_{i}",
            "desc",
            "",
            MetricDescriptorType.GAUGE_DOUBLE,
            label_keys=["a", "b"],
            timeseries=[],
        )
        m.timeseries.append(generate_timeseries(["a", str(i)], [3]))
        metrics.append(m)

    i = 0
    for worker_id in worker_ids:
        agent.proxy_export_metrics(
            [metrics[i], metrics[i + 1]], worker_id_hex=worker_id.hex()
        )
        i += 2

    # All metrics must be available.
    for i in range(len(metrics)):
        assert get_metric(f"{namespace}_test_{i}", agent_port) is not None

    # Mark the worker as dead and make sure metrics are properly cleaned.
    i = 0

    while len(worker_ids):
        for worker_id in worker_ids:
            agent.clean_all_dead_worker_metrics()
            assert get_metric(f"{namespace}_test_{i}", agent_port) is None
            assert get_metric(f"{namespace}_test_{i+1}", agent_port) is None

        worker_ids.pop(0)
        metrics.pop(0)
        metrics.pop(0)

        i = 0
        for worker_id in worker_ids:
            agent.proxy_export_metrics(
                [metrics[i], metrics[i + 1]], worker_id_hex=worker_id.hex()
            )
            i += 2

        # Make sure the rest of metrics are still there because new metrics
        # are reported.
        for j in range(i + 2, len(metrics)):
            assert get_metric(f"{namespace}_test_{j}", agent_port) is not None, j
        i += 2


DELAY = 3


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
@pytest.mark.parametrize("get_agent", [DELAY], indirect=True)
def test_metrics_agent_proxy_record_and_export_from_workers_delay(get_agent):  # noqa
    """
    Test the worker metrics are deleted after the delay.
    """
    namespace = "test"
    agent, agent_port = get_agent
    worker_id = WorkerID.from_random()

    m = generate_protobuf_metric(
        "test",
        "desc",
        "",
        MetricDescriptorType.GAUGE_DOUBLE,
        label_keys=["a", "b"],
        timeseries=[],
    )
    m.timeseries.append(generate_timeseries(["a", "b"], [1, 2, 3]))
    agent.proxy_export_metrics([m], worker_id_hex=worker_id.hex())
    agent.clean_all_dead_worker_metrics()
    start = time.time()

    def verify():
        agent.clean_all_dead_worker_metrics()
        return get_metric(f"{namespace}_test", agent_port) is None

    wait_for_condition(verify)
    assert time.time() - start > DELAY


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_metrics_agent_export_format_correct(get_agent):
    """
    Verifies that there is one metric per metric name and not one
    per metric name + tag combination.
    Also verifies that the prometheus output is in the right format.
    """
    namespace = "test"
    agent, agent_port = get_agent

    # Record a new gauge.
    metric_name = "test"
    test_gauge = Gauge(metric_name, "desc", "unit", ["tag"])
    record_a = Record(
        gauge=test_gauge,
        value=3,
        tags={"tag": "a"},
    )
    agent.record_and_export([record_a])

    # Record a different tag.
    record_b = Record(
        gauge=test_gauge,
        value=4,
        tags={"tag": "b"},
    )
    agent.record_and_export([record_b])

    # Record more than 1 gauge.
    metric_name_2 = "test2"
    test_gauge_2 = Gauge(metric_name_2, "desc", "unit", ["tag"])
    record_c = Record(
        gauge=test_gauge_2,
        value=1,
        tags={"tag": "c"},
    )
    agent.record_and_export([record_c])

    # Basic assertions
    name, samples = get_metric(
        get_prom_metric_name(namespace, metric_name_2), agent_port
    )
    assert name == get_prom_metric_name(namespace, metric_name_2)
    assert len(samples) == 1
    assert samples[0].value == 1
    assert samples[0].labels == {"tag": "c"}

    name, samples = get_metric(get_prom_metric_name(namespace, metric_name), agent_port)
    assert name == get_prom_metric_name(namespace, metric_name)
    assert len(samples) == 2
    assert samples[0].value == 3
    assert samples[0].labels == {"tag": "a"}
    assert samples[1].value == 4
    assert samples[1].labels == {"tag": "b"}

    # Assert there is not multiple HELP text per metric
    # Need to manually parse the prometheus output because the official
    # `prometheus_client.parser` is more lenient than the actual
    # specification and ignores the multiple HELP / TYPE comments.
    metrics_page = "localhost:{}".format(agent_port)
    _, response = list(fetch_raw_prometheus([metrics_page]))[0]
    assert response.count("# HELP test_test desc") == 1
    assert response.count("# TYPE test_test gauge") == 1
    assert response.count("# HELP test_test2 desc") == 1
    assert response.count("# TYPE test_test2 gauge") == 1


@patch(
    "ray._private.metrics_agent.OpenCensusProxyCollector._get_metric_cardinality_level_setting"
)
def test_get_metric_cardinality_level(
    mock_get_metric_cardinality_level_setting,
):
    """
    Test the core metric cardinality level.
    """
    collector = OpenCensusProxyCollector("")
    mock_get_metric_cardinality_level_setting.return_value = "recommended"
    assert (
        collector._get_metric_cardinality_level() == MetricCardinalityLevel.RECOMMENDED
    )

    mock_get_metric_cardinality_level_setting.return_value = "legacy"
    assert collector._get_metric_cardinality_level() == MetricCardinalityLevel.LEGACY

    mock_get_metric_cardinality_level_setting.return_value = "unknown"
    assert collector._get_metric_cardinality_level() == MetricCardinalityLevel.LEGACY


def _stub_node_level_metric(label: str, value: float) -> OpencensusProxyMetric:
    metric = OpencensusProxyMetric(
        name="test_metric_01",
        desc="",
        unit="",
        label_keys=["NodeId"],
    )
    metric.add_data(
        (label,),
        LastValueAggregationData(ValueDouble, value),
    )
    return metric


def _stub_worker_level_metric(label: str, value: float) -> OpencensusProxyMetric:
    metric = OpencensusProxyMetric(
        name="test_metric_01",
        desc="",
        unit="",
        label_keys=["NodeId", WORKER_ID_TAG_KEY],
    )
    metric.add_data(
        (label, "worker_01"),
        LastValueAggregationData(ValueDouble, value),
    )
    return metric


def test_aggregate_metric_data():
    collector = OpenCensusProxyCollector("")
    collector._aggregate_metric_data(
        [
            LastValueAggregationData(ValueDouble, 1.0),
            LastValueAggregationData(ValueDouble, 2.0),
            LastValueAggregationData(ValueDouble, 3.0),
        ]
    ).value == 6.0
    collector._aggregate_metric_data(
        [
            SumAggregationData(ValueDouble, 1.0),
            SumAggregationData(ValueDouble, 4.0),
        ]
    ).sum_data == 5.0
    collector._aggregate_metric_data(
        [
            CountAggregationData(1),
            CountAggregationData(1),
        ]
    ).count_data == 2
    with pytest.raises(ValueError, match="Unsupported aggregation type"):
        collector._aggregate_metric_data(
            [
                DistributionAggregationData(
                    mean_data=1.0,
                    count_data=1,
                    sum_of_sqd_deviations=1.0,
                )
            ]
        )


def test_collect_worker_metrics_with_recommended_cardinality():
    aggregated_metrics = OpenCensusProxyCollector(
        ""
    )._aggregate_with_recommended_cardinality(
        [
            _stub_worker_level_metric("node_01", 1.0),
            _stub_worker_level_metric("node_01", 3.0),
            _stub_worker_level_metric("node_02", 2.0),
        ]
    )
    assert len(aggregated_metrics) == 1
    metric = aggregated_metrics[0]
    assert metric.name == "test_metric_01"
    assert metric.label_keys == ["NodeId"]
    # Check that the worker id is removed from the label keys, and the correct metric
    # values are returned.
    assert metric._data.get(("node_01",)).value == 4.0
    assert metric._data.get(("node_02",)).value == 2.0


def test_collect_node_metrics_with_recommended_cardinality():
    aggregated_metrics = OpenCensusProxyCollector(
        ""
    )._aggregate_with_recommended_cardinality(
        [
            _stub_node_level_metric("node_01", 1.0),
            _stub_node_level_metric("node_02", 2.0),
        ]
    )
    # Metrics are already at node level, so they should be returned as is.
    assert len(aggregated_metrics) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
