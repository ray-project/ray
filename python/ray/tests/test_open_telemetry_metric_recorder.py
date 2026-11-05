import os
import sys
import threading
import time
from contextlib import contextmanager
from math import inf
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.metrics import NoOpHistogram
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    Histogram,
    HistogramDataPoint,
    InMemoryMetricReader,
    Metric,
    ScopeMetrics,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from prometheus_client.core import REGISTRY

from ray._private.metrics_agent import Gauge, Record
from ray._private.telemetry import open_telemetry_metric_recorder
from ray._private.telemetry.aggregated_histogram import _PrometheusMetricReader
from ray._private.telemetry.metric_types import MetricType
from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
    _get_service_name,
)


def _gauge_values(recorder):
    """Returns the recorder's gauge observations with the per-entry TTL timestamp
    stripped, so tests can assert on the recorded values directly."""
    return {
        name: {tag_key: value for tag_key, (value, _ts) in observations.items()}
        for name, observations in recorder._gauge_observations_by_name.items()
    }


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_gauge_metric(mock_get_meter, mock_set_meter_provider):
    """
    Test the register_gauge_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a gauge metric with the correct name and description.
    - Test that a value can be recorded for the gauge metric successfully.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_gauge_metric(name="test_gauge", description="Test Gauge")

    # Record a value for the gauge
    recorder.set_metric_value(
        name="test_gauge",
        tags={"label_key": "label_value"},
        value=42.0,
    )
    assert _gauge_values(recorder) == {
        "test_gauge": {
            frozenset({("label_key", "label_value")}): 42.0,
        }
    }


@patch("ray._private.telemetry.open_telemetry_metric_recorder.time.monotonic")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_gauge_value_retained_within_ttl_then_evicted(
    mock_get_meter, mock_set_meter_provider, mock_monotonic
):
    """
    A gauge value must survive scrapes for the TTL window (not be cleared after the
    first scrape), and be evicted once it has not been refreshed within the TTL.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder(gauge_metric_ttl_seconds=10.0)
    recorder.register_gauge_metric(name="g", description="g")
    callback = recorder._create_observable_callback("g", MetricType.GAUGE)

    # Report a value at t=1000.
    mock_monotonic.return_value = 1000.0
    recorder.set_metric_value(name="g", tags={"k": "v"}, value=7.0)

    # Scrape at t=1005 (within TTL): value is emitted.
    mock_monotonic.return_value = 1005.0
    assert [o.value for o in callback(None)] == [7.0]

    # Scrape again at t=1009 without re-reporting: still within TTL, so the value
    # persists (clear-on-scrape would have dropped it after the first scrape).
    mock_monotonic.return_value = 1009.0
    assert [o.value for o in callback(None)] == [7.0]

    # Scrape at t=1011 (>10s since the last report): the value is evicted.
    mock_monotonic.return_value = 1011.0
    assert callback(None) == []
    assert recorder._gauge_observations_by_name["g"] == {}


@patch("ray._private.telemetry.open_telemetry_metric_recorder.time.monotonic")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_gauge_refresh_extends_ttl(
    mock_get_meter, mock_set_meter_provider, mock_monotonic
):
    """Re-reporting a gauge value refreshes its TTL so it does not get evicted."""
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder(gauge_metric_ttl_seconds=10.0)
    recorder.register_gauge_metric(name="g", description="g")
    callback = recorder._create_observable_callback("g", MetricType.GAUGE)

    mock_monotonic.return_value = 1000.0
    recorder.set_metric_value(name="g", tags={"k": "v"}, value=7.0)

    # Re-report at t=1008 (within TTL): refreshes the timestamp.
    mock_monotonic.return_value = 1008.0
    recorder.set_metric_value(name="g", tags={"k": "v"}, value=7.0)

    # At t=1015 (>10s after the first report, but <10s after the refresh): still live.
    mock_monotonic.return_value = 1015.0
    assert [o.value for o in callback(None)] == [7.0]


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_counter_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_counter_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers an observable counter metric with the correct name and description.
    - Test that values are accumulated in _counter_observations.
    """
    mock_meter = MagicMock()
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_counter_metric(name="test_counter", description="Test Counter")
    assert "test_counter" in recorder._registered_instruments
    assert "test_counter" in recorder._counter_observations_by_name
    recorder.set_metric_value(
        name="test_counter",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    assert recorder._counter_observations_by_name["test_counter"] == {
        frozenset({("label_key", "label_value")}): 10.0
    }

    # Ensure that the value is accumulated correctly
    recorder.set_metric_value(
        name="test_counter",
        tags={"label_key": "label_value"},
        value=5.0,
    )
    assert recorder._counter_observations_by_name["test_counter"] == {
        frozenset({("label_key", "label_value")}): 15.0  # 10 + 5 = 15
    }
    mock_logger_warning.assert_not_called()
    recorder.set_metric_value(
        name="test_counter_unregistered",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    mock_logger_warning.assert_called_once_with(
        "Metric test_counter_unregistered is not registered or unsupported type."
    )


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_sum_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_sum_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers an observable up_down_counter metric.
    - Test that a value can be set for the sum metric successfully without warnings.
    """
    mock_meter = MagicMock()
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_sum_metric(name="test_sum", description="Test Sum")
    assert "test_sum" in recorder._registered_instruments
    assert "test_sum" in recorder._sum_observations_by_name

    recorder.set_metric_value(
        name="test_sum",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    assert recorder._sum_observations_by_name["test_sum"] == {
        frozenset({("label_key", "label_value")}): 10.0
    }

    # Test accumulation with negative value (up_down_counter can go down)
    recorder.set_metric_value(
        name="test_sum",
        tags={"label_key": "label_value"},
        value=-3.0,
    )
    assert recorder._sum_observations_by_name["test_sum"] == {
        frozenset({("label_key", "label_value")}): 7.0  # 10 - 3 = 7
    }
    mock_logger_warning.assert_not_called()


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_histogram_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_histogram_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a histogram metric with the correct name and description.
    - Test that a value can be set for the histogram metric successfully without warnings.
    """
    mock_meter = MagicMock()
    mock_meter.create_histogram.return_value = NoOpHistogram(name="test_histogram")
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_histogram_metric(
        name="test_histogram", description="Test Histogram", buckets=[1.0, 2.0, 3.0]
    )
    assert "test_histogram" in recorder._registered_instruments
    recorder.set_metric_value(
        name="test_histogram",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    mock_logger_warning.assert_not_called()


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_record_and_export(mock_get_meter, mock_set_meter_provider):
    """
    Test the record_and_export method of OpenTelemetryMetricRecorder. Test that
    - The state of _observations_by_gauge_name is correct after recording a metric.
    - If there are multiple records with the same gauge name and tags, only the last
      value is kept.
    - If there are multiple records with the same gauge name but different tags, all
      values are kept.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()
    recorder.record_and_export(
        [
            Record(
                gauge=Gauge(
                    name="hi",
                    description="Hi",
                    unit="unit",
                    tags={},
                ),
                value=1.0,
                tags={"label_key": "label_value"},
            ),
            Record(
                gauge=Gauge(
                    name="w00t",
                    description="w00t",
                    unit="unit",
                    tags={},
                ),
                value=2.0,
                tags={"label_key": "label_value"},
            ),
            Record(
                gauge=Gauge(
                    name="w00t",
                    description="w00t",
                    unit="unit",
                    tags={},
                ),
                value=20.0,
                tags={"another_label_key": "another_label_value"},
            ),
            Record(
                gauge=Gauge(
                    name="hi",
                    description="Hi",
                    unit="unit",
                    tags={},
                ),
                value=3.0,
                tags={"label_key": "label_value"},
            ),
        ],
        global_tags={"global_label_key": "global_label_value"},
    )
    assert _gauge_values(recorder) == {
        "hi": {
            frozenset(
                {
                    ("label_key", "label_value"),
                    ("global_label_key", "global_label_value"),
                }
            ): 3.0
        },
        "w00t": {
            frozenset(
                {
                    ("label_key", "label_value"),
                    ("global_label_key", "global_label_value"),
                }
            ): 2.0,
            frozenset(
                {
                    ("another_label_key", "another_label_value"),
                    ("global_label_key", "global_label_value"),
                }
            ): 20.0,
        },
    }


@contextmanager
def _serving_reader(*producers):
    """Yields a Prometheus reader that serves the producers' metrics, registered on
    a MeterProvider of its own, so tests don't leak producers into the process's
    reader. The reader serves on the global REGISTRY (the exporter versions Ray
    supports don't take another registry), and is unregistered on exit."""
    reader = _PrometheusMetricReader(Resource.create({}))
    provider = MeterProvider(metric_readers=[reader], shutdown_on_exit=False)
    for producer in producers:
        reader.add_metric_producer(producer)
    try:
        yield reader, provider
    finally:
        provider.shutdown()


@contextmanager
def _served_recorder():
    """Yields a recorder whose aggregated histograms are served on REGISTRY."""
    recorder = OpenTelemetryMetricRecorder()
    with _serving_reader(recorder.aggregated_histogram_producer):
        yield recorder


def _families(name):
    return [m for m in REGISTRY.collect() if m.name == name]


def _histogram_dp(tags, bucket_counts, total, bounds=(1.0, 10.0, 100.0)):
    return {
        "tags": tags,
        "explicit_bounds": list(bounds),
        "bucket_counts": bucket_counts,
        "sum": total,
    }


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_record_histogram_aggregated_batch(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """Pre-aggregated bucket counts and sums are added exactly to cumulative
    Prometheus series, one per label set."""
    with _served_recorder() as recorder:
        recorder.record_histogram_aggregated_batch(
            "test_histogram",
            "Test Histogram",
            [
                _histogram_dp({"endpoint": "/api/v1"}, [2, 3, 0, 1], 250.5),
                _histogram_dp({"endpoint": "/api/v2"}, [1, 0, 1, 0], 20.0),
            ],
        )
        # A second export carries only the delta since the previous one.
        recorder.record_histogram_aggregated_batch(
            "test_histogram",
            "Test Histogram",
            [_histogram_dp({"endpoint": "/api/v1"}, [0, 1, 0, 0], 4.0)],
        )

        def sample(suffix, endpoint, le=None):
            labels = {"endpoint": endpoint}
            if le is not None:
                labels["le"] = le
            return REGISTRY.get_sample_value(f"ray_test_histogram_{suffix}", labels)

        # Bucket samples are cumulative across buckets, as Prometheus expects.
        buckets = [
            sample("bucket", "/api/v1", le) for le in ["1.0", "10.0", "100.0", "+Inf"]
        ]
        assert buckets == [2, 6, 6, 7]
        assert sample("count", "/api/v1") == 7
        assert sample("sum", "/api/v1") == 254.5
        assert sample("count", "/api/v2") == 2
        assert sample("sum", "/api/v2") == 20.0
    mock_logger_warning.assert_not_called()


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_record_histogram_aggregated_batch_mixed_label_sets(
    mock_get_meter, mock_set_meter_provider
):
    """Data points with different label keys are served under one label schema, and
    a missing label merges with an empty one."""
    with _served_recorder() as recorder:
        recorder.record_histogram_aggregated_batch(
            "test_histogram",
            "Test Histogram",
            [
                _histogram_dp(
                    {"Component": "a", "SessionName": "s"}, [1, 0, 0, 0], 0.5
                ),
                _histogram_dp({"Component": "b"}, [1, 0, 0, 0], 0.5),
                _histogram_dp({"Component": "b", "SessionName": ""}, [1, 0, 0, 0], 0.5),
            ],
        )

        (family,) = _families("ray_test_histogram")
        count_labels = [s.labels for s in family.samples if s.name.endswith("_count")]
        assert count_labels == [
            {"Component": "a", "SessionName": "s"},
            {"Component": "b", "SessionName": ""},
        ]
        assert (
            REGISTRY.get_sample_value(
                "ray_test_histogram_count", {"Component": "b", "SessionName": ""}
            )
            == 2
        )


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_record_histogram_aggregated_batch_bounds_mismatch(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """A data point whose bounds disagree with the ones the metric was first reported
    with is skipped with a warning instead of raising."""
    with _served_recorder() as recorder:
        recorder.record_histogram_aggregated_batch(
            "test_histogram",
            "Test Histogram",
            [
                _histogram_dp({"model_name": "a"}, [1, 0, 0, 0], 0.5),
                _histogram_dp({"model_name": "b"}, [1, 0, 0], 0.5, bounds=(1.0, 10.0)),
                _histogram_dp({"model_name": "c"}, [1, 0, 0, 0], 0.5, bounds=(1, 2, 3)),
            ],
        )

        def count(model_name):
            return REGISTRY.get_sample_value(
                "ray_test_histogram_count", {"model_name": model_name}
            )

        assert count("a") == 1
        assert count("b") is None
        assert count("c") is None
    mock_logger_warning.assert_called_once()


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_aggregated_histogram_producer_output(mock_get_meter, mock_set_meter_provider):
    """The producer emits each histogram as a cumulative OpenTelemetry histogram in
    the recorder's scope, whose series keep their start time across collections."""
    recorder = OpenTelemetryMetricRecorder()
    assert recorder.aggregated_histogram_producer.produce() == []

    recorder.record_histogram_aggregated_batch(
        "test_histogram",
        "Test Histogram",
        [_histogram_dp({"endpoint": "/api"}, [2, 1, 0, 1], 150.0)],
    )
    (first,) = recorder.aggregated_histogram_producer.produce()
    (second,) = recorder.aggregated_histogram_producer.produce()

    assert first.scope.name == open_telemetry_metric_recorder.__name__
    (metric,) = first.metrics
    assert metric.name == "ray_test_histogram"
    assert metric.description == "Test Histogram"
    assert metric.data.aggregation_temporality == AggregationTemporality.CUMULATIVE
    (dp,) = metric.data.data_points
    assert dict(dp.attributes) == {"endpoint": "/api"}
    assert list(dp.explicit_bounds) == [1.0, 10.0, 100.0]
    assert list(dp.bucket_counts) == [2, 1, 0, 1]
    assert dp.count == 4
    assert dp.sum == 150.0

    (second_dp,) = second.metrics[0].data.data_points
    assert second_dp.start_time_unix_nano == dp.start_time_unix_nano
    assert second_dp.time_unix_nano >= dp.time_unix_nano


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_serve_aggregated_histograms(mock_get_meter, mock_set_meter_provider):
    """A process creates several recorders, so constructing one must not attach its
    producer to the process's reader, which would keep the recorder alive. Its
    aggregated histograms are served once serve_aggregated_histograms is called."""
    recorder = OpenTelemetryMetricRecorder()
    recorder.record_histogram_aggregated_batch(
        "test_histogram",
        "Test Histogram",
        [_histogram_dp({"endpoint": "/api"}, [1, 0, 0, 0], 0.5)],
    )

    def count():
        return REGISTRY.get_sample_value(
            "ray_test_histogram_count", {"endpoint": "/api"}
        )

    # Stand in for the process's reader, so the producer doesn't outlive the test.
    with _serving_reader() as (reader, _), patch.object(
        OpenTelemetryMetricRecorder, "_prometheus_reader", reader
    ):
        assert count() is None

        recorder.serve_aggregated_histograms()
        recorder.serve_aggregated_histograms()
        assert count() == 1
        assert len(_families("ray_test_histogram")) == 1


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_serve_aggregated_histograms_without_reader(
    mock_get_meter, mock_set_meter_provider
):
    """Without a process reader, serving fails with an error that says why."""
    recorder = OpenTelemetryMetricRecorder()
    with patch.object(OpenTelemetryMetricRecorder, "_prometheus_reader", None):
        with pytest.raises(RuntimeError, match="has no Prometheus reader"):
            recorder.serve_aggregated_histograms()


@pytest.mark.parametrize("with_sdk_metrics", [True, False])
def test_prometheus_reader_serves_producers_with_sdk_metrics(with_sdk_metrics):
    """The reader serves the producers' metrics in the same collection as the SDK's,
    without duplicating either, and also when the SDK has no metrics of its own."""
    producer = MagicMock()
    producer.produce.return_value = [
        ScopeMetrics(
            scope=InstrumentationScope(name="test_producer"),
            metrics=[
                Metric(
                    name="test_produced_histogram",
                    description="",
                    unit="1",
                    data=Histogram(
                        data_points=[
                            HistogramDataPoint(
                                attributes={},
                                start_time_unix_nano=0,
                                time_unix_nano=1,
                                count=1,
                                sum=0.5,
                                bucket_counts=[1, 0],
                                explicit_bounds=[1.0],
                                min=inf,
                                max=-inf,
                            )
                        ],
                        aggregation_temporality=AggregationTemporality.CUMULATIVE,
                    ),
                )
            ],
            schema_url="",
        )
    ]
    with _serving_reader(producer) as (_, provider):
        if with_sdk_metrics:
            provider.get_meter("test").create_counter("test_sdk_counter").add(3)

        assert REGISTRY.get_sample_value("test_produced_histogram_count") == 1
        assert len(_families("test_produced_histogram")) == 1
        if with_sdk_metrics:
            assert REGISTRY.get_sample_value("test_sdk_counter_total") == 3
            assert len(_families("test_sdk_counter")) == 1


@patch("ray._private.telemetry.aggregated_histogram.logger.exception")
def test_prometheus_reader_isolates_failing_producer(mock_logger_exception):
    """A producer that raises is skipped, and the SDK's metrics are still served."""
    producer = MagicMock()
    producer.produce.side_effect = RuntimeError("boom")
    with _serving_reader(producer) as (_, provider):
        provider.get_meter("test").create_counter("test_sdk_counter").add(3)

        assert REGISTRY.get_sample_value("test_sdk_counter_total") == 3
    mock_logger_exception.assert_called()


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
@pytest.mark.parametrize(
    "register_metric,metric_type",
    [
        (
            lambda recorder: recorder.register_gauge_metric(
                name="test_metric", description="Test Gauge"
            ),
            MetricType.GAUGE,
        ),
        (
            lambda recorder: recorder.register_counter_metric(
                name="test_metric", description="Test Counter"
            ),
            MetricType.COUNTER,
        ),
        (
            lambda recorder: recorder.register_sum_metric(
                name="test_metric", description="Test Sum"
            ),
            MetricType.SUM,
        ),
    ],
)
def test_observable_callback_normalizes_mixed_attribute_sets(
    mock_get_meter, mock_set_meter_provider, register_metric, metric_type
):
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()
    register_metric(recorder)

    recorder.set_metric_value(
        name="test_metric",
        tags={"Component": "worker_a", "SessionName": "s1", "dataset": "train"},
        value=1.0,
    )
    recorder.set_metric_value(
        name="test_metric",
        tags={"Component": "worker_b", "dataset": "test"},
        value=2.0,
    )

    callback = recorder._create_observable_callback("test_metric", metric_type)
    observations = callback(options=None)

    assert len(observations) == 2
    expected_keys = {"Component", "SessionName", "dataset"}
    assert [set(obs.attributes) for obs in observations] == [
        expected_keys,
        expected_keys,
    ]

    obs_b = next(o for o in observations if o.attributes["Component"] == "worker_b")
    assert obs_b.attributes["SessionName"] == ""

    obs_a = next(o for o in observations if o.attributes["Component"] == "worker_a")
    assert obs_a.attributes["SessionName"] == "s1"


# _init_metrics stores the (mocked) reader on the class; restore it afterwards.
@patch.object(OpenTelemetryMetricRecorder, "_prometheus_reader", None)
@patch("ray._private.telemetry.open_telemetry_metric_recorder.MeterProvider")
@patch("ray._private.telemetry.open_telemetry_metric_recorder._PrometheusMetricReader")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_init_metrics_runs_only_once_per_class(
    mock_get_meter,
    mock_set_meter_provider,
    mock_prometheus_reader,
    mock_meter_provider,
):
    """
    Regression test: _init_metrics must run exactly once per process, regardless of
    how many OpenTelemetryMetricRecorder instances are created. Previously the guard
    flag was written via `self._metrics_initialized = True`, which created an
    instance attribute and left the class attribute as False, so each new instance
    re-ran the body and registered another PrometheusMetricReader on the global
    prometheus_client REGISTRY. That produced duplicate `target_info` series.
    """
    mock_get_meter.return_value = MagicMock()
    # Reset the class-level flag so the test is hermetic regardless of order.
    original_flag = OpenTelemetryMetricRecorder._metrics_initialized
    OpenTelemetryMetricRecorder._metrics_initialized = False
    try:
        for _ in range(3):
            OpenTelemetryMetricRecorder()

        assert mock_prometheus_reader.call_count == 1
        assert mock_meter_provider.call_count == 1
        assert mock_set_meter_provider.call_count == 1
        assert OpenTelemetryMetricRecorder._metrics_initialized is True
    finally:
        OpenTelemetryMetricRecorder._metrics_initialized = original_flag


# _init_metrics stores the (mocked) reader on the class; restore it afterwards.
@patch.object(OpenTelemetryMetricRecorder, "_prometheus_reader", None)
@patch("opentelemetry.sdk.resources.Resource.create")
@patch("ray._private.telemetry.open_telemetry_metric_recorder.MeterProvider")
@patch("ray._private.telemetry.open_telemetry_metric_recorder._PrometheusMetricReader")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_init_metrics_sets_service_name_resource(
    mock_get_meter,
    mock_set_meter_provider,
    mock_prometheus_reader,
    mock_meter_provider,
    mock_resource_create,
):
    """
    Regression test: the Prometheus exporter must not fall back to the default
    OpenTelemetry service.name of unknown_service, otherwise multiple target_info
    samples can collide in a single scrape.
    """
    mock_get_meter.return_value = MagicMock()
    mock_resource = MagicMock()
    mock_resource_create.return_value = mock_resource

    original_flag = OpenTelemetryMetricRecorder._metrics_initialized
    OpenTelemetryMetricRecorder._metrics_initialized = False
    try:
        with patch.dict(
            os.environ,
            {"OTEL_SERVICE_NAME": "", "OTEL_RESOURCE_ATTRIBUTES": ""},
        ):
            OpenTelemetryMetricRecorder()

        mock_resource_create.assert_called_once_with(
            {"service.name": "ray-dashboard-agent"}
        )
        # The reader gets the provider's resource, for when the SDK has no metrics
        # to take it from.
        mock_prometheus_reader.assert_called_once_with(mock_resource)
        mock_meter_provider.assert_called_once_with(
            resource=mock_resource,
            metric_readers=[mock_prometheus_reader.return_value],
        )
        mock_set_meter_provider.assert_called_once_with(
            mock_meter_provider.return_value
        )
    finally:
        OpenTelemetryMetricRecorder._metrics_initialized = original_flag


@pytest.mark.parametrize(
    "register_method,register_args",
    [
        ("register_gauge_metric", ("test_deadlock_gauge", "description")),
        ("register_counter_metric", ("test_deadlock_counter", "description")),
        ("register_sum_metric", ("test_deadlock_sum", "description")),
        # Histogram registration does not go through the measurement-consumer
        # lock today (synchronous instruments are not registered with the
        # consumer), so it could not deadlock pre-fix; included as a tripwire
        # in case that ever changes.
        (
            "register_histogram_metric",
            ("test_deadlock_histogram", "description", [1.0, 10.0]),
        ),
    ],
)
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_does_not_deadlock_with_concurrent_collect(
    mock_get_meter, mock_set_meter_provider, register_method, register_args
):
    """Regression test for an AB/BA deadlock between instrument registration and
    metric collection.

    The OpenTelemetry SDK holds an SDK-internal lock while invoking
    observable-instrument callbacks during collect(), and the recorder's
    callbacks acquire ``recorder._lock``. If registration holds
    ``recorder._lock`` across ``meter.create_*`` (which acquires that same
    SDK-internal lock), a registration that races a collect() deadlocks the
    process permanently:

        registration: recorder._lock -> SDK lock
        collect():    SDK lock       -> recorder._lock

    This test parks a real SDK collect() inside an instrument callback that
    contends on ``recorder._lock`` (exactly like the recorder's own callbacks
    do) while another thread registers a new instrument, and asserts that both
    complete.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()
    # Use a local provider/reader so collect() exercises the real SDK locking,
    # independent of the process-global meter provider.
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    recorder.meter = provider.get_meter(__name__)

    in_callback = threading.Event()
    registration_started = threading.Event()

    def contending_callback(options):
        # Runs inside collect() with the SDK lock held.
        in_callback.set()
        registration_started.wait(timeout=30)
        # Contend on the recorder lock exactly like the recorder's own
        # observable callbacks do on every scrape.
        with recorder._lock:
            return []

    recorder.meter.create_observable_gauge(
        name="test_contending_gauge", callbacks=[contending_callback]
    )

    collect_thread = threading.Thread(target=reader.collect, daemon=True)
    collect_thread.start()
    assert in_callback.wait(timeout=10), "collect() never invoked the callback"

    register_thread = threading.Thread(
        target=getattr(recorder, register_method),
        args=register_args,
        daemon=True,
    )
    register_thread.start()
    # Let the registration reach its lock acquisitions before releasing the
    # callback. Under the pre-fix locking (recorder._lock held across
    # meter.create_*) recorder._lock is what becomes visibly held; under the
    # fixed locking it is _registration_lock — break on either so the barrier
    # is fast in both worlds instead of sleeping out the full budget.
    for _ in range(100):
        if (
            recorder._registration_lock.locked()
            or recorder._lock.locked()
            or not register_thread.is_alive()
        ):
            break
        time.sleep(0.01)
    registration_started.set()

    register_thread.join(timeout=10)
    registration_deadlocked = register_thread.is_alive()
    collect_thread.join(timeout=10)
    assert not registration_deadlocked, (
        f"{register_method}() deadlocked against a concurrent collect(); "
        "instrument creation must not run while holding recorder._lock."
    )
    assert not collect_thread.is_alive(), "collect() did not complete"
    with recorder._lock:
        assert register_args[0] in recorder._registered_instruments


def test_get_service_name_decodes_otel_resource_attributes():
    with patch.dict(
        os.environ,
        {
            "OTEL_SERVICE_NAME": "",
            "OTEL_RESOURCE_ATTRIBUTES": "service.name=ray%20dashboard%2Cagent",
        },
    ):
        assert _get_service_name("ray-dashboard-agent") == "ray dashboard,agent"


if __name__ == "__main__":
    sys.exit(pytest.main(["-svv", __file__]))
