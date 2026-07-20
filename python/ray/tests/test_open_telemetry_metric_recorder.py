import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from ray._private.metrics_agent import Gauge, Record
from ray._private.telemetry.metric_types import MetricType
from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
    _get_service_name,
    _HistogramPrometheusCollector,
)


@pytest.fixture
def clean_histogram_state():
    """Clears the recorder's process-wide histogram aggregation state."""
    OpenTelemetryMetricRecorder._histogram_defs.clear()
    OpenTelemetryMetricRecorder._histogram_states.clear()
    yield
    OpenTelemetryMetricRecorder._histogram_defs.clear()
    OpenTelemetryMetricRecorder._histogram_states.clear()


def _histogram_exposition() -> str:
    """Renders the shared histogram state through a private registry."""
    registry = CollectorRegistry()
    registry.register(_HistogramPrometheusCollector())
    return generate_latest(registry).decode()


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
    mock_get_meter, mock_set_meter_provider, mock_logger_warning, clean_histogram_state
):
    """
    Test the register_histogram_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a histogram metric with the correct name and description.
    - Test that a value can be set for the histogram metric successfully without warnings.
    """
    mock_get_meter.return_value = MagicMock()
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

    # The single observation lands in the +Inf bucket with its true value.
    data = OpenTelemetryMetricRecorder._histogram_states["test_histogram"][
        frozenset({"label_key": "label_value"}.items())
    ]
    assert data.bucket_counts == [0, 0, 0, 1]
    assert data.sum == pytest.approx(10.0)

    recorder.register_histogram_metric(
        name="neg_histogram",
        description="Histogram with negative first boundary",
        buckets=[-5.0, 0.0, 10.0],
    )

    mids = recorder.get_histogram_bucket_midpoints("neg_histogram")
    assert mids == pytest.approx([-7.5, -2.5, 5.0, 20.0])


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


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_record_histogram_aggregated_batch(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning, clean_histogram_state
):
    """
    Test the record_histogram_aggregated_batch method of OpenTelemetryMetricRecorder.
    - Test that per-bucket delta counts accumulate across batches.
    - Test that the sum is approximated from bucket midpoints.
    - Test that it warns if the histogram is not registered.
    """
    mock_get_meter.return_value = MagicMock()
    recorder = OpenTelemetryMetricRecorder()

    # Test warning when histogram not registered
    recorder.record_histogram_aggregated_batch(
        name="unregistered_histogram",
        data_points=[{"tags": {"key": "value"}, "bucket_counts": [1, 2, 3]}],
    )
    mock_logger_warning.assert_called_once_with(
        "Metric unregistered_histogram is not a registered histogram, skipping recording."
    )
    mock_logger_warning.reset_mock()

    # Register histogram. Bucket midpoints are [0.5, 5.5, 55.0, 200.0].
    recorder.register_histogram_metric(
        name="test_histogram",
        description="Test Histogram",
        buckets=[1.0, 10.0, 100.0],
    )

    recorder.record_histogram_aggregated_batch(
        name="test_histogram",
        data_points=[
            {"tags": {"endpoint": "/api/v1"}, "bucket_counts": [2, 3, 0, 1]},
            {"tags": {"endpoint": "/api/v2"}, "bucket_counts": [1, 0, 1, 0]},
        ],
    )
    # A second batch for the same label set accumulates on top of the first.
    recorder.record_histogram_aggregated_batch(
        name="test_histogram",
        data_points=[
            {"tags": {"endpoint": "/api/v1"}, "bucket_counts": [0, 1, 0, 0]},
        ],
    )

    states = OpenTelemetryMetricRecorder._histogram_states["test_histogram"]
    v1 = states[frozenset({"endpoint": "/api/v1"}.items())]
    v2 = states[frozenset({"endpoint": "/api/v2"}.items())]
    assert v1.bucket_counts == [2, 4, 0, 1]
    assert v1.sum == pytest.approx(2 * 0.5 + 4 * 5.5 + 1 * 200.0)
    assert v2.bucket_counts == [1, 0, 1, 0]
    assert v2.sum == pytest.approx(0.5 + 55.0)

    # No warnings should be logged for registered histogram
    mock_logger_warning.assert_not_called()

    # The Prometheus collector renders cumulative buckets, count and sum.
    exposition = _histogram_exposition()
    assert 'ray_test_histogram_bucket{endpoint="/api/v1",le="1.0"} 2.0' in exposition
    assert 'ray_test_histogram_bucket{endpoint="/api/v1",le="10.0"} 6.0' in exposition
    assert 'ray_test_histogram_bucket{endpoint="/api/v1",le="100.0"} 6.0' in exposition
    assert 'ray_test_histogram_bucket{endpoint="/api/v1",le="+Inf"} 7.0' in exposition
    assert 'ray_test_histogram_count{endpoint="/api/v1"} 7.0' in exposition
    assert 'ray_test_histogram_count{endpoint="/api/v2"} 2.0' in exposition


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


@patch("ray._private.telemetry.open_telemetry_metric_recorder.MeterProvider")
@patch("ray._private.telemetry.open_telemetry_metric_recorder.PrometheusMetricReader")
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


@patch("opentelemetry.sdk.resources.Resource.create")
@patch("ray._private.telemetry.open_telemetry_metric_recorder.MeterProvider")
@patch("ray._private.telemetry.open_telemetry_metric_recorder.PrometheusMetricReader")
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
        mock_meter_provider.assert_called_once_with(
            resource=mock_resource,
            metric_readers=[mock_prometheus_reader.return_value],
        )
        mock_set_meter_provider.assert_called_once_with(
            mock_meter_provider.return_value
        )
    finally:
        OpenTelemetryMetricRecorder._metrics_initialized = original_flag


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
