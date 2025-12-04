import sys
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.metrics import NoOpHistogram

from ray._private.metrics_agent import Gauge, Record
from ray._private.telemetry.open_telemetry_metric_recorder import (
    MetricType,
    OpenTelemetryMetricRecorder,
)


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

    # Verify metric type is registered
    assert recorder._metric_name_to_type["test_gauge"] == MetricType.GAUGE

    # Record a value for the gauge
    recorder.set_metric_value(
        name="test_gauge",
        tags={"label_key": "label_value"},
        value=42.0,
    )
    assert recorder._observations_by_name == {
        "test_gauge": {
            frozenset({("label_key", "label_value")}): 42.0,
        }
    }


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_counter_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_counter_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers an observable counter metric.
    - Test that values are accumulated (not replaced) for counters.
    - Test that unregistered metrics log a warning.
    """
    mock_meter = MagicMock()
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_counter_metric(name="test_counter", description="Test Counter")

    # Verify observable counter was created (not synchronous counter)
    mock_meter.create_observable_counter.assert_called_once()
    assert "test_counter" in recorder._registered_instruments
    assert recorder._metric_name_to_type["test_counter"] == MetricType.COUNTER

    # Record values - should accumulate
    recorder.set_metric_value(
        name="test_counter",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    recorder.set_metric_value(
        name="test_counter",
        tags={"label_key": "label_value"},
        value=5.0,
    )
    # Values should be accumulated (10 + 5 = 15)
    assert recorder._observations_by_name["test_counter"] == {
        frozenset({("label_key", "label_value")}): 15.0,
    }
    mock_logger_warning.assert_not_called()

    # Test unregistered metric warning
    recorder.set_metric_value(
        name="test_counter_unregistered",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    mock_logger_warning.assert_called_once_with(
        "Metric not registered: test_counter_unregistered"
    )


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_sum_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_sum_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers an observable up-down counter.
    - Test that values are accumulated for sum metrics.
    """
    mock_meter = MagicMock()
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_sum_metric(name="test_sum", description="Test Sum")

    # Verify observable up-down counter was created
    mock_meter.create_observable_up_down_counter.assert_called_once()
    assert "test_sum" in recorder._registered_instruments
    assert recorder._metric_name_to_type["test_sum"] == MetricType.SUM

    # Record values - should accumulate (including negative values)
    recorder.set_metric_value(
        name="test_sum",
        tags={"label_key": "label_value"},
        value=10.0,
    )
    recorder.set_metric_value(
        name="test_sum",
        tags={"label_key": "label_value"},
        value=-3.0,
    )
    # Values should be accumulated (10 + (-3) = 7)
    assert recorder._observations_by_name["test_sum"] == {
        frozenset({("label_key", "label_value")}): 7.0,
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
    - Test that it registers a histogram metric.
    - Test that recordings are buffered in _histogram_recordings.
    - Test bucket midpoint calculation.
    """
    mock_histogram = MagicMock()
    mock_meter = MagicMock()
    mock_meter.create_histogram.return_value = mock_histogram
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_histogram_metric(
        name="test_histogram", description="Test Histogram", buckets=[1.0, 2.0, 3.0]
    )

    assert "test_histogram" in recorder._registered_instruments
    assert recorder._metric_name_to_type["test_histogram"] == MetricType.HISTOGRAM

    # Record values - should be buffered (not sent to SDK yet)
    recorder.set_metric_value(
        name="test_histogram",
        tags={"label_key": "label_value"},
        value=1.5,
    )
    recorder.set_metric_value(
        name="test_histogram",
        tags={"label_key": "label_value"},
        value=2.5,
    )
    # Verify recordings are buffered, not sent to SDK yet
    assert mock_histogram.record.call_count == 0
    assert len(recorder._histogram_recordings["test_histogram"]) == 2
    assert recorder._histogram_recordings["test_histogram"][0] == (
        frozenset({("label_key", "label_value")}),
        1.5,
    )
    assert recorder._histogram_recordings["test_histogram"][1] == (
        frozenset({("label_key", "label_value")}),
        2.5,
    )
    mock_logger_warning.assert_not_called()

    # Test negative first boundary bucket midpoints
    mock_meter.create_histogram.return_value = NoOpHistogram(name="neg_histogram")
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
    assert recorder._observations_by_name == {
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


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_counter_accumulation_multiple_tags(mock_get_meter, mock_set_meter_provider):
    """
    Test that counters correctly accumulate values across multiple tag sets.
    """
    mock_meter = MagicMock()
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_counter_metric(name="test_counter", description="Test Counter")

    # Record values with different tags
    recorder.set_metric_value(
        name="test_counter",
        tags={"label": "a"},
        value=10.0,
    )
    recorder.set_metric_value(
        name="test_counter",
        tags={"label": "b"},
        value=20.0,
    )
    recorder.set_metric_value(
        name="test_counter",
        tags={"label": "a"},
        value=5.0,
    )

    # Each tag set should accumulate independently
    assert recorder._observations_by_name["test_counter"] == {
        frozenset({("label", "a")}): 15.0,  # 10 + 5
        frozenset({("label", "b")}): 20.0,
    }


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_metric_type_routing(mock_get_meter, mock_set_meter_provider):
    """
    Test that set_metric_value correctly routes to different storage based on metric type.
    """
    mock_histogram = MagicMock()
    mock_meter = MagicMock()
    mock_meter.create_histogram.return_value = mock_histogram
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()

    # Register different metric types
    recorder.register_gauge_metric(name="gauge", description="Gauge")
    recorder.register_counter_metric(name="counter", description="Counter")
    recorder.register_sum_metric(name="sum", description="Sum")
    recorder.register_histogram_metric(
        name="histogram", description="Histogram", buckets=[1.0]
    )

    tags = {"key": "value"}
    tag_key = frozenset(tags.items())

    # Record same value for all metrics
    for metric_name in ["gauge", "counter", "sum", "histogram"]:
        recorder.set_metric_value(name=metric_name, tags=tags, value=10.0)
        recorder.set_metric_value(name=metric_name, tags=tags, value=5.0)

    # Gauge: last value wins
    assert recorder._observations_by_name["gauge"][tag_key] == 5.0

    # Counter: accumulated
    assert recorder._observations_by_name["counter"][tag_key] == 15.0

    # Sum: accumulated
    assert recorder._observations_by_name["sum"][tag_key] == 15.0

    # Histogram: buffered (2 recordings, not sent to SDK yet)
    assert mock_histogram.record.call_count == 0
    assert len(recorder._histogram_recordings["histogram"]) == 2


@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_flush_histograms(mock_get_meter, mock_set_meter_provider):
    """
    Test the flush_histograms method of OpenTelemetryMetricRecorder.
    - Test that buffered recordings are flushed to the OTEL SDK.
    - Test that buffer is cleared after flush.
    """
    mock_histogram = MagicMock()
    mock_meter = MagicMock()
    mock_meter.create_histogram.return_value = mock_histogram
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()

    recorder.register_histogram_metric(
        name="test_histogram", description="Test Histogram", buckets=[1.0, 2.0, 3.0]
    )

    # Buffer some recordings
    recorder.set_metric_value(
        name="test_histogram",
        tags={"label_key": "label_value"},
        value=1.5,
    )
    recorder.set_metric_value(
        name="test_histogram",
        tags={"label_key": "label_value"},
        value=2.5,
    )

    # Verify recordings are buffered
    assert len(recorder._histogram_recordings["test_histogram"]) == 2
    assert mock_histogram.record.call_count == 0

    # Flush
    recorder.flush_histograms()

    # Verify recordings were flushed to SDK
    assert mock_histogram.record.call_count == 2
    mock_histogram.record.assert_any_call(1.5, attributes={"label_key": "label_value"})
    mock_histogram.record.assert_any_call(2.5, attributes={"label_key": "label_value"})

    # Buffer should be cleared
    assert len(recorder._histogram_recordings["test_histogram"]) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-svv", __file__]))
