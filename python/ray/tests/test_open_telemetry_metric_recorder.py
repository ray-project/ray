import sys
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.metrics import NoOpHistogram

from ray._private.metrics_agent import Gauge, Record
from ray._private.telemetry.open_telemetry_metric_recorder import (
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

    # Record a value for the gauge
    recorder.set_metric_value(
        name="test_gauge",
        tags={"label_key": "label_value"},
        value=42.0,
    )
    assert recorder._gauge_observations_by_name == {
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
    assert recorder._gauge_observations_by_name == {
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
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the record_histogram_aggregated_batch method of OpenTelemetryMetricRecorder.
    - Test that it records histogram data for multiple data points in a single batch.
    - Test that it calls instrument.record() for each observation.
    - Test that it warns if the histogram is not registered.
    """
    mock_meter = MagicMock()
    real_histogram = NoOpHistogram(name="test_histogram")
    mock_histogram = MagicMock(wraps=real_histogram, spec=real_histogram)
    mock_meter.create_histogram.return_value = mock_histogram
    mock_get_meter.return_value = mock_meter

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

    # Register histogram
    recorder.register_histogram_metric(
        name="test_histogram",
        description="Test Histogram",
        buckets=[1.0, 10.0, 100.0],
    )

    # Record batch data - 2 data points with different tags
    # bucket_counts: [2, 3, 0, 1] means:
    #   2 observations in bucket 0-1 (midpoint 0.5)
    #   3 observations in bucket 1-10 (midpoint 5.5)
    #   0 observations in bucket 10-100 (midpoint 55.0)
    #   1 observation in bucket 100-Inf+ (midpoint 200.0)
    recorder.record_histogram_aggregated_batch(
        name="test_histogram",
        data_points=[
            {"tags": {"endpoint": "/api/v1"}, "bucket_counts": [2, 3, 0, 1]},
            {"tags": {"endpoint": "/api/v2"}, "bucket_counts": [1, 0, 1, 0]},
        ],
    )

    # Verify record() was called the correct number of times
    # First data point: 2 + 3 + 0 + 1 = 6 calls
    # Second data point: 1 + 0 + 1 + 0 = 2 calls
    # Total: 8 calls
    assert mock_histogram.record.call_count == 8

    # No warnings should be logged for registered histogram
    mock_logger_warning.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-svv", __file__]))
