import sys
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.metrics import NoOpCounter

from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
)
from ray._private.metrics_agent import Record, Gauge


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
    assert (
        recorder._get_observable_metric_value(
            name="test_gauge",
            tags={"label_key": "label_value"},
        )
        == 42.0
    )


@patch("ray._private.telemetry.open_telemetry_metric_recorder.logger.warning")
@patch("opentelemetry.metrics.set_meter_provider")
@patch("opentelemetry.metrics.get_meter")
def test_register_counter_metric(
    mock_get_meter, mock_set_meter_provider, mock_logger_warning
):
    """
    Test the register_counter_metric method of OpenTelemetryMetricRecorder.
    - Test that it registers a counter metric with the correct name and description.
    - Test that a value can be set for the counter metric successfully without warnings.
    """
    mock_meter = MagicMock()
    mock_meter.create_counter.return_value = NoOpCounter(name="test_counter")
    mock_get_meter.return_value = mock_meter
    recorder = OpenTelemetryMetricRecorder()
    recorder.register_counter_metric(name="test_counter", description="Test Counter")
    assert "test_counter" in recorder._registered_instruments
    recorder.set_metric_value(
        name="test_counter",
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
    assert recorder._observations_by_name == {
        "hi": {
            frozenset(
                {
                    "label_key": "label_value",
                    "global_label_key": "global_label_value",
                }.items()
            ): 3.0
        },
        "w00t": {
            frozenset(
                {
                    "label_key": "label_value",
                    "global_label_key": "global_label_value",
                }.items()
            ): 2.0,
            frozenset(
                {
                    "another_label_key": "another_label_value",
                    "global_label_key": "global_label_value",
                }.items()
            ): 20.0,
        },
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-svv", __file__]))
