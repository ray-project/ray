"""Unit tests for the controller ingest health metrics."""

import sys
from unittest import mock

import pytest

from ray.serve._private.controller_health_metrics_tracker import (
    ControllerHealthMetricsTracker,
)


def test_ingest_cpu_fraction_is_a_windowed_rate():
    """Cumulative-over-uptime could never show a controller that saturates late. The
    fraction is anchored on control-loop samples, so it tracks the recent window."""
    tracker = ControllerHealthMetricsTracker()
    tracker.controller_start_time = 0.0
    # Before any loop sample anchors a window, it falls back to cumulative over uptime.
    tracker.record_handle_ingest(500.0)
    with mock.patch("time.time", return_value=1000.0):
        assert abs(tracker.collect_metrics().ingest_cpu_fraction - 0.0005) < 1e-9
    with mock.patch("time.time", return_value=1000.0):
        tracker.record_loop_duration(0.1)  # window anchor
    tracker.record_handle_ingest(500.0)  # 0.5s of ingest inside the window
    with mock.patch("time.time", return_value=1001.0):
        metrics = tracker.collect_metrics()
    assert abs(metrics.ingest_cpu_fraction - 0.5) < 1e-6
    assert metrics.handle_reports_received == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
