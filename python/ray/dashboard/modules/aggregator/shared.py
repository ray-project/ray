"""
Shared OpenTelemetry metric recorder instance for aggregator module components.

This module provides a single shared instance of OpenTelemetryMetricRecorder
to be used across all aggregator components (aggregator_agent, publisher, event_buffer).

TODO(sampan): This is a temporary solution to share the metric recorder instance across all aggregator components. Switch to using the singleton pattern once https://github.com/ray-project/ray/pull/56347 is merged.
"""

from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
)

# Create a shared instance to be used by all aggregator components
metric_recorder = OpenTelemetryMetricRecorder()
metric_prefix = "event_aggregator_agent"
