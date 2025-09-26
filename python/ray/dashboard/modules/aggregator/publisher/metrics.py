from ray._private.telemetry.open_telemetry_metric_recorder import (
    OpenTelemetryMetricRecorder,
)
from ray.dashboard.modules.aggregator.constants import (
    AGGREGATOR_AGENT_METRIC_PREFIX,
)

# OpenTelemetry metrics setup (registered once at import time)
metric_recorder = OpenTelemetryMetricRecorder()

# Counter metrics
published_counter_name = f"{AGGREGATOR_AGENT_METRIC_PREFIX}_published_events"
metric_recorder.register_counter_metric(
    published_counter_name,
    "Total number of events successfully published to the destination.",
)

filtered_counter_name = f"{AGGREGATOR_AGENT_METRIC_PREFIX}_filtered_events"
metric_recorder.register_counter_metric(
    filtered_counter_name,
    "Total number of events filtered out before publishing to the destination.",
)

failed_counter_name = f"{AGGREGATOR_AGENT_METRIC_PREFIX}_publish_failures"
metric_recorder.register_counter_metric(
    failed_counter_name,
    "Total number of events that failed to publish after retries.",
)

# Histogram metric
publish_latency_hist_name = f"{AGGREGATOR_AGENT_METRIC_PREFIX}_publish_latency_seconds"
metric_recorder.register_histogram_metric(
    publish_latency_hist_name,
    "Duration of publish calls in seconds.",
    [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5],
)

# Gauge metrics
consecutive_failures_gauge_name = (
    f"{AGGREGATOR_AGENT_METRIC_PREFIX}_consecutive_failures_since_last_success"
)
metric_recorder.register_gauge_metric(
    consecutive_failures_gauge_name,
    "Number of consecutive failed publish attempts since the last success.",
)

time_since_last_success_gauge_name = (
    f"{AGGREGATOR_AGENT_METRIC_PREFIX}_time_since_last_success_seconds"
)
metric_recorder.register_gauge_metric(
    time_since_last_success_gauge_name,
    "Seconds since the last successful publish to the destination.",
)
