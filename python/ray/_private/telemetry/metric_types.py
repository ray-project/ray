from enum import Enum


class MetricType(Enum):
    """Types of metrics supported by the telemetry system.

    Note: SUMMARY metric type is not supported. SUMMARY is a Prometheus metric type
    that is not explicitly supported in OpenTelemetry. Use HISTOGRAM instead for
    similar use cases (e.g., latency distributions with quantiles).
    """

    GAUGE = 0
    COUNTER = 1
    SUM = 2
    HISTOGRAM = 3
