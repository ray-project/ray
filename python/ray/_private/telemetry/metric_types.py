from enum import Enum


class MetricType(Enum):
    """Types of metrics supported by the telemetry system."""

    GAUGE = 0
    COUNTER = 1
    SUM = 2
    HISTOGRAM = 3
