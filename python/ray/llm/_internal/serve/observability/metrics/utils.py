import time
from enum import Enum
from typing import (
    List,
    TypeVar,
)

# Histogram buckets for short-range latencies measurements:
# Min=1ms, Max=30s
#
# NOTE: Number of buckets have to be bounded (and not exceed 30)
#       to avoid overloading metrics sub-system
SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS: List[float] = [
    1,
    5,
    10,
    25,
    50,
    100,
    150,
    250,
    500,
    1000,
    1500,
    2500,
    5000,
    7500,
    10000,
    20000,
    30000,
]

# Histogram buckets for long-range latencies measurements:
# Min=10ms, Max=300s
LONG_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS = [
    x * 10 for x in SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS
]


class ClockUnit(int, Enum):
    ms = 1000
    s = 1


class MsClock:
    """A clock that tracks intervals in milliseconds"""

    def __init__(self, unit: ClockUnit = ClockUnit.ms):
        self.reset()
        self.unit = unit.value
        self.start_time = time.perf_counter()

    def reset(self):
        self.start_time = time.perf_counter()

    def interval(self):
        return (time.perf_counter() - self.start_time) * self.unit

    def reset_interval(self):
        interval = self.interval()
        self.reset()
        return interval


T = TypeVar("T")
