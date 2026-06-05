from typing import List

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
