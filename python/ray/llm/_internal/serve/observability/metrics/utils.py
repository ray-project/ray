import time
from enum import Enum
from typing import (
    AsyncGenerator,
    Callable,
    List,
    Set,
    TypeVar,
)

from ray.util import metrics

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


class InstrumentTokenAsyncGenerator:
    """This class instruments an asynchronous generator.

    It gathers 3 metrics:
    1. Time to first time
    2. Time between tokens
    3. Total completion time

    Usage:

    @InstrumentTokenAsyncGenerator("my_special_fn")
    async def to_instrument():
        yield ...
    """

    all_instrument_names: Set[str] = set()

    def __init__(
        self, generator_name: str, latency_histogram_buckets: List[float] = None
    ):
        self.generator_name = f"rayllm_{generator_name}"

        target_latency_histogram_buckets = (
            latency_histogram_buckets or SHORT_RANGE_LATENCY_HISTOGRAM_BUCKETS_MS
        )

        assert (
            self.generator_name not in self.all_instrument_names
        ), "This generator name was already used elsewhere. Please specify another one."
        self.all_instrument_names.add(self.generator_name)

        self.token_latency_histogram = metrics.Histogram(
            f"{self.generator_name}_per_token_latency_ms",
            f"Generator metrics for {self.generator_name}",
            boundaries=target_latency_histogram_buckets,
        )

        self.first_token_latency_histogram = metrics.Histogram(
            f"{self.generator_name}_first_token_latency_ms",
            f"Generator metrics for {self.generator_name}",
            boundaries=target_latency_histogram_buckets,
        )
        self.total_latency_histogram = metrics.Histogram(
            f"{self.generator_name}_total_latency_ms",
            f"Generator metrics for {self.generator_name}",
            boundaries=target_latency_histogram_buckets,
        )

    def __call__(
        self, async_generator_fn: Callable[..., AsyncGenerator[T, None]]
    ) -> Callable[..., AsyncGenerator[T, None]]:
        async def new_gen(*args, **kwargs):
            interval_clock = MsClock()
            total_clock = MsClock()
            is_first_token = True
            try:
                async for x in async_generator_fn(*args, **kwargs):
                    if is_first_token:
                        self.first_token_latency_histogram.observe(
                            total_clock.interval()
                        )
                        interval_clock.reset()
                        is_first_token = False
                    else:
                        self.token_latency_histogram.observe(
                            interval_clock.reset_interval()
                        )
                    yield x
            finally:
                self.total_latency_histogram.observe(total_clock.interval())

        return new_gen
