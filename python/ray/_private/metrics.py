import functools
import time
from ray.util.metrics import Counter, Histogram
from typing import Dict, Callable


class FunctionMetricsCollector:
    """A helper class that implements the context manager and decorator
    for recording function/block execution.
    """
    HISTOGRAMS: Dict[str, Histogram] = dict()
    SUCCESS_COUNTERS: Dict[str, Counter] = dict()
    FAILURE_COUNTERS: Dict[str, Counter] = dict()

    def __init__(self, name: str):
        self._latency_historgram = self.get_latency_histogram(name)
        self._success_counter = self.get_success_counter(name)
        self._failure_counter = self.get_failure_counter(name)

    def __enter__(self):
        self._start_time = time.time()
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._latency_historgram.observe(
            int((time.time() - self._start_time) * 1000))
        if exc_val:
            self._failure_counter.inc()
        else:
            self._success_counter.inc()

    def __call__(self, f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            with self:
                return f(*args, **kwargs)

        return wrapped

    def get_latency_histogram(self, name: str) -> Histogram:
        if name not in self.HISTOGRAMS:
            self.HISTOGRAMS[name] = Histogram(
                f"{name}.latency_ms", boundaries=[1.0, 2.0])
        return self.HISTOGRAMS[name]

    def get_success_counter(self, name: str) -> Counter:
        if name not in self.SUCCESS_COUNTERS:
            self.SUCCESS_COUNTERS[name] = Counter(f"{name}.success")
        return self.SUCCESS_COUNTERS[name]

    def get_failure_counter(self, name: str) -> Counter:
        if name not in self.FAILURE_COUNTERS:
            self.FAILURE_COUNTERS[name] = Counter(f"{name}.failure")
        return self.FAILURE_COUNTERS[name]


def record_function(name: str) -> FunctionMetricsCollector:
    """Record the latency and success/failure counter in a
    block of code or function.

    Can be used as a function decorator or context manager.
    Upon function/block execution finishes, it records the
    latency into {name}.latency_ms histogram. It also bumps the
    {name}.success/{name}.failure counter depending on
    wether the function/block raises exception.
    """
    return FunctionMetricsCollector(name)
