import time

from ray.util.metrics import Histogram

_num_buckets = 31
_coeff = 4
_short_event_min = 0.0001  # 0.1 ms
_short_event_max = 1.5
_long_event_min = 0.1
_long_event_max = 600.0


def _create_buckets(coeff, event_min, event_max, num):
    """Generates a list of `num` buckets between `event_min` and `event_max`.
    `coeff` - specifies how much denser at the low end
    """
    if num == 1:
        return [event_min]
    step = 1 / (num - 1)
    return [
        (0 + step * i) ** coeff * (event_max - event_min) + event_min
        for i in range(num)
    ]


DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS = _create_buckets(
    coeff=_coeff,
    event_min=_short_event_min,
    event_max=_short_event_max,
    num=_num_buckets,
)
DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS = _create_buckets(
    coeff=_coeff,
    event_min=_long_event_min,
    event_max=_long_event_max,
    num=_num_buckets,
)


class TimerAndPrometheusLogger:
    """Context manager for timing code execution.

    Elapsed time is automatically logged to the provided Prometheus Histogram.

    Example:
        with TimerAndPrometheusLogger(Histogram):
            learner.update()
    """

    def __init__(self, histogram: Histogram):
        self._histogram = histogram

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.elapsed = time.perf_counter() - self.start
        self._histogram.observe(self.elapsed)
