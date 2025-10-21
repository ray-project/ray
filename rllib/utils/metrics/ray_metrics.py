import time

# import numpy as np

from ray.util.metrics import Histogram

_num_buckets = 31
_coeff = 4  # how much denser at the low end
_short_event_min = 0.0001  # 0.1 ms
_short_event_max = 1.5
_long_event_min = 0.1
_long_event_max = 600.0

# More dense at the low end; timing short events.
# DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS = (
#     np.linspace(start=0, stop=1, num=_num_buckets) ** _coeff
# ) * (_short_event_max - _short_event_min) + _short_event_min
#
# # More dense at the low end; timing long events.
# DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS = (
#     np.linspace(start=0, stop=1, num=_num_buckets) ** _coeff
# ) * (_long_event_max - _long_event_min) + _long_event_min

DEFAULT_HISTOGRAM_BOUNDARIES_SHORT_EVENTS = None
DEFAULT_HISTOGRAM_BOUNDARIES_LONG_EVENTS = None


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
