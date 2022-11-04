import time

from contextlib import ContextDecorator
from ray.util.metrics import Histogram, Counter


class monitor(ContextDecorator):
    """A context manager/decorator that monitors the latency, number of calls,
    and successful calls for a block or a function.

     It can be used as follows (both on the driver or within a task).

    .. code-block:: python
        import ray._private.metrics as metrics

        with metrics.monitor("metrics_name"):
            # Do some computation here.

        @metrics.monitor("metrics_name"):
        def foo():"""

    def __init__(self, name: str):
        super().__init__()
        self.total_calls = Counter(f"{name}.total")
        self.success_calls = Counter(f"{name}.success")
        self.latency_ms = Histogram(f"{name}.latency_ms", boundaries=[0.01, 0.05, 0.1])

    def __enter__(self):
        self.total_calls.inc()
        self.start = time.time()

    def __exit__(self, exc, value, tb):
        self.latency_ms.observe(int(1000 * (time.time() - self.start)))
        if not exc:
            self.success_calls.inc()
