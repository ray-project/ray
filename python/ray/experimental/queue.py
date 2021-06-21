import warnings

from ray.util.queue import Empty, Full, Queue

warnings.warn(
    DeprecationWarning(
        "ray.experimental.queue has been moved to ray.util.queue. "
        "Please update your import path."),
    stacklevel=2)

__all__ = ["Empty", "Full", "Queue"]
