"""Utilities for telemetry."""

from threading import Lock
from typing import Callable

DEFAULT_GPU_TYPE = "UNSPECIFIED"


class Once:
    """Execute a function exactly once and block all callers until the function returns

    Same as golang's `sync.Once <https://pkg.go.dev/sync#Once>`_

    Took this directly from OpenTelemetry's Python SDK:
    Ref: https://github.com/open-telemetry/opentelemetry-python/blob
        /c6fab7d4c339dc5bf9eb9ef2723caad09d69bfca/opentelemetry-api/src/opentelemetry
        /util/_once.py
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._done = False

    def do_once(self, func: Callable[[], None]) -> bool:
        """Execute ``func`` if it hasn't been executed or return.

        Will block until ``func`` has been called by one thread.

        Returns:
            Whether or not ``func`` was executed in this call
        """

        # fast path, try to avoid locking
        if self._done:
            return False

        with self._lock:
            if not self._done:
                func()
                self._done = True
                return True
        return False
