"""
Generic model definitions for common utilities.

These models represent generic concepts that can be used by both
serve and batch components.
"""

import asyncio
import threading
from functools import partial
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


# DiskMultiplexConfig removed - it's serve-specific and belongs in serve/configs/server_models.py


class GlobalIdManager:
    """Thread-safe global ID manager for assigning unique IDs."""

    def __init__(self):
        self._counter = 0
        self._lock = threading.Lock()

    def next(self) -> int:
        """Get the next unique ID."""
        with self._lock:
            self._counter += 1
            return self._counter


# Global instance
global_id_manager = GlobalIdManager()


def make_async(_func: Callable[..., T]) -> Callable[..., Awaitable[T]]:
    """Take a blocking function, and run it on in an executor thread.

    This function prevents the blocking function from blocking the asyncio event loop.
    The code in this function needs to be thread safe.
    """

    def _async_wrapper(*args, **kwargs) -> asyncio.Future:
        loop = asyncio.get_event_loop()
        func = partial(_func, *args, **kwargs)
        return loop.run_in_executor(executor=None, func=func)

    return _async_wrapper
