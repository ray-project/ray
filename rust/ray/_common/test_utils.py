"""Test utilities for the Rust Ray backend.

Provides SignalActor and wait_for_condition matching the C++ backend's
python/ray/_common/test_utils.py interface.
"""

import asyncio
import time
from typing import Any, Callable

import ray


@ray.remote(num_cpus=0)
class SignalActor:
    """A Ray actor for coordinating test execution through signals."""

    def __init__(self):
        self.ready_event = asyncio.Event()
        self.num_waiters = 0

    def send(self, clear: bool = False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait: bool = True):
        if should_wait:
            self.num_waiters += 1
            await self.ready_event.wait()
            self.num_waiters -= 1

    def cur_num_waiters(self):
        return self.num_waiters


def wait_for_condition(
    condition_predictor: Callable[..., bool],
    timeout: float = 10,
    retry_interval_ms: float = 100,
    raise_exceptions: bool = False,
    **kwargs: Any,
):
    """Wait until a condition is met or time out with an exception.

    Args:
        condition_predictor: A function that predicts the condition.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.
        raise_exceptions: If true, re-raise exceptions from the predicate.
        **kwargs: Arguments to pass to the condition_predictor.
    """
    start = time.monotonic()
    last_ex = None
    while time.monotonic() - start <= timeout:
        try:
            if condition_predictor(**kwargs):
                return
        except Exception as ex:
            if raise_exceptions:
                raise
            last_ex = ex
        time.sleep(retry_interval_ms / 1000.0)

    message = f"The condition wasn't met before the timeout ({timeout}s)."
    if last_ex is not None:
        message += f" Last exception: {last_ex}"
    raise RuntimeError(message)
