import asyncio
import time
from typing import Optional, AsyncGenerator


async def sleep_till(wake_time: float) -> None:
    """Sleeps until designated time, unless designated time is in the past"""
    delta = wake_time - time.time()
    if delta > 0:
        await asyncio.sleep(delta)


async def ticker(
    *,
    interval_s: float,
    timeout_s: Optional[float] = None,
    max_iterations: Optional[int] = None,
) -> AsyncGenerator[int, None]:
    """Returns an `AsyncGenerator` that produces a new integer every `interval_secs` until either
    `timeout_secs` elapses or `max_iterations` have occurred
    """
    now = time.time()
    end_time = now + timeout_s if timeout_s else None
    count = 0
    while (
        # Both of the following conditions have to be satisfied
        #   - Now should be before the "end_time" (if set)
        #   - Current iteration should be less than "max_iterations" (if set)
        (not end_time or now < end_time)
        and (not max_iterations or count < max_iterations)
    ):
        count += 1
        # NOTE: Polling time is set before we yield to make sure that if
        #       execution (that we're yielding to) will be taking longer than
        #       the polling period, we won't incur additional and unnecessary
        #       waiting upon resuming the execution here
        wake_time = time.time() + interval_s
        yield count
        # Sleep (if needed) upon continuation
        await sleep_till(wake_time)
        now = time.time()
