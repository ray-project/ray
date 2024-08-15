# Adapted from [aiodebug](https://gitlab.com/quantlane/libs/aiodebug)
import asyncio
import asyncio.events
from asyncio import CancelledError
from typing import Callable, Optional, Type, Any, Coroutine, Iterable


# Copyright 2016-2022 Quantlane s.r.o.
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#        http://www.apache.org/licenses/LICENSE-2.0
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# Modifications:
# - Removed the dependency to `logwood`.
# - Renamed `monitor_loop_lag.enable()` to just `enable_monitor_loop_lag()`.
# - Miscellaneous changes to make it work with Ray.


async def async_call_with_retry(
    f: Callable[[], Coroutine[Any, Any, Any]],
    *,
    exception_classes: Iterable[Type] = (),
    max_attempts: int = 3,
    initial_delay_s: float = 0.5,
    max_backoff_s: float = 5,
    on_exception: Optional[Callable[[Exception], None]] = None,
) -> Any:
    """
    Helper utility retrying a provided async function with exponential backoff
    policy.

    NOTE: We're currently hard-coding 2 as an exponent base, meaning that
          `initial_delay_s` will be doubled with every retry attempt up to
          `max_backoff_s`.

    Args:
        c: Coroutine to be awaited on.
        exception_classes: list of exception classes that should be retried (note that,
                            all of the exceptions have to inherit from `BaseException`
                            and could NOT extend `CancelledError`).
        max_attempts: The maximum number of attempts to invoke.
        initial_delay_s: Initial delay before the first retry
        max_backoff_s: The maximum number of seconds to backoff.
        on_exception: Callback to be invoked with an exception being retried.
    """
    assert all(
        [issubclass(c, BaseException) for c in exception_classes]
    ), "Provided exception classes have to inherit from BaseException"

    assert not any(
        [issubclass(c, CancelledError) for c in exception_classes]
    ), "`CancelledError` should not be retried"

    assert (
        initial_delay_s >= 0
    ), f"`initial_delay_s`, must be non-negative value (got {initial_delay_s}"
    assert (
        max_backoff_s >= 0
    ), f"`max_backoff_s`, must be non-negative value (got {max_backoff_s}"
    assert max_attempts >= 1, f"`max_attempts` must be >= 1 (got {max_attempts})"

    for i in range(max_attempts):
        try:
            return await f()
        except Exception as e:
            is_retryable = any(
                [isinstance(e, exc_class) for exc_class in exception_classes]
            )
            if is_retryable and i + 1 < max_attempts:
                # Invoke callback if provided
                if on_exception is not None:
                    on_exception(e)

                # Retry with (capped) exponential backoff
                backoff = min((initial_delay_s * 2 ** (i + 1)), max_backoff_s)
                await asyncio.sleep(backoff)
            else:
                raise e

    raise ValueError("This part should not be reachable")

def enable_monitor_loop_lag(
    callback: Callable[[float], None],
    interval_s: float = 0.25,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> None:
    """
    Start logging event loop lags to the callback. In ideal circumstances they should be
    very close to zero. Lags may increase if event loop callbacks block for too long.

    Note: this works for all event loops, including uvloop.

    :param callback: Callback to call with the lag in seconds.
    """
    if loop is None:
        loop = asyncio.get_running_loop()
    if loop is None:
        raise ValueError("No provided loop, nor running loop found.")

    async def monitor():
        while loop.is_running():
            t0 = loop.time()
            await asyncio.sleep(interval_s)
            lag = loop.time() - t0 - interval_s  # Should be close to zero.
            callback(lag)

    loop.create_task(monitor(), name="async_utils.monitor_loop_lag")
