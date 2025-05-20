# Adapted from [aiodebug](https://gitlab.com/quantlane/libs/aiodebug)

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

from typing import Callable, Optional
import asyncio
import asyncio.events


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
