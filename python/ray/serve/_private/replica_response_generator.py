import asyncio
import time
from typing import Any, AsyncGenerator, Callable, Optional

from ray.serve._private.proxy_response_generator import (
    _ProxyResponseGeneratorBase,
    swallow_cancelled,
)
from ray.serve._private.utils import calculate_remaining_timeout
from ray.serve.exceptions import RequestCancelledError


class ReplicaResponseGenerator(_ProxyResponseGeneratorBase):
    """Generic wrapper that adds disconnect detection to any async generator.

    This can be used to wrap any async generator and add timeout and disconnect
    detection capabilities. When a disconnect is detected, the generator will
    raise asyncio.CancelledError.
    """

    def __init__(
        self,
        async_generator: AsyncGenerator[Any, None],
        *,
        timeout_s: Optional[float] = None,
        disconnected_task: Optional[asyncio.Task] = None,
        result_callback: Optional[Callable[[Any], Any]] = None,
    ):
        super().__init__(
            timeout_s=timeout_s,
            disconnected_task=disconnected_task,
            result_callback=result_callback,
        )
        self._async_generator = async_generator
        self._done = False

    async def __anext__(self):
        if self._done:
            raise StopAsyncIteration

        try:
            result = await self._get_next_result()
            if self._result_callback is not None:
                result = self._result_callback(result)
            return result
        except (StopAsyncIteration, asyncio.CancelledError) as e:
            self._done = True
            raise e from None
        except Exception as e:
            self._done = True
            raise e from None

    async def _await_response_anext(self) -> Any:
        return await self._async_generator.__anext__()

    async def _cancel_next_result_task(self, next_result_task: asyncio.Task) -> None:
        """Cancel the pending `__anext__` task and wait for it to exit so that
        a subsequent `await async_generator.aclose()` by the caller does not
        fail with `RuntimeError: aclose(): asynchronous generator is already running`.
        """
        if not next_result_task.done():
            next_result_task.cancel()
        try:
            await next_result_task
        except (RequestCancelledError, StopAsyncIteration):
            pass
        except asyncio.CancelledError:
            if not next_result_task.done():
                raise
        finally:
            if not next_result_task.done():
                next_result_task.add_done_callback(swallow_cancelled)

    async def _get_next_result(self) -> Any:
        """Get the next result from the async generator with disconnect detection."""
        # If there's no disconnect detection needed, use direct await to preserve
        # cancellation propagation (important for gRPC cancellation)
        remaining_timeout = calculate_remaining_timeout(
            timeout_s=self._timeout_s,
            start_time_s=self._start_time_s,
            curr_time_s=time.time(),
        )
        if self._disconnected_task is None:
            try:
                return await asyncio.wait_for(
                    self._await_response_anext(), timeout=remaining_timeout
                )
            except asyncio.TimeoutError:
                raise TimeoutError()
        # Otherwise use asyncio.wait for disconnect detection
        next_result_task = asyncio.create_task(self._await_response_anext())
        tasks = [next_result_task, self._disconnected_task]

        try:
            done, _ = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=remaining_timeout,
            )
        except asyncio.CancelledError:
            await self._cancel_next_result_task(next_result_task)
            raise

        if next_result_task in done:
            return next_result_task.result()
        elif self._disconnected_task in done:
            await self._cancel_next_result_task(next_result_task)
            raise asyncio.CancelledError()
        else:
            # Timeout occurred
            await self._cancel_next_result_task(next_result_task)
            raise TimeoutError()
