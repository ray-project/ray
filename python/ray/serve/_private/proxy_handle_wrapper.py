import asyncio
import logging
import time
from abc import ABC, abstractmethod
from asyncio.tasks import FIRST_COMPLETED
from typing import Any, AsyncIterator, Callable, Optional, Union

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import calculate_remaining_timeout
from ray.serve.handle import (
    DeploymentHandle,
    DeploymentResponse,
    DeploymentResponseGenerator,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class AbstractProxyHandleWrapper(ABC):
    @abstractmethod
    async def stream_request(
        self,
        request_arg: Any,
        *,
        timeout_s: Optional[float],
        disconnected_task: asyncio.Task,
    ) -> AsyncIterator[Any]:
        """Yields the outputs.

        Raises:
            - TimeoutError on timeout.
            - asyncio.CancelledError on disconnect.
            - StopAsyncIteration when the stream is completed.
        """
        raise NotImplementedError


class ProxyHandleWrapper(AbstractProxyHandleWrapper):
    def __init__(
        self,
        handle: DeploymentHandle,
        *,
        result_callback: Optional[Callable[[Any], Any]] = None,
    ):
        self._handle = handle
        self._result_callback = result_callback

    async def _consume_streaming_generator_with_timeout(
        self,
        response: DeploymentResponseGenerator,
        *,
        timeout_s: Optional[float] = None,
        disconnected_task: Optional[asyncio.Task] = None,
        stop_checking_disconnected_event: Optional[asyncio.Event] = None,
    ) -> AsyncIterator[Any]:
        """Yield the results from the generator with an optional timeout.

        Raises:
            - `TimeoutError` if `timeout_s` is exceeded before the request finishes.
            - `asyncio.CancelledError` if `disconnected_task` exits before the request
              finishes.

        If either of the above cases occur, the request will be cancelled.
        """

        async def await_next_result() -> Any:
            return await response.__anext__()

        start_time_s = time.time()
        while True:
            next_result_task = asyncio.ensure_future(await_next_result())
            tasks = [next_result_task]
            if disconnected_task is not None and (
                stop_checking_disconnected_event is None
                or not stop_checking_disconnected_event.is_set()
            ):
                tasks.append(disconnected_task)

            done, _ = await asyncio.wait(
                tasks,
                return_when=FIRST_COMPLETED,
                timeout=calculate_remaining_timeout(
                    timeout_s=timeout_s,
                    start_time_s=start_time_s,
                    curr_time_s=time.time(),
                ),
            )
            if next_result_task in done:
                try:
                    yield next_result_task.result()
                except StopAsyncIteration:
                    return
            elif disconnected_task is not None and disconnected_task in done:
                next_result_task.cancel()
                response.cancel()
                raise asyncio.CancelledError()
            else:
                next_result_task.cancel()
                response.cancel()
                raise TimeoutError()

    async def _await_unary_response_with_timeout(
        self,
        response: DeploymentResponse,
        *,
        timeout_s: Optional[float] = None,
        disconnected_task: Optional[asyncio.Task] = None,
    ) -> AsyncIterator[Any]:
        """Await the response and return its result with an optional timeout.

        Raises:
            - `TimeoutError` if `timeout_s` is exceeded before the request finishes.
            - `asyncio.CancelledError` if `disconnected_task` exits before the request
              finishes.

        If either of the above cases occur, the request will be cancelled.
        """

        async def await_response() -> Any:
            return await response

        result_task = asyncio.ensure_future(await_response())
        tasks = [result_task]
        if disconnected_task is not None:
            tasks.append(disconnected_task)

        done, _ = await asyncio.wait(
            tasks, return_when=FIRST_COMPLETED, timeout=timeout_s
        )
        if result_task in done:
            return result_task.result()
        elif disconnected_task is not None and disconnected_task in done:
            response.cancel()
            raise asyncio.CancelledError()
        else:
            response.cancel()
            raise TimeoutError()

    async def stream_request(
        self,
        request_arg: Any,
        *,
        timeout_s: Optional[float] = None,
        disconnected_task: Optional[asyncio.Task] = None,
        stop_checking_disconnected_event: Optional[asyncio.Event] = None,
    ) -> AsyncIterator[Any]:
        response: Union[
            DeploymentResponse, DeploymentResponseGenerator
        ] = self._handle.remote(request_arg)
        if isinstance(response, DeploymentResponseGenerator):
            async for result in self._consume_streaming_generator_with_timeout(
                response,
                timeout_s=timeout_s,
                disconnected_task=disconnected_task,
                stop_checking_disconnected_event=stop_checking_disconnected_event,
            ):
                if self._result_callback is not None:
                    result = self._result_callback(result)

                yield result
        else:
            result = await self._await_unary_response_with_timeout(
                response,
                timeout_s=timeout_s,
                disconnected_task=disconnected_task,
            )
            if self._result_callback is not None:
                result = self._result_callback(result)

            yield result
