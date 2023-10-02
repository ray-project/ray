import asyncio
import logging
import time
from abc import ABC, abstractmethod
from asyncio.tasks import FIRST_COMPLETED
from typing import Any, Callable, Optional, Union

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import calculate_remaining_timeout
from ray.serve.handle import DeploymentResponse, DeploymentResponseGenerator

logger = logging.getLogger(SERVE_LOGGER_NAME)


class _ProxyDeploymentResponseGeneratorBase(ABC):
    def __init__(
        self,
        *,
        timeout_s: Optional[float] = None,
        disconnected_task: Optional[asyncio.Task] = None,
        result_callback: Optional[Callable[[Any], Any]] = None,
    ):
        """Implements a generator wrapping a deployment response.

        Implements timeouts and disconnect detection.

        The `result_callback` will be called on each result before it's returned.
        """
        self._timeout_s = timeout_s
        self._start_time_s = time.time()
        self._disconnected_task = disconnected_task
        self._should_check_disconnected = disconnected_task is not None
        self._result_callback = result_callback

    def __aiter__(self):
        return self

    @abstractmethod
    async def __anext__(self):
        """Return the next message in the stream.

        Raises:
            - TimeoutError on timeout.
            - asyncio.CancelledError on disconnect.
            - StopAsyncIteration when the stream is completed.
        """
        pass

    def stop_checking_for_disconnect(self):
        self._should_check_disconnected = False


class ProxyDeploymentResponseGenerator(_ProxyDeploymentResponseGeneratorBase):
    def __init__(
        self,
        response: Union[DeploymentResponse, DeploymentResponseGenerator],
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
        self._done = False
        self._response = response

    async def __anext__(self):
        """Yield the results from the generator with an optional timeout.

        Raises:
            - `TimeoutError` if `timeout_s` is exceeded before the request finishes.
            - `asyncio.CancelledError` if `disconnected_task` exits before the request
              finishes.

        If either of the above cases occur, the request will be cancelled.
        """
        if self._done:
            raise StopAsyncIteration

        try:
            if isinstance(self._response, DeploymentResponseGenerator):
                result = await self._get_next_streaming_result()
            else:
                result = await self._get_unary_result()
                self._done = True

            if self._result_callback is not None:
                result = self._result_callback(result)
        except Exception as e:
            self._done = True
            raise e from None

        return result

    async def _get_next_streaming_result(self) -> Any:
        async def await_next_result() -> Any:
            return await self._response.__anext__()

        next_result_task = asyncio.ensure_future(await_next_result())
        tasks = [next_result_task]
        if self._should_check_disconnected:
            tasks.append(self._disconnected_task)

        done, _ = await asyncio.wait(
            tasks,
            return_when=FIRST_COMPLETED,
            timeout=calculate_remaining_timeout(
                timeout_s=self._timeout_s,
                start_time_s=self._start_time_s,
                curr_time_s=time.time(),
            ),
        )
        if next_result_task in done:
            return next_result_task.result()
        elif self._disconnected_task in done:
            next_result_task.cancel()
            self._response.cancel()
            raise asyncio.CancelledError()
        else:
            next_result_task.cancel()
            self._response.cancel()
            raise TimeoutError()

    async def _get_unary_result(self) -> Any:
        """Await the response and return its result with an optional timeout.

        Raises:
            - `TimeoutError` if `timeout_s` is exceeded before the request finishes.
            - `asyncio.CancelledError` if `disconnected_task` exits before the request
              finishes.

        If either of the above cases occur, the request will be cancelled.
        """

        async def await_response() -> Any:
            return await self._response

        result_task = asyncio.ensure_future(await_response())
        tasks = [result_task]
        if self._should_check_disconnected:
            tasks.append(self._disconnected_task)

        done, _ = await asyncio.wait(
            tasks, return_when=FIRST_COMPLETED, timeout=self._timeout_s
        )
        if result_task in done:
            return result_task.result()
        elif self._disconnected_task in done:
            self._response.cancel()
            raise asyncio.CancelledError()
        else:
            self._response.cancel()
            raise TimeoutError()
