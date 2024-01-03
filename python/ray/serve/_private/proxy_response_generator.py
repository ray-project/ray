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


class _ProxyResponseGeneratorBase(ABC):
    def __init__(
        self,
        *,
        timeout_s: Optional[float] = None,
        disconnected_task: Optional[asyncio.Task] = None,
        result_callback: Optional[Callable[[Any], Any]] = None,
    ):
        """Implements a generator wrapping a deployment response.

        Args:
            - timeout_s: an end-to-end timeout for the request. If this expires and the
              response is not completed, the request will be cancelled. If `None`,
              there's no timeout.
            - disconnected_task: a task whose completion signals that the client has
              disconnected. When this happens, the request will be cancelled. If `None`,
              disconnects will not be detected.
            - result_callback: will be called on each result before it's returned. If
              `None`, the unmodified result is returned.
        """
        self._timeout_s = timeout_s
        self._start_time_s = time.time()
        self._disconnected_task = disconnected_task
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
        """Once this is called, the disconnected_task will be ignored."""
        self._disconnected_task = None


class ProxyResponseGenerator(_ProxyResponseGeneratorBase):
    """Wraps a unary DeploymentResponse or streaming DeploymentResponseGenerator.

    In the case of a unary DeploymentResponse, __anext__ will only ever return one
    result.
    """

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

    def cancelled(self) -> bool:
        return self._response.cancelled()

    async def __anext__(self):
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
        except asyncio.CancelledError as e:
            # This is specifically for gRPC. The cancellation can happen from client
            # dropped connection before the request is completed. If self._response is
            # not already cancelled, we want to explicitly cancel the task, so it
            # doesn't waste cluster resource in this case and can be terminated
            # gracefully.
            if not self._response.cancelled():
                self._response.cancel()
                self._done = True

            raise e from None
        except Exception as e:
            self._done = True
            raise e from None

        return result

    async def _await_response_anext(self) -> Any:
        return await self._response.__anext__()

    async def _get_next_streaming_result(self) -> Any:
        next_result_task = asyncio.create_task(self._await_response_anext())
        tasks = [next_result_task]
        if self._disconnected_task is not None:
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

    async def _await_response(self) -> Any:
        return await self._response

    async def _get_unary_result(self) -> Any:
        result_task = asyncio.create_task(self._await_response())
        tasks = [result_task]
        if self._disconnected_task is not None:
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
