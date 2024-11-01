import asyncio
import concurrent.futures
import logging
import queue
import time
from typing import Any, Callable, Dict, Optional, Tuple

from ray import cloudpickle
from ray.serve._private.common import DeploymentID, RequestMetadata
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.replica import UserCallableWrapper
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.router import Router
from ray.serve._private.utils import GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR
from ray.serve.deployment import Deployment
from ray.serve.handle import (
    DeploymentHandle,
    DeploymentResponse,
    DeploymentResponseGenerator,
)

# TODO: figure out how to get logs to all go to stderr.
logger = logging.getLogger(SERVE_LOGGER_NAME)


def make_local_deployment_handle(
    deployment: Deployment,
    app_name: str,
) -> DeploymentHandle:
    # XXX: comment.
    def _create_local_router(
        handle_id: str, deployment_id: DeploymentID, handle_options: Any
    ) -> Router:
        return _LocalRouter(
            UserCallableWrapper(
                deployment.func_or_class,
                deployment.init_args,
                deployment.init_kwargs,
                deployment_id=deployment_id,
            ),
            deployment_id=deployment_id,
            handle_options=handle_options,
        )

    return DeploymentHandle(
        deployment.name,
        app_name,
        _create_router=_create_local_router,
    )


class _LocalReplicaResult(ReplicaResult):
    def __init__(
        self,
        future: concurrent.futures.Future,
        *,
        is_streaming: bool = False,
        generator_result_queue: Optional[queue.Queue] = None,
    ):
        self._future = future
        self._lazy_asyncio_future = None
        self._is_streaming = is_streaming
        self._generator_result_queue = generator_result_queue

    @property
    def _asyncio_future(self) -> asyncio.Future:
        if self._lazy_asyncio_future is None:
            self._lazy_asyncio_future = asyncio.wrap_future(self._future)

        return self._lazy_asyncio_future

    def get(self, timeout_s: Optional[float]):
        assert (
            not self._is_streaming
        ), "get() can only be called on a non-streaming result."

        try:
            return self._future.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Result not available within {timeout_s}s.")

    async def get_async(self):
        assert (
            not self._is_streaming
        ), "get_async() can only be called on a non-streaming result."

        return await self._asyncio_future

    def __next__(self):
        assert self._is_streaming, "next() can only be called on a streaming result."

        while True:
            if self._future.done() and self._generator_result_queue.empty():
                if self._future.exception():
                    raise self._future.exception()
                else:
                    raise StopIteration

            try:
                return self._generator_result_queue.get(timeout=0.01)
            except queue.Empty:
                pass

    async def __anext__(self):
        assert self._is_streaming, "anext() can only be called on a streaming result."

        # TODO: comment.
        def _wait_for_result():
            while True:
                if self._future.done() or not self._generator_result_queue.empty():
                    return
                time.sleep(0.01)

        wait_for_result_task = asyncio.get_running_loop().create_task(
            asyncio.to_thread(_wait_for_result),
        )
        done, _ = await asyncio.wait(
            [self._asyncio_future, wait_for_result_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not self._generator_result_queue.empty():
            return self._generator_result_queue.get()

        if self._asyncio_future.exception():
            raise self._asyncio_future.exception()

        raise StopAsyncIteration

    def add_done_callback(self, callback: Callable):
        self._future.add_done_callback(callback)

    def cancel(self):
        self._future.cancel()


class _LocalRouter(Router):
    def __init__(
        self,
        user_callable_wrapper: UserCallableWrapper,
        deployment_id: DeploymentID,
        handle_options: Any,
    ):
        logger.info(f"Initializing local replica for '{deployment_id}'")
        self._deployment_id = deployment_id
        self._user_callable_wrapper = user_callable_wrapper
        self._user_callable_wrapper.initialize_callable().result()

    def running_replicas_populated(self) -> bool:
        return True

    def _resolve_deployment_responses(
        self, request_args: Tuple[Any], request_kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any], Dict[str, Any]]:
        def _new_arg(arg: Any) -> Any:
            if isinstance(arg, DeploymentResponse):
                new_arg = arg.result(_skip_asyncio_check=True)
            elif isinstance(arg, DeploymentResponseGenerator):
                raise GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR
            else:
                new_arg = arg

            return new_arg

        return (
            tuple(_new_arg(arg) for arg in request_args),
            {k: _new_arg(v) for k, v in request_kwargs.items()},
        )

    def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> concurrent.futures.Future[_LocalReplicaResult]:
        request_args, request_kwargs = self._resolve_deployment_responses(
            request_args, request_kwargs
        )
        # Serialize and deserialize the arguments to mimic remote call behavior.
        request_args, request_kwargs = cloudpickle.loads(
            cloudpickle.dumps(
                (request_args, request_kwargs),
            )
        )

        if request_meta.is_streaming:
            generator_result_queue = queue.Queue()

            def generator_result_callback(item: Any):
                generator_result_queue.put_nowait(item)

        else:
            generator_result_queue = None
            generator_result_callback = None

        noop_future = concurrent.futures.Future()
        noop_future.set_result(
            _LocalReplicaResult(
                self._user_callable_wrapper.call_user_method(
                    request_meta,
                    request_args,
                    request_kwargs,
                    generator_result_callback=generator_result_callback,
                ),
                is_streaming=request_meta.is_streaming,
                generator_result_queue=generator_result_queue,
            )
        )
        return noop_future

    def shutdown(self):
        pass
