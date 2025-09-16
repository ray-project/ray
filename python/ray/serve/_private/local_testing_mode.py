import asyncio
import concurrent.futures
import inspect
import logging
import queue
import time
from functools import wraps
from typing import Any, Callable, Coroutine, Dict, Optional, Tuple, Union

import ray
from ray import cloudpickle
from ray.serve._private.common import DeploymentID, RequestMetadata
from ray.serve._private.constants import (
    RAY_SERVE_RUN_SYNC_IN_THREADPOOL,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica import UserCallableWrapper
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.router import Router
from ray.serve._private.utils import GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR
from ray.serve.deployment import Deployment
from ray.serve.exceptions import RequestCancelledError
from ray.serve.handle import (
    DeploymentHandle,
    DeploymentResponse,
    DeploymentResponseGenerator,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _validate_deployment_options(
    deployment: Deployment,
    deployment_id: DeploymentID,
):
    if "num_gpus" in deployment.ray_actor_options:
        logger.warning(
            f"Deployment {deployment_id} has num_gpus configured. "
            "CUDA_VISIBLE_DEVICES is not managed automatically in local testing mode. "
        )

    if "runtime_env" in deployment.ray_actor_options:
        logger.warning(
            f"Deployment {deployment_id} has runtime_env configured. "
            "runtime_envs are ignored in local testing mode."
        )


def make_local_deployment_handle(
    deployment: Deployment,
    app_name: str,
) -> DeploymentHandle:
    """Constructs an in-process DeploymentHandle.

    This is used in the application build process for local testing mode,
    where all deployments of an app run in the local process which enables
    faster dev iterations and use of tooling like PDB.

    The user callable will be run on an asyncio loop in a separate thread
    (sharing the same code that's used in the replica).

    The constructor for the user callable is run eagerly in this function to
    ensure that any exceptions are raised during `serve.run`.
    """
    deployment_id = DeploymentID(deployment.name, app_name)
    _validate_deployment_options(deployment, deployment_id)
    user_callable_wrapper = UserCallableWrapper(
        deployment.func_or_class,
        deployment.init_args,
        deployment.init_kwargs,
        deployment_id=deployment_id,
        run_sync_methods_in_threadpool=RAY_SERVE_RUN_SYNC_IN_THREADPOOL,
        run_user_code_in_separate_thread=True,
        local_testing_mode=True,
    )
    try:
        logger.info(f"Initializing local replica class for {deployment_id}.")
        user_callable_wrapper.initialize_callable().result()
        user_callable_wrapper.call_reconfigure(deployment.user_config)
    except Exception:
        logger.exception(f"Failed to initialize deployment {deployment_id}.")
        raise

    def _create_local_router(
        handle_id: str, deployment_id: DeploymentID, handle_options: Any
    ) -> Router:
        return LocalRouter(
            user_callable_wrapper,
            deployment_id=deployment_id,
            handle_options=handle_options,
        )

    return DeploymentHandle(
        deployment.name,
        app_name,
        _create_router=_create_local_router,
    )


class LocalReplicaResult(ReplicaResult):
    """ReplicaResult used by in-process Deployment Handles."""

    OBJ_REF_NOT_SUPPORTED_ERROR = RuntimeError(
        "Converting DeploymentResponses to ObjectRefs is not supported "
        "in local testing mode."
    )
    REJECTION_NOT_SUPPORTED_ERROR = RuntimeError(
        "Request rejection is not supported in local testing mode."
    )

    def __init__(
        self,
        future: concurrent.futures.Future,
        *,
        request_id: str,
        is_streaming: bool = False,
        generator_result_queue: Optional[queue.Queue] = None,
    ):
        self._future = future
        self._lazy_asyncio_future = None
        self._request_id = request_id
        self._is_streaming = is_streaming

        # For streaming requests, results must be written to this queue.
        # The queue will be consumed until the future is completed.
        self._generator_result_queue = generator_result_queue
        if self._is_streaming:
            assert (
                self._generator_result_queue is not None
            ), "generator_result_queue must be provided for streaming results."

    @property
    def _asyncio_future(self) -> asyncio.Future:
        if self._lazy_asyncio_future is None:
            self._lazy_asyncio_future = asyncio.wrap_future(self._future)

        return self._lazy_asyncio_future

    def _process_response(f: Union[Callable, Coroutine]):
        @wraps(f)
        def wrapper(self, *args, **kwargs):
            try:
                return f(self, *args, **kwargs)
            except (asyncio.CancelledError, concurrent.futures.CancelledError):
                raise RequestCancelledError(self._request_id)

        @wraps(f)
        async def async_wrapper(self, *args, **kwargs):
            try:
                return await f(self, *args, **kwargs)
            except (asyncio.CancelledError, concurrent.futures.CancelledError):
                raise RequestCancelledError(self._request_id)

        if inspect.iscoroutinefunction(f):
            return async_wrapper
        else:
            return wrapper

    @_process_response
    async def get_rejection_response(self):
        raise self.REJECTION_NOT_SUPPORTED_ERROR

    @_process_response
    def get(self, timeout_s: Optional[float]):
        assert (
            not self._is_streaming
        ), "get() can only be called on a non-streaming result."

        try:
            return self._future.result(timeout=timeout_s)
        except concurrent.futures.TimeoutError:
            raise TimeoutError("Timed out waiting for result.")

    @_process_response
    async def get_async(self):
        assert (
            not self._is_streaming
        ), "get_async() can only be called on a non-streaming result."

        return await self._asyncio_future

    @_process_response
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

    @_process_response
    async def __anext__(self):
        assert self._is_streaming, "anext() can only be called on a streaming result."

        # This callback does not pull from the queue, only checks that a result is
        # available, else there is a race condition where the future finishes and the
        # queue is empty, but this function hasn't returned the result yet.
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

    def to_object_ref(self, timeout_s: Optional[float]) -> ray.ObjectRef:
        raise self.OBJ_REF_NOT_SUPPORTED_ERROR

    async def to_object_ref_async(self) -> ray.ObjectRef:
        raise self.OBJ_REF_NOT_SUPPORTED_ERROR

    def to_object_ref_gen(self) -> ray.ObjectRefGenerator:
        raise self.OBJ_REF_NOT_SUPPORTED_ERROR


class LocalRouter(Router):
    def __init__(
        self,
        user_callable_wrapper: UserCallableWrapper,
        deployment_id: DeploymentID,
        handle_options: Any,
    ):
        self._deployment_id = deployment_id
        self._user_callable_wrapper = user_callable_wrapper
        assert (
            self._user_callable_wrapper._callable is not None
        ), "User callable must already be initialized."

    def running_replicas_populated(self) -> bool:
        return True

    def _resolve_deployment_responses(
        self, request_args: Tuple[Any], request_kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any], Dict[str, Any]]:
        """Replace DeploymentResponse objects with their results.

        NOTE(edoakes): this currently calls the blocking `.result()` method
        on the responses to resolve them to their values. This is a divergence
        from the remote codepath where they're resolved concurrently.
        """

        def _new_arg(arg: Any) -> Any:
            if isinstance(arg, DeploymentResponse):
                new_arg = arg.result(_skip_asyncio_check=True)
            elif isinstance(arg, DeploymentResponseGenerator):
                raise GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR
            else:
                new_arg = arg

            return new_arg

        # Serialize and deserialize the arguments to mimic remote call behavior.
        return cloudpickle.loads(
            cloudpickle.dumps(
                (
                    tuple(_new_arg(arg) for arg in request_args),
                    {k: _new_arg(v) for k, v in request_kwargs.items()},
                )
            )
        )

    def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> concurrent.futures.Future[LocalReplicaResult]:
        request_args, request_kwargs = self._resolve_deployment_responses(
            request_args, request_kwargs
        )

        if request_meta.is_streaming:
            generator_result_queue = queue.Queue()

            def generator_result_callback(item: Any):
                generator_result_queue.put_nowait(item)

        else:
            generator_result_queue = None
            generator_result_callback = None

        # Conform to the router interface of returning a future to the ReplicaResult.
        if request_meta.is_http_request:
            fut = self._user_callable_wrapper._call_http_entrypoint(
                request_meta,
                request_args,
                request_kwargs,
                generator_result_callback=generator_result_callback,
            )
        elif request_meta.is_streaming:
            fut = self._user_callable_wrapper._call_user_generator(
                request_meta,
                request_args,
                request_kwargs,
                enqueue=generator_result_callback,
            )
        else:
            fut = self._user_callable_wrapper.call_user_method(
                request_meta,
                request_args,
                request_kwargs,
            )
        noop_future = concurrent.futures.Future()
        noop_future.set_result(
            LocalReplicaResult(
                fut,
                request_id=request_meta.request_id,
                is_streaming=request_meta.is_streaming,
                generator_result_queue=generator_result_queue,
            )
        )
        return noop_future

    def shutdown(self):
        noop_future = concurrent.futures.Future()
        noop_future.set_result(None)
        return noop_future
