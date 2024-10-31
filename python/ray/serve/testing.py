import asyncio
import concurrent.futures
import logging
import time
from typing import Any, Callable, Optional

from ray.serve._private.common import DeploymentID, RequestMetadata
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.replica import UserCallableWrapper
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.router import Router
from ray.serve.handle import DeploymentHandle
from ray.serve.deployment import Deployment

# TODO: figure out how to get logs to all go to stderr.
logger = logging.getLogger(SERVE_LOGGER_NAME)

def make_local_deployment_handle(
    deployment: Deployment,
    app_name: str,
) -> DeploymentHandle:
    # XXX: comment.
    def _create_local_router(handle_id: str, deployment_id: DeploymentID, handle_options: Any) -> Router:
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
    def __init__(self, future: concurrent.futures.Future, *, is_streaming: bool = False):
        self._future = future
        self._is_streaming: bool = is_streaming

    def get(self, timeout_s: Optional[float]):
        assert (
            not self._is_streaming
        ), "get() can only be called on a non-streaming _LocalReplicaResult"

        return self._future.result(timeout=timeout_s)

    async def get_async(self):
        assert (
            not self._is_streaming
        ), "get_async() can only be called on a non-streaming _LocalReplicaResult"

        return await asyncio.wrap_future(self._future)

    def __next__(self):
        assert self._streaming, (
            "next() can only be called on a streaming _LocalReplicaResult."
        )

        raise NotImplementedError("Streaming not implemented yet.")

    async def __anext__(self):
        assert self._obj_ref_gen is not None, (
            "anext() can only be called on a streaming _LocalReplicaResult."
        )

        raise NotImplementedError("Streaming not implemented yet.")

    def add_done_callback(self, callback: Callable):
        self._future.add_done_callback(callback)

    def cancel(self):
        self._future.cancel()

class _LocalRouter(Router):
    def __init__(self, user_callable_wrapper: UserCallableWrapper, deployment_id: DeploymentID, handle_options: Any):
        logger.info(f"Initializing local replica for '{deployment_id}'")
        self._deployment_id = deployment_id
        self._user_callable_wrapper = user_callable_wrapper
        self._user_callable_wrapper.initialize_callable().result()

    def running_replicas_populated(self) -> bool:
        return True

    def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> concurrent.futures.Future[_LocalReplicaResult]:
        assert not request_meta.is_streaming, "Streaming not supported yet."

        dummy_fut = concurrent.futures.Future()
        dummy_fut.set_result(
            _LocalReplicaResult(
                self._user_callable_wrapper.call_user_method(
                    request_meta, request_args, request_kwargs
                ),
            )
        )
        return dummy_fut

    def shutdown(self):
        pass
