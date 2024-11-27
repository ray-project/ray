import asyncio
import concurrent.futures

from ray.serve._private.common import RequestMetadata
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.router import AsyncioRouter, Router


class CurrentLoopRouter(Router):
    """Wrapper class that runs an AsyncioRouter on the current asyncio loop.
    Note that this class is NOT THREAD-SAFE, and all methods are expected to be
    invoked from a single asyncio event loop.
    """

    def __init__(self, **passthrough_kwargs):
        assert (
            "event_loop" not in passthrough_kwargs
        ), "CurrentLoopRouter uses the current event loop."

        self._asyncio_loop = asyncio.get_running_loop()
        self._asyncio_router = AsyncioRouter(
            event_loop=self._asyncio_loop, **passthrough_kwargs
        )

    def running_replicas_populated(self) -> bool:
        return self._asyncio_router.running_replicas_populated()

    def assign_request(
        self,
        request_meta: RequestMetadata,
        *request_args,
        **request_kwargs,
    ) -> concurrent.futures.Future[ReplicaResult]:
        # TODO(zcin): for now we use asyncio.run_coroutine_threadsafe(),
        # which is the simplest way to get a concurrent.futures.Future
        # object from a submitted asyncio task, as the Router interface
        # expects a concurrent.futures.Future return value. We should
        # refactor the interface to accomodate asyncio future-like objects.
        return asyncio.run_coroutine_threadsafe(
            self._asyncio_router.assign_request(
                request_meta, *request_args, **request_kwargs
            ),
            self._asyncio_loop,
        )

    def shutdown(self) -> concurrent.futures.Future:
        return asyncio.run_coroutine_threadsafe(
            self._asyncio_router.shutdown(), loop=self._asyncio_loop
        )
