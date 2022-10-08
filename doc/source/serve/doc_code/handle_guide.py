# flake8: noqa

# __begin_sync_handle__
from starlette.requests import Request

import ray
from ray import serve
from ray.serve.handle import RayServeSyncHandle


@serve.deployment
class Model:
    def __call__(self) -> str:
        return "hello"


handle: RayServeSyncHandle = serve.run(Model.bind())
ref: ray.ObjectRef = handle.remote()  # blocks until request is assigned to replica
assert ray.get(ref) == "hello"
# __end_sync_handle__

# __begin_async_handle__
import asyncio
import random
import ray
from ray import serve
from ray.serve.handle import RayServeDeploymentHandle, RayServeSyncHandle


@serve.deployment
class Model:
    def __call__(self) -> str:
        return "hello"


@serve.deployment
class DynamicDispatcher:
    def __init__(
        self, handle_a: RayServeDeploymentHandle, handle_b: RayServeDeploymentHandle
    ):
        self.handle_a = handle_a
        self.handle_b = handle_b

    async def __call__(self):
        handle_chosen = self.handle_a if random.random() < 0.5 else self.handle_b

        # The request is enqueued.
        submission_task: asyncio.Task = handle_chosen.remote()
        # The request is assigned to a replica.
        ref: ray.ObjectRef = await submission_task
        # The request has been processed by the replica.
        result = await ref

        return result


handle: RayServeSyncHandle = serve.run(
    DynamicDispatcher.bind(Model.bind(), Model.bind())
)
ref: ray.ObjectRef = handle.remote()
assert ray.get(ref) == "hello"

# __end_async_handle__

# __begin_async_handle_chain__
import asyncio
import ray
from ray import serve
from ray.serve.handle import RayServeDeploymentHandle, RayServeSyncHandle


@serve.deployment
class Model:
    def __call__(self, inp):
        return "hello " + inp


@serve.deployment
class Chain:
    def __init__(
        self, handle_a: RayServeDeploymentHandle, handle_b: RayServeDeploymentHandle
    ):
        self.handle_a = handle_a
        self.handle_b = handle_b

    async def __call__(self, inp):
        ref: asyncio.Task = await self.handle_b.remote(
            # Serve can handle enqueued-task as dependencies.
            self.handle_a.remote(inp)
        )
        return await ref


handle: RayServeSyncHandle = serve.run(Chain.bind(Model.bind(), Model.bind()))
ref: ray.ObjectRef = handle.remote("Serve")
assert ray.get(ref) == "hello hello Serve"

# __end_async_handle_chain__


# __begin_handle_method__
import ray
from ray import serve
from ray.serve.handle import RayServeSyncHandle


@serve.deployment
class Deployment:
    def method1(self, arg: str) -> str:
        return f"Method1: {arg}"

    def __call__(self, arg: str) -> str:
        return f"__call__: {arg}"


handle: RayServeSyncHandle = serve.run(Deployment.bind())

ray.get(handle.remote("hi"))  # Defaults to calling the __call__ method.
ray.get(handle.method1.remote("hi"))  # Call a different method.

# __end_handle_method__
