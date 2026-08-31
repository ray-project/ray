import asyncio
import time

import ray
from ray import serve
from ray.exceptions import RayActorError
from ray.serve.config import DeploymentActorConfig


# __begin_define_deployment_scoped_actor__
@ray.remote
class SharedCounter:
    def __init__(self, start: int = 0):
        self._value = start

    def increment(self) -> int:
        self._value += 1
        return self._value


@serve.deployment(
    deployment_actors=[
        DeploymentActorConfig(
            name="counter",
            actor_class=SharedCounter,
            init_kwargs={"start": 10},
            actor_options={"num_cpus": 0},
        ),
    ],
)
class SharedCounterDeployment:
    async def __call__(self) -> int:
        counter = serve.get_deployment_actor("counter")
        return await counter.increment.remote()


# __end_define_deployment_scoped_actor__


# __begin_cached_handle_refresh__
@serve.deployment(
    deployment_actors=[
        DeploymentActorConfig(
            name="counter",
            actor_class=SharedCounter,
            init_kwargs={"start": 0},
            actor_options={"num_cpus": 0},
        ),
    ],
)
class CachedHandleDeployment:
    def __init__(self):
        self._counter = serve.get_deployment_actor("counter")

    async def _refresh_counter(self) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                self._counter = serve.get_deployment_actor("counter")
                return
            except ValueError:
                # The replacement actor might not be registered yet.
                await asyncio.sleep(0.05)

        raise TimeoutError("Timed out waiting for the deployment-scoped actor.")

    async def __call__(self) -> int:
        try:
            return await self._counter.increment.remote()
        except RayActorError:
            await self._refresh_counter()
            return await self._counter.increment.remote()


# __end_cached_handle_refresh__


if __name__ == "__main__":
    ray.init()
    try:
        # __begin_run_deployment_scoped_actor_example__
        handle = serve.run(SharedCounterDeployment.bind())
        print(handle.remote().result())
        print(handle.remote().result())
        # __end_run_deployment_scoped_actor_example__
    finally:
        serve.shutdown()
        ray.shutdown()
