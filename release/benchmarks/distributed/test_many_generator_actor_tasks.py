import asyncio
import time

import ray
from ray.util.state import list_objects
from ray._private.test_utils import wait_for_condition

ray.init()


@ray.remote
class ProxyActor:
    async def get_data(self, child):
        gen = child.get_data.options(num_returns="streaming").remote()
        async for ref in gen:
            yield ref
            del ref


@ray.remote
class Actor:
    async def get_data(self):
        for i in range(1000):
            yield "word" * (i + 1)


actors = []

cpus = ray.cluster_resources()["CPU"]
for _ in range(int(cpus)):
    actor = Actor.remote()
    actors.append(actor)

proxy_actor = ProxyActor.remote()


async def get_stream(proxy_actor, actor):
    i = 0
    s = time.time()
    async for ref in proxy_actor.get_data.options(num_returns="streaming").remote(
        actor
    ):
        ref = await ref
        value = await ref
        assert "word" * (i + 1), value
        i += 1
    print(
        f"Took {time.time() - s} seconds to run and get "
        "the result from a single generator task."
    )


async def main():
    for i in range(1):
        s = time.time()
        await asyncio.gather(*[get_stream(proxy_actor, actor) for actor in actors])
        print(f"Took {time.time() - s} seconds to run a iteration {i}")
    result = list_objects(raise_on_missing_output=False)
    ref_types = set()
    for r in result:
        ref_types.add(r.reference_type)
    # Verify no leaks
    assert ref_types == {"ACTOR_HANDLE"}


asyncio.run(main())


def verify():
    result = list_objects(raise_on_missing_output=False)
    print(result)
    ref_types = set()
    for r in result:
        ref_types.add(r.reference_type)
    # Verify no leaks
    print(ref_types)
    return ref_types == {"ACTOR_HANDLE"}


wait_for_condition(verify)
