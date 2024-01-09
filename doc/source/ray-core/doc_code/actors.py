# flake8: noqa

# __cancel_start__
import ray
import asyncio
import time


@ray.remote
class Actor:
    async def f(self):
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print("Actor task canceled.")


actor = Actor.remote()
ref = actor.f.remote()

# Wait until task is scheduled.
time.sleep(1)
ray.cancel(ref)

try:
    ray.get(ref)
except ray.exceptions.RayTaskError:
    print("Object reference was cancelled.")
# __cancel_end__
