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


# __enable_task_events_start__
@ray.remote
class FooActor:

    # Disable task events reporting for this method.
    @ray.method(enable_task_events=False)
    def foo(self):
        pass


foo_actor = FooActor.remote()
ray.get(foo_actor.foo.remote())


# __enable_task_events_end__
