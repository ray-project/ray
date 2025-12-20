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


# __cancel_graceful_actor_start__
import ray
import time


@ray.remote
class SyncActor:
    def __init__(self):
        self.is_canceled = False

    def long_running_method(self):
        """A sync actor method that checks for cancellation periodically."""
        for i in range(100):
            # For sync actor tasks, is_canceled() can be checked in the task body
            if ray.get_runtime_context().is_canceled():
                self.is_canceled = True
                print("Actor task canceled, cleaning up...")
                return "canceled"
            time.sleep(0.1)
        return "completed"

    def get_cancel_status(self):
        return self.is_canceled


# Sync actor task cancellation with periodic checking
actor = SyncActor.remote()
actor_task_ref = actor.long_running_method.remote()

# Wait until task is scheduled.
time.sleep(1)
ray.cancel(actor_task_ref)

# The TaskCancelledError will be raised when calling ray.get
try:
    result = ray.get(actor_task_ref)
except ray.exceptions.TaskCancelledError:
    print("Actor task was cancelled")

# The get_cancel_status will return True after cancellation
cancel_status = ray.get(actor.get_cancel_status.remote())
print(f"Actor detected cancellation: {cancel_status}")
# __cancel_graceful_actor_end__


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
