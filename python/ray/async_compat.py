"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import ray
import asyncio
from collections import namedtuple


def sync_to_async(func):
    """Convert a blocking function to async function"""

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# Class encapsualte the get result from direct actor.
# Case 1: success=True, plasma_id=None, result=<Object>
# Case 2: success=False, plasma_id=ObjectID, result=None
AsyncGetResponse = namedtuple("AsyncGetResponse", ["success", "plasma_id", "result"])
gc_god = []

def get_async(object_id):
    """Asyncio compatible version of ray.get"""
    # Delayed import because raylet import this file and it creates circular imports.
    from ray.experimental.async_api import init as async_api_init, as_future
    from ray.experimental.async_plasma import PlasmaObjectFuture

    async_api_init() # Noop when called twice.
    loop = asyncio.get_event_loop()
    core_worker = ray.worker.global_worker.core_worker
    user_future = loop.create_future()

    def done_callback(future):
        result = future.result()
        # Result from async plasma, transparently pass it to user future
        if isinstance(future, PlasmaObjectFuture):
            user_future.set_result(result)
        else: # Result from direct call.
            assert isinstance(result, AsyncGetResponse)
            if result.success:
                user_future.set_result(result.result)
            else:
                # Schedule plasma to async get, use the the same callback.
                plasma_future = as_future(result.plasma_id)
                plasma_future.add_done_callback(done_callback)
                gc_god.append(plasma_future)

    if object_id.is_direct_call_type():
        inner_future = loop.create_future()
        core_worker.get_async(object_id, inner_future)
    else:
        inner_future = as_future(object_id)

    inner_future.add_done_callback(done_callback)
    gc_god.append(inner_future)
    return user_future
