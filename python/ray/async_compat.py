"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import asyncio
from collections import namedtuple
import time
import inspect

try:
    import uvloop
except ImportError:
    uvloop = None

import ray


def get_new_event_loop():
    """Construct a new event loop. Ray will use uvloop if it exists"""
    if uvloop:
        return uvloop.new_event_loop()
    else:
        return asyncio.new_event_loop()


def sync_to_async(func):
    """Convert a blocking function to async function"""

    if inspect.iscoroutinefunction(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# Class encapsulate the get result from direct actor.
# Case 1: fallback_to_plasma=False, result=<Object>
# Case 2: fallback_to_plasma=True, result=None
AsyncGetResponse = namedtuple("AsyncGetResponse",
                              ["fallback_to_plasma", "result"])


def get_async(object_id):
    """Asyncio compatible version of ray.get"""
    # Delayed import because raylet import this file and
    # it creates circular imports.
    from ray.experimental.async_api import init as async_api_init, as_future
    from ray.experimental.async_plasma import PlasmaObjectFuture

    assert isinstance(object_id, ray.ObjectID), "Batched get is not supported."

    # Setup
    async_api_init()
    loop = asyncio.get_event_loop()
    core_worker = ray.worker.global_worker.core_worker

    # Here's the callback used to implement async get logic.
    # What we want:
    # - If direct call, first try to get it from in memory store.
    #   If the object if promoted to plasma, retry it from plasma API.
    # - If not direct call, directly use plasma API to get it.
    user_future = loop.create_future()

    # We have three future objects here.
    # user_future is directly returned to the user from this function.
    #     and it will be eventually fulfilled by the final result.
    # inner_future is the first attempt to retrieve the object. It can be
    #     fulfilled by core_worker.get_async. When inner_future completes,
    #     in_memory_done_callback will be invoked. This callback sets the final
    #     result in user_future if the object hasn't been promoted by plasma,
    #     otherwise it will retry from plasma.
    # retry_plasma_future is only created when we are getting objects stored
    #     in plasma. The callback for this future sets the final result in
    #     user_future.
    def in_memory_done_callback(future):
        # Result from `Get()` on the in-memory store.
        result = future.result()
        assert isinstance(result, AsyncGetResponse), result
        if result.fallback_to_plasma:

            def plasma_done_callback(future):
                # Result from `Get()` on the plasma store.
                result = future.result()
                assert isinstance(future, PlasmaObjectFuture)
                if isinstance(result, ray.exceptions.RayTaskError):
                    ray.worker.last_task_error_raise_time = time.time()
                    user_future.set_exception(result.as_instanceof_cause())
                else:
                    user_future.set_result(result)
                del user_future.retry_plasma_future
                # We have the value now, so we don't need the object ID
                # anymore.
                del user_future.object_id

            # Schedule async get to plasma to async get.
            retry_plasma_future = as_future(user_future.object_id)
            retry_plasma_future.add_done_callback(plasma_done_callback)
            # A hack to keep reference to the future so it doesn't get GCed.
            user_future.retry_plasma_future = retry_plasma_future
        else:
            # If this future has result set already, we just need to
            # skip the set result/exception procedure.
            if user_future.done():
                return

            if isinstance(result.result, ray.exceptions.RayTaskError):
                ray.worker.last_task_error_raise_time = time.time()
                user_future.set_exception(result.result.as_instanceof_cause())
            else:
                user_future.set_result(result.result)
            # We have the value now, so we don't need the object ID anymore.
            del user_future.object_id
        # We got the result from the in-memory store, so this future has been
        # fulfilled.
        del user_future.inner_future

    # A hack to keep a reference to the object ID for ref counting.
    # We store this before adding the callbacks to make sure that we have
    # access to the object ID if we need to fetch the corresponding value from
    # the plasma store.
    user_future.object_id = object_id

    inner_future = loop.create_future()
    # We must add the done_callback before sending to in_memory_store_get
    inner_future.add_done_callback(in_memory_done_callback)
    core_worker.in_memory_store_get_async(object_id, inner_future)
    # A hack to keep reference to inner_future so it doesn't get GC.
    user_future.inner_future = inner_future

    return user_future
