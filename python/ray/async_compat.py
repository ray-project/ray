"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import asyncio
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


def get_async(object_ref):
    """C++ Asyncio version of ray.get"""
    loop = asyncio.get_event_loop()
    core_worker = ray.worker.global_worker.core_worker

    future = loop.create_future()
    core_worker.get_async(object_ref, future)
    # A hack to keep a reference to the object ref for ref counting.
    future.object_ref = object_ref
    return future
