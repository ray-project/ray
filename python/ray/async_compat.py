"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import ray
import asyncio


def sync_to_async(func):
    """Convert a blocking function to async function"""

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def get_async(object_id):
    """Asyncio compatible version of ray.get"""
    from ray.experimental.async_api import init, as_future
    init()
    loop = asyncio.get_event_loop()
    core_worker = ray.worker.global_worker.core_worker

    if object_id.is_direct_call_type():
        future = loop.create_future()
        core_worker.get_async(object_id, future)
        return future
    else:
        return as_future(object_id)
