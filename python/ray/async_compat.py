"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import ray
from ray.experimental.async_api import init, as_future
import asyncio


def sync_to_async(func):
    """Convert a blocking function to async function"""

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


async def get_async(object_id: ray.ObjectID):
    """Asyncio compatible version of ray.get"""
    init()

    if object_id.is_direct_call_type():
        core_worker = ray.worker.global_worker.core_worker
        future = core_worker.get_async(object_id)
        result = await future
    else:
        result = await as_future(object_id)

    return result

