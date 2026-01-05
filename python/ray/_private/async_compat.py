"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import asyncio
import inspect
from functools import lru_cache

from ray._private.ray_constants import env_bool

try:
    import uvloop
except ImportError:
    uvloop = None


def get_new_event_loop():
    """Construct a new event loop. Ray will use uvloop if it exists and is enabled"""
    if uvloop and env_bool("RAY_USE_UVLOOP", True):
        return uvloop.new_event_loop()
    else:
        return asyncio.new_event_loop()


def try_install_uvloop():
    """Installs uvloop as event-loop implementation for asyncio (if available and enabled)"""
    if uvloop and env_bool("RAY_USE_UVLOOP", True):
        uvloop.install()
    else:
        pass


def is_async_func(func) -> bool:
    """Return True if the function is an async or async generator method."""
    return inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)


@lru_cache(maxsize=2**10)
def has_async_methods(cls: object) -> bool:
    """Return True if the class has any async methods."""
    return len(inspect.getmembers(cls, predicate=is_async_func)) > 0


@lru_cache(maxsize=2**10)
def sync_to_async(func):
    """Wrap a blocking function in an async function"""

    if is_async_func(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
