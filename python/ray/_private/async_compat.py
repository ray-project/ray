"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import asyncio
from asyncio.events import AbstractEventLoop
import inspect
from typing import Callable, Optional

from ray._private.utils import get_or_create_event_loop

try:
    import uvloop
except ImportError:
    uvloop = None

event_loop: Optional[AbstractEventLoop] = None


def get_new_event_loop():
    """Construct a new event loop, using uvloop if possible."""
    if uvloop:
        return uvloop.new_event_loop()
    else:
        return asyncio.new_event_loop()


def is_async_func(func):
    """Return True if the function is an async or async generator method."""
    return inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)


def sync_to_async(func: Callable):
    """Convert a blocking function to async function"""

    if is_async_func(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def wrap_sync_func_with_run_in_executor(func: Callable):
    global event_loop
    if event_loop is None:
        event_loop = get_or_create_event_loop()

    if is_async_func(func):
        return func

    async def run_in_executor_wrapper(*args, **kwargs):
        return await event_loop.run_in_executor(None, lambda: func(*args, **kwargs))

    return run_in_executor_wrapper
