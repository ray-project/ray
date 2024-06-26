"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""
import asyncio
import inspect
from functools import lru_cache

try:
    import uvloop
except ImportError:
    uvloop = None


def get_new_event_loop():
    """Construct a new event loop. Ray will use uvloop if it exists"""
    if uvloop:
        return uvloop.new_event_loop()
    else:
        return asyncio.new_event_loop()


def try_install_uvloop():
    """Installs uvloop as event-loop implementation for asyncio (if available)"""
    if uvloop:
        uvloop.install()
    else:
        pass


@lru_cache(maxsize=2**10)
def is_async_func(func) -> bool:
    """Return True if the function is an async or async generator method."""
    return inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)


@lru_cache(maxsize=2**10)
def has_async_methods(cls: object) -> bool:
    """Return True if the class has any async methods."""
    return bool(  # a non-empty result becomes True
        inspect.getmembers(
            # We need to check whether the member is a method first,
            # because the @lru_cache on is_async_func can't handle
            # non-hashable types, and any given class might have some
            # non-hashable members.
            cls,
            predicate=lambda m: inspect.ismethod(m) and is_async_func(m),
        )
    )


@lru_cache(maxsize=2**10)
def sync_to_async(func) -> bool:
    """Wrap a blocking function in an async function"""

    if is_async_func(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
