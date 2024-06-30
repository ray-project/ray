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


def is_async_func(func):
    """
    Returns True if the passed function is a coroutine or an async generator.

    note: we prefer this version as Python Coroutines != Cython Coroutines
    and Cython Coroutines are not detected properly by the inspect module

    this is the optimized version from the links below, using the flag as defined by
    the inspect module
    https://github.com/cython/cython/blob/master/tests/run/test_coroutines_pep492.pyx#L889
    https://github.com/hugapi/hug/blob/develop/hug/introspect.py#L33

    The issues above outline the PR where this was added to ensure async flag support
    on Cython
    https://github.com/cython/cython/pull/4902
    """
    is_async_cython = func.__code__.co_flags & 0x80 or getattr(
        func, "_is_coroutine", False
    )

    return (
        is_async_cython
        or inspect.iscoroutinefunction(func)
        or inspect.isasyncgenfunction(func)
    )


def sync_to_async(func):
    """Convert a blocking function to async function"""

    if is_async_func(func):
        return func

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
