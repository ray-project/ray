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


try:
    # This function has been added in Python 3.7. Prior to Python 3.7,
    # the low-level asyncio.ensure_future() function can be used instead.
    from asyncio import create_task  # noqa: F401
except ImportError:
    from asyncio import ensure_future as create_task  # noqa: F401


try:
    from asyncio import get_running_loop  # noqa: F401
except ImportError:
    from asyncio import _get_running_loop as get_running_loop  # noqa: F401


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


try:
    from contextlib import asynccontextmanager
except ImportError:
    # Copy from https://github.com/python-trio/async_generator
    # for compatible with Python 3.6
    import sys
    from functools import wraps
    from inspect import isasyncgenfunction

    class _aclosing:
        def __init__(self, aiter):
            self._aiter = aiter

        async def __aenter__(self):
            return self._aiter

        async def __aexit__(self, *args):
            await self._aiter.aclose()

    # Very much derived from the one in contextlib, by copy/pasting and then
    # asyncifying everything. (Also I dropped the obscure support for using
    # context managers as function decorators. It could be re-added; I just
    # couldn't be bothered.)
    # So this is a derivative work licensed under the PSF License, which requires
    # the following notice:
    #
    # Copyright © 2001-2017 Python Software Foundation; All Rights Reserved
    class _AsyncGeneratorContextManager:
        def __init__(self, func, args, kwds):
            self._func_name = func.__name__
            self._agen = func(*args, **kwds).__aiter__()

        async def __aenter__(self):
            if sys.version_info < (3, 5, 2):
                self._agen = await self._agen
            try:
                return await self._agen.asend(None)
            except StopAsyncIteration:
                raise RuntimeError("async generator didn't yield") from None

        async def __aexit__(self, type, value, traceback):
            async with _aclosing(self._agen):
                if type is None:
                    try:
                        await self._agen.asend(None)
                    except StopAsyncIteration:
                        return False
                    else:
                        raise RuntimeError("async generator didn't stop")
                else:
                    # It used to be possible to have type != None, value == None:
                    #    https://bugs.python.org/issue1705170
                    # but AFAICT this can't happen anymore.
                    assert value is not None
                    try:
                        await self._agen.athrow(type, value, traceback)
                        raise RuntimeError("async generator didn't stop after athrow()")
                    except StopAsyncIteration as exc:
                        # Suppress StopIteration *unless* it's the same exception
                        # that was passed to throw(). This prevents a
                        # StopIteration raised inside the "with" statement from
                        # being suppressed.
                        return exc is not value
                    except RuntimeError as exc:
                        # Don't re-raise the passed in exception. (issue27112)
                        if exc is value:
                            return False
                        # Likewise, avoid suppressing if a StopIteration exception
                        # was passed to throw() and later wrapped into a
                        # RuntimeError (see PEP 479).
                        if (
                            isinstance(value, (StopIteration, StopAsyncIteration))
                            and exc.__cause__ is value
                        ):
                            return False
                        raise
                    except:  # noqa: E722
                        # only re-raise if it's *not* the exception that was
                        # passed to throw(), because __exit__() must not raise an
                        # exception unless __exit__() itself failed. But throw()
                        # has to raise the exception to signal propagation, so
                        # this fixes the impedance mismatch between the throw()
                        # protocol and the __exit__() protocol.
                        #
                        if sys.exc_info()[1] is value:
                            return False
                        raise

        def __enter__(self):
            raise RuntimeError(
                "use 'async with {func_name}(...)', not 'with {func_name}(...)'".format(
                    func_name=self._func_name
                )
            )

        def __exit__(self):  # pragma: no cover
            assert False, """Never called, but should be defined"""

    def asynccontextmanager(func):
        """Like @contextmanager, but async."""
        if not isasyncgenfunction(func):
            raise TypeError(
                "must be an async generator (native or from async_generator; "
                "if using @async_generator then @acontextmanager must be on top."
            )

        @wraps(func)
        def helper(*args, **kwds):
            return _AsyncGeneratorContextManager(func, args, kwds)

        # A hint for sphinxcontrib-trio:
        helper.__returns_acontextmanager__ = True
        return helper
