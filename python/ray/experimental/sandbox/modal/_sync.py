"""Turn an async-native implementation class into a blocking one with an ``.aio`` twin.

Every class in this package is written once, async-native, with a leading
underscore (``_Sandbox``). At the bottom of its module it is passed through
:func:`synchronize_api`, which produces the public blocking class
(``Sandbox``). On the blocking class, every method is callable two ways::

    sandbox.exec("echo", "hi")          # blocks, returns the result
    await sandbox.exec.aio("echo", "hi")  # awaits in the caller's own loop

This is a small stand-in for Modal's ``synchronicity`` dependency, covering
only what this package needs: coroutine methods, async-generator methods,
static factories, context managers, and properties that return other wrapped
objects. Where it covers the same ground it behaves the same way: a Ctrl-C
during a blocking call cancels the call, a forked child gets a fresh loop,
each implementation object has one wrapper, and a blocking call made inside a
running event loop warns with ``AsyncUsageWarning``.

The blocking path runs coroutines on a single daemon event loop owned by this
module. The ``.aio`` path never touches that loop -- it hands the coroutine
back for the caller's own loop to drive. That is the one deliberate difference
from synchronicity, which runs ``.aio`` calls on its own loop too and relays
the results. Both paths translate wrapper objects to implementations on the
way in and implementations to wrappers on the way out.

Because ``.aio`` runs on the caller's loop, an object's loop-bound state -- a
stream's read in flight, the task that prints a ``StreamType.STDOUT`` process's
output -- belongs to whichever loop first drove it. So:

* do not mix the two surfaces on one object's streams; and
* do not carry an object used through ``.aio`` into a second
  ``asyncio.run()``.
"""

import asyncio
import collections
import concurrent.futures
import functools
import inspect
import os
import sys
import threading
import warnings
import weakref
from typing import Any, Callable, Coroutine, Dict, Optional, Type

import ray
from ray.experimental.sandbox.modal.exception import AsyncUsageWarning, InvalidError

# Implementation class -> blocking class, populated by synchronize_api().
_IMPL_TO_BLOCKING: Dict[type, type] = {}


_NEEDS_RAY_ATTR = "_sandbox_needs_ray"


class _RayConnectionRequired(Exception):
    """Internal signal: this call needs Ray, and only the caller can connect.

    Never escapes :meth:`_BoundMethod.__call__`, which catches it, connects, and
    runs the call again.
    """


def needs_ray(fn: Callable) -> Callable:
    """Mark a method whose *blocking* form may have to connect to Ray.

    Blocking calls run their coroutine on the daemon loop, so a method that
    creates an actor would otherwise have the first such call auto-initialize
    Ray from that daemon thread. ``ray.init`` installs the driver's SIGTERM
    handler only on the main thread, and otherwise logs a warning and skips it
    -- so a detail of this module would decide whether the host program handles
    SIGTERM.

    Connecting up front instead would be simpler and wrong: argument validation
    happens inside the coroutine, so every refused ``Sandbox.create(...)`` would
    start a cluster before saying no. The marked method instead calls
    :func:`require_ray_connection` at the point it actually needs Ray, and the
    call is replayed once with a connection in hand. Everything before that
    point is validation, so replaying it costs nothing and changes nothing.
    """
    setattr(fn, _NEEDS_RAY_ATTR, True)
    return fn


def require_ray_connection() -> None:
    """Demand a Ray connection, from wherever this call can get one.

    A no-op unless this is the daemon loop -- on any other thread the caller
    chose where their code runs, so letting Ray auto-initialize there is
    right. On the daemon loop it is not, so the work is handed back.
    """
    if ray.is_initialized():
        return
    if threading.current_thread() is _loop_thread.thread:
        raise _RayConnectionRequired()


# How often a blocking caller checks that the loop thread is still alive.
_LIVENESS_POLL_SECONDS = 1.0


class _EventLoopThread:
    """A lazily-started daemon thread running one asyncio event loop."""

    def __init__(self):
        self._reset()

    def _reset(self) -> None:
        """Forget the loop and its thread, as a forked child must.

        Only the forking thread survives a fork, so in the child the loop
        belongs to a thread that does not exist, and every call handed to it
        would wait forever. The lock is replaced too: had another thread held
        it at the moment of the fork, it would stay held in the child for
        good.
        """
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._died_of: Optional[BaseException] = None
        self._lock = threading.Lock()

    @property
    def thread(self) -> Optional[threading.Thread]:
        """The loop's thread, or None while the loop has never been needed."""
        return self._thread

    def get_loop(self) -> asyncio.AbstractEventLoop:
        # Double-checked so the common case costs no lock. _thread is always
        # set before _loop, so a loop seen here has its thread.
        loop, thread = self._loop, self._thread
        if loop is not None and thread.is_alive():
            return loop
        with self._lock:
            if self._loop is not None:
                if self._thread.is_alive():
                    return self._loop
                raise self._dead()
            loop = asyncio.new_event_loop()
            ready = threading.Event()

            def _run():
                asyncio.set_event_loop(loop)
                ready.set()
                try:
                    loop.run_forever()
                except BaseException as exc:
                    # A SystemExit or KeyboardInterrupt raised inside a task
                    # propagates out of run_forever and ends this thread.
                    self._died_of = exc
                    raise

            thread = threading.Thread(
                target=_run, name="ray-sandbox-modal-sync", daemon=True
            )
            thread.start()
            ready.wait()
            self._thread = thread
            self._loop = loop
            return loop

    def _dead(self) -> RuntimeError:
        # Not restarted: every stream, look-ahead and task the dead loop held
        # went with it, so a fresh loop would serve new calls while objects
        # created before it hung. Failing is what synchronicity does too.
        error = RuntimeError("The Sandbox event loop thread died unexpectedly.")
        error.__cause__ = self._died_of
        return error

    def run(self, coro: Coroutine) -> Any:
        """Run ``coro`` to completion on the background loop and return its result.

        A ``KeyboardInterrupt`` while waiting cancels the coroutine, waits for
        it to finish unwinding, and then re-raises -- as synchronicity does.
        Returning at once instead would leave the interrupted work, an exec
        or a Sandbox being created, carrying on after the caller gave up on
        it. A second interrupt stops the wait.

        Args:
            coro: The coroutine to run.

        Returns:
            The coroutine's result.
        """
        if not asyncio.iscoroutine(coro):
            raise TypeError(f"A coroutine object is required, not {coro!r}")
        try:
            loop = self.get_loop()
        except BaseException:
            coro.close()
            raise
        thread = self._thread
        if threading.current_thread() is thread:
            # Blocking on our own loop would deadlock it.
            coro.close()
            raise InvalidError(
                "Blocking Sandbox APIs cannot be called from inside the "
                "Sandbox event loop. Use the .aio variant instead."
            )

        done = concurrent.futures.Future()
        started = []

        def _start():
            task = loop.create_task(coro)
            started.append(task)
            task.add_done_callback(functools.partial(_settle, done))

        def _cancel():
            # Callbacks run in the order they were scheduled, so if _start
            # was scheduled at all, it has made the task by now.
            if started:
                started[0].cancel()
            else:
                coro.close()
                _cancel_future(done)

        try:
            loop.call_soon_threadsafe(_start)
            return self._wait(done, thread)
        except KeyboardInterrupt:
            loop.call_soon_threadsafe(_cancel)
            try:
                self._wait(done, thread)
            except BaseException:
                # Normally the cancellation itself, or a second interrupt.
                # Either way, the first interrupt is what the caller sees.
                pass
            raise

    def _wait(self, done: concurrent.futures.Future, thread: threading.Thread) -> Any:
        # Polled rather than waited on outright: a loop thread that dies
        # mid-call never delivers the result, and the call would hang.
        while True:
            try:
                return done.result(timeout=_LIVENESS_POLL_SECONDS)
            except concurrent.futures.TimeoutError:
                # Since 3.11 this is the builtin TimeoutError, so it is also
                # what the call raises when the call itself timed out -- and
                # then the future is done.
                if done.done():
                    break
                if not thread.is_alive():
                    raise self._dead()
        return done.result()


def _settle(done: concurrent.futures.Future, task: asyncio.Task) -> None:
    """Copy a finished task's outcome to the future its caller waits on."""
    if task.cancelled():
        _cancel_future(done)
    elif task.exception() is not None:
        done.set_exception(task.exception())
    else:
        done.set_result(task.result())


def _cancel_future(future: concurrent.futures.Future) -> None:
    future.cancel()
    # cancel() alone leaves the future CANCELLED, which concurrent.futures.wait
    # does not count as done; this moves it on to CANCELLED_AND_NOTIFIED, as
    # asyncio's own future chaining does.
    future.set_running_or_notify_cancel()


_loop_thread = _EventLoopThread()


# This module's own directory. Frames from here and from the rest of the
# package are stepped over, so the warning points at the caller's line.
_PACKAGE_DIR = os.path.dirname(__file__)


def _async_warnings_enabled() -> bool:
    # Modal's setting, parsed the way modal/config.py's _to_boolean does.
    return os.environ.get("MODAL_ASYNC_WARNINGS", "1").lower() not in {
        "",
        "0",
        "false",
    }


def _in_ipython() -> bool:
    # Notebooks run cells inside an event loop, and blocking calls are how
    # people use them there -- Modal stays quiet in IPython for that reason.
    ipython = sys.modules.get("IPython")
    if ipython is None:
        return False
    try:
        return ipython.get_ipython() is not None
    except Exception:
        return False


def _warn_if_blocking_in_async(what: str, instead: str) -> None:
    """Warn, as Modal does, before a blocking call stalls a running event loop.

    Args:
        what: The blocking operation, for the message.
        instead: Its async equivalent, for the message.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    if threading.current_thread() is _loop_thread.thread:
        # Refused outright by _EventLoopThread.run; a warning first is noise.
        return
    if not _async_warnings_enabled() or _in_ipython():
        return
    frame, stacklevel = sys._getframe(1), 2
    while frame is not None and os.path.dirname(frame.f_code.co_filename) == (
        _PACKAGE_DIR
    ):
        frame, stacklevel = frame.f_back, stacklevel + 1
    warnings.warn(
        f"{what} blocks, and is being called from inside a running event "
        f"loop, which stalls until it returns. Use {instead} instead. Set "
        f"MODAL_ASYNC_WARNINGS=0 to silence this warning.",
        AsyncUsageWarning,
        stacklevel=stacklevel,
    )


# Containers are recursed into only when they are exactly these types, as in
# synchronicity: a subclass such as a namedtuple cannot be rebuilt from an
# iterable, and is passed through as it is.


def _translate_in(value: Any) -> Any:
    """Replace blocking wrappers with their implementations."""
    if isinstance(value, _BlockingBase):
        return value._impl
    kind = type(value)
    if kind is list or kind is tuple:
        return kind(_translate_in(item) for item in value)
    if kind is dict:
        return {key: _translate_in(item) for key, item in value.items()}
    return value


def _translate_out(value: Any) -> Any:
    """Replace implementations with their blocking wrappers."""
    blocking_cls = _IMPL_TO_BLOCKING.get(type(value))
    if blocking_cls is not None:
        return blocking_cls._from_impl(value)
    kind = type(value)
    if kind is list or kind is tuple:
        return kind(_translate_out(item) for item in value)
    if kind is dict:
        return {key: _translate_out(item) for key, item in value.items()}
    return value


def _translate_args(args, kwargs):
    return (
        tuple(_translate_in(a) for a in args),
        {k: _translate_in(v) for k, v in kwargs.items()},
    )


# How many items one crossing to the loop thread may collect. Only reached
# when the generator has that many ready without waiting -- a line-buffered
# stream does, because a single chunk splits into hundreds of lines with no
# I/O between them. A stream whose next item needs I/O batches one at a time,
# so this never delays output that has not arrived.
_ITER_BATCH_LIMIT = 256

# The same bound for an iterator with a ``take_ready(limit)`` method, which
# returns the items it already holds without waiting (see
# ``io_streams._Flatten``). Those cost nothing to collect, so a crossing takes
# far more of them: a 1 MiB chunk of short lines is thousands.
_READY_BATCH_LIMIT = 4096


class _Lookahead:
    """Items taken off a generator ahead of the caller, and the fetch in flight.

    Kept per generator rather than per loop: a generator that outlives one
    ``for`` loop -- a StreamReader's is memoized, so ``for ... break`` followed
    by another ``for`` reaches the same one -- must hand the next loop what the
    last one had already pulled, not drop it, and must never be asked for a
    second item while an earlier fetch is still running inside it.
    """

    __slots__ = ("buffer", "pending", "exhausted", "__weakref__")

    def __init__(self):
        self.buffer = collections.deque()
        # An __anext__ that was started but had not finished within a loop
        # turn. Kept rather than cancelled: cancelling one in flight can lose
        # an item the generator has already taken off its source, or leave the
        # generator unusable. It is awaited first on the next fill.
        self.pending = None
        self.exhausted = False


# Generator -> its look-ahead. Weak, so a generator nobody holds is not kept
# alive by having once been iterated.
_LOOKAHEAD: "weakref.WeakKeyDictionary[Any, _Lookahead]" = weakref.WeakKeyDictionary()


def _lookahead_for(agen) -> _Lookahead:
    try:
        state = _LOOKAHEAD.get(agen)
        if state is None:
            state = _LOOKAHEAD[agen] = _Lookahead()
        return state
    except TypeError:
        # Not weak-referenceable; such a generator gets a private look-ahead.
        return _Lookahead()


class _BlockingIterator:
    """Drives an async generator from synchronous code.

    Items are collected in batches rather than one per call. Each crossing to
    the loop thread costs a coroutine schedule, a thread wake and a park on a
    condition variable -- around 200us, measured -- which for a line-buffered
    stream is several hundred times the cost of producing the line itself.
    Draining what is already available amortizes that over a whole chunk.
    """

    def __init__(self, agen):
        self._agen = agen
        self._state = _lookahead_for(agen)

    def __iter__(self):
        return self

    def __next__(self):
        state = self._state
        if not state.buffer and not state.exhausted:
            _loop_thread.run(self._fill())
        if not state.buffer:
            raise StopIteration
        return _translate_out(state.buffer.popleft())

    async def _fill(self):
        """Take one item, then everything else already available.

        Items go straight into the look-ahead rather than back as a result,
        so a Ctrl-C that lands after the fill, before the caller has them,
        leaves them there for the next loop instead of dropping them.
        """
        state = self._state
        # An iterator that can say what it already holds hands it all over in
        # one call; probing item by item below costs a task and a loop turn
        # each, so only what it does not hold yet is probed for.
        take_ready = getattr(self._agen, "take_ready", None)
        limit = _ITER_BATCH_LIMIT if take_ready is None else _READY_BATCH_LIMIT
        task = state.pending or asyncio.ensure_future(self._agen.__anext__())
        state.pending = None
        try:
            # Shielded, so that cancelling this fill leaves the fetch running
            # rather than cancelling it too.
            state.buffer.append(await asyncio.shield(task))
            while True:
                if take_ready is not None:
                    state.buffer.extend(take_ready(limit - len(state.buffer)))
                if len(state.buffer) >= limit:
                    return
                task = asyncio.ensure_future(self._agen.__anext__())
                # One turn is all a ready item needs: a task runs until it
                # returns or hits a real suspension, so anything still pending
                # after this is waiting on I/O and is not worth holding the
                # caller for.
                await asyncio.sleep(0)
                if not task.done():
                    state.pending = task
                    return
                state.buffer.append(task.result())
        except StopAsyncIteration:
            state.exhausted = True
        except asyncio.CancelledError:
            # A Ctrl-C on the blocking caller. Keep the fetch in flight for
            # the next loop over this generator: cancelling a fetch inside a
            # generator closes the generator, which for a memoized stream
            # reader would end the stream for good.
            if not task.cancelled():
                state.pending = task
            raise

    def close(self):
        try:
            _loop_thread.run(self._aclose())
        except StopAsyncIteration:
            pass

    async def _aclose(self):
        state = self._state
        if state.pending is not None:
            # Safe to drop only now: nothing will read from this generator
            # again, so an item lost with it cannot be missed.
            state.pending.cancel()
            state.pending = None
        state.buffer.clear()
        state.exhausted = True
        await self._agen.aclose()


class _AsyncIterator:
    """Wraps an async generator so yielded values are translated."""

    def __init__(self, agen):
        self._agen = agen

    def __aiter__(self):
        return self

    async def __anext__(self):
        return _translate_out(await self._agen.__anext__())

    async def aclose(self):
        await self._agen.aclose()


class _BoundMethod:
    """A callable that blocks, and carries an ``.aio`` twin that does not."""

    __slots__ = ("_fn", "_impl", "_is_gen")

    def __init__(self, fn: Callable, impl: Any, is_gen: bool):
        self._fn = fn
        self._impl = impl
        self._is_gen = is_gen

    @property
    def __doc__(self):
        return self._fn.__doc__

    def __call__(self, *args, **kwargs):
        name = _public_name(self._fn)
        if self._is_gen:
            _warn_if_blocking_in_async(
                f"{name}()", f"`async for ... in {name}.aio(...)`"
            )
        else:
            _warn_if_blocking_in_async(f"{name}()", f"`await {name}.aio(...)`")
        args, kwargs = _translate_args(args, kwargs)
        if self._is_gen:
            return _BlockingIterator(self._call_raw(args, kwargs))
        try:
            return _translate_out(_loop_thread.run(self._call_raw(args, kwargs)))
        except _RayConnectionRequired:
            # The call got as far as needing Ray and stopped there so that the
            # connection could be made here, on the caller's thread rather than
            # the daemon loop. See needs_ray. A fresh coroutine, since the one
            # above is spent; at most one retry, because ray.is_initialized()
            # stays true for the rest of the process.
            ray.init()
        return _translate_out(_loop_thread.run(self._call_raw(args, kwargs)))

    def _call_raw(self, args, kwargs):
        if self._impl is _UNBOUND:
            return self._fn(*args, **kwargs)
        return self._fn(self._impl, *args, **kwargs)

    def aio(self, *args, **kwargs):
        """Async variant, driven by the caller's own event loop."""
        args, kwargs = _translate_args(args, kwargs)
        if self._is_gen:
            return _AsyncIterator(self._call_raw(args, kwargs))

        async def _await():
            return _translate_out(await self._call_raw(args, kwargs))

        return _await()


def _public_name(fn: Callable) -> str:
    """``_Sandbox.exec`` -> ``Sandbox.exec``, for messages."""
    return getattr(fn, "__qualname__", getattr(fn, "__name__", "method")).lstrip("_")


class _Unbound:
    def __repr__(self):
        return "<unbound>"


_UNBOUND = _Unbound()


class _MethodDescriptor:
    """Exposes an async implementation method as a :class:`_BoundMethod`."""

    def __init__(self, fn: Callable, is_gen: bool, is_static: bool):
        self._fn = fn
        self._is_gen = is_gen
        self._is_static = is_static
        self._unbound = _make_unbound(fn, is_gen)
        functools.update_wrapper(self, fn)

    def __get__(self, obj, objtype=None):
        if self._is_static:
            return _BoundMethod(self._fn, _UNBOUND, self._is_gen)
        if obj is None:
            # Class-level access. Returning a real function rather than the
            # descriptor keeps the method callable as `Cls.method(instance)`
            # and lets documentation tools read its signature and docstring.
            return self._unbound
        return _BoundMethod(self._fn, obj._impl, self._is_gen)


def _make_unbound(fn: Callable, is_gen: bool) -> Callable:
    """Build the plain function seen when a method is read off the class."""

    @functools.wraps(fn)
    def unbound(self, *args, **kwargs):
        return _BoundMethod(fn, self._impl, is_gen)(*args, **kwargs)

    def aio(self, *args, **kwargs):
        return _BoundMethod(fn, self._impl, is_gen).aio(*args, **kwargs)

    unbound.aio = aio
    return unbound


class _PropertyDescriptor:
    """Forwards a property to the implementation, translating the result."""

    def __init__(self, prop: property):
        self._prop = prop
        self.__doc__ = prop.__doc__

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        return _translate_out(self._prop.fget(obj._impl))

    def __set__(self, obj, value):
        if self._prop.fset is None:
            raise AttributeError("can't set attribute")
        self._prop.fset(obj._impl, _translate_in(value))


# id(implementation) -> its wrapper, while the wrapper lives, so that every
# route to one implementation object -- `sb.stdout` read twice, a method that
# returns `self` -- yields the same wrapper, as synchronicity's does. Weak and
# kept here, not on the implementation as synchronicity keeps it: that would
# make a cycle, and a Sandbox whose handles were all dropped would then wait
# for the cycle collector before its actor was released. While an entry
# exists its wrapper holds the implementation, so the id cannot be reused.
_WRAPPERS: "weakref.WeakValueDictionary[int, _BlockingBase]" = (
    weakref.WeakValueDictionary()
)
_WRAPPERS_LOCK = threading.Lock()


class _BlockingBase:
    """Common base for generated blocking classes."""

    _impl_class: Type = None

    def __init__(self, *args, **kwargs):
        raise InvalidError(
            f"{type(self).__name__} has no public constructor. "
            f"Use its class methods instead."
        )

    @classmethod
    def _from_impl(cls, impl):
        with _WRAPPERS_LOCK:
            obj = _WRAPPERS.get(id(impl))
            if obj is not None and type(obj) is cls and obj._impl is impl:
                return obj
            obj = cls.__new__(cls)
            obj._impl = impl
            _WRAPPERS[id(impl)] = obj
            return obj

    def __class_getitem__(cls, item):
        # Modal's public classes are Generic, so ported code subscripts them --
        # `typing.cast(StreamReader[str], ...)`, `ContainerProcess[bytes]` in an
        # annotation that something later evaluates. The element type carries no
        # meaning here, but refusing the subscript turns those into TypeError.
        return cls

    def __repr__(self):
        return repr(self._impl)


def _reinitialize_after_fork() -> None:
    """Give a forked child its own loop, as synchronicity does.

    Look-ahead state goes too: its fetches in flight are tasks on the
    parent's loop, which the child can never drive.
    """
    global _WRAPPERS_LOCK
    _loop_thread._reset()
    _LOOKAHEAD.clear()
    _WRAPPERS_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reinitialize_after_fork)


def synchronize_api(impl_class: type, name: Optional[str] = None) -> type:
    """Build the public blocking class for ``impl_class``.

    Args:
        impl_class: The async-native implementation class. By convention its
            name starts with an underscore.
        name: Name for the generated class. Defaults to ``impl_class``'s name
            with the leading underscore stripped.

    Returns:
        A new class whose methods block, each also exposing an ``.aio``
        variant for use inside an event loop.
    """
    blocking_name = name or impl_class.__name__.lstrip("_")
    namespace: Dict[str, Any] = {
        "__doc__": impl_class.__doc__,
        "__module__": impl_class.__module__,
        "_impl_class": impl_class,
    }

    # Inherited definitions count, with the most derived one winning, as when
    # synchronicity wraps a class's bases. Bases from the standard library --
    # typing.Generic, abc.ABC -- are plumbing, not API.
    members: Dict[str, Any] = {}
    for klass in reversed(impl_class.__mro__):
        if klass.__module__ not in ("builtins", "typing", "abc"):
            members.update(vars(klass))

    # An iterable implementation stays iterable on both surfaces: `for x in obj`
    # from blocking code, `async for x in obj` from an event loop.
    aiter_fn = members.get("__aiter__")
    if aiter_fn is not None:
        namespace["__iter__"] = _make_blocking_iter(aiter_fn, blocking_name)
        namespace["__aiter__"] = _make_async_iter(aiter_fn)

    # And an implementation that is its own iterator stays one. The loop below
    # skips every dunder, so without this `next(reader)` raises TypeError even
    # though the implementation defines __anext__ -- which Modal keeps on its
    # StreamReader explicitly for backwards compatibility.
    anext_fn = members.get("__anext__")
    if anext_fn is not None:
        namespace["__next__"] = _make_blocking_next(anext_fn)
        namespace["__anext__"] = _make_async_forwarder(anext_fn)

    # An async context manager is also a blocking one, as synchronicity maps
    # __aenter__/__aexit__ onto __enter__/__exit__.
    aenter_fn = members.get("__aenter__")
    aexit_fn = members.get("__aexit__")
    if aenter_fn is not None and aexit_fn is not None:
        namespace["__enter__"] = _make_blocking_enter(aenter_fn, blocking_name)
        namespace["__exit__"] = _make_blocking_exit(aexit_fn)
        namespace["__aenter__"] = _make_async_forwarder(aenter_fn)
        namespace["__aexit__"] = _make_async_forwarder(aexit_fn)

    # `aclose` is renamed rather than given the usual `.aio`, because that is
    # what Modal's synchronizer does with it: an implementation's `aclose`
    # becomes a blocking `close()` plus an async `aclose()`. Under the generic
    # rule both spellings a ported program uses break -- `reader.close()` raises
    # AttributeError, and `await reader.aclose()` awaits the None that the
    # blocking call returned.
    aclose_fn = members.get("aclose")
    if aclose_fn is not None and inspect.iscoroutinefunction(aclose_fn):
        namespace["close"] = _make_blocking_close(aclose_fn, blocking_name)
        namespace["aclose"] = _make_async_forwarder(aclose_fn)

    for attr, value in members.items():
        if attr.startswith("__") or attr in namespace:
            continue

        if isinstance(value, property):
            namespace[attr] = _PropertyDescriptor(value)
        elif isinstance(value, staticmethod):
            fn = value.__func__
            if inspect.isasyncgenfunction(fn):
                namespace[attr] = _MethodDescriptor(fn, True, True)
            elif inspect.iscoroutinefunction(fn):
                namespace[attr] = _MethodDescriptor(fn, False, True)
            elif not attr.startswith("_"):
                # A private sync static is implementation detail, not API. It
                # used to pass straight through, which put helpers like
                # `_raise_for_exit_reason` on the public class.
                namespace[attr] = value
        elif isinstance(value, classmethod):
            fn = value.__func__
            is_gen = inspect.isasyncgenfunction(fn)
            if is_gen or inspect.iscoroutinefunction(fn):
                # Bind the implementation class, so the descriptor can treat it
                # as a static from there on. Left to the branches below, a
                # classmethod matches none of them -- it is not `callable()` --
                # and would vanish from the public class.
                bound = functools.partial(fn, impl_class)
                functools.update_wrapper(bound, fn)
                namespace[attr] = _MethodDescriptor(bound, is_gen, True)
            elif not attr.startswith("_"):
                namespace[attr] = value
        elif inspect.isasyncgenfunction(value):
            namespace[attr] = _MethodDescriptor(value, True, False)
        elif inspect.iscoroutinefunction(value):
            namespace[attr] = _MethodDescriptor(value, False, False)
        elif callable(value) and not attr.startswith("_"):
            # A plain sync helper: forward it to the implementation.
            namespace[attr] = _make_sync_forwarder(value)

    blocking_class = type(blocking_name, (_BlockingBase,), namespace)
    _IMPL_TO_BLOCKING[impl_class] = blocking_class
    return blocking_class


def _make_blocking_iter(aiter_fn: Callable, class_name: str) -> Callable:
    @functools.wraps(aiter_fn)
    def __iter__(self):
        _warn_if_blocking_in_async(
            f"Iterating a {class_name} with `for`", "`async for`"
        )
        return _BlockingIterator(aiter_fn(self._impl))

    return __iter__


def _make_async_iter(aiter_fn: Callable) -> Callable:
    @functools.wraps(aiter_fn)
    def __aiter__(self):
        return _AsyncIterator(aiter_fn(self._impl))

    return __aiter__


# Neither `__next__` nor `__exit__` warns when called inside an event loop:
# Modal skips both, since the `for` or `with` that led to them already did.


def _make_blocking_next(anext_fn: Callable) -> Callable:
    @functools.wraps(anext_fn)
    def __next__(self):
        try:
            return _translate_out(_loop_thread.run(anext_fn(self._impl)))
        except StopAsyncIteration:
            # The blocking surface must end iteration the blocking way, or
            # `next(reader)` leaks StopAsyncIteration into ordinary code.
            raise StopIteration from None

    return __next__


def _make_blocking_enter(aenter_fn: Callable, class_name: str) -> Callable:
    @functools.wraps(aenter_fn)
    def __enter__(self):
        _warn_if_blocking_in_async(f"`with` on a {class_name}", "`async with`")
        return _translate_out(_loop_thread.run(aenter_fn(self._impl)))

    __enter__.__name__ = "__enter__"
    __enter__.__qualname__ = "__enter__"
    return __enter__


def _make_blocking_exit(aexit_fn: Callable) -> Callable:
    @functools.wraps(aexit_fn)
    def __exit__(self, exc_type, exc, tb):
        return _translate_out(_loop_thread.run(aexit_fn(self._impl, exc_type, exc, tb)))

    __exit__.__name__ = "__exit__"
    __exit__.__qualname__ = "__exit__"
    return __exit__


def _make_blocking_close(aclose_fn: Callable, class_name: str) -> Callable:
    @functools.wraps(aclose_fn)
    def close(self):
        _warn_if_blocking_in_async(
            f"{class_name}.close()", f"`await {class_name}.aclose()`"
        )
        return _translate_out(_loop_thread.run(aclose_fn(self._impl)))

    close.__name__ = "close"
    close.__qualname__ = "close"
    return close


def _make_async_forwarder(fn: Callable) -> Callable:
    """An async dunder or ``aclose``, run in the caller's own event loop."""

    @functools.wraps(fn)
    async def forwarder(self, *args, **kwargs):
        args, kwargs = _translate_args(args, kwargs)
        return _translate_out(await fn(self._impl, *args, **kwargs))

    return forwarder


def _make_sync_forwarder(fn: Callable) -> Callable:
    @functools.wraps(fn)
    def forwarder(self, *args, **kwargs):
        args, kwargs = _translate_args(args, kwargs)
        return _translate_out(fn(self._impl, *args, **kwargs))

    return forwarder
