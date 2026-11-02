"""Turn an async-native implementation class into a blocking one with an ``.aio`` twin.

Every class in this package is written once, async-native, with a leading
underscore (``_Sandbox``). At the bottom of its module it is passed through
:func:`synchronize_api`, which produces the public blocking class
(``Sandbox``). On the blocking class, every method is callable two ways::

    sandbox.exec("echo", "hi")          # blocks, returns the result
    await sandbox.exec.aio("echo", "hi")  # awaitable from any event loop

This is a small stand-in for Modal's ``synchronicity`` dependency, covering
only what this package needs: coroutine methods, async-generator methods,
static factories, context managers, and properties that return other wrapped
objects. Where it covers the same ground it behaves the same way: a Ctrl-C
during a blocking call cancels the call, a forked child gets a fresh loop,
each implementation object has one wrapper, and a blocking call made inside a
running event loop warns with ``AsyncUsageWarning``.

Both surfaces run the implementation on a single daemon event loop owned by
this module, as synchronicity does: a blocking call waits for it there, and an
``.aio`` call is awaited from the caller's own loop while the work runs there.
An object's loop-bound state -- an open stream, the task that prints a
``StreamType.STDOUT`` process's output -- therefore belongs to that one loop,
whichever surface or ``asyncio.run()`` drove it. Both paths translate wrapper
objects to implementations on the way in and implementations to wrappers on
the way out.

Also as synchronicity does:

* An interrupt during a blocking call -- a Ctrl-C, or a ``SystemExit`` from a
  signal handler -- cancels the work and waits for it to unwind before it is
  raised, and cancelling an ``.aio`` call does the same.
* At interpreter exit the loop is stopped: its tasks are cancelled and
  allowed to unwind, and async generators are closed.
* Tracebacks leave out the frames of carrying a call across threads;
  ``SYNCHRONICITY_TRACEBACK=1`` keeps them.
* A ``for`` broken out of closes its generator before the next statement.
* Only exactly registered classes are wrapped; an implementation subclass
  that was not passed to :func:`synchronize_api` is returned as it is.
* A loop thread that dies fails every later call rather than being restarted,
  and a forked child gets a fresh loop. Objects created before a fork stay
  tied to the parent's loop, and are not usable in the child.

Where it deliberately differs:

* If the work catches its cancellation and returns after a Ctrl-C,
  synchronicity returns the value and drops the interrupt. Here the
  interrupt is raised regardless: a Ctrl-C is never silently ignored.
* A step of a generator interrupted part-way is not cancelled, which would
  end the generator: it is kept, and handed to whichever loop asks next. A
  StreamReader keeps its own position besides, so a loop or read() after an
  interrupt resumes exactly where the last one stopped, as on Modal.
* Blocking iterators have no ``send()`` or ``throw()``: nothing public is a
  generator that takes either.
* What iterating returns is an iterator object rather than a generator, so
  ``inspect.isgenerator(iter(proc.stdout))`` -- and ``isasyncgen`` for an
  ``async for`` -- is false where synchronicity's is true. It is what keeps
  the look-ahead and a StreamReader's chunk-at-a-time hand-over; nothing
  public is a generator method.
"""

import asyncio
import atexit
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

    Caught by :meth:`_BoundMethod.__call__` and its ``.aio``, which connect
    and run the call again -- for a method marked ``@needs_ray`` only. From
    anywhere else it reaches the caller as a RuntimeError; see _Boundary.
    """


def needs_ray(fn: Callable) -> Callable:
    """Mark a method that may have to connect to Ray.

    Both surfaces run their coroutine on the daemon loop, so a method that
    creates an actor would otherwise have the first such call auto-initialize
    Ray from that daemon thread. ``ray.init`` installs the driver's SIGTERM
    handler only on the main thread, and otherwise logs a warning and skips it
    -- so a detail of this module would decide whether the host program handles
    SIGTERM.

    Connecting up front instead would be simpler and wrong: argument validation
    happens inside the coroutine, so every refused ``Sandbox.create(...)`` would
    start a cluster before saying no. The marked method instead calls
    :func:`require_ray_connection` at the point it actually needs Ray, and the
    call is replayed once with a connection in hand.

    The rules that make the replay safe:

    * Only a coroutine method, static or class method marked with this is
      replayed, from its blocking call or its ``.aio``. Generators,
      iterators, context managers and dunders never are: one of those that
      reached require_ray_connection() fails with a RuntimeError instead.
    * The marked method calls require_ray_connection() before anything with a
      side effect -- everything before it is validation, so running it twice
      changes nothing. ``Sandbox.create`` does exactly that.
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

# How long stopping the loop at interpreter exit waits for what was running on
# it to unwind, and how long closing an abandoned iterator waits: both the
# bound synchronicity uses for finalizing a generator.
_SHUTDOWN_TIMEOUT_SECONDS = 10.0


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
        # The process that started the loop: the exit hook stops it only
        # there, never from a forked child that inherited the hook.
        self._owner_pid: Optional[int] = None

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
            self._owner_pid = os.getpid()
            return loop

    def close(self, timeout: float = _SHUTDOWN_TIMEOUT_SECONDS) -> None:
        """Stop the loop, letting what runs on it unwind first. Never raises.

        Registered to run at interpreter exit, as synchronicity stops its own
        loop there. Without it the daemon thread was simply abandoned mid-task:
        a task's ``finally`` never ran, an open stream was never closed, and a
        task still writing a ``StreamType.STDOUT`` process's output to stdout
        could do so while the interpreter finalized -- which can kill it with
        a fatal error. Every task is cancelled and given ``timeout`` to finish,
        async generators are closed, and the thread is joined. A call made
        after this starts a fresh loop.

        Args:
            timeout: Seconds to wait for the tasks, and then for the thread.
        """
        with self._lock:
            loop, thread = self._loop, self._thread
            if loop is None or thread is None or self._owner_pid != os.getpid():
                return
            self._loop = None
            self._thread = None
        if not thread.is_alive() or loop.is_closed():
            return

        async def _unwind():
            current = asyncio.current_task()
            tasks = [task for task in asyncio.all_tasks() if task is not current]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.wait(tasks, timeout=timeout)
            await loop.shutdown_asyncgens()

        try:
            asyncio.run_coroutine_threadsafe(_unwind(), loop).result(timeout + 1)
        except BaseException:
            pass
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            return
        thread.join(timeout)

    def _dead(self) -> RuntimeError:
        # Not restarted: every stream, look-ahead and task the dead loop held
        # went with it, so a fresh loop would serve new calls while objects
        # created before it hung. Failing is what synchronicity does too.
        error = RuntimeError("The Sandbox event loop thread died unexpectedly.")
        error.__cause__ = self._died_of
        return error

    def run(self, coro: Coroutine) -> Any:
        """Run ``coro`` to completion on the background loop and return its result.

        Anything that interrupts the wait -- a ``KeyboardInterrupt``, or a
        ``SystemExit`` raised by a signal handler -- cancels the coroutine,
        waits for it to finish unwinding, and then re-raises, as synchronicity
        does. Returning at once instead would leave the interrupted work, an
        exec or a Sandbox being created, carrying on after the caller gave up
        on it. A second interrupt stops the wait.

        If the coroutine handles its cancellation and returns anyway, the
        interrupt is still raised. synchronicity returns the value instead;
        here a Ctrl-C is never silently dropped.

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
            _start_task(loop, coro, done, started)

        def _cancel():
            # Callbacks run in the order they were scheduled, so if _start
            # was scheduled at all, it has made the task by now.
            if started:
                started[0].cancel()
            elif not done.done():
                coro.close()
                _cancel_future(done)

        scheduled = False
        try:
            loop.call_soon_threadsafe(_start)
            scheduled = True
            return self._wait(done, thread)
        except BaseException:
            # The work's own outcome -- it raised -- or a loop already gone:
            # there is nothing left to stop.
            if not scheduled or done.done() or not thread.is_alive():
                raise
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

    async def run_async(self, coro: Coroutine) -> Any:
        """Run ``coro`` on the background loop, and await it from the caller's.

        The ``.aio`` counterpart of :meth:`run`, and what synchronicity does
        for Modal's ``.aio`` calls: the work runs where every blocking call
        runs, so an object's loop-bound state -- an open stream, a task a call
        started -- outlives the caller's loop and is shared by both surfaces.

        Cancelling the await cancels the work and waits for it to finish
        unwinding before the cancellation reaches the caller, as synchronicity
        does; a second cancellation stops that wait. Called from the
        background loop itself, the coroutine is simply awaited there.

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
            return await coro

        done = concurrent.futures.Future()
        started = []

        def _start():
            _start_task(loop, coro, done, started)

        def _cancel():
            # Scheduled after _start, so the task exists by now -- unless
            # starting it failed, which has already settled `done`.
            if started:
                started[0].cancel()

        loop.call_soon_threadsafe(_start)
        # Never cancelled by the caller directly: chained cancellation would
        # cancel `done` itself, and _settle would then fail to resolve it.
        waiter = asyncio.wrap_future(done)
        try:
            return await self._wait_async(waiter, thread)
        except asyncio.CancelledError:
            loop.call_soon_threadsafe(_cancel)
            try:
                await self._wait_async(waiter, thread)
            except asyncio.CancelledError:
                # The work's own cancellation, or a second one of ours.
                pass
            # Re-raised even if the work caught its cancellation and
            # returned, as synchronicity does: the caller asked to stop.
            raise

    async def _wait_async(self, waiter: asyncio.Future, thread: threading.Thread):
        # asyncio.wait rather than awaiting the future: it neither cancels the
        # future on a timeout nor when the caller is cancelled, and it lets a
        # loop thread that died mid-call fail the call instead of hanging it.
        while True:
            finished, _ = await asyncio.wait({waiter}, timeout=_LIVENESS_POLL_SECONDS)
            if finished:
                return waiter.result()
            if not thread.is_alive():
                raise self._dead()


class _Escaped(Exception):
    """Carries a KeyboardInterrupt or SystemExit raised inside a call.

    asyncio lets those two propagate out of the event loop, which would end
    the loop thread and with it every later call. They are carried to the
    caller instead and re-raised there, as synchronicity does.
    """

    def __init__(self, exc: BaseException):
        super().__init__(exc)
        self.exc = exc


async def _guarded(awaitable) -> Any:
    """Await ``awaitable``, catching what would otherwise kill the loop."""
    try:
        return await awaitable
    except (KeyboardInterrupt, SystemExit) as exc:
        raise _Escaped(exc) from None


def _start_task(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine,
    done: concurrent.futures.Future,
    started: list,
) -> None:
    """Start ``coro`` as a task on ``loop``, settling ``done`` when it ends.

    Runs on the loop. Starting can fail -- the loop closing under it -- and
    then ``done`` takes the error: left unresolved, a blocking caller waited
    on it for as long as the loop thread stayed alive, which was forever.
    """
    try:
        task = loop.create_task(_guarded(coro))
    except BaseException as exc:
        coro.close()
        if not done.done():
            done.set_exception(exc)
        return
    started.append(task)
    task.add_done_callback(functools.partial(_settle, done))


def _settle(done: concurrent.futures.Future, task: asyncio.Task) -> None:
    """Copy a finished task's outcome to the future its caller waits on."""
    if task.cancelled():
        _cancel_future(done)
    elif task.exception() is not None:
        exc = task.exception()
        done.set_exception(exc.exc if isinstance(exc, _Escaped) else exc)
    else:
        done.set_result(task.result())


def _cancel_future(future: concurrent.futures.Future) -> None:
    future.cancel()
    # cancel() alone leaves the future CANCELLED, which concurrent.futures.wait
    # does not count as done; this moves it on to CANCELLED_AND_NOTIFIED, as
    # asyncio's own future chaining does.
    future.set_running_or_notify_cancel()


_loop_thread = _EventLoopThread()
# Stopped as the interpreter exits, as synchronicity registers its own loop's
# close; see _EventLoopThread.close.
atexit.register(_loop_thread.close)


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


def _full_tracebacks() -> bool:
    # synchronicity's own switch, honoured the same way.
    return os.environ.get("SYNCHRONICITY_TRACEBACK", "0") == "1"


# Frames a caller never needs to see: this module's, and asyncio's and
# concurrent.futures' from carrying a call across threads.
_THIS_FILE = os.path.abspath(__file__)
_HIDDEN_DIRS = (
    os.path.dirname(asyncio.__file__) + os.sep,
    os.path.dirname(concurrent.futures.__file__) + os.sep,
)


def _is_hidden(filename: str) -> bool:
    return filename == _THIS_FILE or filename.startswith(_HIDDEN_DIRS)


def _may_connect(fn: Callable) -> bool:
    """Whether ``fn`` may be run again after connecting; see needs_ray."""
    return getattr(fn, _NEEDS_RAY_ATTR, False)


class _Boundary:
    """Where a call leaves this module for its caller.

    Trims the frames of carrying the call across threads off the front of an
    escaping exception's traceback, as synchronicity does, so the caller's
    own line leads straight to the code that raised: it was eight frames,
    six of them this module's and concurrent.futures', where synchronicity
    shows two. ``SYNCHRONICITY_TRACEBACK=1`` keeps them all. Effective from
    Python 3.11, where a re-raised exception keeps the traceback set on it.

    Also turns a ``_RayConnectionRequired`` that got this far into an error
    naming the call. Only a method marked ``@needs_ray`` is run again once
    connected; anything else cannot be assumed safe to run twice, and the
    internal signal used to leak out of iterators and dunders as it was.
    """

    __slots__ = ("_what",)

    def __init__(self, what: str):
        self._what = what

    def __enter__(self) -> "_Boundary":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            return False
        if isinstance(exc, _RayConnectionRequired):
            raise RuntimeError(
                f"{self._what} needs a Ray connection, but is not marked "
                f"@needs_ray, so it cannot be run again once connected. Call "
                f"ray.init() before it."
            ) from None
        if not _full_tracebacks():
            root = tb
            while root is not None and _is_hidden(root.tb_frame.f_code.co_filename):
                root = root.tb_next
            if root is not None:
                exc.with_traceback(root)
        return False


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


class _Lookahead:
    """An item fetched but not yet handed over, and a fetch interrupted mid-way.

    Kept per generator rather than per loop, so that whichever loop over the
    generator comes next -- blocking or ``async for`` -- receives the item a
    Ctrl-C or a cancellation interrupted, and never asks the generator for a
    second item while an earlier fetch is still running inside it.
    """

    __slots__ = ("buffer", "pending", "exhausted", "lock", "__weakref__")

    def __init__(self):
        self.buffer = collections.deque()
        # An __anext__ that was started but had not finished within a loop
        # turn. Kept rather than cancelled: cancelling one in flight can lose
        # an item the generator has already taken off its source, or leave the
        # generator unusable. It is awaited first on the next fill.
        self.pending = None
        self.exhausted = False
        # Takes turns between fills and the close: Python refuses to advance
        # or close a generator that is mid-step ("asynchronous generator is
        # already running"), which two consumers of one generator, or a close
        # racing a fill, ran into. Made on first use, on the loop.
        self.lock: Optional[asyncio.Lock] = None


def _turn(state: _Lookahead) -> asyncio.Lock:
    """The look-ahead's lock, made on first use. On the loop."""
    if state.lock is None:
        state.lock = asyncio.Lock()
    return state.lock


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


def _keeps_ready(agen) -> bool:
    """Whether ``agen`` keeps its own queue of ready items, as a StreamReader's
    loop does: ``fill_ready()`` loads it on the loop, ``pop_ready()`` takes
    from it on any thread, and ``NOTHING_READY`` is what an empty one returns.
    """
    return getattr(agen, "fill_ready", None) is not None


def _take_ready(agen) -> Any:
    """An item ``agen`` holds decoded and waiting, or ``_NO_ITEM``. Never waits.

    Taken on the caller's thread, with no crossing to the loop, and only at
    the moment it is handed over: until then it stays in the iterator's own
    queue, so a ``break``, a timeout or a Ctrl-C leaves it for the next
    read() or loop, as on Modal.
    """
    if not _keeps_ready(agen):
        return _NO_ITEM
    item = agen.pop_ready()
    if item is getattr(agen, "NOTHING_READY", None):
        return _NO_ITEM
    return item


_NO_ITEM = object()


async def _fill(agen, state: _Lookahead) -> None:
    """Fetch the generator's next item into the look-ahead. On the loop.

    One item per call, as synchronicity advances a generator: it runs only as
    far as the consumer has asked, so a producer's side effects interleave
    with the consumer's. An iterator that keeps its own ready queue is asked
    to fill that instead -- a StreamReader decodes a whole chunk of lines per
    fill -- and its items never pass through the look-ahead at all.

    The item goes into the look-ahead rather than back as a result, so a
    Ctrl-C or a cancellation that lands after the fetch, before the caller has
    it, leaves it for the next loop instead of dropping it.
    """
    if _keeps_ready(agen):
        if not await agen.fill_ready():
            state.exhausted = True
        return
    async with _turn(state):
        if state.exhausted:
            # Another consumer reached the end while this one waited its turn.
            return
        task = state.pending or asyncio.ensure_future(_guarded(agen.__anext__()))
        state.pending = None
        try:
            # Shielded, so that cancelling this fill leaves the fetch running
            # rather than cancelling it too.
            state.buffer.append(await asyncio.shield(task))
        except StopAsyncIteration:
            state.exhausted = True
        except asyncio.CancelledError:
            # A Ctrl-C on the blocking caller, or a cancelled `async for`
            # step. Keep the fetch for the next loop: cancelling a fetch inside
            # a generator closes the generator. Kept even when it has already
            # finished -- one that completed in the very turn this fill was
            # cancelled holds an item, and dropping it lost that item. Only a
            # fetch that was itself cancelled carries nothing to keep.
            if not task.cancelled():
                state.pending = task
            raise


async def _close(agen, state: _Lookahead) -> None:
    """Close ``agen`` for good, dropping what was fetched ahead. On the loop."""
    async with _turn(state):
        if state.exhausted:
            return
        pending, state.pending = state.pending, None
        state.buffer.clear()
        state.exhausted = True
        if pending is not None:
            # Safe to drop only now: nothing will read from this generator
            # again, so an item lost with it cannot be missed. Settled before
            # aclose(): a generator still running the cancelled fetch refused
            # to close ("aclose(): asynchronous generator is already running").
            pending.cancel()
            await asyncio.wait({pending})
            if not pending.cancelled():
                # Retrieved, so a fetch that failed is not reported later as
                # an exception nobody looked at.
                pending.exception()
        await agen.aclose()


# Closes of abandoned iterators in flight, held so their tasks are not
# collected before they finish.
_CLOSING: set = set()


def _close_soon(agen, state: _Lookahead) -> None:
    """Close an abandoned iterator's generator on the loop, without waiting.

    For an ``async for`` that stops part-way, and for a blocking one whose
    iterator is released on the loop thread itself; see _close_on_release.
    For a StreamReader that ends the actor's streaming task and frees the
    chunks in flight; the reader's position is kept, so a later read resumes
    there. Never blocks.
    """
    if state.exhausted or sys.is_finalizing():
        return
    loop = _loop_thread._loop
    if loop is None or loop.is_closed():
        return

    def _spawn():
        task = loop.create_task(_close_quietly(agen, state))
        _CLOSING.add(task)
        task.add_done_callback(_CLOSING.discard)

    try:
        loop.call_soon_threadsafe(_spawn)
    except RuntimeError:
        # The loop closed in between.
        pass


def _close_on_release(agen, state: _Lookahead) -> None:
    """Close an iterator's generator when the iterator itself is released.

    That is ``for line in proc.stdout: break``: the generator is closed before
    the statement after the loop runs, as synchronicity's closes in its own
    ``finally`` -- it used to happen a moment after. Waits at most
    ``_SHUTDOWN_TIMEOUT_SECONDS``, synchronicity's bound. On the loop thread,
    where waiting would deadlock it, and while the interpreter exits, the
    close is only scheduled.
    """
    if state.exhausted or sys.is_finalizing():
        return
    loop = _loop_thread._loop
    if loop is None or loop.is_closed():
        return
    if threading.current_thread() is _loop_thread.thread:
        _close_soon(agen, state)
        return
    try:
        closing = asyncio.run_coroutine_threadsafe(_close_quietly(agen, state), loop)
        closing.result(_SHUTDOWN_TIMEOUT_SECONDS)
    except BaseException:
        # From __del__, which has nowhere to report it; a close that times
        # out carries on by itself.
        pass


async def _close_quietly(agen, state: _Lookahead) -> None:
    # Nobody awaits this, so its failure has nowhere to go.
    try:
        await _close(agen, state)
    except BaseException:
        pass


def _iteration_name(agen) -> str:
    """What an iteration is called in messages: ``_Cls.method`` -> ``Cls.method``."""
    name = getattr(agen, "__qualname__", None) or type(agen).__qualname__
    return f"Iterating {name.lstrip('_')}"


def _pop_buffered(state: _Lookahead) -> Any:
    # Not `if state.buffer: popleft()`: another consumer of the same generator
    # can take the last item between the two.
    try:
        return state.buffer.popleft()
    except IndexError:
        return _NO_ITEM


class _BlockingIterator:
    """Drives an async generator from synchronous code.

    One crossing to the loop per item, except for an iterator that keeps its
    own decoded items (``pop_ready``): a StreamReader's lines are handed over
    from this thread, a whole chunk per crossing. Each crossing costs a
    coroutine schedule, a thread wake and a park -- around 200us, measured --
    which per line would be hundreds of times the cost of the line itself.
    """

    def __init__(self, agen):
        self._agen = agen
        self._state = _lookahead_for(agen)

    def __iter__(self):
        return self

    def __next__(self):
        with _Boundary(_iteration_name(self._agen)):
            while True:
                item = _pop_buffered(self._state)
                if item is _NO_ITEM:
                    item = _take_ready(self._agen)
                if item is not _NO_ITEM:
                    return _translate_out(item)
                if self._state.exhausted:
                    raise StopIteration
                _loop_thread.run(_fill(self._agen, self._state))

    def close(self):
        with _Boundary(_iteration_name(self._agen)):
            try:
                _loop_thread.run(_close(self._agen, self._state))
            except StopAsyncIteration:
                pass

    def __del__(self):
        _close_on_release(self._agen, self._state)


class _AsyncIterator:
    """Drives an async generator from the caller's event loop.

    The generator itself runs on the background loop, as with every ``.aio``
    call, with the same look-ahead as :class:`_BlockingIterator` -- so
    ``async for`` and ``for`` over one generator, or one ``async for`` spread
    across two ``asyncio.run()`` calls, continue each other rather than
    colliding.
    """

    def __init__(self, agen):
        self._agen = agen
        self._state = _lookahead_for(agen)

    def __aiter__(self):
        return self

    async def __anext__(self):
        with _Boundary(_iteration_name(self._agen)):
            while True:
                item = _pop_buffered(self._state)
                if item is _NO_ITEM:
                    item = _take_ready(self._agen)
                if item is not _NO_ITEM:
                    return _translate_out(item)
                if self._state.exhausted:
                    raise StopAsyncIteration
                await _loop_thread.run_async(_fill(self._agen, self._state))

    async def aclose(self):
        with _Boundary(_iteration_name(self._agen)):
            await _loop_thread.run_async(_close(self._agen, self._state))

    def __del__(self):
        # Scheduled, never waited for: this runs on the caller's event loop
        # thread, which a wait would stall. synchronicity's async generators
        # are likewise closed by a task their finalizer schedules.
        _close_soon(self._agen, self._state)


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
        with _Boundary(f"{name}()"):
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
                # The call got as far as needing Ray and stopped there so that
                # the connection could be made here, on the caller's thread
                # rather than the daemon loop -- for a method marked to allow
                # it; see needs_ray. A fresh coroutine, since the one above is
                # spent; at most one retry, because ray.is_initialized() stays
                # true for the rest of the process.
                if not _may_connect(self._fn):
                    raise
                ray.init()
            return _translate_out(_loop_thread.run(self._call_raw(args, kwargs)))

    def _call_raw(self, args, kwargs):
        if self._impl is _UNBOUND:
            return self._fn(*args, **kwargs)
        return self._fn(self._impl, *args, **kwargs)

    @property
    def aio(self) -> Callable:
        """The async variant: awaitable from any loop, run on the background one.

        A real ``async def`` carrying the method's own name and docstring, as
        synchronicity's ``.aio`` is: ``inspect.iscoroutinefunction`` is true
        of it, so code that checks before awaiting awaits it, and
        ``help(sandbox.exec.aio)`` shows ``exec``'s documentation. It used to
        be a plain method returning a coroutine, documented as this helper.
        A generator's variant returns an async iterator instead.
        """
        fn = self._fn
        if self._is_gen:

            @functools.wraps(fn)
            def aio_gen(*args, **kwargs):
                args, kwargs = _translate_args(args, kwargs)
                return _AsyncIterator(self._call_raw(args, kwargs))

            return aio_gen

        name = _public_name(fn)

        @functools.wraps(fn)
        async def aio(*args, **kwargs):
            with _Boundary(f"{name}.aio()"):
                args, kwargs = _translate_args(args, kwargs)
                try:
                    result = await _loop_thread.run_async(self._call_raw(args, kwargs))
                except _RayConnectionRequired:
                    # As for a blocking call: connect here, on the caller's
                    # thread, and run the call again -- if the method allows
                    # it. ray.init() blocks this loop while it connects, as
                    # Ray's own connect-on-first-use did when .aio ran on the
                    # caller's loop.
                    if not _may_connect(fn):
                        raise
                    ray.init()
                    result = await _loop_thread.run_async(self._call_raw(args, kwargs))
                return _translate_out(result)

        return aio


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
        return _BoundMethod(fn, _impl_of(self), is_gen)(*args, **kwargs)

    # Typed and documented like the method, as on an instance: a coroutine
    # function for a coroutine method, a plain function returning an async
    # iterator for a generator.
    if is_gen:

        @functools.wraps(fn)
        def aio(self, *args, **kwargs):
            with _Boundary(f"{_public_name(fn)}.aio()"):
                return _BoundMethod(fn, _impl_of(self), is_gen).aio(*args, **kwargs)

    else:

        @functools.wraps(fn)
        async def aio(self, *args, **kwargs):
            with _Boundary(f"{_public_name(fn)}.aio()"):
                return await _BoundMethod(fn, _impl_of(self), is_gen).aio(
                    *args, **kwargs
                )

    unbound.aio = aio
    return unbound


class _PropertyDescriptor:
    """Forwards a property to the implementation, translating the result."""

    def __init__(self, prop: property):
        self._prop = prop
        self.__doc__ = prop.__doc__
        self._name = getattr(prop.fget, "__qualname__", "property").lstrip("_")

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        with _Boundary(self._name):
            return _translate_out(self._prop.fget(obj._impl))

    def __set__(self, obj, value):
        with _Boundary(self._name):
            if self._prop.fset is None:
                raise AttributeError("can't set attribute")
            self._prop.fset(obj._impl, _translate_in(value))

    def __delete__(self, obj):
        # Forwarded like the getter and setter, as synchronicity forwards a
        # property's deleter; refused the same way when there is none.
        with _Boundary(self._name):
            if self._prop.fdel is None:
                raise AttributeError("can't delete attribute")
            self._prop.fdel(obj._impl)


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
                # `_raise_for_exit_reason` on the public class. A public one
                # is forwarded with its arguments and result translated, as
                # synchronicity does; passed through, it returned bare
                # implementation objects.
                namespace[attr] = staticmethod(_make_sync_static(fn))
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
                # Bound to the implementation class too, and translated both
                # ways, as synchronicity does. Passed through, `cls` was this
                # public class -- whose constructor refuses -- and the result
                # came back untranslated.
                bound = functools.partial(fn, impl_class)
                functools.update_wrapper(bound, fn)
                namespace[attr] = staticmethod(_make_sync_static(bound))
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
        with _Boundary(f"Iterating a {class_name}"):
            _warn_if_blocking_in_async(
                f"Iterating a {class_name} with `for`", "`async for`"
            )
            return _BlockingIterator(aiter_fn(self._impl))

    return __iter__


def _make_async_iter(aiter_fn: Callable) -> Callable:
    @functools.wraps(aiter_fn)
    def __aiter__(self):
        with _Boundary(_public_name(aiter_fn)):
            return _AsyncIterator(aiter_fn(self._impl))

    return __aiter__


# Neither `__next__` nor `__exit__` warns when called inside an event loop:
# Modal skips both, since the `for` or `with` that led to them already did.


def _make_blocking_next(anext_fn: Callable) -> Callable:
    @functools.wraps(anext_fn)
    def __next__(self):
        with _Boundary(_public_name(anext_fn)):
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
        with _Boundary(f"`with` on a {class_name}"):
            _warn_if_blocking_in_async(f"`with` on a {class_name}", "`async with`")
            return _translate_out(_loop_thread.run(aenter_fn(self._impl)))

    __enter__.__name__ = "__enter__"
    __enter__.__qualname__ = "__enter__"
    return __enter__


def _make_blocking_exit(aexit_fn: Callable) -> Callable:
    @functools.wraps(aexit_fn)
    def __exit__(self, exc_type, exc, tb):
        with _Boundary(_public_name(aexit_fn)):
            return _translate_out(
                _loop_thread.run(aexit_fn(self._impl, exc_type, exc, tb))
            )

    __exit__.__name__ = "__exit__"
    __exit__.__qualname__ = "__exit__"
    return __exit__


def _make_blocking_close(aclose_fn: Callable, class_name: str) -> Callable:
    @functools.wraps(aclose_fn)
    def close(self):
        with _Boundary(f"{class_name}.close()"):
            _warn_if_blocking_in_async(
                f"{class_name}.close()", f"`await {class_name}.aclose()`"
            )
            return _translate_out(_loop_thread.run(aclose_fn(self._impl)))

    close.__name__ = "close"
    close.__qualname__ = "close"
    return close


def _make_async_forwarder(fn: Callable) -> Callable:
    """An async dunder or ``aclose``, run on the background loop like ``.aio``."""

    @functools.wraps(fn)
    async def forwarder(self, *args, **kwargs):
        with _Boundary(_public_name(fn)):
            args, kwargs = _translate_args(args, kwargs)
            return _translate_out(
                await _loop_thread.run_async(fn(self._impl, *args, **kwargs))
            )

    return forwarder


def _make_sync_forwarder(fn: Callable) -> Callable:
    @functools.wraps(fn)
    def forwarder(self, *args, **kwargs):
        with _Boundary(_public_name(fn)):
            args, kwargs = _translate_args(args, kwargs)
            return _translate_out(fn(_impl_of(self), *args, **kwargs))

    return forwarder


def _make_sync_static(fn: Callable) -> Callable:
    """A sync static or class method, with its arguments and result translated."""

    @functools.wraps(fn)
    def forwarder(*args, **kwargs):
        with _Boundary(_public_name(fn)):
            args, kwargs = _translate_args(args, kwargs)
            return _translate_out(fn(*args, **kwargs))

    return forwarder


def _impl_of(obj: Any) -> Any:
    """The implementation behind a wrapper; anything else as it is.

    ``Cls.method(x)`` with an ``x`` that is not a wrapper reaches the
    implementation unchanged, as in synchronicity, so its own checks report
    the mistake rather than a missing ``_impl`` attribute.
    """
    return obj._impl if isinstance(obj, _BlockingBase) else obj
