"""Unit tests for the sync/async wrapper behind the Modal-compatible API.

Nothing here touches Ray or runsc; it exercises :func:`synchronize_api` on
small purpose-built classes. Behavior synchronicity also has is tested once, in
``test_modal_sync_parity.py``, against both; this file holds the rest.
"""

import asyncio
import collections
import concurrent.futures
import gc
import os
import signal
import sys
import threading
import time
import warnings
import weakref

import pytest

from ray.experimental.sandbox.modal import _sync
from ray.experimental.sandbox.modal._sync import synchronize_api
from ray.experimental.sandbox.modal.exception import AsyncUsageWarning, InvalidError


class _Inner:
    """A wrapped object returned by another wrapped object."""

    def __init__(self, value):
        self.value = value

    async def get(self):
        return self.value


class _Outer:
    def __init__(self, value=1):
        self.value = value
        self._inner = _Inner(value * 10)

    async def double(self):
        return self.value * 2

    async def echo(self, value):
        """Records what arrived, and hands it back."""
        self.received = value
        return value

    async def me(self):
        return self

    @staticmethod
    async def build(value):
        return _Outer(value)

    @classmethod
    async def of(cls, value):
        return cls(value)

    @staticmethod
    def _private_helper():
        """Implementation detail. Must not reach the public class."""
        return "leaked"

    async def aclose(self):
        self.closed = True

    @property
    def inner(self):
        return self._inner


Inner = synchronize_api(_Inner)
Outer = synchronize_api(_Outer)


def _make(value=1):
    return Outer._from_impl(_Outer(value))


# -- .aio surface ----------------------------------------------------------


def test_aio_static_factory_returns_a_wrapped_object():
    async def main():
        built = await Outer.build.aio(5)
        assert isinstance(built, Outer)
        return await built.double.aio()

    assert asyncio.run(main()) == 10


class _Stream:
    """Loop-bound state of the kind a Sandbox's objects hold: a stream opened
    on first read and kept open."""

    def __init__(self):
        self.queue = None

    async def next_item(self):
        if self.queue is None:
            self.queue = asyncio.Queue()

            async def produce():
                for i in range(80):
                    await self.queue.put(i)
                    await asyncio.sleep(0.001)

            self.producer = asyncio.ensure_future(produce())
        return await self.queue.get()


Stream = synchronize_api(_Stream, name="StreamForLoopState")


def test_aio_calls_from_several_threads_share_one_object():
    stream = Stream._from_impl(_Stream())

    def reader(_):
        async def main():
            return [await stream.next_item.aio() for _ in range(20)]

        return asyncio.run(main())

    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        seen = [item for batch in pool.map(reader, range(4)) for item in batch]
    assert sorted(seen) == list(range(80))


class _Connects:
    @_sync.needs_ray
    async def create(self):
        _sync.require_ray_connection()
        return "created"


Connects = synchronize_api(_Connects, name="ConnectsForRay")


def test_aio_connects_to_ray_on_the_callers_thread(monkeypatch):
    """The call runs on the loop thread now, so a first one hands the connect
    back to the caller's thread, as a blocking call does."""
    connected = []
    monkeypatch.setattr(_sync.ray, "is_initialized", lambda: bool(connected))
    monkeypatch.setattr(
        _sync.ray, "init", lambda: connected.append(threading.current_thread())
    )

    async def main():
        return await Connects._from_impl(_Connects()).create.aio()

    assert asyncio.run(main()) == "created"
    assert connected == [threading.current_thread()]


class _Tracked:
    def __init__(self):
        self.events = []

    async def items(self):
        try:
            for i in range(5):
                self.events.append(f"yield {i}")
                yield i
        finally:
            await asyncio.sleep(0.01)
            self.events.append("closed")


Tracked = synchronize_api(_Tracked, name="TrackedForClose")


def _until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.01)
    return predicate()


def test_an_async_loop_that_stops_part_way_closes_its_generator():
    tracked = Tracked._from_impl(_Tracked())

    async def main():
        async for _ in tracked.items.aio():
            break

    asyncio.run(main())
    assert _until(lambda: "closed" in tracked._impl.events)
    assert tracked._impl.events == ["yield 0", "closed"]


# -- guard rails -----------------------------------------------------------


def test_blocking_call_from_inside_the_background_loop_is_refused():
    """Blocking on the loop that would run the coroutine would deadlock."""
    outer = _make(2)

    class _Probe:
        async def reenter(self):
            # Runs on the background loop, then calls back in blocking-style.
            return outer.double()

    Probe = synchronize_api(_Probe, name="ProbeForReentrancy")
    with pytest.raises(InvalidError, match="cannot be called from inside"):
        Probe._from_impl(_Probe()).reenter()


# -- batched iteration -----------------------------------------------------


class _Counted:
    """An async iterable that records how it was consumed."""

    def __init__(self, count, delay=0.0):
        self.count = count
        self.delay = delay
        self.produced = 0

    async def __aiter__(self):
        for i in range(self.count):
            if self.delay:
                await asyncio.sleep(self.delay)
            self.produced += 1
            yield i


Counted = synchronize_api(_Counted, name="CountedForIteration")


def test_a_generator_runs_only_as_far_as_the_consumer_has_asked():
    """As synchronicity advances one: no read-ahead, on either surface, so a
    producer's side effects interleave with the consumer's."""
    impl = _Counted(50)
    iterator = iter(Counted._from_impl(impl))
    assert next(iterator) == 0
    assert impl.produced == 1

    async def main():
        aimpl = _Counted(50)
        async for _ in Counted._from_impl(aimpl):
            break
        return aimpl.produced

    assert asyncio.run(main()) == 1


def test_a_slow_producer_is_not_held_back_by_batching():
    """A stream whose next item needs I/O must not wait for the batch to fill.

    This is what keeps `for line in process.stdout:` interactive: output that
    has not been produced yet cannot be worth delaying delivered output for.
    """
    impl = _Counted(50, delay=0.01)
    iterator = iter(Counted._from_impl(impl))

    started = time.perf_counter()
    first = next(iterator)
    elapsed = time.perf_counter() - started

    assert first == 0
    # One item's delay, not fifty. Generous bound so the assertion is about
    # the batch not filling, not about scheduler precision.
    assert elapsed < 0.10, f"first item waited {elapsed:.3f}s for a batch"


# -- Modal's wrapper conventions -------------------------------------------


def test_aclose_becomes_a_sync_close_and_an_async_aclose():
    """The one method Modal's synchronizer renames rather than giving `.aio`.

    Ported code writes `reader.close()` from blocking code and
    `await reader.aclose()` from async code; the generic rule would have made
    those an AttributeError and an `await None`.
    """
    obj = _make()
    assert Outer.close(obj) is None
    assert obj._impl.closed is True

    other = _make()
    asyncio.run(other.aclose())
    assert other._impl.closed is True

    # `close` is the blocking half, so it carries no `.aio` of its own.
    assert not hasattr(Outer.close, "aio")


def test_private_sync_statics_stay_off_the_public_class():
    """A leading underscore means implementation detail on either surface.

    `Sandbox._raise_for_exit_reason` used to reach the public class this way,
    because plain statics passed straight through unchecked.
    """
    assert not hasattr(Outer, "_private_helper")
    assert _Outer._private_helper() == "leaked"


def test_async_classmethods_survive_the_wrapper():
    """A classmethod matches none of the dispatch branches on its own.

    It is not `callable()`, so before it was given a branch it vanished from
    the generated class entirely.
    """
    built = Outer.of(7)
    assert isinstance(built, Outer)
    assert built.double() == 14

    async def via_aio():
        return await Outer.of.aio(3)

    assert asyncio.run(via_aio()).double() == 6


# -- translation and identity ----------------------------------------------


def test_one_implementation_object_has_one_wrapper():
    """As in synchronicity: `sb.stdout is sb.stdout`, and `self` comes back as is."""
    outer = _make(3)
    assert outer.inner is outer.inner
    assert outer.me() is outer
    assert Outer.build(1) is not Outer.build(1)


def test_the_wrapper_cache_keeps_nothing_alive():
    """No cycle, through the loop either: dropping the last wrapper frees the
    implementation at once.

    That matters because a Sandbox lives only as long as its handle -- a cycle
    would leave its actor running until the cycle collector got round to it.
    """
    impl = _Outer(1)
    wrapper = Outer._from_impl(impl)
    inner = wrapper.inner
    assert asyncio.run(wrapper.double.aio()) == 2
    impl_ref = weakref.ref(impl)
    gc.disable()
    try:
        del impl, wrapper, inner
        assert impl_ref() is None, "the implementation outlived its wrappers"
    finally:
        gc.enable()


def test_dict_values_are_translated_both_ways():
    outer = _make()
    inner = Inner._from_impl(_Inner(5))

    returned = outer.echo({"key": inner})

    assert outer._impl.received == {"key": inner._impl}
    assert returned["key"] is inner


def test_container_subclasses_pass_through_untranslated():
    """A namedtuple cannot be rebuilt from an iterable, so it is left alone."""
    Point = collections.namedtuple("Point", "x y")
    assert _make().echo(Point(1, 2)) == Point(1, 2)


class _Base:
    async def ping(self):
        return "base"

    async def shared(self):
        return "base"


class _Derived(_Base):
    async def shared(self):
        return "derived"


Derived = synchronize_api(_Derived, name="DerivedForInheritance")


class _SelfChecking:
    async def check(self):
        if not isinstance(self, _SelfChecking):
            raise ValueError(f"expected a _SelfChecking, got {type(self).__name__}")
        return "ok"


SelfChecking = synchronize_api(_SelfChecking, name="SelfCheckingForClassCalls")


def test_an_async_method_called_on_the_class_reaches_the_implementation():
    """A foreign self is passed through, so the method's own check is what
    raises. The parity file holds synchronicity to this for sync methods; its
    async form fails there with a TypeError, so it is pinned here alone."""
    obj = SelfChecking._from_impl(_SelfChecking())
    assert SelfChecking.check(obj) == "ok"
    with pytest.raises(ValueError, match="got str"):
        SelfChecking.check("not one")


def test_inherited_methods_are_wrapped_and_overrides_win():
    derived = Derived._from_impl(_Derived())
    assert derived.ping() == "base"
    assert derived.shared() == "derived"


# -- context managers ------------------------------------------------------


class _Managed:
    def __init__(self):
        self.events = []

    async def __aenter__(self):
        self.events.append("enter")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.events.append(("exit", exc_type))
        return False


Managed = synchronize_api(_Managed, name="ManagedForWith")


def test_an_async_context_manager_works_with_both_with_forms():
    managed = Managed._from_impl(_Managed())

    with managed as entered:
        assert entered is managed

    async def main():
        async with managed as entered:
            assert entered is managed

    asyncio.run(main())
    assert managed._impl.events == ["enter", ("exit", None)] * 2


def test_an_exception_in_the_body_reaches_exit():
    managed = Managed._from_impl(_Managed())
    with pytest.raises(ValueError):
        with managed:
            raise ValueError("body")
    assert managed._impl.events[-1] == ("exit", ValueError)


# -- blocking inside an event loop -----------------------------------------


def test_a_blocking_call_inside_an_event_loop_warns_at_the_callers_line():
    async def main():
        with pytest.warns(AsyncUsageWarning, match=r"Outer\.double\(\)") as record:
            _make(1).double()
        return record

    record = asyncio.run(main())
    assert record[0].filename == __file__
    assert "Outer.double.aio" in str(record[0].message)


def test_aio_and_calls_outside_a_loop_do_not_warn():
    with warnings.catch_warnings():
        warnings.simplefilter("error", AsyncUsageWarning)
        _make(1).double()

        async def main():
            return await _make(1).double.aio()

        assert asyncio.run(main()) == 2


def test_modal_async_warnings_setting_silences_the_warning(monkeypatch):
    monkeypatch.setenv("MODAL_ASYNC_WARNINGS", "0")

    async def main():
        with warnings.catch_warnings():
            warnings.simplefilter("error", AsyncUsageWarning)
            return _make(1).double()

    assert asyncio.run(main()) == 2


class _Ticker:
    def __init__(self):
        self.count = 0

    async def __anext__(self):
        self.count += 1
        return self.count


Ticker = synchronize_api(_Ticker, name="TickerForNext")


def test_next_and_exit_do_not_warn():
    """Modal skips both: the `for` or `with` that led to them already warned."""

    async def main():
        with warnings.catch_warnings():
            warnings.simplefilter("error", AsyncUsageWarning)
            assert next(Ticker._from_impl(_Ticker())) == 1
        with pytest.warns(AsyncUsageWarning) as record:
            with Managed._from_impl(_Managed()):
                pass
        return record

    assert len(asyncio.run(main())) == 1


# -- Ctrl-C ----------------------------------------------------------------


def _interrupt_main_after(event, delay=0.2, sig=signal.SIGINT):
    """Deliver ``sig`` to the main thread -- SIGINT, as Ctrl-C does -- once
    ``event`` is set.

    The delay lets the main thread get back to waiting on the loop first.
    """

    def _fire():
        if event.wait(10):
            time.sleep(delay)
            signal.pthread_kill(threading.main_thread().ident, sig)

    threading.Thread(target=_fire, daemon=True).start()


needs_sigint = pytest.mark.skipif(
    not hasattr(signal, "pthread_kill")
    or threading.current_thread() is not threading.main_thread(),
    reason="needs to interrupt the main thread",
)


class _Slow:
    def __init__(self):
        self.started = threading.Event()
        self.unwound = False

    async def cancel_itself(self):
        asyncio.current_task().cancel()
        await asyncio.sleep(30)

    async def hang(self):
        self.started.set()
        try:
            await asyncio.sleep(30)
        finally:
            # Unwinding takes a moment, and the caller must wait it out.
            await asyncio.sleep(0.2)
            self.unwound = True


Slow = synchronize_api(_Slow, name="SlowForInterrupt")


class _TimesOut:
    async def give_up(self):
        raise TimeoutError("the call's own timeout")


TimesOut = synchronize_api(_TimesOut, name="TimesOutForWait")


def test_a_call_that_raises_timeout_error_is_not_mistaken_for_a_poll(monkeypatch):
    """The wait polls with a timeout, and since 3.11 that raises the same class."""
    monkeypatch.setattr(_sync, "_LIVENESS_POLL_SECONDS", 0.01)
    with pytest.raises(TimeoutError, match="the call's own timeout"):
        TimesOut._from_impl(_TimesOut()).give_up()


def test_a_call_cancelled_on_the_loop_raises_instead_of_hanging():
    with pytest.raises(concurrent.futures.CancelledError):
        Slow._from_impl(_Slow()).cancel_itself()


@needs_sigint
def test_ctrl_c_cancels_the_call_and_waits_for_it_to_unwind():
    """Otherwise an interrupted exec or create carries on in the background."""
    impl = _Slow()
    _interrupt_main_after(impl.started)

    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        Slow._from_impl(impl).hang()

    assert impl.unwound, "returned before the interrupted call finished unwinding"
    assert time.monotonic() - started < 10


class _Feed:
    """A StreamReader's shape: each loop gets a view over one shared position,
    so closing a loop's view -- as a loop that stops part-way does -- never
    loses the stream's place."""

    def __init__(self):
        self.waiting = threading.Event()
        self.opened = threading.Event()
        self.position = 0

    def __aiter__(self):
        return _FeedView(self)


class _FeedView:
    def __init__(self, feed):
        self._feed = feed

    def __aiter__(self):
        return self

    async def __anext__(self):
        feed = self._feed
        if feed.position >= 3:
            raise StopAsyncIteration
        if feed.position == 1:
            feed.waiting.set()
            while not feed.opened.is_set():
                await asyncio.sleep(0.01)
        # Moved only once the item is certain to be handed over.
        feed.position += 1
        return feed.position

    async def aclose(self):
        pass


Feed = synchronize_api(_Feed, name="FeedForInterrupt")


@needs_sigint
def test_ctrl_c_mid_stream_loses_nothing():
    """The interrupted fetch is cancelled with its loop, and the stream's own
    position is what the next loop resumes from."""
    impl = _Feed()
    feed = Feed._from_impl(impl)
    seen = []
    _interrupt_main_after(impl.waiting)

    with pytest.raises(KeyboardInterrupt):
        for item in feed:
            seen.append(item)

    impl.opened.set()
    seen.extend(feed)
    assert seen == [1, 2, 3]


# -- process and thread lifecycle ------------------------------------------


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_a_forked_child_gets_its_own_loop():
    """The parent's loop thread does not survive a fork; the child's calls hung."""
    outer = _make(4)
    assert outer.double() == 8

    pid = os.fork()
    if pid == 0:
        try:
            ok = outer.double() == 8
        except BaseException:
            ok = False
        os._exit(0 if ok else 1)

    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        reaped, status = os.waitpid(pid, os.WNOHANG)
        if reaped:
            assert os.waitstatus_to_exitcode(status) == 0
            return
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    pytest.fail("the forked child's blocking call hung")


@pytest.fixture
def fresh_loop_afterwards():
    """For tests that kill the loop: give the tests after them a new one."""
    yield
    _sync._loop_thread._reset()


def test_a_dead_loop_thread_fails_calls_instead_of_hanging(fresh_loop_afterwards):
    outer = _make(1)
    outer.double()
    loop = _sync._loop_thread.get_loop()
    thread = _sync._loop_thread.thread

    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)
    loop.close()

    with pytest.raises(RuntimeError, match="died"):
        outer.double()


class _Stopper:
    async def stop_the_loop(self):
        # Ends the loop thread with this call still unanswered.
        asyncio.get_running_loop().stop()
        await asyncio.sleep(30)


Stopper = synchronize_api(_Stopper, name="StopperForDeadLoop")


def test_a_loop_thread_that_dies_mid_call_fails_that_call(
    fresh_loop_afterwards, monkeypatch
):
    monkeypatch.setattr(_sync, "_LIVENESS_POLL_SECONDS", 0.05)
    with pytest.raises(RuntimeError, match="died"):
        Stopper._from_impl(_Stopper()).stop_the_loop()


def test_a_loop_thread_that_dies_mid_aio_call_fails_that_call(
    fresh_loop_afterwards, monkeypatch
):
    monkeypatch.setattr(_sync, "_LIVENESS_POLL_SECONDS", 0.05)

    async def main():
        await Stopper._from_impl(_Stopper()).stop_the_loop.aio()

    with pytest.raises(RuntimeError, match="died"):
        asyncio.run(main())


class _Exiter:
    async def raise_it(self, exc):
        raise exc


Exiter = synchronize_api(_Exiter, name="ExiterForBaseExceptions")


@pytest.mark.parametrize("exc", [SystemExit(3), KeyboardInterrupt()])
def test_a_base_exception_reaches_the_caller_and_spares_the_loop(exc):
    """asyncio lets these two out of the loop, which ended the loop thread and
    every call after it. synchronicity re-raises them to the caller instead."""
    exiter = Exiter._from_impl(_Exiter())
    with pytest.raises(type(exc)):
        exiter.raise_it(exc)
    assert _make(4).double() == 8

    async def main():
        with pytest.raises(type(exc)):
            await exiter.raise_it.aio(exc)
        return await _make(5).double.aio()

    assert asyncio.run(main()) == 10


# -- parity with synchronicity ---------------------------------------------
#
# Each of these was measured against synchronicity 0.12.6, as Modal 1.6.1
# configures it, and failed here before the change it guards.


@needs_sigint
def test_system_exit_during_a_blocking_call_unwinds_the_work_first():
    """A signal handler that exits -- Ray's SIGTERM handler does -- raised
    SystemExit and left the work running; only Ctrl-C was cancelled and waited
    for. synchronicity treats every interruption the same way."""
    impl = _Slow()
    previous = signal.signal(signal.SIGUSR1, lambda *_: sys.exit(3))
    try:
        _interrupt_main_after(impl.started, sig=signal.SIGUSR1)
        with pytest.raises(SystemExit):
            Slow._from_impl(impl).hang()
    finally:
        signal.signal(signal.SIGUSR1, previous)
    assert impl.unwound, "raised before the interrupted call finished unwinding"


class _Exits:
    async def leave(self):
        raise SystemExit(4)


Exits = synchronize_api(_Exits, name="ExitsOnItsOwn")


def test_work_that_raises_system_exit_itself_is_not_cancelled_first():
    """Its own outcome, not an interruption: nothing to cancel or wait for."""
    with pytest.raises(SystemExit) as excinfo:
        Exits._from_impl(_Exits()).leave()
    assert excinfo.value.code == 4


_PENDING_WORK_AT_EXIT = """
import asyncio, sys
sys.path.insert(0, {python_dir!r})
from ray.experimental.sandbox.modal._sync import synchronize_api

class _Background:
    async def start(self):
        async def run():
            try:
                await asyncio.sleep(3600)
            finally:
                print("finalized at exit", flush=True)
        self.task = asyncio.get_running_loop().create_task(run())

Background = synchronize_api(_Background, name="Background")
Background._from_impl(_Background()).start()
print("script ends", flush=True)
"""


def test_work_still_running_at_exit_is_unwound_before_the_interpreter_goes(tmp_path):
    """synchronicity stops its loop at exit, cancelling what runs there. This
    loop's daemon thread was just abandoned: a task's `finally` never ran, and
    one still printing to stdout could do so during finalization."""
    import subprocess

    script = tmp_path / "pending_at_exit.py"
    python_dir = os.path.dirname(os.path.dirname(os.path.dirname(_sync.__file__)))
    script.write_text(
        _PENDING_WORK_AT_EXIT.format(python_dir=os.path.dirname(python_dir))
    )
    result = subprocess.run(
        [sys.executable, "-u", str(script)],
        capture_output=True,
        encoding="utf-8",
        timeout=60,
        cwd=tmp_path,
    )
    assert result.stdout == "script ends\nfinalized at exit\n", result.stderr
    assert result.returncode == 0


def test_a_call_whose_task_cannot_start_fails_instead_of_hanging(monkeypatch):
    """A failure to start the task on the loop left the caller waiting for a
    result that would never come, for as long as the loop thread lived."""

    def refuse(coro):
        coro.close()
        raise RuntimeError("cannot start")

    monkeypatch.setattr(_sync, "_guarded", refuse)
    outcome = {}

    def call():
        try:
            _make(2).double()
        except BaseException as exc:
            outcome["exc"] = exc

    caller = threading.Thread(target=call, daemon=True)
    caller.start()
    caller.join(10)
    assert not caller.is_alive(), "the call hung"
    assert isinstance(outcome.get("exc"), RuntimeError)


@pytest.mark.parametrize("how", ["break", "close()"])
def test_stopping_a_loop_part_way_closes_its_generator_before_the_next_line(how):
    """synchronicity closes it on the spot; this closed it a moment later."""
    tracked = Tracked._from_impl(_Tracked())
    if how == "break":
        for _ in tracked.items():
            break
    else:
        items = iter(tracked.items())
        next(items)
        items.close()
    assert tracked._impl.events == ["yield 0", "closed"]


# Driving the look-ahead directly, as a blocking and an async loop over one
# generator both do, to put a cancellation exactly where it races.


def test_an_item_fetched_as_its_fill_is_cancelled_is_not_lost():
    """The fetch completed in the very turn its fill was cancelled, and the
    fill dropped it: the next loop got the item after it."""

    async def scenario():
        state = _sync._Lookahead()
        fill = {}

        async def produce():
            # Cancel the fill just as this item is handed back.
            asyncio.get_running_loop().call_soon(fill["task"].cancel)
            yield "first"
            yield "second"

        agen = produce()
        fill["task"] = asyncio.ensure_future(_sync._fill(agen, state))
        with pytest.raises(asyncio.CancelledError):
            await fill["task"]
        await _sync._fill(agen, state)
        await _sync._fill(agen, state)
        return list(state.buffer)

    assert asyncio.run(scenario()) == ["first", "second"]


def test_closing_with_a_fetch_still_running_settles_it_first():
    """close() cancelled the fetch and closed the generator at once, while it
    was still running: RuntimeError, generator already running."""
    events = []

    async def scenario():
        state = _sync._Lookahead()

        async def produce():
            try:
                await asyncio.sleep(30)
                yield "never"
            finally:
                events.append("closed")

        agen = produce()
        fill = asyncio.ensure_future(_sync._fill(agen, state))
        await asyncio.sleep(0.05)
        fill.cancel()
        with pytest.raises(asyncio.CancelledError):
            await fill
        assert state.pending is not None, "the fetch is kept for the next loop"
        await _sync._close(agen, state)
        return state.exhausted

    assert asyncio.run(scenario()) is True
    assert events == ["closed"]


def test_two_consumers_of_one_generator_take_turns():
    """Two fills at once advanced the generator twice at once: RuntimeError,
    asynchronous generator is already running."""

    async def scenario():
        state = _sync._Lookahead()

        async def produce():
            for i in range(3):
                await asyncio.sleep(0.01)
                yield i

        agen = produce()
        await asyncio.gather(_sync._fill(agen, state), _sync._fill(agen, state))
        return list(state.buffer)

    assert asyncio.run(scenario()) == [0, 1]


def test_a_close_cancelled_while_it_settles_a_fetch_leaves_no_error_behind():
    async def scenario():
        state = _sync._Lookahead()

        async def produce():
            try:
                await asyncio.sleep(30)
                yield "never"
            finally:
                # Unwinding takes a moment, during which the close waits.
                await asyncio.sleep(0.1)

        agen = produce()
        fill = asyncio.ensure_future(_sync._fill(agen, state))
        await asyncio.sleep(0.05)
        fill.cancel()
        with pytest.raises(asyncio.CancelledError):
            await fill
        close = asyncio.ensure_future(_sync._close(agen, state))
        await asyncio.sleep(0.02)
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
        # Closed for good: a later loop ends at once rather than reopening.
        await _sync._fill(agen, state)
        return state.exhausted, list(state.buffer)

    assert asyncio.run(scenario()) == (True, [])


class _NeedsRayUnmarked:
    def __init__(self):
        self.runs = 0

    async def connect(self):
        self.runs += 1
        _sync.require_ray_connection()
        return "connected"


NeedsRayUnmarked = synchronize_api(_NeedsRayUnmarked, name="NeedsRayUnmarked")


@pytest.mark.parametrize("surface", ["blocking", "aio"])
def test_only_a_method_marked_needs_ray_is_run_again_after_connecting(
    surface, monkeypatch
):
    """Any method that reached require_ray_connection() was replayed, side
    effects and all, though only Sandbox.create is written to allow it."""
    inits = []
    monkeypatch.setattr(_sync.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(_sync.ray, "init", lambda *a, **k: inits.append(1))
    impl = _NeedsRayUnmarked()
    obj = NeedsRayUnmarked._from_impl(impl)
    with pytest.raises(RuntimeError, match="not marked @needs_ray"):
        if surface == "blocking":
            obj.connect()
        else:
            asyncio.run(obj.connect.aio())
    assert (impl.runs, inits) == (1, []), "neither connected nor run again"


class _Factory:
    def __init__(self, value):
        self.value = value

    @classmethod
    def build(cls, value):
        assert cls is _Factory, f"got {cls.__name__}, not the implementation"
        return cls(value)

    @staticmethod
    def build_static(value):
        return _Factory(value)

    @property
    def size(self):
        return self.value

    @size.deleter
    def size(self):
        self.value = None


Factory = synchronize_api(_Factory, name="Factory")


@pytest.mark.parametrize("method", ["build", "build_static"])
def test_sync_factories_get_the_implementation_and_return_a_wrapper(method):
    """Passed through untouched, a sync classmethod got this public class --
    whose constructor refuses -- and either kind returned a bare
    implementation object; synchronicity translates both ways."""
    made = getattr(Factory, method)(7)
    assert type(made) is Factory
    assert made.size == 7


def test_a_property_deleter_is_forwarded():
    made = Factory.build(7)
    del made.size
    assert made.size is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
