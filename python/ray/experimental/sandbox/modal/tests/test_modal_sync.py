"""Unit tests for the sync/async wrapper behind the Modal-compatible API.

Nothing here touches Ray or runsc; it exercises :func:`synchronize_api` on
small purpose-built classes.
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

    async def add(self, amount, *, scale=1):
        return (self.value + amount) * scale

    async def boom(self):
        raise RuntimeError("kaboom")

    async def counts(self, n):
        for i in range(n):
            await asyncio.sleep(0)
            yield i

    async def unwrap(self, other):
        """Receives an object that the caller passed as a blocking wrapper."""
        assert isinstance(other, _Inner), type(other)
        return await other.get()

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


# -- blocking surface ------------------------------------------------------


def test_coroutine_method_blocks_and_returns_value():
    assert _make(21).double() == 42


def test_coroutine_method_forwards_args_and_kwargs():
    assert _make(2).add(3, scale=10) == 50


def test_static_factory_returns_a_wrapped_object():
    built = Outer.build(7)
    assert isinstance(built, Outer)
    assert built.double() == 14


def test_property_returns_a_wrapped_object():
    inner = _make(3).inner
    assert isinstance(inner, Inner)
    assert inner.get() == 30


def test_wrapper_arguments_are_unwrapped_on_the_way_in():
    outer = _make(1)
    assert outer.unwrap(Inner._from_impl(_Inner(99))) == 99


def test_async_generator_is_iterable_from_blocking_code():
    assert list(_make().counts(3)) == [0, 1, 2]


def test_exception_propagates_across_the_loop_boundary():
    with pytest.raises(RuntimeError, match="kaboom"):
        _make().boom()


def test_direct_construction_is_rejected():
    with pytest.raises(InvalidError, match="no public constructor"):
        Outer(1)


# -- .aio surface ----------------------------------------------------------


def test_aio_method_runs_on_the_callers_loop():
    async def main():
        outer = _make(21)
        # A loop of the caller's own, not the module's background one.
        assert await outer.double.aio() == 42
        assert await outer.add.aio(3, scale=2) == 48

    asyncio.run(main())


def test_aio_static_factory_returns_a_wrapped_object():
    async def main():
        built = await Outer.build.aio(5)
        assert isinstance(built, Outer)
        return await built.double.aio()

    assert asyncio.run(main()) == 10


def test_aio_async_generator_is_async_iterable():
    async def main():
        return [value async for value in _make().counts.aio(3)]

    assert asyncio.run(main()) == [0, 1, 2]


def test_aio_exception_propagates():
    async def main():
        with pytest.raises(RuntimeError, match="kaboom"):
            await _make().boom.aio()

    asyncio.run(main())


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


def test_repeated_blocking_calls_reuse_one_loop():
    outer = _make(1)
    assert [outer.double() for _ in range(5)] == [2] * 5


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


@pytest.mark.parametrize("count", [0, 1, 5, 300])
def test_batching_yields_every_item_in_order(count):
    """Batch size must not change what iteration produces."""
    assert list(Counted._from_impl(_Counted(count))) == list(range(count))


def test_a_ready_generator_is_drained_in_one_crossing():
    """The whole point: consecutive ready items must not each cost a crossing."""
    impl = _Counted(50)
    iterator = iter(Counted._from_impl(impl))

    first = next(iterator)

    assert first == 0
    # Nothing here awaits, so everything was available and should have come
    # back with the first item rather than one crossing at a time.
    assert impl.produced > 1, "ready items were not batched"


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


def test_closing_part_way_through_does_not_error():
    iterator = iter(Counted._from_impl(_Counted(300)))
    next(iterator)
    iterator.close()


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


def test_generated_classes_accept_a_generic_subscript():
    """Modal's public classes are Generic, so ported annotations subscript them."""
    assert Outer[int] is Outer


# -- translation and identity ----------------------------------------------


def test_one_implementation_object_has_one_wrapper():
    """As in synchronicity: `sb.stdout is sb.stdout`, and `self` comes back as is."""
    outer = _make(3)
    assert outer.inner is outer.inner
    assert outer.me() is outer
    assert Outer.build(1) is not Outer.build(1)


def test_the_wrapper_cache_keeps_nothing_alive():
    """No cycle: dropping the last wrapper frees the implementation at once.

    That matters because a Sandbox lives only as long as its handle -- a cycle
    would leave its actor running until the cycle collector got round to it.
    """
    impl = _Outer(1)
    wrapper = Outer._from_impl(impl)
    inner = wrapper.inner
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


def _interrupt_main_after(event, delay=0.2):
    """Deliver SIGINT to the main thread, as Ctrl-C does, once ``event`` is set.

    The delay lets the main thread get back to waiting on the loop first.
    """

    def _fire():
        if event.wait(10):
            time.sleep(delay)
            signal.pthread_kill(threading.main_thread().ident, signal.SIGINT)

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
    """A memoized stream, as StreamReader's is: every `for` resumes one generator."""

    def __init__(self):
        self.waiting = threading.Event()
        self.opened = threading.Event()
        self._gen = self._produce()

    async def _produce(self):
        yield 1
        self.waiting.set()
        while not self.opened.is_set():
            await asyncio.sleep(0.01)
        yield 2
        yield 3

    def __aiter__(self):
        return self._gen


Feed = synchronize_api(_Feed, name="FeedForInterrupt")


@needs_sigint
def test_ctrl_c_mid_stream_loses_nothing():
    """The fetch in flight survives, so a later loop picks up where this one was."""
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


class _Exiter:
    async def exit_the_loop(self):
        raise SystemExit(3)


Exiter = synchronize_api(_Exiter, name="ExiterForDeadLoop")


def test_a_loop_thread_that_dies_mid_call_fails_that_call(
    fresh_loop_afterwards, monkeypatch
):
    """A SystemExit inside a task takes the loop down before it can reply."""
    monkeypatch.setattr(_sync, "_LIVENESS_POLL_SECONDS", 0.05)

    with pytest.raises(RuntimeError, match="died") as info:
        Exiter._from_impl(_Exiter()).exit_the_loop()

    assert isinstance(info.value.__cause__, SystemExit)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
