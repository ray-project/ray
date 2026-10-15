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


def test_aio_method_runs_on_the_background_loop():
    """As in synchronicity: awaited from the caller's loop, run on the module's."""

    class _Where:
        async def thread(self):
            return threading.current_thread()

    Where = synchronize_api(_Where, name="WhereForAio")

    async def main():
        outer = _make(21)
        assert await outer.double.aio() == 42
        assert await outer.add.aio(3, scale=2) == 48
        return await Where._from_impl(_Where()).thread.aio()

    ran_on = asyncio.run(main())
    assert ran_on is _sync._loop_thread.thread
    assert ran_on is not threading.current_thread()


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


# -- .aio runs where blocking calls run -------------------------------------


class _Stream:
    """Loop-bound state of the kinds a Sandbox's objects hold: a stream opened
    on first read and kept open, and a task a call leaves running."""

    def __init__(self):
        self.queue = None
        self.printed = []

    async def next_item(self):
        if self.queue is None:
            self.queue = asyncio.Queue()

            async def produce():
                for i in range(1000):
                    await self.queue.put(i)
                    await asyncio.sleep(0.001)

            self.producer = asyncio.ensure_future(produce())
        return await self.queue.get()

    async def start_printer(self, count):
        async def printer():
            for i in range(count):
                await asyncio.sleep(0.01)
                self.printed.append(i)

        self.printer = asyncio.ensure_future(printer())


Stream = synchronize_api(_Stream, name="StreamForLoopState")


def test_a_stream_opened_by_aio_is_read_on_in_a_second_asyncio_run():
    """It used to be bound to the first run's loop, and the second raised."""
    stream = Stream._from_impl(_Stream())
    assert asyncio.run(stream.next_item.aio()) == 0
    assert asyncio.run(stream.next_item.aio()) == 1


def test_aio_and_blocking_calls_read_one_open_stream():
    stream = Stream._from_impl(_Stream())
    assert asyncio.run(stream.next_item.aio()) == 0
    assert stream.next_item() == 1
    assert asyncio.run(stream.next_item.aio()) == 2


def test_aio_calls_from_several_threads_share_one_object():
    stream = Stream._from_impl(_Stream())

    def reader(_):
        async def main():
            return [await stream.next_item.aio() for _ in range(20)]

        return asyncio.run(main())

    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        seen = [item for batch in pool.map(reader, range(4)) for item in batch]
    assert sorted(seen) == list(range(80))


def test_a_task_started_by_aio_outlives_the_callers_loop():
    """As StreamType.STDOUT printing does: the caller's asyncio.run() ending
    used to cancel it there."""
    stream = Stream._from_impl(_Stream())

    async def main():
        await stream.start_printer.aio(5)

    asyncio.run(main())
    deadline = time.monotonic() + 5
    while len(stream._impl.printed) < 5 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert stream._impl.printed == [0, 1, 2, 3, 4]


class _Work:
    def __init__(self):
        self.states = []
        self.release = threading.Event()

    async def run(self, swallow=False, cancel_self=False):
        self.states.append("ready")
        if cancel_self:
            asyncio.current_task().cancel()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            self.states.append("cancelled")
            await asyncio.sleep(0.1)
            self.states.append("handled cancellation")
            if not swallow:
                raise
        return "done"

    async def block_the_loop(self):
        # Holds the background loop, so a cancellation cannot reach it yet.
        self.release.wait(5)
        return 1

    async def spin(self, seconds):
        time.sleep(seconds)

    async def sleep(self, seconds):
        await asyncio.sleep(seconds)
        return seconds


Work = synchronize_api(_Work, name="WorkForCancellation")


async def _started(work, **kwargs):
    task = asyncio.ensure_future(work.run.aio(**kwargs))
    while "ready" not in work._impl.states:
        await asyncio.sleep(0.01)
    return task


@pytest.mark.parametrize("swallow", [False, True])
def test_cancelling_aio_cancels_the_work_and_waits_for_it_to_unwind(swallow):
    """synchronicity's test_async_cancellation: the caller is cancelled only
    once the work has handled its cancellation -- and is cancelled even if the
    work swallowed it, since the caller asked to stop."""
    work = Work._from_impl(_Work())

    async def main():
        task = await _started(work, swallow=swallow)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    assert work._impl.states == ["ready", "cancelled", "handled cancellation"]


@pytest.mark.parametrize("swallow,outcome", [(False, "cancelled"), (True, "done")])
def test_work_that_cancels_itself_is_reported_as_it_ended(swallow, outcome):
    work = Work._from_impl(_Work())

    async def main():
        task = await _started(work, swallow=swallow, cancel_self=True)
        try:
            return await task
        except asyncio.CancelledError:
            return "cancelled"

    assert asyncio.run(main()) == outcome


def test_a_cancelled_aio_call_that_then_completes_still_raises():
    """synchronicity's test_async_cancel_completes_successfully_still_cancels:
    the work finished before the cancellation could reach it, and the caller,
    who asked to stop, is still told it was cancelled."""
    work = Work._from_impl(_Work())

    async def main():
        task = asyncio.ensure_future(work.block_the_loop.aio())
        await asyncio.sleep(0.1)
        task.cancel()
        await asyncio.sleep(0.1)
        # Waiting for the work: not cancelled yet.
        assert not task.done()
        work._impl.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())


def test_a_timeout_around_aio_waits_for_the_work_to_unwind():
    work = Work._from_impl(_Work())

    async def main():
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(work.run.aio(), 0.2)
        return list(work._impl.states)

    assert asyncio.run(main()) == ["ready", "cancelled", "handled cancellation"]


def test_blocking_work_inside_aio_does_not_stall_the_callers_loop():
    """synchronicity's test_function_async_block_event_loop."""
    work = Work._from_impl(_Work())

    async def main():
        started = time.monotonic()
        await asyncio.gather(work.spin.aio(0.5), asyncio.sleep(0.5))
        return time.monotonic() - started

    assert asyncio.run(main()) < 0.9


def test_many_aio_calls_run_concurrently():
    """synchronicity's test_function_many_parallel_async."""
    work = Work._from_impl(_Work())

    async def main():
        started = time.monotonic()
        coros = [work.sleep.aio(0.5) for _ in range(20)]
        assert all(asyncio.iscoroutine(c) for c in coros)
        assert await asyncio.gather(*coros) == [0.5] * 20
        return time.monotonic() - started

    assert asyncio.run(main()) < 1.0


def test_blocking_calls_from_many_threads_run_concurrently():
    """synchronicity's test_multithreaded."""
    work = Work._from_impl(_Work())
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(20) as pool:
        assert list(pool.map(work.sleep, [0.5] * 20)) == [0.5] * 20
    assert time.monotonic() - started < 1.0


_CALLER = __import__("contextvars").ContextVar("caller", default="unset")


class _Context:
    async def read(self):
        return _CALLER.get()


Context = synchronize_api(_Context, name="ContextForAio")


def test_aio_sees_the_callers_context_variables():
    async def main():
        _CALLER.set("from the caller")
        return await Context._from_impl(_Context()).read.aio()

    assert asyncio.run(main()) == "from the caller"


class _Nested:
    def __init__(self, outer):
        # A wrapper held, not passed: arguments arrive unwrapped.
        self.outer = outer

    async def inner_from_the_loop(self):
        # Implementation code on the loop awaiting another wrapper's .aio:
        # run inline, not handed to the loop it is already on.
        got = await self.outer.me.aio()
        return (
            got is self.outer,
            threading.current_thread() is _sync._loop_thread.thread,
        )


Nested = synchronize_api(_Nested, name="NestedForInline")


def test_aio_awaited_on_the_loop_runs_inline_and_still_translates():
    """synchronicity's test_async_inner_still_translates."""
    nested = Nested._from_impl(_Nested(_make(1)))
    assert nested.inner_from_the_loop() == (True, True)


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


def test_the_wrapper_cache_keeps_nothing_alive_after_aio_calls():
    """No cycle through the loop either: an object used only through .aio is
    freed as soon as its last wrapper goes."""
    impl = _Outer(1)
    wrapper = Outer._from_impl(impl)
    assert asyncio.run(wrapper.double.aio()) == 2
    impl_ref = weakref.ref(impl)
    gc.disable()
    try:
        del impl, wrapper
        assert impl_ref() is None
    finally:
        gc.enable()


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


def test_a_blocking_loop_that_stops_part_way_closes_its_generator():
    """synchronicity's test_ensure_stop_async_iteration: the generator is closed
    rather than left suspended -- for a StreamReader, the actor's stream."""
    tracked = Tracked._from_impl(_Tracked())
    for _ in tracked.items():
        break
    assert _until(lambda: "closed" in tracked._impl.events)
    assert tracked._impl.events == ["yield 0", "closed"]


def test_an_async_loop_that_stops_part_way_closes_its_generator():
    tracked = Tracked._from_impl(_Tracked())

    async def main():
        async for _ in tracked.items.aio():
            break

    asyncio.run(main())
    assert _until(lambda: "closed" in tracked._impl.events)
    assert tracked._impl.events == ["yield 0", "closed"]


def test_aclose_on_an_aio_generator_finishes_its_cleanup_first():
    """synchronicity's test_run_async_gen_runs_aclose_async."""
    tracked = Tracked._from_impl(_Tracked())

    async def main():
        items = tracked.items.aio()
        assert await items.__anext__() == 0
        await items.aclose()
        return list(tracked._impl.events)

    assert asyncio.run(main()) == ["yield 0", "closed"]


class _SelfChecking:
    def check(self):
        if not isinstance(self, _SelfChecking):
            raise ValueError(f"expected a _SelfChecking, got {type(self).__name__}")
        return "ok"

    async def acheck(self):
        if not isinstance(self, _SelfChecking):
            raise ValueError(f"expected a _SelfChecking, got {type(self).__name__}")
        return "ok"


SelfChecking = synchronize_api(_SelfChecking, name="SelfCheckingForClassCalls")


def test_a_class_level_call_with_a_foreign_self_reaches_the_implementation():
    """synchronicity's test_instance_method_called_on_class_passes_through."""
    obj = SelfChecking._from_impl(_SelfChecking())
    assert SelfChecking.check(obj) == "ok"
    assert SelfChecking.acheck(obj) == "ok"
    with pytest.raises(ValueError, match="got str"):
        SelfChecking.check("not one")
    with pytest.raises(ValueError, match="got str"):
        SelfChecking.acheck("not one")


_CTRL_C_UNDER_ASYNCIO_RUN = """
import asyncio, sys
sys.path.insert(0, {python_dir!r})
from ray.experimental.sandbox.modal._sync import synchronize_api

class _Runner:
    async def run(self):
        try:
            while True:
                await asyncio.sleep(0.2)
                print("running", flush=True)
        except asyncio.CancelledError:
            print("cancelled", flush=True)
            await asyncio.sleep(0.1)
            print("handled cancellation", flush=True)
            raise
        finally:
            await asyncio.sleep(0.1)
            print("exit async", flush=True)

Runner = synchronize_api(_Runner, name="Runner")

try:
    asyncio.run(Runner._from_impl(_Runner()).run.aio())
except KeyboardInterrupt:
    print("keyboard interrupt", flush=True)
"""


@pytest.mark.skipif(not hasattr(signal, "SIGINT"), reason="needs SIGINT")
def test_ctrl_c_during_an_aio_call_unwinds_the_work_before_the_program_sees_it(
    tmp_path,
):
    """synchronicity's test_shutdown_during_async_run, under asyncio.run."""
    import subprocess

    script = tmp_path / "ctrl_c_aio.py"
    python_dir = os.path.dirname(os.path.dirname(os.path.dirname(_sync.__file__)))
    script.write_text(
        _CTRL_C_UNDER_ASYNCIO_RUN.format(python_dir=os.path.dirname(python_dir))
    )
    with subprocess.Popen(
        [sys.executable, "-u", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ) as proc:
        assert proc.stdout.readline() == "running\n"
        proc.send_signal(signal.SIGINT)
        line = proc.stdout.readline()
        while line == "running\n":
            line = proc.stdout.readline()
        assert line == "cancelled\n"
        rest, stderr = proc.communicate(timeout=20)
    assert rest == "handled cancellation\nexit async\nkeyboard interrupt\n", stderr
    assert stderr == ""


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
def test_iteration_yields_every_item_in_order(count):
    assert list(Counted._from_impl(_Counted(count))) == list(range(count))


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
