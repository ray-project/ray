"""The wrapper behind the Modal-compatible API, held to synchronicity's behavior.

Modal builds its blocking classes and their ``.aio`` twins with the
``synchronicity`` package; this package does the same with its own
:mod:`~ray.experimental.sandbox.modal._sync`. Every test here runs twice,
once against a real ``synchronicity.Synchronizer`` and once against ``_sync``,
with identical assertions, so a behavior that differs fails one of the two.

Most are synchronicity's own tests (``test/*_test.py`` at v0.12.6), ported from
wrapped functions to the wrapped classes this package exposes. The rest pin
down where an ``.aio`` call runs, which is what a Sandbox's streams depend on.

Not covered, because ``_sync`` does not offer them and no API here needs them:
wrapping bare functions, constructing a wrapped class directly, ``_future=``,
and ``send``/``throw`` on a wrapped generator.

Skipped unless ``synchronicity`` is installed; it is not a Ray dependency.
"""

import asyncio
import concurrent.futures
import contextvars
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

synchronicity = pytest.importorskip("synchronicity")

from ray.experimental.sandbox.modal import _sync  # noqa: E402

SLEEP_DELAY = 0.2


class _Synchronicity:
    name = "synchronicity"

    def __init__(self):
        self.synchronizer = synchronicity.Synchronizer()

    def wrap(self, impl_cls, name):
        return self.synchronizer.create_blocking(impl_cls, name)

    def make(self, wrapper_cls, impl):
        return self.synchronizer._translate_out(impl)

    def impl(self, wrapper):
        return self.synchronizer._translate_in(wrapper)

    def loop_thread(self):
        return self.synchronizer._thread

    def close(self):
        self.synchronizer._close_loop()


class _RaySync:
    name = "ray"

    def wrap(self, impl_cls, name):
        return _sync.synchronize_api(impl_cls, name)

    def make(self, wrapper_cls, impl):
        return wrapper_cls._from_impl(impl)

    def impl(self, wrapper):
        return wrapper._impl

    def loop_thread(self):
        return _sync._loop_thread.thread

    def close(self):
        pass


@pytest.fixture(scope="module", params=["synchronicity", "ray"])
def backend(request):
    chosen = _Synchronicity() if request.param == "synchronicity" else _RaySync()
    yield chosen
    chosen.close()


# -- synchronicity_test.py: classes ----------------------------------------


class _MyClass:
    def __init__(self, x):
        self._x = x

    async def start(self):
        async def task():
            await asyncio.sleep(SLEEP_DELAY)
            return self._x

        self._task = asyncio.get_event_loop().create_task(task())

    async def get_result(self):
        return (await self._task) ** 2

    async def __aenter__(self):
        await asyncio.sleep(SLEEP_DELAY)
        return 42

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(SLEEP_DELAY)

    @staticmethod
    async def my_static_method():
        await asyncio.sleep(SLEEP_DELAY)
        return 43

    @classmethod
    async def my_class_method(cls):
        await asyncio.sleep(SLEEP_DELAY)
        return 44

    async def __aiter__(self):
        for i in range(self._x):
            yield i


def _between(started, low, high):
    elapsed = time.monotonic() - started
    assert low <= elapsed < high, elapsed


def test_class_sync(backend):
    MyClass = backend.wrap(_MyClass, "MyClassSync")
    obj = backend.make(MyClass, _MyClass(42))
    assert isinstance(obj, MyClass)
    # A task the first call starts, which the second awaits.
    obj.start()
    assert obj.get_result() == 1764

    started = time.monotonic()
    with obj as z:
        assert z == 42
        _between(started, SLEEP_DELAY, 2 * SLEEP_DELAY)
    assert time.monotonic() - started >= 2 * SLEEP_DELAY

    started = time.monotonic()
    assert MyClass.my_static_method() == 43
    _between(started, SLEEP_DELAY, 2 * SLEEP_DELAY)
    assert MyClass.my_class_method() == 44
    assert list(obj) == list(range(42))


def test_class_async_as_method_attribute(backend):
    MyClass = backend.wrap(_MyClass, "MyClassAio")
    obj = backend.make(MyClass, _MyClass(42))

    async def main():
        await obj.start.aio()
        coro = obj.get_result.aio()
        assert asyncio.iscoroutine(coro)
        assert await coro == 1764
        started = time.monotonic()
        async with obj as z:
            assert z == 42
            _between(started, SLEEP_DELAY, 2 * SLEEP_DELAY)
        assert [z async for z in obj] == list(range(42))
        assert await obj.my_static_method.aio() == 43
        assert await obj.my_class_method.aio() == 44

    asyncio.run(main())


class _Foo:
    async def get_self(self):
        return self


def test_blocking_nested_aio_returns_blocking_obj(backend):
    Foo = backend.wrap(_Foo, "FooNested")
    original = backend.make(Foo, _Foo())
    assert original.get_self() is original

    async def main():
        return await original.get_self.aio()

    assert asyncio.run(main()) is original


async def _some_async_gen():
    yield "foo"
    yield "bar"


class _It:
    def __aiter__(self):
        self._gen = _some_async_gen()
        return self

    async def __anext__(self):
        return await self._gen.__anext__()

    async def aclose(self):
        await self._gen.aclose()


def test_non_async_aiter(backend):
    It = backend.wrap(_It, "ItNonAsyncAiter")

    async def main():
        it = backend.make(It, _It())
        assert [v async for v in it] == ["foo", "bar"]
        await it.aclose()

    asyncio.run(main())
    it = backend.make(It, _It())
    assert list(it) == ["foo", "bar"]
    it.close()


class _Cancellable:
    def __init__(self):
        self.states = []
        self.release = threading.Event()

    async def foo(self, abort_cancellation, cancel_self=False):
        self.states.append("ready")
        if cancel_self:
            asyncio.current_task().cancel()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            self.states.append("cancelled")
            await asyncio.sleep(0.1)
            self.states.append("handled cancellation")
            if not abort_cancellation:
                raise
        return "done"

    async def well_behaved(self):
        self.release.wait(5)
        return 1


def test_async_cancellation(backend):
    Cancellable = backend.wrap(_Cancellable, "CancellableParity")

    async def start_task(obj, **kwargs):
        backend.impl(obj).states.clear()
        task = asyncio.create_task(obj.foo.aio(**kwargs))
        while "ready" not in backend.impl(obj).states:
            await asyncio.sleep(0.01)
        return task

    async def main():
        obj = backend.make(Cancellable, _Cancellable())
        expected = ["ready", "cancelled", "handled cancellation"]

        # Case 1: a cancel in the caller goes in and comes back out.
        task = await start_task(obj, abort_cancellation=False)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert backend.impl(obj).states == expected

        # Case 3: the work cancelling itself comes back out.
        task = await start_task(obj, abort_cancellation=False, cancel_self=True)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert backend.impl(obj).states == expected

        # Case 4: the work cancels itself and catches it: not propagated.
        task = await start_task(obj, abort_cancellation=True, cancel_self=True)
        assert await task == "done"
        assert backend.impl(obj).states == expected

    asyncio.run(main())


def test_async_cancel_completes_successfully_still_cancels(backend):
    Cancellable = backend.wrap(_Cancellable, "CancellableRace")
    obj = backend.make(Cancellable, _Cancellable())

    async def main():
        task = asyncio.create_task(obj.well_behaved.aio())
        await asyncio.sleep(0.1)
        assert not task.done()
        task.cancel()
        await asyncio.sleep(0.1)
        assert not task.cancelled()
        backend.impl(obj).release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())


class _Timing:
    async def spinlock(self):
        time.sleep(SLEEP_DELAY)

    async def f(self, x):
        await asyncio.sleep(SLEEP_DELAY)
        return x**2


def test_function_async_block_event_loop(backend):
    Timing = backend.wrap(_Timing, "TimingBlock")
    obj = backend.make(Timing, _Timing())

    async def main():
        started = time.monotonic()
        await asyncio.gather(obj.spinlock.aio(), asyncio.sleep(SLEEP_DELAY))
        _between(started, SLEEP_DELAY, 1.5 * SLEEP_DELAY)

    asyncio.run(main())


def test_function_many_parallel_async(backend):
    Timing = backend.wrap(_Timing, "TimingParallel")
    obj = backend.make(Timing, _Timing())

    async def main():
        started = time.monotonic()
        coros = [obj.f.aio(i) for i in range(20)]
        assert asyncio.iscoroutine(coros[0])
        assert time.monotonic() - started < 0.05
        assert await asyncio.gather(*coros) == [z**2 for z in range(20)]
        _between(started, SLEEP_DELAY, 2 * SLEEP_DELAY)

    asyncio.run(main())


def test_multithreaded(backend):
    """threading_test.py."""
    Timing = backend.wrap(_Timing, "TimingThreads")
    obj = backend.make(Timing, _Timing())
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        assert list(executor.map(obj.f, range(20))) == [i**2 for i in range(20)]
    _between(started, SLEEP_DELAY, 2 * SLEEP_DELAY)


class _Translates:
    async def inner(self):
        return _Foo()

    async def outer(self):
        # On the loop, through a wrapper's .aio: the result comes back as a
        # wrapper, not the implementation. (No class is passed in to compare
        # against -- synchronicity translates class arguments as well.)
        v = await self.wrapper.inner.aio()
        return not isinstance(v, _Foo) and hasattr(v, "get_self")


def test_async_inner_still_translates(backend):
    backend.wrap(_Foo, "FooInner")
    Translates = backend.wrap(_Translates, "TranslatesInner")
    impl = _Translates()
    impl.wrapper = backend.make(Translates, impl)
    assert impl.wrapper.outer() is True


class _Finalizing:
    def __init__(self):
        self.states = []

    async def genfunc(self, finalization_delay):
        self.states.append("init")
        try:
            for i in range(2):
                await asyncio.sleep(0.01)
                self.states.append(f"yield {i}")
                yield i
                self.states.append(f"send after {i}")
        except BaseException:
            self.states.append("start finalization")
            await asyncio.sleep(finalization_delay)
            self.states.append("done")


def _until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.01)
    return predicate()


def test_run_async_gen_runs_aclose(backend):
    Finalizing = backend.wrap(_Finalizing, "FinalizingSync")
    obj = backend.make(Finalizing, _Finalizing())
    states = backend.impl(obj).states
    for _ in obj.genfunc(0.01):
        pass
    assert states == ["init", "yield 0", "send after 0", "yield 1", "send after 1"]

    states.clear()
    agen = iter(obj.genfunc(0.01))
    next(agen)
    del agen
    # synchronicity finishes this before `del` returns; _sync schedules it on
    # the loop. Either way it runs, and runs promptly.
    assert _until(lambda: states[-1:] == ["done"])
    assert states == ["init", "yield 0", "start finalization", "done"]


def test_run_async_gen_runs_aclose_async(backend):
    Finalizing = backend.wrap(_Finalizing, "FinalizingAsync")
    obj = backend.make(Finalizing, _Finalizing())
    states = backend.impl(obj).states

    async def main():
        async for _ in obj.genfunc.aio(0.01):
            pass
        assert states == ["init", "yield 0", "send after 0", "yield 1", "send after 1"]
        states.clear()
        agen = obj.genfunc.aio(0.01)
        assert await agen.__anext__() == 0
        await agen.aclose()
        assert states == ["init", "yield 0", "start finalization", "done"]

    asyncio.run(main())


# -- generators_test.py ----------------------------------------------------


class _Producer:
    def __init__(self):
        self.events = []

    async def async_producer(self):
        for i in range(10):
            self.events.append("producer")
            yield i

    async def ensure_stop(self):
        try:
            yield 42
            yield 43
        except BaseException as exc:
            self.events.append(exc)


def test_generator_order_sync(backend):
    Producer = backend.wrap(_Producer, "ProducerSync")
    obj = backend.make(Producer, _Producer())
    events = backend.impl(obj).events
    for _ in obj.async_producer():
        events.append("consumer")
    assert events == ["producer", "consumer"] * 10


def test_generator_order_async(backend):
    Producer = backend.wrap(_Producer, "ProducerAsync")
    obj = backend.make(Producer, _Producer())
    events = backend.impl(obj).events

    async def main():
        async for _ in obj.async_producer.aio():
            events.append("consumer")

    asyncio.run(main())
    assert events == ["producer", "consumer"] * 10


def test_ensure_stop_async_iteration(backend):
    Producer = backend.wrap(_Producer, "ProducerStop")
    obj = backend.make(Producer, _Producer())
    events = backend.impl(obj).events

    def create_generator():
        for _ in obj.ensure_stop():
            break

    create_generator()
    assert _until(lambda: len(events) == 1)
    assert isinstance(events[0], GeneratorExit)


class _MyGenerator:
    def __init__(self):
        self.events = []

    def __aiter__(self):
        return self._producer()

    async def _producer(self):
        for i in range(10):
            self.events.append("producer")
            yield i


def test_custom_generator(backend):
    MyGenerator = backend.wrap(_MyGenerator, "MyGeneratorCustom")
    obj = backend.make(MyGenerator, _MyGenerator())
    events = backend.impl(obj).events
    for _ in obj:
        events.append("consumer")
    assert events == ["producer", "consumer"] * 10


# -- exception_test.py -----------------------------------------------------


class CustomExceptionCause(Exception):
    pass


class CustomException(Exception):
    pass


class CustomBaseException(BaseException):
    pass


class _Raises:
    async def f_raises(self, exc):
        await asyncio.sleep(0.01)
        raise exc

    async def f_raises_with_cause(self):
        await asyncio.sleep(0.01)
        raise CustomException("something failed") from CustomExceptionCause("cause")

    async def ok(self):
        return "alive"


def test_function_raises_with_cause_sync_and_async(backend):
    Raises = backend.wrap(_Raises, "RaisesCause")
    obj = backend.make(Raises, _Raises())
    with pytest.raises(CustomException) as info:
        obj.f_raises_with_cause()
    assert isinstance(info.value.__cause__, CustomExceptionCause)

    async def main():
        with pytest.raises(CustomException) as info:
            await obj.f_raises_with_cause.aio()
        return info.value

    assert isinstance(asyncio.run(main()).__cause__, CustomExceptionCause)


def test_function_raises_without_a_confusing_context(backend):
    Raises = backend.wrap(_Raises, "RaisesContext")
    obj = backend.make(Raises, _Raises())
    with pytest.raises(CustomException) as info:
        obj.f_raises(CustomException("something failed"))
    assert info.value.__suppress_context__ or info.value.__context__ is None


@pytest.mark.parametrize(
    "exc",
    [
        Exception("foo"),
        CustomException("bar"),
        KeyboardInterrupt(),
        SystemExit(),
        CustomBaseException(),
        TimeoutError(),
    ],
    ids=lambda exc: type(exc).__name__,
)
def test_raising_various_exceptions(backend, exc):
    Raises = backend.wrap(_Raises, f"Raises{type(exc).__name__}")
    obj = backend.make(Raises, _Raises())
    with pytest.raises(type(exc)):
        obj.f_raises(exc)
    # And the loop survives it.
    assert obj.ok() == "alive"

    async def main():
        with pytest.raises(type(exc)):
            await obj.f_raises.aio(exc)
        return await obj.ok.aio()

    assert asyncio.run(main()) == "alive"


# -- translate_test.py -----------------------------------------------------


class _Thing:
    pass


class _ThingProvider:
    def __init__(self):
        self.thing = _Thing()

    async def get(self):
        return self.thing

    async def get2(self):
        return [self.thing, self.thing]

    @property
    def pget(self):
        return self.thing

    async def set(self, thing):
        assert type(thing) is _Thing
        self.thing = thing


def test_translate(backend):
    Thing = backend.wrap(_Thing, "ThingTranslate")
    ThingProvider = backend.wrap(_ThingProvider, "ThingProviderTranslate")
    provider = backend.make(ThingProvider, _ThingProvider())
    first = provider.get()
    assert first is provider.get()
    assert provider.get2() == [first, first]
    assert isinstance(provider.pget, Thing)
    other = backend.make(Thing, _Thing())
    provider.set(other)
    assert provider.get() is other


# -- where an .aio call runs ------------------------------------------------


class _LoopState:
    def __init__(self):
        self.queue = None
        self.printed = []

    async def where(self):
        return threading.current_thread()

    async def next_item(self):
        if self.queue is None:
            self.queue = asyncio.Queue()

            async def produce():
                for i in range(100):
                    await self.queue.put(i)
                    await asyncio.sleep(0.001)

            self.producer = asyncio.ensure_future(produce())
        return await self.queue.get()

    async def start_printer(self):
        async def printer():
            for i in range(5):
                await asyncio.sleep(0.02)
                self.printed.append(i)

        self.task = asyncio.ensure_future(printer())

    async def read_var(self):
        return _VAR.get()


_VAR = contextvars.ContextVar("parity_var", default="unset")


def test_aio_runs_on_the_background_loop(backend):
    LoopState = backend.wrap(_LoopState, "LoopStateWhere")
    obj = backend.make(LoopState, _LoopState())
    assert asyncio.run(obj.where.aio()) is backend.loop_thread()
    assert obj.where() is backend.loop_thread()


def test_a_stream_survives_into_a_second_asyncio_run(backend):
    LoopState = backend.wrap(_LoopState, "LoopStateStream")
    obj = backend.make(LoopState, _LoopState())
    assert asyncio.run(obj.next_item.aio()) == 0
    assert asyncio.run(obj.next_item.aio()) == 1
    assert obj.next_item() == 2


def test_a_task_started_by_aio_outlives_the_callers_loop(backend):
    LoopState = backend.wrap(_LoopState, "LoopStateTask")
    obj = backend.make(LoopState, _LoopState())

    async def main():
        await obj.start_printer.aio()

    asyncio.run(main())
    assert _until(lambda: backend.impl(obj).printed == [0, 1, 2, 3, 4])


def test_aio_sees_the_callers_context_variables(backend):
    LoopState = backend.wrap(_LoopState, "LoopStateContext")
    obj = backend.make(LoopState, _LoopState())

    async def main():
        _VAR.set("caller")
        return await obj.read_var.aio()

    assert asyncio.run(main()) == "caller"


class _SelfChecking:
    def check(self):
        if not isinstance(self, _SelfChecking):
            raise ValueError(f"expected a _SelfChecking, got {type(self).__name__}")
        return "ok"


def test_instance_method_called_on_class_passes_through(backend):
    SelfChecking = backend.wrap(_SelfChecking, "SelfCheckingParity")
    obj = backend.make(SelfChecking, _SelfChecking())
    assert SelfChecking.check(obj) == "ok"
    with pytest.raises(ValueError, match="got str"):
        SelfChecking.check("not_an_instance")


# -- shutdown_test.py: Ctrl-C during an .aio call under asyncio.run ---------


_SCRIPT = """
import asyncio, sys
sys.path.insert(0, {python_dir!r})

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

if {backend!r} == "synchronicity":
    from synchronicity import Synchronizer
    s = Synchronizer()
    Runner = s.create_blocking(_Runner, "Runner")
    runner = s._translate_out(_Runner())
else:
    from ray.experimental.sandbox.modal._sync import synchronize_api
    Runner = synchronize_api(_Runner, "Runner")
    runner = Runner._from_impl(_Runner())

try:
    asyncio.run(runner.run.aio())
except KeyboardInterrupt:
    print("keyboard interrupt", flush=True)
"""


@pytest.mark.skipif(not hasattr(signal, "SIGINT"), reason="needs SIGINT")
def test_shutdown_during_async_run(backend, tmp_path):
    python_dir = os.path.abspath(
        os.path.join(os.path.dirname(_sync.__file__), "..", "..", "..", "..")
    )
    script = tmp_path / "shutdown_async_run.py"
    script.write_text(_SCRIPT.format(python_dir=python_dir, backend=backend.name))
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
