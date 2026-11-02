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

The ``synchronicity`` runs are skipped unless it is installed; it is not a Ray
dependency, so CI runs only the ``_sync`` half.
"""

import asyncio
import concurrent.futures
import contextvars
import inspect
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

from ray.experimental.sandbox.modal import _sync

try:
    import synchronicity
except ImportError:
    synchronicity = None

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

    def start_loop(self):
        return self.synchronizer._start_loop()

    def stop_loop(self):
        self.synchronizer._close_loop()

    def full_tracebacks(self, monkeypatch):
        monkeypatch.setattr("synchronicity.exceptions.SYNCHRONICITY_TRACEBACK", True)

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

    def start_loop(self):
        return _sync._loop_thread.get_loop()

    def stop_loop(self):
        _sync._loop_thread.close()

    def full_tracebacks(self, monkeypatch):
        monkeypatch.setenv("SYNCHRONICITY_TRACEBACK", "1")

    def close(self):
        pass


@pytest.fixture(scope="module", params=["synchronicity", "ray"])
def backend(request):
    if request.param == "synchronicity" and synchronicity is None:
        pytest.skip("synchronicity is not installed")
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


# -- inspect_test.py / docstring_test.py: what .aio looks like ---------------


class _Api:
    """Api docs"""

    def blocking_func(self):
        return 1

    async def async_func(self):
        """async_func docs"""
        return 2

    async def items(self):
        """items docs"""
        yield 3


def test_inspect_coroutinefunction(backend):
    Api = backend.wrap(_Api, "ApiInspect")
    obj = backend.make(Api, _Api())
    assert inspect.iscoroutinefunction(Api.blocking_func) is False
    assert inspect.iscoroutinefunction(Api.async_func) is False
    assert hasattr(obj.blocking_func, "aio") is False
    # What frameworks check before deciding to await a callable.
    assert inspect.iscoroutinefunction(obj.async_func.aio) is True
    assert inspect.iscoroutinefunction(Api.async_func.aio) is True


def test_docs_and_doc_transfer(backend):
    Api = backend.wrap(_Api, "ApiDocs")
    obj = backend.make(Api, _Api())
    assert Api.__doc__ == "Api docs"
    assert Api.async_func.__doc__ == "async_func docs"
    assert Api.async_func.aio.__doc__ == "async_func docs"
    assert obj.async_func.__doc__ == "async_func docs"
    assert obj.async_func.aio.__doc__ == "async_func docs"
    assert obj.items.aio.__doc__ == "items docs"


def test_generator_types(backend):
    """synchronicity's test_generator_sync/async check these; here the
    iterators are objects of their own, a documented difference that keeps
    the look-ahead and a StreamReader's chunked hand-over."""
    Api = backend.wrap(_Api, "ApiGeneratorTypes")
    obj = backend.make(Api, _Api())
    is_synchronicity = backend.name == "synchronicity"
    blocking, asynchronous = obj.items(), obj.items.aio()
    assert inspect.isgenerator(blocking) is is_synchronicity
    assert inspect.isasyncgen(asynchronous) is is_synchronicity
    # Either way they iterate the same.
    assert list(blocking) == [3]

    async def drain():
        return [item async for item in asynchronous]

    assert asyncio.run(drain()) == [3]


# -- synchronicity_test.py: .aio as an attribute ------------------------------


class _Squarer:
    # A wrapper this implementation holds itself. synchronicity's own test
    # passes a function's .aio as an argument; a *bound* .aio passed that way
    # is translated by synchronicity into the unbound implementation method,
    # so the port holds it here rather than passing it.
    peer = None

    async def square(self, x):
        await asyncio.sleep(SLEEP_DELAY)
        return x * x

    async def square_through(self, x):
        # An .aio called from the background loop itself runs inline there.
        return await self.peer.square.aio(x)


def test_function_async_as_function_attribute(backend):
    Squarer = backend.wrap(_Squarer, "SquarerAio")
    impl = _Squarer()
    obj = backend.make(Squarer, impl)
    impl.peer = obj

    async def main():
        started = time.monotonic()
        coro = obj.square.aio(42)
        assert inspect.iscoroutine(coro)
        assert time.monotonic() - started < SLEEP_DELAY, "ran before it was awaited"
        assert await coro == 1764
        _between(started, SLEEP_DELAY - 0.02, 2 * SLEEP_DELAY)
        assert await obj.square_through.aio(42) == 1764

    asyncio.run(main())


# -- tracebacks_test.py ---------------------------------------------------------

needs_311 = pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="frames are trimmed from 3.11, where a re-raise keeps the traceback",
)


class _TracebackError(Exception):
    pass


class _TracebackRaises:
    async def raise_something(self, exc):
        raise exc

    async def gen(self):
        raise _TracebackError("gen boom!")
        yield

    async def recurse(self, n):
        if n == 0:
            raise _TracebackError("boom!")
        return await self.recurse(n - 1)

    async def __aenter__(self):
        raise _TracebackError("enter boom!")

    async def __aexit__(self, *exc_info):
        pass


def _outside_frames(tb):
    """Frames from anywhere but this file: the machinery of crossing threads."""
    import traceback

    frames = traceback.extract_tb(tb)
    assert any(frame.filename == __file__ for frame in frames)
    return [frame.filename for frame in frames if frame.filename != __file__]


@needs_311
@pytest.mark.parametrize("call", ["sync", "aio", "sync_gen", "aio_gen", "recursive"])
def test_tracebacks_show_only_user_code(backend, call):
    Raises = backend.wrap(_TracebackRaises, f"Raises_{call}")
    obj = backend.make(Raises, _TracebackRaises())

    async def iterate_aio():
        async for _ in obj.gen.aio():
            pass

    with pytest.raises(_TracebackError) as exc_info:
        if call == "sync":
            obj.raise_something(_TracebackError("boom!"))
        elif call == "aio":
            asyncio.run(obj.raise_something.aio(_TracebackError("boom!")))
        elif call == "sync_gen":
            for _ in obj.gen():
                pass
        elif call == "aio_gen":
            asyncio.run(iterate_aio())
        else:
            obj.recurse(10)
    outside = _outside_frames(exc_info.tb)
    if call == "aio" or call == "aio_gen":
        # asyncio.run() itself, which is the caller's own frame stack.
        outside = [f for f in outside if "asyncio" not in f]
    assert outside == []


@needs_311
@pytest.mark.parametrize("form", ["with", "async with"])
def test_tracebacks_through_a_context_manager_show_only_user_code(backend, form):
    Raises = backend.wrap(_TracebackRaises, f"RaisesCtx_{form.replace(' ', '_')}")
    obj = backend.make(Raises, _TracebackRaises())

    async def enter_async():
        async with obj:
            pass

    with pytest.raises(_TracebackError) as exc_info:
        if form == "with":
            with obj:
                pass
        else:
            asyncio.run(enter_async())
    outside = [f for f in _outside_frames(exc_info.tb) if "asyncio" not in f]
    assert outside == []


def test_full_traceback_flag(backend, monkeypatch):
    backend.full_tracebacks(monkeypatch)
    Raises = backend.wrap(_TracebackRaises, "RaisesFull")
    obj = backend.make(Raises, _TracebackRaises())
    with pytest.raises(_TracebackError) as exc_info:
        obj.raise_something(_TracebackError("boom!"))
    assert _outside_frames(exc_info.tb), "the internal frames are kept"


# -- threading_test.py / synchronicity_test.py: the loop itself -------------------


def test_start_loop(backend):
    """No race between threads starting the loop at once: they get one."""
    backend.stop_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loops = list(executor.map(lambda _: backend.start_loop(), range(1000)))
    assert len({id(loop) for loop in loops}) == 1
    assert isinstance(loops[0], asyncio.AbstractEventLoop)


def test_event_loop_restarts_after_it_was_stopped(backend):
    Squarer = backend.wrap(_Squarer, "SquarerRestart")
    obj = backend.make(Squarer, _Squarer())
    assert obj.square(3) == 9
    first = backend.loop_thread()
    assert first.is_alive()
    backend.stop_loop()
    assert not first.is_alive()
    assert obj.square(4) == 16, "a call after stopping starts a fresh loop"
    assert backend.loop_thread() is not first
    assert backend.loop_thread().is_alive()


# -- shutdown_test.py: Ctrl-C around a context manager --------------------------

_CTX_MGR_SCRIPT = """
import asyncio, sys, time
sys.path.insert(0, {python_dir!r})

class _CtxMgr:
    async def __aenter__(self):
        if sys.argv[1] == "enter":
            try:
                while True:
                    print("enter", flush=True)
                    await asyncio.sleep(0.3)
            finally:
                print("exit", flush=True)
        return self

    async def __aexit__(self, *exc_info):
        print("exit", flush=True)

if {backend!r} == "synchronicity":
    from synchronicity import Synchronizer
    s = Synchronizer()
    CtxMgr = s.create_blocking(_CtxMgr, "CtxMgr")
    ctx_mgr = s._translate_out(_CtxMgr())
else:
    from ray.experimental.sandbox.modal._sync import synchronize_api
    CtxMgr = synchronize_api(_CtxMgr, "CtxMgr")
    ctx_mgr = CtxMgr._from_impl(_CtxMgr())

try:
    with ctx_mgr:
        while True:
            print("in ctx", flush=True)
            time.sleep(0.3)
except KeyboardInterrupt:
    print("keyboard interrupt", flush=True)
"""


@pytest.mark.skipif(not hasattr(signal, "SIGINT"), reason="needs SIGINT")
@pytest.mark.parametrize("stage, line", [("enter", "enter"), ("yield", "in ctx")])
def test_shutdown_during_ctx_mgr(backend, tmp_path, stage, line):
    """synchronicity's test_shutdown_during_ctx_mgr_setup and _yield, with a
    class context manager: both forms are what this package's wrapper maps."""
    python_dir = os.path.abspath(
        os.path.join(os.path.dirname(_sync.__file__), "..", "..", "..", "..")
    )
    script = tmp_path / "shutdown_ctx_mgr.py"
    script.write_text(
        _CTX_MGR_SCRIPT.format(python_dir=python_dir, backend=backend.name)
    )
    with subprocess.Popen(
        [sys.executable, "-u", str(script), stage],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ) as proc:
        for _ in range(2):
            assert proc.stdout.readline() == f"{line}\n"
        proc.send_signal(signal.SIGINT)
        rest, stderr = proc.communicate(timeout=20)
    # Any repeats of the last line are timing; what follows them is not.
    rest = rest.replace(f"{line}\n", "")
    assert rest == "exit\nkeyboard interrupt\n", stderr
    assert stderr == ""


# -- fork_test.py: a fork while the loop is starting ------------------------------

_FORK_LOCK_SCRIPT = """
import multiprocessing, os, sys, threading
sys.path.insert(0, {python_dir!r})

class _Hello:
    async def hello(self):
        return "hello from pid=%d" % os.getpid()

if {backend!r} == "synchronicity":
    from synchronicity import Synchronizer
    syn = Synchronizer()
    Hello = syn.create_blocking(_Hello, "Hello")
    hello = syn._translate_out(_Hello())
    stop_loop, start_loop = syn._close_loop, syn._start_loop
else:
    from ray.experimental.sandbox.modal import _sync
    Hello = _sync.synchronize_api(_Hello, "Hello")
    hello = Hello._from_impl(_Hello())
    stop_loop, start_loop = _sync._loop_thread.close, _sync._loop_thread.get_loop

def child(queue):
    try:
        queue.put(hello.hello())
    except Exception as exc:
        queue.put("error: %s" % exc)

def main():
    multiprocessing.set_start_method("fork")
    hello.hello()
    hangs, total = 0, 30
    for _ in range(total):
        stop_loop()
        started = threading.Event()
        def bg():
            started.set()
            start_loop()
        thread = threading.Thread(target=bg, daemon=True)
        thread.start()
        started.wait()
        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=child, args=(queue,))
        proc.start()
        proc.join(timeout=5)
        if proc.is_alive():
            hangs += 1
            proc.kill()
            proc.join()
        thread.join(timeout=2)
    print("FAIL" if hangs else "PASS", hangs, total, flush=True)

main()
"""


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_fork_during_start_loop_no_deadlock(backend, tmp_path):
    """A lock held mid-start at the moment of a fork stays held in the child
    for good, unless the child replaces it."""
    python_dir = os.path.abspath(
        os.path.join(os.path.dirname(_sync.__file__), "..", "..", "..", "..")
    )
    script = tmp_path / "fork_lock.py"
    script.write_text(
        _FORK_LOCK_SCRIPT.format(python_dir=python_dir, backend=backend.name)
    )
    result = subprocess.run(
        [sys.executable, "-u", str(script)],
        capture_output=True,
        encoding="utf-8",
        timeout=240,
    )
    assert result.stdout.startswith("PASS 0 30"), (result.stdout, result.stderr)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
