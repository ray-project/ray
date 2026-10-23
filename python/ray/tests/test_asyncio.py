# coding: utf-8
import asyncio
import contextlib
import ctypes
import os
import sys
import threading
import time
import weakref

import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.client_mode_hook import client_mode_should_convert
from ray._private.test_utils import (
    kill_actor_and_wait_for_failure,
    wait_for_pid_to_exit,
)
from ray.util.state import get_actor


def test_asyncio_actor(ray_start_regular_shared):
    @ray.remote
    class AsyncBatcher:
        def __init__(self):
            self.batch = []
            self.event = asyncio.Event()

        async def add(self, x):
            self.batch.append(x)
            if len(self.batch) >= 3:
                self.event.set()
            else:
                await self.event.wait()
            return sorted(self.batch)

    a = AsyncBatcher.remote()
    x1 = a.add.remote(1)
    x2 = a.add.remote(2)
    x3 = a.add.remote(3)
    r1 = ray.get(x1)
    r2 = ray.get(x2)
    r3 = ray.get(x3)
    assert r1 == [1, 2, 3]
    assert r1 == r2 == r3


def test_asyncio_actor_same_thread(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def sync_thread_id(self):
            return threading.current_thread().ident

        async def async_thread_id(self):
            return threading.current_thread().ident

    a = Actor.remote()
    sync_id, async_id = ray.get([a.sync_thread_id.remote(), a.async_thread_id.remote()])
    assert sync_id == async_id


def test_asyncio_actor_concurrency(ray_start_regular_shared):
    @ray.remote
    class RecordOrder:
        def __init__(self):
            self.history = []

        async def do_work(self):
            self.history.append("STARTED")
            # Force a context switch
            await asyncio.sleep(0)
            self.history.append("ENDED")

        def get_history(self):
            return self.history

    num_calls = 10

    a = RecordOrder.options(max_concurrency=1).remote()
    ray.get([a.do_work.remote() for _ in range(num_calls)])
    history = ray.get(a.get_history.remote())

    # We only care about ordered start-end-start-end sequence because
    # coroutines may be executed out of enqueued order.
    answer = []
    for _ in range(num_calls):
        for status in ["STARTED", "ENDED"]:
            answer.append(status)

    assert history == answer


def test_asyncio_actor_high_concurrency(ray_start_regular_shared):
    # This tests actor can handle concurrency above recursionlimit.

    @ray.remote
    class AsyncConcurrencyBatcher:
        def __init__(self, batch_size):
            self.batch = []
            self.event = asyncio.Event()
            self.batch_size = batch_size

        async def add(self, x):
            self.batch.append(x)
            if len(self.batch) >= self.batch_size:
                self.event.set()
            else:
                await self.event.wait()
            return sorted(self.batch)

    batch_size = sys.getrecursionlimit()
    actor = AsyncConcurrencyBatcher.options(max_concurrency=batch_size * 2).remote(
        batch_size
    )
    result = ray.get([actor.add.remote(i) for i in range(batch_size)])
    assert result[0] == list(range(batch_size))
    assert result[-1] == list(range(batch_size))


@pytest.mark.asyncio
async def test_asyncio_get(ray_start_regular_shared, event_loop):
    loop = event_loop
    asyncio.set_event_loop(loop)
    loop.set_debug(True)

    # Test Async Plasma
    @ray.remote
    def task():
        return 1

    assert await task.remote() == 1

    @ray.remote
    def task_throws():
        _ = 1 / 0

    with pytest.raises(ray.exceptions.RayTaskError):
        await task_throws.remote()

    # Test actor calls.
    str_len = 200 * 1024

    @ray.remote
    class Actor:
        def echo(self, i):
            return i

        def big_object(self):
            # 100Kb is the limit for direct call
            return "a" * (str_len)

        def throw_error(self):
            _ = 1 / 0

    actor = Actor.remote()

    assert await actor.echo.remote(2) == 2

    assert await actor.big_object.remote() == "a" * str_len

    with pytest.raises(ray.exceptions.RayTaskError):
        await actor.throw_error.remote()

    # Wrap in Remote Function to work with Ray client.
    kill_actor_ref = ray.remote(kill_actor_and_wait_for_failure).remote(actor)
    ray.get(kill_actor_ref)

    with pytest.raises(ray.exceptions.RayActorError):
        await actor.echo.remote(1)


def test_asyncio_actor_async_get(ray_start_regular_shared):
    @ray.remote
    def remote_task():
        return 1

    @ray.remote
    class AsyncGetter:
        async def get(self):
            return await remote_task.remote()

        async def plasma_get(self, plasma_object):
            return await plasma_object[0]

    plasma_object = ray.put(2)
    getter = AsyncGetter.remote()
    assert ray.get(getter.get.remote()) == 1
    assert ray.get(getter.plasma_get.remote([plasma_object])) == 2


@pytest.mark.asyncio
async def test_asyncio_double_await(ray_start_regular_shared):
    # This is a regression test for
    # https://github.com/ray-project/ray/issues/8841

    signal = SignalActor.remote()
    waiting = signal.wait.remote()

    future = asyncio.ensure_future(waiting)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(future, timeout=0.1)
    assert future.cancelled()

    # We are explicitly waiting multiple times here to test asyncio state
    # override.
    await signal.send.remote()
    await waiting
    await waiting


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ray_start_regular_shared",
    [{"include_dashboard": True}],
    indirect=True,
)
async def test_asyncio_exit_actor(ray_start_regular_shared):
    # https://github.com/ray-project/ray/issues/12649
    # exit_actor() must terminate the actor and not leak the process.
    # exit_actor() drains in-flight tasks before exiting (matching
    # threaded actors). In-flight task draining on graceful exit is covered in
    # test_worker_graceful_shutdown.py.

    @ray.remote
    class Actor:
        async def exit(self):
            ray.actor.exit_actor()

        async def ping(self):
            return os.getpid()

    a = Actor.options(max_task_retries=0).remote()
    pid = ray.get(a.ping.remote())
    # exit_actor() exits the actor, so the caller observes the actor's death.
    with pytest.raises(ray.exceptions.RayError):
        ray.get(a.exit.remote())

    # New calls should just error.
    with pytest.raises(ray.exceptions.RayError):
        ray.get(a.ping.remote())

    # The actor should be dead in the actor table.
    # Using ray task so it works in Ray Client as well.
    @ray.remote
    def check_actor_gone_now():
        def cond():
            return get_actor(id=a._ray_actor_id.hex()).state != "ALIVE"

        wait_for_condition(cond)

    ray.get(check_actor_gone_now.remote())

    # Make sure there is no process leak
    wait_for_pid_to_exit(pid)


def test_asyncio_exit_actor_with_concurrency_group(ray_start_regular_shared):
    @ray.remote(concurrency_groups={"async": 2})
    class Actor:
        async def getpid(self):
            return os.getpid()

        async def exit(self):
            ray.actor.exit_actor()

        @ray.method(concurrency_group="async")
        async def echo(self, value):
            return value

    a = Actor.remote()
    pid = ray.get(a.getpid.remote())
    # Exercise the concurrency group, then exit the actor.
    assert ray.get(a.echo.remote("hi")) == "hi"
    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.exit.remote())
    wait_for_pid_to_exit(pid)


def test_async_callback(ray_start_regular_shared):
    global_set = set()

    ref = ray.put(None)
    ref._on_completed(lambda _: global_set.add("completed-1"))
    wait_for_condition(lambda: "completed-1" in global_set)

    signal = SignalActor.remote()

    @ray.remote
    def wait():
        ray.get(signal.wait.remote())

    ref = wait.remote()
    ref._on_completed(lambda _: global_set.add("completed-2"))
    assert "completed-2" not in global_set
    signal.send.remote()
    wait_for_condition(lambda: "completed-2" in global_set)


@pytest.mark.skipif(
    client_mode_should_convert(),
    reason="_on_ready and _ready are not supported on Ray Client ObjectRefs.",
)
@pytest.mark.asyncio
async def test_async_wait_for_object_ref_ready(ray_start_regular_shared):
    signal = SignalActor.remote()

    @ray.remote
    def wait():
        ray.get(signal.wait.remote())
        return "secret"

    ref = wait.remote()
    ready_task = asyncio.create_task(ref._ready())
    _, pending = await asyncio.wait({ready_task}, timeout=1)
    assert ready_task in pending, "_ready resolved before the object was produced"
    await signal.send.remote()
    assert await ready_task is ref
    assert await ref == "secret"


@pytest.mark.skipif(
    client_mode_should_convert(),
    reason="_on_ready and _ready are not supported on Ray Client ObjectRefs.",
)
@pytest.mark.asyncio
async def test_async_wait_for_object_ref_ready_cancel(ray_start_regular_shared):
    signal = SignalActor.remote()

    @ray.remote
    def wait():
        ray.get(signal.wait.remote())
        return "secret"

    ref = wait.remote()
    ready_task = asyncio.create_task(ref._ready())
    _, pending = await asyncio.wait({ready_task}, timeout=1)
    assert ready_task in pending, "_ready resolved before the object was produced"
    ready_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await ready_task
    await signal.send.remote()
    assert await ref == "secret"


@pytest.mark.skipif(
    client_mode_should_convert(),
    reason="_on_ready and _ready are not supported on Ray Client ObjectRefs.",
)
def test_on_ready_ref_drop_does_not_invoke_callback(ray_start_regular_shared):
    """Dropping the ObjectRef leaves _on_ready pending, same as _on_completed."""
    signal = SignalActor.remote()

    @ray.remote
    def wait():
        ray.get(signal.wait.remote())
        return "secret"

    ref = wait.remote()
    done = threading.Event()
    seen = []

    def cb(exc):
        seen.append(exc)
        done.set()

    ref._on_ready(cb)
    del ref
    assert not done.wait(timeout=1), "_on_ready ran when the ref was dropped"
    ray.get(signal.send.remote())
    assert done.wait(timeout=10), "_on_ready was not invoked after the object was ready"
    assert seen == [None]


@pytest.mark.parametrize("raise_in_callback", [False, True])
@pytest.mark.skipif(
    client_mode_should_convert(), reason="Different ref counting in Ray client."
)
def test_on_completed_callback_refcount(ray_start_regular_shared, raise_in_callback):
    """Check that the _on_completed callback is ref counted properly."""
    signal = SignalActor.remote()

    def callback(result):
        if raise_in_callback:
            raise Exception("ruh-roh")

    @ray.remote
    def wait():
        ray.get(signal.wait.remote())

    ref = wait.remote()

    initial_refcount = sys.getrefcount(callback)
    ref._on_completed(callback)

    # Python ref count should be incremented to avoid the callback being GC'd while the
    # C++ core worker holds a ref to it.
    assert sys.getrefcount(callback) > initial_refcount

    # Trigger the task to finish so the callback should execute.
    ray.get(signal.send.remote())

    # Now the refcount should drop back down to the initial count.
    wait_for_condition(lambda: sys.getrefcount(callback) == initial_refcount)


def test_async_function_errored(ray_start_regular_shared):
    with pytest.raises(ValueError):

        @ray.remote
        async def f():
            pass


@pytest.mark.asyncio
async def test_async_obj_unhandled_errors(ray_start_regular_shared):
    @ray.remote
    def f():
        raise ValueError()

    num_exceptions = 0

    def interceptor(e):
        nonlocal num_exceptions
        num_exceptions += 1

    # Test we report unhandled exceptions.
    ray._private.worker._unhandled_error_handler = interceptor
    x1 = f.remote()
    # NOTE: Unhandled exception is from waiting for the value of x1's ObjectID
    # in x1's destructor, and receiving an exception from f() instead.
    del x1
    wait_for_condition(lambda: num_exceptions == 1)

    # Test we don't report handled exceptions.
    x1 = f.remote()
    with pytest.raises(ray.exceptions.RayError):
        await x1
    del x1
    await asyncio.sleep(1)
    assert num_exceptions == 1, num_exceptions


# This case tests that the asyncio actor shouldn't create thread
# pool with max_concurrency threads. Otherwise it will allocate
# too many resources for threads to lead worker crash.
def test_asyncio_actor_with_large_concurrency(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def sync_thread_id(self):
            time.sleep(2)
            return threading.current_thread().ident

        async def async_thread_id(self):
            time.sleep(2)
            return threading.current_thread().ident

    a = Actor.options(max_concurrency=100000).remote()
    sync_id, async_id = ray.get([a.sync_thread_id.remote(), a.async_thread_id.remote()])
    assert sync_id == async_id


def test_asyncio_actor_shutdown_when_non_async_method_mixed(ray_start_regular_shared):
    # Regression test for:  https://github.com/ray-project/ray/issues/32376
    # Ensure the core worker doesn't crash when exit_actor is used while mixing async
    # and sync actor tasks.
    @ray.remote
    class A:
        def __init__(self, *, exit_after: int):
            self._remaining = exit_after
            self._event = asyncio.Event()

        async def wait_then_exit(self):
            await self._event.wait()
            ray.actor.exit_actor()

        def ping(self):
            self._remaining -= 1
            if self._remaining == 0:
                self._event.set()

    # Exit after 1/2 of the ping tasks have executed to ensure interleaving.
    a = A.remote(exit_after=500)
    exit_ref = a.wait_then_exit.remote()
    ping_refs = [a.ping.remote() for _ in range(1000)]

    with pytest.raises(
        ray.exceptions.RayActorError,
        match="INTENDED_USER_EXIT",
    ):
        ray.get([exit_ref] + ping_refs)


def test_asyncio_actor_argument_collision(ray_start_regular_shared):
    """Regression test for https://github.com/ray-project/ray/issues/41272."""

    @ray.remote
    class A:
        async def hi_async(self, task_id: str, specified_cgname: str):
            return f"Hi from async: {task_id}! cgname: {specified_cgname}."

        def hi_sync(self, task_id: str, *, specified_cgname: str):
            return f"Hi from sync: {task_id}! cgname: {specified_cgname}."

    a = A.remote()
    assert (
        ray.get(a.hi_async.remote(task_id="TEST", specified_cgname="test2"))
        == "Hi from async: TEST! cgname: test2."
    )
    assert (
        ray.get(a.hi_sync.remote(task_id="TEST", specified_cgname="test2"))
        == "Hi from sync: TEST! cgname: test2."
    )


def test_async_actor_finalizes_objects_dropped_on_fiber(ray_start_regular_shared):
    """Objects dropped as an async-actor task returns must be finalized promptly.

    Async-actor tasks execute on boost fiber stacks. CPython's C-stack overflow
    checks are keyed on the bounds it recorded for the *thread*, so a fiber stack
    can be mistaken for an exhausted one -- and on CPython 3.14 the deallocator
    then parks every object it frees on a per-thread-state list that is never
    drained, leaking the task's entire object graph.

    A task's return value is serialized on the fiber, so the last reference the
    actor process holds to the payload below is released there. If deallocation
    is being deferred, the payload's finalizer never runs.

    max_concurrency > 1 is required for coverage rather than realism: with a
    single fiber, anchoring once at task entry would suffice, so only interleaved
    fibers exercise the re-anchoring that happens after a fiber resumes.
    """

    class Payload:
        pass

    @ray.remote(max_concurrency=2)
    class Probe:
        def __init__(self, unused):
            # Taking a constructor argument is deliberate: it makes the creation
            # task deserialize arguments, which is one of the paths that reaches
            # the fiber bookkeeping from a thread that is not running a fiber.
            self._finalizers = []

        async def make_payload(self):
            payload = Payload()
            self._finalizers.append(weakref.finalize(payload, lambda: None))
            return payload

        async def num_finalized(self):
            return sum(not f.alive for f in self._finalizers)

    num_tasks = 50
    probe = Probe.remote("unused")
    ray.get([probe.make_payload.remote() for _ in range(num_tasks)])

    num_finalized = ray.get(probe.num_finalized.remote())
    assert num_finalized == num_tasks, (
        f"{num_tasks - num_finalized} of {num_tasks} payloads returned by an async "
        "actor were never finalized in the actor process, so their deallocation is "
        "being deferred and never completed"
    )


@pytest.mark.skipif(
    sys.version_info < (3, 14)
    or sys.platform == "win32"
    or not hasattr(ctypes.pythonapi, "PyUnstable_ThreadState_SetStackProtection")
    or ctypes.sizeof(ctypes.c_void_p) != 8,
    reason="Fiber stack re-anchoring only applies to 64-bit CPython >= 3.14.2.",
)
def test_async_actor_reanchors_fiber_stack_before_first_python_call(
    ray_start_regular_shared,
):
    """
    Checks Stack protection must be re-anchored before any Python runs on a new fiber.

    While any fiber is in flight, all fibers on the runner thread share one
    thread state, so a new fiber starts with the stack bounds left by the last
    fiber that ran. If the first Python call in task_execution_handler runs
    before the re-anchor, CPython checks it against those stale bounds. Whether
    that crashes depends on where the new fiber stack was allocated
    (https://github.com/ray-project/ray/issues/66399), so this test makes the
    stale bounds deterministic: every task poisons the bounds as it exits, and
    the next task checks them on entry.

    The poisoned bounds sit above every real stack. This makes python think
    its hopelessly overflowed so _Py_Dealloc defers the deallocation of every
    GC object. Thus a canary dropped in the first Python call is freed
    immediately only if the bounds were re-anchored before that call.
    """
    # The poisoned stack base must sit above every real stack pointer.
    poisoned_stack_base = 1 << 62
    # FiberState::kStackSize, the size of a real fiber stack.
    fiber_stack_size = 256 * 1024

    @ray.remote
    class Probe:
        def __init__(self):
            api = ctypes.pythonapi
            api.PyThreadState_Get.restype = ctypes.c_void_p
            set_stack_protection = api.PyUnstable_ThreadState_SetStackProtection
            set_stack_protection.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_size_t,
            ]
            set_stack_protection.restype = ctypes.c_int

            class Canary:
                pass

            def canary_freed_immediately():
                freed = []
                canary = Canary()
                ref = weakref.ref(canary, lambda _: freed.append(True))  # noqa: F841
                del canary
                return bool(freed)

            self._parked = asyncio.Event()
            self._release = asyncio.Event()
            self._poisoned = False
            self._freed_on_entry = []
            real_disable_client_hook = ray._raylet.disable_client_hook

            # Stands in for disable_client_hook(), the first Python call that
            # task_execution_handler makes on the fiber. Installing it here
            # covers every later task. The creation task itself runs on a
            # regular thread, not a fiber, and already entered the real hook.
            @contextlib.contextmanager
            def probing_disable_client_hook():
                if self._poisoned:
                    self._freed_on_entry.append(canary_freed_immediately())
                try:
                    with real_disable_client_hook():
                        yield
                finally:
                    assert (
                        set_stack_protection(
                            api.PyThreadState_Get(),
                            poisoned_stack_base,
                            fiber_stack_size,
                        )
                        == 0
                    )
                    self._poisoned = True

            ray._raylet.disable_client_hook = probing_disable_client_hook

        async def park(self):
            self._parked.set()
            await self._release.wait()

        async def run(self):
            await self._parked.wait()

        async def release(self):
            self._release.set()

        async def freed_on_entry(self):
            return self._freed_on_entry

    probe = Probe.remote()
    # The parked task keeps the runner thread's thread state alive, as other
    # in-flight tasks do under load. Without it, each task gets a fresh thread
    # state and never sees the bounds the previous task left behind.
    parked = probe.park.remote()
    # Submit one at a time so that each task starts on the bounds the previous
    # task left behind, with no other fiber re-anchoring in between.
    for _ in range(5):
        ray.get(probe.run.remote())

    freed_on_entry = ray.get(probe.freed_on_entry.remote())
    ray.get(probe.release.remote())
    ray.get(parked)
    ray.kill(probe)
    assert freed_on_entry and all(freed_on_entry), (
        f"Deallocation was deferred on entry to async actor tasks {freed_on_entry}: "
        "the first Python call ran before the fiber stack was re-anchored"
    )


@pytest.mark.skipif(
    sys.version_info < (3, 14)
    or sys.platform == "win32"
    or not hasattr(ctypes.pythonapi, "PyUnstable_ThreadState_SetStackProtection"),
    reason="Fiber stack re-anchoring only applies to CPython >= 3.14.2.",
)
def test_idle_async_actor_internal_task_does_not_leak(ray_start_regular_shared):
    """A task that never yields its fiber must not leak on an idle async actor.

    On an async actor, Ray runs sync methods whose names start with `__ray` (such
    as `__ray_ready__`) directly on the fiber, without handing off to the event
    loop. The test calls such a method many times, waiting for each call to
    finish before sending the next, so that the actor has nothing else running
    when each call starts. Each call drops a payload on the fiber; if its
    deallocation is deferred, the payload's finalizer never runs.

    Both the idle actor and a task that never hands off are needed to expose the
    leak. When no other task is in flight, a task that is not re-anchored at
    start runs with the stack bounds of the OS thread that runs the actor's
    fibers, rather than the previous fiber's own stack. CPython then thinks the
    stack is nearly full, so it defers freeing objects instead of freeing them.
    Ordinary async methods are re-anchored when they hand off to the event loop,
    and the deferred objects are freed then. A `__ray`-prefixed sync method
    never hands off, so without the re-anchor at start, everything it defers is
    lost when it finishes.
    """

    class Payload:
        pass

    @ray.remote
    class AsyncActor:
        def __init__(self):
            self._finalizers = []

        def __ray_drop_payload__(self):
            payload = Payload()
            self._finalizers.append(weakref.finalize(payload, lambda: None))

        async def num_finalized(self):
            return sum(not f.alive for f in self._finalizers)

    num_tasks = 50
    actor = AsyncActor.remote()
    # Submit one at a time, so that the actor is idle when each task starts.
    for _ in range(num_tasks):
        ray.get(actor.__ray_drop_payload__.remote())

    num_finalized = ray.get(actor.num_finalized.remote())
    assert num_finalized == num_tasks, (
        f"{num_tasks - num_finalized} of {num_tasks} payloads dropped by tasks that "
        "never yield on an idle async actor were never finalized, so their "
        "deallocation is being deferred and never completed"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
