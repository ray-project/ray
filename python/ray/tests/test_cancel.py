import os
import random
import signal
import sys
import threading
import _thread
import time

import pytest

import ray
from ray.exceptions import (
    TaskCancelledError,
    RayTaskError,
    GetTimeoutError,
    WorkerCrashedError,
    ObjectLostError,
)
from ray._private.utils import DeferSigint
from ray._private.test_utils import SignalActor


def valid_exceptions(use_force):
    if use_force:
        return (RayTaskError, TaskCancelledError, WorkerCrashedError, ObjectLostError)
    else:
        return (RayTaskError, TaskCancelledError)


@pytest.mark.parametrize("use_force", [True, False])
def test_cancel_chain(ray_start_regular, use_force):
    signaler = SignalActor.remote()

    @ray.remote
    def wait_for(t):
        return ray.get(t[0])

    obj1 = wait_for.remote([signaler.wait.remote()])
    obj2 = wait_for.remote([obj1])
    obj3 = wait_for.remote([obj2])
    obj4 = wait_for.remote([obj3])

    assert len(ray.wait([obj1], timeout=0.1)[0]) == 0
    ray.cancel(obj1, force=use_force)
    for ob in [obj1, obj2, obj3, obj4]:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(ob)

    signaler2 = SignalActor.remote()
    obj1 = wait_for.remote([signaler2.wait.remote()])
    obj2 = wait_for.remote([obj1])
    obj3 = wait_for.remote([obj2])
    obj4 = wait_for.remote([obj3])

    assert len(ray.wait([obj3], timeout=0.1)[0]) == 0
    ray.cancel(obj3, force=use_force)
    for ob in [obj3, obj4]:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(ob)

    with pytest.raises(GetTimeoutError):
        ray.get(obj1, timeout=0.1)

    with pytest.raises(GetTimeoutError):
        ray.get(obj2, timeout=0.1)

    signaler2.send.remote()
    ray.get(obj1)


@pytest.mark.parametrize("use_force", [True, False])
def test_cancel_during_arg_deser(ray_start_regular, use_force):
    time_to_sleep = 5

    class SlowToDeserialize:
        def __reduce__(self):
            def reconstruct():
                import time

                time.sleep(time_to_sleep)
                return SlowToDeserialize()

            return reconstruct, ()

    @ray.remote
    def dummy(a: SlowToDeserialize):
        # Task should never execute.
        assert False

    arg = SlowToDeserialize()
    obj = dummy.remote(arg)
    # Check that task isn't done.
    assert len(ray.wait([obj], timeout=0.1)[0]) == 0
    # Cancel task.
    ray.cancel(obj, force=use_force)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(obj)


def test_defer_sigint():
    # Tests a helper context manager for deferring SIGINT signals until after the
    # context is left. This is used by Ray's task cancellation to defer cancellation
    # interrupts during problematic areas, e.g. task argument deserialization.
    signal_was_deferred = False
    orig_sigint_handler = signal.getsignal(signal.SIGINT)
    try:
        with DeferSigint():
            # Send singal to current process.
            # NOTE: We use _thread.interrupt_main() instead of os.kill() in order to
            # support Windows.
            _thread.interrupt_main()
            # Wait for signal to be delivered.
            time.sleep(1)
            # Signal should have been delivered by here, so we consider it deferred if
            # this is reached.
            signal_was_deferred = True
    except KeyboardInterrupt:
        # Check that SIGINT was deferred until the end of the context.
        assert signal_was_deferred
        # Check that original SIGINT handler was restored.
        assert signal.getsignal(signal.SIGINT) is orig_sigint_handler
    else:
        pytest.fail("SIGINT signal was never sent in test")


def test_defer_sigint_monkey_patch():
    # Tests that setting a SIGINT signal handler within a DeferSigint context is not
    # allowed.
    orig_sigint_handler = signal.getsignal(signal.SIGINT)
    with pytest.raises(ValueError):
        with DeferSigint():
            signal.signal(signal.SIGINT, orig_sigint_handler)


def test_defer_sigint_noop_in_non_main_thread():
    # Tests that we don't try to defer SIGINT when not in the main thread.

    # Check that DeferSigint.create_if_main_thread() does not return DeferSigint when
    # not in the main thread.
    def check_no_defer():
        cm = DeferSigint.create_if_main_thread()
        assert not isinstance(cm, DeferSigint)

    check_no_defer_thread = threading.Thread(target=check_no_defer)
    try:
        check_no_defer_thread.start()
        check_no_defer_thread.join()
    except AssertionError as e:
        pytest.fail(
            "DeferSigint.create_if_main_thread() unexpected returned a DeferSigint "
            f"instance when not in the main thread: {e}"
        )

    # Check that signal is not deferred when trying to defer it in not the main thread.
    signal_was_deferred = False

    def maybe_defer():
        nonlocal signal_was_deferred

        with DeferSigint.create_if_main_thread() as cm:
            # Check that DeferSigint context manager was NOT returned.
            assert not isinstance(cm, DeferSigint)
            # Send singal to current process.
            # NOTE: We use _thread.interrupt_main() instead of os.kill() in order to
            # support Windows.
            _thread.interrupt_main()
            # Wait for signal to be delivered.
            time.sleep(1)
            # Signal should have been delivered by here, so we consider it deferred if
            # this is reached.
            signal_was_deferred = True

    # Create thread that will maybe defer SIGINT.
    maybe_defer_thread = threading.Thread(target=maybe_defer)
    try:
        maybe_defer_thread.start()
        maybe_defer_thread.join()
        # KeyboardInterrupt should get raised in main thread.
    except KeyboardInterrupt:
        # Check that SIGINT was not deferred.
        assert not signal_was_deferred
        # Check that original SIGINT handler was not overridden.
        assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    else:
        pytest.fail("SIGINT signal was never sent in test")


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=(
        "Flaky on OSX. Fine-tuned test timeout period needed. "
        "TODO(https://github.com/ray-project/ray/issues/30899): tune timeout."
    ),
)
def test_cancel_during_arg_deser_non_reentrant_import(ray_start_regular):
    # This test ensures that task argument deserialization properly defers task
    # cancellation interrupts until after deserialization completes, in order to ensure
    # that non-reentrant imports that happen during both task argument deserialization
    # and during error storage are not interrupted.

    # We test this by doing the following:
    #  - register a custom serializer for (a) a task argument that triggers
    #  non-reentrant imports on deserialization, and (b) RayTaskError that triggers
    #  non-reentrant imports on serialization; in our case, we chose pandas it is both
    #  non-reentrant and expensive, with an import time ~0.5 seconds, giving us a wide
    #  cancellation target,
    #  - wait until those serializers are registered on all workers,
    #  - launch the task and wait until we are confident that the cancellation signal
    #  will be received by the workers during task argument deserialization (currently a
    #  200 ms wait).
    #  - check that a graceful task cancellation error is raised, not a
    # WorkerCrashedError.
    def non_reentrant_import():
        # NOTE: Pandas has a non-reentrant import and should take ~0.5 seconds to
        # import, giving us a wide cancellation target.
        import pandas  # noqa

    def non_reentrant_import_and_delegate(obj):
        # Custom serializer for task argument and task error resulting in non-reentrant
        # imports being imported on both serialization and deserialization. We use the
        # same custom serializer for both, doing non-reentrant imports on both
        # serialization and deserialization, for the sake of simplicity/reuse.

        # Import on serialization.
        non_reentrant_import()

        reduced = obj.__reduce__()
        func = reduced[0]
        args = reduced[1]
        others = reduced[2:]

        def non_reentrant_import_on_reconstruction(*args, **kwargs):
            # Import on deserialization.
            non_reentrant_import()

            return func(*args, **kwargs)

        out = (non_reentrant_import_on_reconstruction, args) + others
        return out

    # Dummy task argument for which we register a serializer that will trigger
    # non-reentrant imports on deserialization.
    class DummyArg:
        pass

    def register_non_reentrant_import_and_delegate_reducer(worker_info):
        from ray.exceptions import RayTaskError

        context = ray._private.worker.global_worker.get_serialization_context()
        # Register non-reentrant import serializer for task argument.
        context._register_cloudpickle_reducer(
            DummyArg, non_reentrant_import_and_delegate
        )
        # Register non-reentrant import serializer for RayTaskError.
        context._register_cloudpickle_reducer(
            RayTaskError, non_reentrant_import_and_delegate
        )

    ray._private.worker.global_worker.run_function_on_all_workers(
        register_non_reentrant_import_and_delegate_reducer,
    )

    # Wait for function to run on all workers.
    time.sleep(3)

    @ray.remote
    def run_and_fail(a: DummyArg):
        # Should never be reached.
        assert False

    arg = DummyArg()
    obj = run_and_fail.remote(arg)
    # Check that task isn't done.
    # NOTE: This timeout was finely tuned to ensure that task cancellation happens while
    # we are deserializing task arguments (10/10 runs when this comment was added).
    timeout_to_reach_arg_deserialization = 0.2
    assert len(ray.wait([obj], timeout=timeout_to_reach_arg_deserialization)[0]) == 0

    # Cancel task.
    use_force = False
    ray.cancel(obj, force=use_force)

    # Should raise RayTaskError or TaskCancelledError, NOT WorkerCrashedError.
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(obj)


@pytest.mark.parametrize("use_force", [True, False])
def test_cancel_multiple_dependents(ray_start_regular, use_force):
    signaler = SignalActor.remote()

    @ray.remote
    def wait_for(t):
        return ray.get(t[0])

    head = wait_for.remote([signaler.wait.remote()])
    deps = []
    for _ in range(3):
        deps.append(wait_for.remote([head]))

    assert len(ray.wait([head], timeout=0.1)[0]) == 0
    ray.cancel(head, force=use_force)
    for d in deps:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(d)

    head2 = wait_for.remote([signaler.wait.remote()])

    deps2 = []
    for _ in range(3):
        deps2.append(wait_for.remote([head]))

    for d in deps2:
        ray.cancel(d, force=use_force)

    for d in deps2:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(d)

    signaler.send.remote()
    ray.get(head2)


@pytest.mark.parametrize("use_force", [True, False])
def test_single_cpu_cancel(shutdown_only, use_force):
    ray.init(num_cpus=1)
    signaler = SignalActor.remote()

    @ray.remote
    def wait_for(t):
        return ray.get(t[0])

    obj1 = wait_for.remote([signaler.wait.remote()])
    obj2 = wait_for.remote([obj1])
    obj3 = wait_for.remote([obj2])
    indep = wait_for.remote([signaler.wait.remote()])

    assert len(ray.wait([obj3], timeout=0.1)[0]) == 0
    ray.cancel(obj3, force=use_force)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(obj3)

    ray.cancel(obj1, force=use_force)

    for d in [obj1, obj2]:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(d)

    signaler.send.remote()
    ray.get(indep)


@pytest.mark.parametrize("use_force", [True, False])
def test_comprehensive(ray_start_regular, use_force):
    signaler = SignalActor.remote()

    @ray.remote
    def wait_for(t):
        ray.get(t[0])
        return "Result"

    @ray.remote
    def combine(a, b):
        return str(a) + str(b)

    a = wait_for.remote([signaler.wait.remote()])
    b = wait_for.remote([signaler.wait.remote()])
    combo = combine.remote(a, b)
    a2 = wait_for.remote([a])

    assert len(ray.wait([a, b, a2, combo], timeout=1)[0]) == 0

    ray.cancel(a, force=use_force)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(a, timeout=10)

    with pytest.raises(valid_exceptions(use_force)):
        ray.get(a2, timeout=40)

    signaler.send.remote()

    with pytest.raises(valid_exceptions(use_force)):
        ray.get(combo)


# Running this test with use_force==False is flaky.
# TODO(ilr): Look into the root of this flakiness.
@pytest.mark.parametrize("use_force", [True])
def test_stress(shutdown_only, use_force):
    ray.init(num_cpus=1)

    @ray.remote
    def infinite_sleep(y):
        if y:
            while True:
                time.sleep(1 / 10)

    first = infinite_sleep.remote(True)

    sleep_or_no = [random.randint(0, 1) for _ in range(100)]
    tasks = [infinite_sleep.remote(i) for i in sleep_or_no]
    cancelled = set()

    # Randomly kill queued tasks (infinitely sleeping or not).
    for t in tasks:
        if random.random() > 0.5:
            ray.cancel(t, force=use_force)
            cancelled.add(t)

    ray.cancel(first, force=use_force)
    cancelled.add(first)

    for done in cancelled:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(done, timeout=120)

    # Kill all infinitely sleeping tasks (queued or not).
    for indx, t in enumerate(tasks):
        if sleep_or_no[indx]:
            ray.cancel(t, force=use_force)
            cancelled.add(t)
    for indx, t in enumerate(tasks):
        if t in cancelled:
            with pytest.raises(valid_exceptions(use_force)):
                ray.get(t, timeout=120)
        else:
            ray.get(t, timeout=120)


@pytest.mark.parametrize("use_force", [True, False])
def test_fast(shutdown_only, use_force):
    ray.init(num_cpus=2)

    @ray.remote
    def fast(y):
        return y

    signaler = SignalActor.remote()
    ids = list()
    for _ in range(100):
        x = fast.remote("a")
        # NOTE If a non-force Cancellation is attempted in the time
        # between a worker receiving a task and the worker executing
        # that task (specifically the python execution), Cancellation
        # can fail.

        time.sleep(0.1)
        ray.cancel(x, force=use_force)
        ids.append(x)

    @ray.remote
    def wait_for(y):
        return y

    sig = signaler.wait.remote()
    for _ in range(5000):
        x = wait_for.remote(sig)
        ids.append(x)

    for idx in range(100, 5100):
        if random.random() > 0.95:
            ray.cancel(ids[idx], force=use_force)
    signaler.send.remote()
    for i, obj_ref in enumerate(ids):
        try:
            ray.get(obj_ref, timeout=120)
        except Exception as e:
            assert isinstance(
                e, valid_exceptions(use_force)
            ), f"Failure on iteration: {i}"


@pytest.mark.parametrize("use_force", [True, False])
def test_remote_cancel(ray_start_regular, use_force):
    signaler = SignalActor.remote()

    @ray.remote
    def wait_for(y):
        return ray.get(y[0])

    @ray.remote
    def remote_wait(sg):
        return [wait_for.remote([sg[0]])]

    sig = signaler.wait.remote()

    outer = remote_wait.remote([sig])
    inner = ray.get(outer)[0]

    with pytest.raises(GetTimeoutError):
        ray.get(inner, timeout=1)

    ray.cancel(inner, force=use_force)

    with pytest.raises(valid_exceptions(use_force)):
        ray.get(inner, timeout=10)


@pytest.mark.parametrize("use_force", [True, False])
def test_recursive_cancel(shutdown_only, use_force):
    ray.init(num_cpus=4)

    @ray.remote(num_cpus=1)
    def inner():
        while True:
            time.sleep(0.1)

    @ray.remote(num_cpus=1)
    def outer():

        x = [inner.remote()]
        print(x)
        while True:
            time.sleep(0.1)

    @ray.remote(num_cpus=4)
    def many_resources():
        return 300

    outer_fut = outer.remote()
    many_fut = many_resources.remote()
    with pytest.raises(GetTimeoutError):
        ray.get(many_fut, timeout=1)
    ray.cancel(outer_fut)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(outer_fut, timeout=10)

    assert ray.get(many_fut, timeout=30)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
