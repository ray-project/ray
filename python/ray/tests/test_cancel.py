import _thread
import random
import signal
import sys
import threading
import time
from typing import List

import numpy as np
import pytest

import ray
from ray._common.test_utils import SignalActor, wait_for_condition
from ray._private.utils import DeferSigint
from ray.exceptions import (
    GetTimeoutError,
    RayTaskError,
    TaskCancelledError,
    WorkerCrashedError,
)
from ray.types import ObjectRef
from ray.util.state import list_tasks


def valid_exceptions(use_force):
    if use_force:
        return (RayTaskError, TaskCancelledError, WorkerCrashedError)
    else:
        return TaskCancelledError


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
            # Send signal to current process.
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


def test_defer_sigint_monkey_patch_handler_called_when_exit():
    # Tests that the SIGINT signal handlers set within a DeferSigint
    # is triggered at most once and only at context exit.
    orig_sigint_handler = signal.getsignal(signal.SIGINT)
    handler_called_times = 0

    def new_sigint_handler(signum, frame):
        nonlocal handler_called_times
        handler_called_times += 1

    with DeferSigint():
        signal.signal(signal.SIGINT, new_sigint_handler)
        for _ in range(3):
            _thread.interrupt_main()
        time.sleep(1)
        assert handler_called_times == 0

    assert handler_called_times == 1

    # Restore original SIGINT handler.
    signal.signal(signal.SIGINT, orig_sigint_handler)


def test_defer_sigint_monkey_patch_only_last_handler_called():
    # Tests that only the last SIGINT signal handler set within a DeferSigint
    # is triggered at most once and only at context exit.
    orig_sigint_handler = signal.getsignal(signal.SIGINT)

    handler_1_called_times = 0
    handler_2_called_times = 0

    def sigint_handler_1(signum, frame):
        nonlocal handler_1_called_times
        handler_1_called_times += 1

    def sigint_handler_2(signum, frame):
        nonlocal handler_2_called_times
        handler_2_called_times += 1

    with DeferSigint():
        signal.signal(signal.SIGINT, sigint_handler_1)
        for _ in range(3):
            _thread.interrupt_main()
        time.sleep(1)
        signal.signal(signal.SIGINT, sigint_handler_2)
        for _ in range(3):
            _thread.interrupt_main()
        time.sleep(1)
        assert handler_1_called_times == 0
        assert handler_2_called_times == 0

    assert handler_1_called_times == 0
    assert handler_2_called_times == 1

    # Restore original SIGINT handler.
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
def test_remote_cancel(ray_start_cluster, use_force):
    # NOTE: We need to use a cluster with 2 nodes to test the remote cancel.
    # Otherwise both wait_for and remote_wait will be scheduled on the same worker
    # process and the cancel on wait_for will also kill remote_wait. This is because
    # remote_wait also makes a remote call and returns instantly meaning it can
    # be reused from the worker pool for wait_for.
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"worker1": 1})
    cluster.add_node(num_cpus=1, resources={"worker2": 1})
    signaler = SignalActor.remote()

    @ray.remote(num_cpus=1, resources={"worker1": 1})
    def wait_for(y):
        return ray.get(y[0])

    @ray.remote(num_cpus=1, resources={"worker2": 1})
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
    ray.init(num_cpus=2)

    @ray.remote(num_cpus=1)
    def inner(signal_actor):
        signal_actor.send.remote()
        while True:
            time.sleep(0.1)

    @ray.remote(num_cpus=1)
    def outer(signal_actor):
        _ = inner.remote(signal_actor)
        while True:
            time.sleep(0.1)

    @ray.remote(num_cpus=2)
    def many_resources():
        return True

    signal_actor = SignalActor.remote()
    outer_fut = outer.remote(signal_actor)
    # Wait until both inner and outer are running
    ray.get(signal_actor.wait.remote())
    many_fut = many_resources.remote()
    with pytest.raises(GetTimeoutError):
        ray.get(many_fut, timeout=1)
    ray.cancel(outer_fut, force=use_force)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(outer_fut, timeout=10)

    assert ray.get(many_fut, timeout=30)


def test_recursive_cancel_actor_task(shutdown_only):
    ray.init()

    @ray.remote(num_cpus=0)
    class Semaphore:
        def wait(self):
            import time

            time.sleep(600)

    @ray.remote(num_cpus=0)
    class Actor2:
        def __init__(self, obj):
            (self.obj,) = obj

        def cancel(self):
            ray.cancel(self.obj)

    @ray.remote
    def task(sema):
        return ray.get(sema.wait.remote())

    sema = Semaphore.remote()

    t = task.remote(sema)

    def wait_until_wait_task_starts():
        wait_state = list_tasks(
            filters=[("func_or_class_name", "=", "Semaphore.wait")]
        )[0]
        return wait_state["state"] == "RUNNING"

    wait_for_condition(wait_until_wait_task_starts)

    # Make sure this will not crash ray.
    # https://github.com/ray-project/ray/issues/31398
    a2 = Actor2.remote((t,))
    a2.cancel.remote()

    with pytest.raises(RayTaskError, match="TaskCancelledError"):
        ray.get(t)

    wait_state = list_tasks(filters=[("func_or_class_name", "=", "Semaphore.wait")])
    assert len(wait_state) == 1
    wait_state = wait_state[0]
    task_state = list_tasks(filters=[("func_or_class_name", "=", "task")])
    assert len(task_state) == 1
    task_state = task_state[0]

    def verify():
        wait_state = list_tasks(filters=[("func_or_class_name", "=", "Semaphore.wait")])
        assert len(wait_state) == 1
        wait_state = wait_state[0]
        task_state = list_tasks(filters=[("func_or_class_name", "=", "task")])
        assert len(task_state) == 1
        task_state = task_state[0]

        assert task_state["state"] == "FINISHED"
        assert wait_state["state"] == "RUNNING"

        return True

    wait_for_condition(verify)


@pytest.mark.parametrize("use_force", [True, False])
def test_cancel_with_dependency(shutdown_only, use_force):
    ray.init(num_cpus=4)

    @ray.remote(num_cpus=1)
    def wait_forever_task():
        while True:
            time.sleep(1000)

    @ray.remote(num_cpus=1)
    def square(x):
        return x * x

    wait_forever_obj = wait_forever_task.remote()
    wait_forever_as_dep = square.remote(wait_forever_obj)
    ray.cancel(wait_forever_as_dep, force=use_force)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(wait_forever_as_dep)


def test_ray_task_cancel_and_retry_race_condition(ray_start_cluster):
    """
    This test is to verify that when a task is cancelled, the retry task will fail
    probably with a TaskCancelledError and is not crashing.

    The test is to:
    1. Start a ray cluster with one head node and one worker node.
    2. Submit a task to the worker node to generate an object big enough to store in the object store.
    3. Cancel the task.
    4. Remove the worker node.
    5. Add a new worker node.
    6. Force a retry task to be scheduled on the new worker node to reconstruct the big object.
    7. Verify that the retry task fails with a TaskCancelledError.
    """
    cluster = ray_start_cluster
    # Add a head node with 0 CPU.
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    # Add one worker node.
    worker_node = cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    @ray.remote(num_cpus=2)
    def producer() -> np.ndarray:
        return np.zeros(1024 * 1000)

    @ray.remote(num_cpus=2)
    def consumer(object_refs: List[ObjectRef[np.ndarray]]) -> np.ndarray:
        return ray.get(object_refs[0])

    # Generate the big object in the object store of the worker node, then kill the worker
    # node. This causes the object to be lost.
    producer_ref = producer.remote()
    ray.wait([producer_ref], fetch_local=False)
    ray.cancel(producer_ref)
    cluster.remove_node(worker_node)

    # Add a new worker node. Run another task that depends on the previously lost big
    # object. This will force a retry task to be scheduled on the new worker node.
    cluster.add_node(num_cpus=2)
    cluster.wait_for_nodes()

    # Test that the retry task fails with a TaskCancelledError because it was previously
    # cancelled.
    with pytest.raises(TaskCancelledError):
        ray.get(consumer.remote([producer_ref]))


def test_is_canceled_with_keyboard_interrupt(ray_start_regular):
    """Test checking is_canceled() within KeyboardInterrupt in normal tasks.

    is_canceled() will be True in KeyboardInterrupt exception block.
    """

    @ray.remote
    def task_handling_keyboard_interrupt(signal_actor):
        try:
            ray.get(signal_actor.wait.remote())
        except KeyboardInterrupt:
            # is_canceled() should be true here
            if ray.get_runtime_context().is_canceled():
                return "canceled_via_keyboard_interrupt"
        return "completed"

    sig = SignalActor.remote()
    ref = task_handling_keyboard_interrupt.remote(sig)

    wait_for_condition(lambda: ray.get(sig.cur_num_waiters.remote()) == 1)

    ray.cancel(ref)

    # Send signal to unblock the task
    ray.get(sig.send.remote())

    # We will get the return value
    assert ray.get(ref) == "canceled_via_keyboard_interrupt"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
