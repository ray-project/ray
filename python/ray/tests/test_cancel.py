import random
import sys
import time

import pytest

import ray
from ray.exceptions import TaskCancelledError, RayTaskError, \
                           GetTimeoutError, WorkerCrashedError, \
                           ObjectLostError
from ray.test_utils import SignalActor


def valid_exceptions(use_force):
    if use_force:
        return (RayTaskError, TaskCancelledError, WorkerCrashedError,
                ObjectLostError)
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

    assert len(ray.wait([obj1], timeout=.1)[0]) == 0
    ray.cancel(obj1, force=use_force)
    for ob in [obj1, obj2, obj3, obj4]:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(ob)

    signaler2 = SignalActor.remote()
    obj1 = wait_for.remote([signaler2.wait.remote()])
    obj2 = wait_for.remote([obj1])
    obj3 = wait_for.remote([obj2])
    obj4 = wait_for.remote([obj3])

    assert len(ray.wait([obj3], timeout=.1)[0]) == 0
    ray.cancel(obj3, force=use_force)
    for ob in [obj3, obj4]:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(ob)

    with pytest.raises(GetTimeoutError):
        ray.get(obj1, timeout=.1)

    with pytest.raises(GetTimeoutError):
        ray.get(obj2, timeout=.1)

    signaler2.send.remote()
    ray.get(obj1)


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

    assert len(ray.wait([head], timeout=.1)[0]) == 0
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

    assert len(ray.wait([obj3], timeout=.1)[0]) == 0
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
        ray.get(a2, timeout=10)

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
                e, valid_exceptions(use_force)), f"Failure on iteration: {i}"


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
    sys.exit(pytest.main(["-v", __file__]))
