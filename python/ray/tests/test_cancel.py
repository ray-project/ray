import pytest
import ray
import random
import sys
import time
from ray.exceptions import RayTaskError, RayTimeoutError, \
                            RayCancellationError, RayWorkerError
from ray.test_utils import SignalActor


def valid_exceptions(use_force):
    if use_force:
        return (RayTaskError, RayCancellationError, RayWorkerError)
    else:
        return (RayTaskError, RayCancellationError)


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
    ray.cancel(obj1, use_force)
    for ob in [obj1, obj2, obj3, obj4]:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(ob)

    signaler2 = SignalActor.remote()
    obj1 = wait_for.remote([signaler2.wait.remote()])
    obj2 = wait_for.remote([obj1])
    obj3 = wait_for.remote([obj2])
    obj4 = wait_for.remote([obj3])

    assert len(ray.wait([obj3], timeout=.1)[0]) == 0
    ray.cancel(obj3, use_force)
    for ob in [obj3, obj4]:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(ob)

    with pytest.raises(RayTimeoutError):
        ray.get(obj1, timeout=.1)

    with pytest.raises(RayTimeoutError):
        ray.get(obj2, timeout=.1)

    signaler2.send.remote()
    ray.get(obj1, timeout=10)


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
    ray.cancel(head, use_force)
    for d in deps:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(d)

    head2 = wait_for.remote([signaler.wait.remote()])

    deps2 = []
    for _ in range(3):
        deps2.append(wait_for.remote([head]))

    for d in deps2:
        ray.cancel(d, use_force)

    for d in deps2:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(d)

    signaler.send.remote()
    ray.get(head2, timeout=1)


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
    ray.cancel(obj3, use_force)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(obj3, 10)

    ray.cancel(obj1, use_force)

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

    ray.cancel(a, use_force)
    with pytest.raises(valid_exceptions(use_force)):
        ray.get(a, 10)

    with pytest.raises(valid_exceptions(use_force)):
        ray.get(a2, 10)

    signaler.send.remote()

    with pytest.raises(valid_exceptions(use_force)):
        ray.get(combo, 10)


@pytest.mark.parametrize("use_force", [True, False])
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
    for t in tasks:
        if random.random() > 0.5:
            ray.cancel(t, use_force)
            cancelled.add(t)

    ray.cancel(first, use_force)
    cancelled.add(first)

    for done in cancelled:
        with pytest.raises(valid_exceptions(use_force)):
            ray.get(done, 10)

    for indx in range(len(tasks)):
        t = tasks[indx]
        if sleep_or_no[indx]:
            ray.cancel(t, use_force)
            cancelled.add(t)
        if t in cancelled:
            with pytest.raises(valid_exceptions(use_force)):
                ray.get(t, 10)
        else:
            ray.get(t)


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
        ray.cancel(x)
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
            ray.cancel(ids[idx])
    signaler.send.remote()
    for obj_id in ids:
        try:
            ray.get(obj_id, 10)
        except Exception as e:
            assert isinstance(e, valid_exceptions(use_force))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
