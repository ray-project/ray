import pytest
import ray
import random
import sys
import time
from ray.exceptions import RayTaskError, RayTimeoutError, RayCancellationError
from ray.test_utils import SignalActor


@pytest.mark.parametrize("use_force", [True, False])
def test_kill_chain(ray_start_regular, use_force):
    """A helper method for chain of events tests"""
    signaler = SignalActor.remote()

    @ray.remote
    def wait_for(t):
        return ray.get(t[0])

    obj1 = wait_for.remote([signaler.wait.remote()])
    obj2 = wait_for.remote([obj1])
    obj3 = wait_for.remote([obj2])
    obj4 = wait_for.remote([obj3])

    assert len(ray.wait([obj1], timeout=.1)[0]) == 0
    ray.kill(obj1, use_force)
    for ob in [obj1, obj2, obj3, obj4]:
        with pytest.raises((RayTaskError, RayCancellationError)):
            ray.get(ob)

    signaler2 = SignalActor.remote()
    obj1 = wait_for.remote([signaler2.wait.remote()])
    obj2 = wait_for.remote([obj1])
    obj3 = wait_for.remote([obj2])
    obj4 = wait_for.remote([obj3])

    assert len(ray.wait([obj3], timeout=.1)[0]) == 0
    ray.kill(obj3, use_force)
    for ob in [obj3, obj4]:
        with pytest.raises((RayTaskError, RayCancellationError)):
            ray.get(ob)

    with pytest.raises(RayTimeoutError):
        ray.get(obj1, timeout=.1)

    signaler2.send.remote()
    ray.get(obj1, timeout=.1)


@pytest.mark.parametrize("use_force", [True, False])
def test_kill_multiple_dependents(ray_start_regular, use_force):
    """A helper method for multiple waiters on events tests"""
    signaler = SignalActor.remote()

    @ray.remote
    def wait_for(t):
        return ray.get(t[0])

    head = wait_for.remote([signaler.wait.remote()])
    deps = []
    for _ in range(3):
        deps.append(wait_for.remote([head]))

    assert len(ray.wait([head], timeout=.1)[0]) == 0
    ray.kill(head, use_force)
    for d in deps:
        with pytest.raises((RayTaskError, RayCancellationError)):
            ray.get(d)

    head2 = wait_for.remote([signaler.wait.remote()])

    deps2 = []
    for _ in range(3):
        deps2.append(wait_for.remote([head]))

    for d in deps2:
        ray.kill(d, use_force)

    for d in deps2:
        with pytest.raises((RayTaskError, RayCancellationError)):
            ray.get(d)

    signaler.send.remote()
    ray.get(head2, timeout=1)


@pytest.mark.parametrize("use_force", [True, False])
def test_single_cpu_kill(shutdown_only, use_force):
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
    ray.kill(obj3, use_force)
    with pytest.raises((RayTaskError, RayCancellationError)):
        ray.get(obj3, 0.1)

    ray.kill(obj1, use_force)

    for d in [obj1, obj2]:
        with pytest.raises((RayTaskError, RayCancellationError)):
            ray.get(d)

    signaler.send.remote()
    ray.get(indep)


def test_kill_dependency_waiting(ray_start_regular):
    @ray.remote
    def slp(t):
        time.sleep(t)
        return t

    a1 = slp.remote(1000)
    a2 = slp.remote(a1)
    a3 = slp.remote(a2)

    ray.kill(a3, True)

    # with pytest.raises((RayTaskError, RayCancellationError)):
    #     ray.get(a3, 10)

    ray.kill(a1, True)

    with pytest.raises((RayTaskError, RayCancellationError)):
        ray.get(a1, 10)

    with pytest.raises((RayTaskError, RayCancellationError)):
        ray.get(a2, 10)


def test_comprehensive(ray_start_regular):
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

    ray.kill(a, False)
    with pytest.raises((RayTaskError, RayCancellationError)):
        ray.get(a, 1)

    with pytest.raises((RayTaskError, RayCancellationError)):
        ray.get(a2, 1)

    signaler.send.remote()
    # ray.get(b)

    with pytest.raises((RayTaskError, RayCancellationError)):
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
    killed = set()
    for t in tasks:
        if random.random() > 0.5:
            ray.kill(t, use_force)
            killed.add(t)

    ray.kill(first, use_force)
    killed.add(first)

    for done in killed:
        with pytest.raises((RayTaskError, RayCancellationError)):
            ray.get(done, 10)

    for indx in range(len(tasks)):
        t = tasks[indx]
        if sleep_or_no[indx]:
            ray.kill(t, use_force)
            killed.add(t)
        if t in killed:
            with pytest.raises((RayTaskError, RayCancellationError)):
                ray.get(t, 10)
        else:
            ray.get(t)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
