import os
import sys
import tempfile

import ray

from ray._private.test_utils import (Semaphore)


def test_nested_tasks(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def inc(self):
            self.count += 1
            # Since we relex the cap after a timeout we can have slightly more
            # than 1 task. We should never have 20 though since that takes 2^20
            # * 10ms time.
            assert self.count < 20

        def dec(self):
            self.count -= 1

    counter = Counter.remote()

    @ray.remote(num_cpus=1)
    def g():
        return None

    @ray.remote(num_cpus=1)
    def f():
        ray.get(counter.inc.remote())
        res = ray.get(g.remote())
        ray.get(counter.dec.remote())
        return res

    ready, _ = ray.wait(
        [f.remote() for _ in range(1000)], timeout=60.0, num_returns=1000)
    assert len(ready) == 1000, len(ready)
    # Ensure the assertion in `inc` didn't fail.
    ray.get(ready)


def test_recursion(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def summer(n):
        if n == 0:
            return 0
        return n + ray.get(summer.remote(n - 1))

    assert ray.get(summer.remote(10)) == sum(range(11))


def test_out_of_order_scheduling(shutdown_only):
    """Ensure that when a task runs before its dependency, and they're of the same
       scheduling class, the dependency is eventually able to run."""
    ray.init(num_cpus=1)

    @ray.remote
    def foo(arg, path):
        ref, = arg
        should_die = not os.path.exists(path)
        with open(path, "w") as f:
            f.write("")
        if should_die:
            print("dying!!!")
            os._exit(-1)

        if ref:
            print("hogging the only available slot for a while")
            ray.get(ref)
            return "done!"

    with tempfile.TemporaryDirectory() as tmpdir:
        path = f"{tmpdir}/temp.txt"
        first = foo.remote((None, ), path)
        second = foo.remote((first, ), path)
        print(ray.get(second))


def test_limit_concurrency(shutdown_only):
    ray.init(num_cpus=1)

    block_task = Semaphore.remote(0)
    block_driver = Semaphore.remote(0)

    ray.get([block_task.locked.remote(), block_driver.locked.remote()])

    @ray.remote(num_cpus=1)
    def foo():
        ray.get(block_driver.release.remote())
        ray.get(block_task.acquire.remote())

    refs = [foo.remote() for _ in range(20)]

    block_driver_refs = [block_driver.acquire.remote() for _ in range(20)]

    # Some of the tasks will run since we relax the cap, but not all because it
    # should take exponentially long for the cap to be increased.
    ready, not_ready = ray.wait(block_driver_refs, timeout=1, num_returns=20)
    assert len(not_ready) >= 1

    # Now the first instance of foo finishes, so the second starts to run.
    ray.get([block_task.release.remote() for _ in range(19)])

    ready, not_ready = ray.wait(block_driver_refs, timeout=1, num_returns=20)
    assert len(not_ready) == 0

    ready, not_ready = ray.wait(refs, num_returns=20, timeout=5)
    assert len(ready) == 19
    assert len(not_ready) == 1


def test_zero_cpu_scheduling(shutdown_only):
    ray.init(num_cpus=1)

    block_task = Semaphore.remote(0)
    block_driver = Semaphore.remote(0)

    @ray.remote(num_cpus=0)
    def foo():
        ray.get(block_driver.release.remote())
        ray.get(block_task.acquire.remote())

    foo.remote()
    foo.remote()

    ray.get(block_driver.acquire.remote())

    block_driver_ref = block_driver.acquire.remote()

    # Both tasks should be running, so the driver should be unblocked.
    ready, not_ready = ray.wait([block_driver_ref], timeout=1)
    assert len(not_ready) == 0


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
