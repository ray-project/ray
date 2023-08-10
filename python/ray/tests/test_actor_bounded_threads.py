import sys
import os
import psutil

import ray
import ray.cluster_utils
import logging

import pytest

logger = logging.getLogger(__name__)

# Tests a lot of workers sending tasks to an actor, the number of threads for that
# actor should not infinitely go up.


def test_threaded_actor_have_bounded_num_of_threads(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        def my_number_of_threads(self):
            pid = os.getpid()
            return psutil.Process(pid).num_threads()

        def add(self, i, j):
            return i + j

    # Spawns a lot of workers, each making 1 call to A.
    @ray.remote
    def fibonacci(a, i):
        if i < 2:
            return 1
        f1 = fibonacci.remote(a, i - 1)
        f2 = fibonacci.remote(a, i - 2)
        return ray.get(a.add.remote(f1, f2))

    a = A.options(max_concurrency=2).remote()

    ray.get(a.my_number_of_threads.remote())
    assert ray.get(fibonacci.remote(a, 1)) == 1
    n = ray.get(a.my_number_of_threads.remote())
    # Creates a lot of workers sending to actor
    assert ray.get(fibonacci.remote(a, 10)) == 89
    assert ray.get(a.my_number_of_threads.remote()) == n


def test_async_actor_have_bounded_num_of_threads(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        async def my_number_of_threads(self):
            pid = os.getpid()
            return psutil.Process(pid).num_threads()

        async def add(self, i, j):
            return i + j

    # Spawns a lot of workers, each making 1 call to A.
    @ray.remote
    def fibonacci(a, i):
        if i < 2:
            return 1
        f1 = fibonacci.remote(a, i - 1)
        f2 = fibonacci.remote(a, i - 2)
        return ray.get(a.add.remote(f1, f2))

    a = A.options(max_concurrency=2).remote()

    ray.get(a.my_number_of_threads.remote())
    assert ray.get(fibonacci.remote(a, 1)) == 1
    n = ray.get(a.my_number_of_threads.remote())
    # Creates a lot of workers sending to actor
    assert ray.get(fibonacci.remote(a, 10)) == 89
    assert ray.get(a.my_number_of_threads.remote()) == n


def test_async_actor_cg_have_bounded_num_of_threads(shutdown_only):
    ray.init()

    @ray.remote(concurrency_groups={"io": 2, "compute": 4})
    class A:
        async def my_number_of_threads(self):
            pid = os.getpid()
            return psutil.Process(pid).num_threads()

        @ray.method(concurrency_group="io")
        async def io_add(self, i, j):
            return i + j

        @ray.method(concurrency_group="compute")
        async def compute_add(self, i, j):
            return i + j

        async def default_add(self, i, j):
            return i + j

    # Spawns a lot of workers, each making 1 call to A.
    @ray.remote
    def fibonacci(a, i):
        if i < 2:
            return 1
        f1 = fibonacci.remote(a, i - 1)
        f2 = fibonacci.remote(a, i - 2)
        assert ray.get(a.io_add.remote(1, 2)) == 3
        assert ray.get(a.compute_add.remote(4, 5)) == 9
        return ray.get(a.default_add.remote(f1, f2))

    a = A.options(max_concurrency=2).remote()

    ray.get(a.my_number_of_threads.remote())
    assert ray.get(fibonacci.remote(a, 1)) == 1
    n = ray.get(a.my_number_of_threads.remote())
    # Creates a lot of workers sending to actor
    assert ray.get(fibonacci.remote(a, 10)) == 89
    assert ray.get(a.my_number_of_threads.remote()) == n


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
