import sys
import os

import ray
import logging
from typing import Dict
import psutil

import pytest

logger = logging.getLogger(__name__)


def my_sockets() -> Dict[str, int]:
    """
    Returns [(thread_id, thread_name)]
    """
    return psutil.Process().connections("unix")


def assert_2_ray_sockets(sockets):
    """
    Each ray worker should have exactly 2 Ray related unix sockets. Maybe also some other sockets e.g. to DNS responder.
    Example:
    pconn(fd=6, family=<AddressFamily.AF_UNIX: 1>, type=<SocketKind.SOCK_STREAM: 1>, laddr='', raddr='/var/run/mDNSResponder', status='NONE'),  # noqa: E501
    pconn(fd=31, family=<AddressFamily.AF_UNIX: 1>, type=<SocketKind.SOCK_STREAM: 1>, laddr='', raddr='/tmp/ray/session_2023-11-14_13-21-21_747315_27107/sockets/raylet', status='NONE'),  # noqa: E501
    pconn(fd=78, family=<AddressFamily.AF_UNIX: 1>, type=<SocketKind.SOCK_STREAM: 1>, laddr='', raddr='/tmp/ray/session_2023-11-14_13-21-21_747315_27107/sockets/plasma_store', status='NONE')  # noqa: E501
    """
    raylet_sockets = []
    plasma_store_sockets = []
    for socket in sockets:
        if socket.raddr.endswith("/sockets/raylet"):
            raylet_sockets.append(socket)
        if socket.raddr.endswith("/sockets/plasma_store"):
            plasma_store_sockets.append(socket)
    print(f"totally {len(sockets)} sockets.")
    assert len(raylet_sockets) == 1, raylet_sockets
    assert len(plasma_store_sockets) == 1, plasma_store_sockets


# Spawns a lot of workers, each making 1 call to A.
@ray.remote
def fibonacci(a, i):
    if i < 2:
        return 1
    f1 = fibonacci.remote(a, i - 1)
    f2 = fibonacci.remote(a, i - 2)
    return ray.get(a.add.remote(f1, f2))


@ray.remote
def fibonacci_and_assert_sockets(i):
    if i < 2:
        return 1
    f1 = fibonacci_and_assert_sockets.remote(i - 1)
    f2 = fibonacci_and_assert_sockets.remote(i - 2)
    assert_2_ray_sockets(my_sockets())
    return sum(ray.get([f1, f2]))


@pytest.mark.skipif(sys.platform == "win32", reason="unix sockets are not on windows")
def test_tasks_have_2_ray_sockets(shutdown_only):
    ray.init()

    assert_2_ray_sockets(my_sockets())
    assert ray.get(fibonacci_and_assert_sockets.remote(1)) == 1
    assert ray.get(fibonacci_and_assert_sockets.remote(10)) == 89
    assert_2_ray_sockets(my_sockets())


@pytest.mark.skipif(sys.platform == "win32", reason="unix sockets are not on windows")
def test_threaded_actor_have_2_ray_sockets(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        def get_my_sockets(self):
            return my_sockets()

        def add(self, i, j):
            return i + j

    a = A.options(max_concurrency=2).remote()

    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))

    assert ray.get(fibonacci.remote(a, 1)) == 1
    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))

    # Creates a lot of workers sending to actor
    assert ray.get(fibonacci.remote(a, 10)) == 89
    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))


@pytest.mark.skipif(sys.platform == "win32", reason="unix sockets are not on windows")
def test_async_actor_have_bounded_num_of_threads(shutdown_only):
    ray.init()

    @ray.remote
    class A:
        async def get_my_sockets(self):
            return my_sockets()

        async def add(self, i, j):
            return i + j

    a = A.options(max_concurrency=2).remote()

    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))

    assert ray.get(fibonacci.remote(a, 1)) == 1
    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))

    # Creates a lot of workers sending to actor
    assert ray.get(fibonacci.remote(a, 10)) == 89
    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))


@pytest.mark.skipif(sys.platform == "win32", reason="unix sockets are not on windows")
def test_async_actor_cg_have_bounded_num_of_threads(shutdown_only):
    ray.init()

    @ray.remote(concurrency_groups={"io": 2, "compute": 4})
    class A:
        async def get_my_sockets(self):
            return my_sockets()

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
    def fibonacci_cg(a, i):
        if i < 2:
            return 1
        f1 = fibonacci_cg.remote(a, i - 1)
        f2 = fibonacci_cg.remote(a, i - 2)
        assert ray.get(a.io_add.remote(1, 2)) == 3
        assert ray.get(a.compute_add.remote(4, 5)) == 9
        return ray.get(a.default_add.remote(f1, f2))

    a = A.options(max_concurrency=2).remote()

    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))

    assert ray.get(fibonacci_cg.remote(a, 1)) == 1
    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))

    # Creates a lot of workers sending to actor
    assert ray.get(fibonacci_cg.remote(a, 10)) == 89
    assert_2_ray_sockets(ray.get(a.get_my_sockets.remote()))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
