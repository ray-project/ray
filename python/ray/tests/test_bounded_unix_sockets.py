import logging
import sys

import pytest

import ray

import psutil

logger = logging.getLogger(__name__)

"""
Detects number of sockets created by Core Workers.

We need exact 2 unix sockets to Raylet: one for `raylet` and one for `plasma_store`.
Example:

    pconn(fd=31, family=<AddressFamily.AF_UNIX: 1>, type=<SocketKind.SOCK_STREAM: 1>, laddr='', raddr='/tmp/ray/session_2023-11-14_13-21-21_747315_27107/sockets/raylet', status='NONE'),  # noqa: E501
    pconn(fd=78, family=<AddressFamily.AF_UNIX: 1>, type=<SocketKind.SOCK_STREAM: 1>, laddr='', raddr='/tmp/ray/session_2023-11-14_13-21-21_747315_27107/sockets/plasma_store', status='NONE')  # noqa: E501

However it's a long overdue bug that we never closed FDs on raylet's fork to create core
workers, making each core worker end up with O(N^2) open sockets. This test ensures this is fixed.

However it's not that easy:

- We can't determine Ray sockets by name: in Linux, it does not show the address as in
    pconn.raddr, so we don't have a way to make sure it's the socket we are asserting.
- We can't assert that len(all_sockets) == 2, because in MacOS, there are some
    system-created sockets like '/var/run/mDNSResponder'.

So we do it good old way (see python/ray/tests/test_actor_bounded_threads.py), test that
number of sockets do not grow as time goes, across workers.
"""


def my_sockets():
    return psutil.Process().connections("unix")


def assert_bounded_sockets(prev_sockets, new_sockets):
    print(new_sockets)
    assert len(new_sockets) <= len(
        prev_sockets
    ), f"prev {prev_sockets}, new {new_sockets}"


@ray.remote
def fibonacci_and_assert_sockets(i):
    """
    returns (fib[i], my_sockets)
    """
    prev_sockets = my_sockets()
    if i < 2:
        return 1, prev_sockets
    f1, s1 = ray.get(fibonacci_and_assert_sockets.remote(i - 1))
    f2, s2 = ray.get(fibonacci_and_assert_sockets.remote(i - 2))
    assert_bounded_sockets(prev_sockets, s1)
    assert_bounded_sockets(prev_sockets, s2)
    assert_bounded_sockets(prev_sockets, my_sockets())
    return f1 + f2, my_sockets()


@pytest.mark.skipif(sys.platform == "win32", reason="unix sockets are not on windows")
def test_tasks_have_bounded_num_of_sockets(shutdown_only):
    ray.init()
    prev_sockets = my_sockets()
    f1, s1 = ray.get(fibonacci_and_assert_sockets.remote(1))
    f10, s10 = ray.get(fibonacci_and_assert_sockets.remote(10))
    assert f1 == 1
    assert f10 == 89
    assert_bounded_sockets(prev_sockets, s1)
    assert_bounded_sockets(prev_sockets, s10)


@pytest.mark.skipif(sys.platform == "win32", reason="unix sockets are not on windows")
def test_actors_have_bounded_num_of_sockets(shutdown_only):
    """
    As number of actor grows, each new actor still gets same number of sockets as the
    driver.
    """

    @ray.remote
    class A:
        def sum(self, a, b) -> int:
            return a + b

        def sockets(self):
            return my_sockets()

    ray.init()
    driver_sockets = my_sockets()
    actors = []
    for i in range(10):
        a = A.remote()
        actors.append(a)
        assert_bounded_sockets(driver_sockets, ray.get(a.sockets.remote()))
        assert ray.get(a.sum.remote(2, 3)) == 5
        assert_bounded_sockets(driver_sockets, ray.get(a.sockets.remote()))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
