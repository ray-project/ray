import os
import pytest
import sys

import ray
from ray._private.test_utils import close_common_connections
from ray.exceptions import ActorUnavailableError
import signal

import time


def test_actor_unavailable_conn_broken(ray_start_regular):
    @ray.remote
    class Counter:
        def __init__(self) -> None:
            self.c = 0

        def slow_increment(self, i, secs):
            self.c += i
            print(f"incrementing {i} to {self.c}")
            time.sleep(secs)
            return self.c

        def getpid(self):
            return os.getpid()

        def read(self):
            return self.c

    a = Counter.remote()
    assert ray.get(a.slow_increment.remote(2, 0.1)) == 2
    pid = ray.get(a.getpid.remote())
    task = a.slow_increment.remote(3, 5)

    # Break the grpc connection from this process to the actor process. The `ray.get`
    # call would fail because of grpc connection break, with ActorUnavailableError.
    # However, the actor is still alive, and the side effect still happened.
    # Also, a next remote() call would try to reestablish the connection and the actor
    # can continue to work.
    close_common_connections(pid)
    with pytest.raises(ActorUnavailableError):
        ray.get(task)
    assert ray.get(a.read.remote()) == 5
    assert ray.get(a.slow_increment.remote(4, 0.1)) == 9


def test_actor_unavailable_restarting(ray_start_cluster):
    @ray.remote(max_restarts=-1)
    class Actor:
        def sum(self, i, j):
            print(f"sum({i} + {j}) = {i+j}")
            return i + j

        def getpid(self):
            return os.getpid()

    cluster = ray_start_cluster
    ray.init(address=cluster.address)

    a = Actor.remote()
    assert ray.get(a.sum.remote(1, 2)) == 3
    pid = ray.get(a.getpid.remote())

    # Kill the actor process. The caller gets a connection reset so it's unavailable
    os.kill(pid, signal.SIGKILL)
    with pytest.raises(ActorUnavailableError):
        print(ray.get(a.sum.remote(1, 2)))

    # A second remote call will try to re-establish the conn and fail; so it will
    # restart the actor
    for i in range(10):
        assert ray.get(a.sum.remote(1, 2)) == 3


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
