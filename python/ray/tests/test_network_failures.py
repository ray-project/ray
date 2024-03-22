import os
import pytest
import sys

import ray
from ray._private.test_utils import Cgroup2NetworkBlocker
from ray.exceptions import ActorUnavailableError


@pytest.mark.skipif(
    sys.platform == "linux", "Cgroup2NetworkBlocker only works on Linux."
)
def test_network_blocker_works():
    import requests

    with Cgroup2NetworkBlocker(os.getpid()):
        with pytest.raises(requests.exceptions.RequestException):
            print(requests.get("http://example.com", timeout=5))
    print(requests.get("http://example.com", timeout=5))


@pytest.mark.skipif(
    sys.platform == "linux", "Cgroup2NetworkBlocker only works on Linux."
)
def test_actor_unavailable(ray_start_cluster):
    """
    An actor raises ActorUnavailableError when there's a network isolation.
    After the actor reconnected, it can be used again.
    """

    @ray.remote
    class Actor:
        def sum(self, i, j):
            return i + j

        def getpid(self):
            return os.getpid()

    cluster = ray_start_cluster
    ray.init(address=cluster.address)

    a = Actor.remote()
    assert ray.get(a.sum.remote(1, 2)) == 3
    pid = ray.get(a.getpid.remote())

    with Cgroup2NetworkBlocker(pid):
        with pytest.raises(ActorUnavailableError):
            ray.get(a.sum.remote(1, 2))

    assert ray.get(a.sum.remote(1, 2)) == 3


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
