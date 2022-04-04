# coding: utf-8
import logging
import sys
import time

import numpy as np
import pytest

from ray.cluster_utils import Cluster, cluster_not_supported
from ray._private.test_utils import client_test_enabled
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.exceptions import GetTimeoutError

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


@pytest.mark.xfail(cluster_not_supported, reason="cluster not supported")
@pytest.mark.skipif(client_test_enabled(), reason="init issue")
def test_system_config_when_connecting(ray_start_cluster_enabled):
    config = {"object_timeout_milliseconds": 200}
    cluster = Cluster()
    cluster.add_node(_system_config=config, object_store_memory=100 * 1024 * 1024)
    cluster.wait_for_nodes()

    # Specifying _system_config when connecting to a cluster is disallowed.
    with pytest.raises(ValueError):
        ray.init(address=cluster.address, _system_config=config)

    # Check that the config was picked up (object pinning is disabled).
    ray.init(address=cluster.address)
    obj_ref = ray.put(np.zeros(40 * 1024 * 1024, dtype=np.uint8))

    for _ in range(5):
        put_ref = ray.put(np.zeros(40 * 1024 * 1024, dtype=np.uint8))
    del put_ref

    ray.get(obj_ref)


def test_get_multiple(ray_start_regular_shared):
    object_refs = [ray.put(i) for i in range(10)]
    assert ray.get(object_refs) == list(range(10))

    # Get a random choice of object refs with duplicates.
    indices = list(np.random.choice(range(10), 5))
    indices += indices
    results = ray.get([object_refs[i] for i in indices])
    assert results == indices


def test_get_with_timeout(ray_start_regular_shared):
    SignalActor = create_remote_signal_actor(ray)
    signal = SignalActor.remote()

    # Check that get() returns early if object is ready.
    start = time.time()
    ray.get(signal.wait.remote(should_wait=False), timeout=30)
    assert time.time() - start < 30

    # Check that get() raises a TimeoutError after the timeout if the object
    # is not ready yet.
    result_id = signal.wait.remote()
    with pytest.raises(GetTimeoutError):
        ray.get(result_id, timeout=0.1)

    # Check that a subsequent get() returns early.
    ray.get(signal.send.remote())
    start = time.time()
    ray.get(result_id, timeout=30)
    assert time.time() - start < 30


# https://github.com/ray-project/ray/issues/6329
def test_call_actors_indirect_through_tasks(ray_start_regular_shared):
    @ray.remote
    class Counter:
        def __init__(self, value):
            self.value = int(value)

        def increase(self, delta):
            self.value += int(delta)
            return self.value

    @ray.remote
    def foo(object):
        return ray.get(object.increase.remote(1))

    @ray.remote
    def bar(object):
        return ray.get(object.increase.remote(1))

    @ray.remote
    def zoo(object):
        return ray.get(object[0].increase.remote(1))

    c = Counter.remote(0)
    for _ in range(0, 100):
        ray.get(foo.remote(c))
        ray.get(bar.remote(c))
        ray.get(zoo.remote([c]))


def test_inline_arg_memory_corruption(ray_start_regular_shared):
    @ray.remote
    def f():
        return np.zeros(1000, dtype=np.uint8)

    @ray.remote
    class Actor:
        def __init__(self):
            self.z = []

        def add(self, x):
            self.z.append(x)
            for prev in self.z:
                assert np.sum(prev) == 0, ("memory corruption detected", prev)

    a = Actor.remote()
    for i in range(100):
        ray.get(a.add.remote(f.remote()))


@pytest.mark.skipif(client_test_enabled(), reason="internal api")
def test_skip_plasma(ray_start_regular_shared):
    @ray.remote
    class Actor:
        def __init__(self):
            pass

        def f(self, x):
            return x * 2

    a = Actor.remote()
    obj_ref = a.f.remote(1)
    # it is not stored in plasma
    assert not ray.worker.global_worker.core_worker.object_exists(obj_ref)
    assert ray.get(obj_ref) == 2


if __name__ == "__main__":
    import pytest

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    sys.exit(pytest.main(["-v", __file__]))
