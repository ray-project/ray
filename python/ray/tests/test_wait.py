# coding: utf-8

import pytest
import numpy as np
import time
import logging
import sys
import os

from ray._private.test_utils import client_test_enabled


if client_test_enabled():
    from ray.util.client import ray
else:
    import ray

logger = logging.getLogger(__name__)


def test_wait(ray_start_regular):
    @ray.remote
    def f(delay):
        time.sleep(delay)
        return

    object_refs = [f.remote(0), f.remote(0), f.remote(0), f.remote(0)]
    ready_ids, remaining_ids = ray.wait(object_refs)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 3
    ready_ids, remaining_ids = ray.wait(object_refs, num_returns=4)
    assert set(ready_ids) == set(object_refs)
    assert remaining_ids == []

    object_refs = [f.remote(0), f.remote(5)]
    ready_ids, remaining_ids = ray.wait(object_refs, timeout=0.5, num_returns=2)
    assert len(ready_ids) == 1
    assert len(remaining_ids) == 1

    # Verify that calling wait with duplicate object refs throws an
    # exception.
    x = ray.put(1)
    with pytest.raises(Exception):
        ray.wait([x, x])

    # Make sure it is possible to call wait with an empty list.
    ready_ids, remaining_ids = ray.wait([])
    assert ready_ids == []
    assert remaining_ids == []

    # Test semantics of num_returns with no timeout.
    obj_refs = [ray.put(i) for i in range(10)]
    (found, rest) = ray.wait(obj_refs, num_returns=2)
    assert len(found) == 2
    assert len(rest) == 8

    # Verify that incorrect usage raises a TypeError.
    x = ray.put(1)
    with pytest.raises(TypeError):
        ray.wait(x)
    with pytest.raises(TypeError):
        ray.wait(1)
    with pytest.raises(TypeError):
        ray.wait([1])


def test_wait_timing(ray_start_2_cpus):
    @ray.remote
    def f():
        time.sleep(1)

    future = f.remote()

    start = time.time()
    ready, not_ready = ray.wait([future], timeout=0.2)
    assert 0.2 < time.time() - start < 0.3
    assert len(ready) == 0
    assert len(not_ready) == 1


def test_wait_always_fetch_local(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=500e6)  # head node
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=1, object_store_memory=80e6)

    @ray.remote(num_cpus=1)
    def return_large_object():
        # 100mb so will spill on worker, but not once on head
        return np.zeros(100 * 1024 * 1024, dtype=np.uint8)

    @ray.remote(num_cpus=0)
    def small_local_task():
        return 1

    put_on_worker = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        worker_node.node_id, soft=False
    )
    x = small_local_task.remote()
    y = return_large_object.options(scheduling_strategy=put_on_worker).remote()
    z = return_large_object.options(scheduling_strategy=put_on_worker).remote()
    # even though x will be found in local, requests should be made
    # to start pulling y and z
    ray.wait([x, y, z], num_returns=1, fetch_local=True)
    time.sleep(3)

    start_time = time.perf_counter()
    ray.get([y, z])
    # y and z should be immediately available as pull requests should've
    # been made immediately on the ray.wait call
    time_to_get = time.perf_counter() - start_time
    assert time_to_get < 0.2


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
