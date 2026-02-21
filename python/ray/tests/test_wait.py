# coding: utf-8

import logging
import sys
import time

import numpy as np
import pytest

from ray._private.test_utils import client_test_enabled

if client_test_enabled():
    from ray.util.client import ray
else:
    import ray
    import ray.util.state

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


@pytest.mark.skipif(client_test_enabled(), reason="util not available with ray client")
def test_wait_always_fetch_local(monkeypatch, ray_start_cluster):
    monkeypatch.setenv("RAY_scheduler_report_pinned_bytes_only", "false")
    cluster = ray_start_cluster
    head_node = cluster.add_node(num_cpus=0, object_store_memory=300e6)
    ray.init(address=cluster.address)
    worker_node = cluster.add_node(num_cpus=1, object_store_memory=300e6)

    @ray.remote(num_cpus=1)
    def return_large_object():
        # 100mb so will spill on worker, but not once on head
        return np.zeros(100 * 1024 * 1024, dtype=np.uint8)

    @ray.remote(num_cpus=0)
    def small_local_task():
        return 1

    put_on_head = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        head_node.node_id, soft=False
    )
    put_on_worker = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        worker_node.node_id, soft=False
    )
    x = small_local_task.options(scheduling_strategy=put_on_head).remote()
    y = return_large_object.options(scheduling_strategy=put_on_worker).remote()
    z = return_large_object.options(scheduling_strategy=put_on_worker).remote()

    # will return when tasks are done
    ray.wait([x, y, z], num_returns=3, fetch_local=False)
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        > 250e6
    )

    # x should be immediately available locally, start fetching y and z
    ray.wait([x, y, z], num_returns=1, fetch_local=True)
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        > 250e6
    )

    time.sleep(5)
    # y, z should be pulled here
    assert (
        ray._private.state.available_resources_per_node()[head_node.node_id][
            "object_store_memory"
        ]
        < 150e6
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
