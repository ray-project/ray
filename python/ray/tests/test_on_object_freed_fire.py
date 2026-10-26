import sys
import time

import numpy as np
import pytest

import ray
from ray._private.test_utils import wait_for_condition

MiB = 1024 * 1024


@ray.remote
class Producer:
    def __init__(self):
        self.freed = []

    def __on_object_freed__(self, object_id):
        self.freed.append(object_id)

    def produce(self, size):
        return np.zeros(size, dtype=np.uint8)

    def put(self, size):
        # Wrapped so that the caller borrows the ref instead of getting the value.
        return [ray.put(np.zeros(size, dtype=np.uint8))]

    def get_freed(self):
        return self.freed


def assert_freed_once(producer, object_ids):
    """Wait for the hook to run for every object, then check it ran only once each."""
    expected = sorted(object_ids)
    wait_for_condition(lambda: sorted(ray.get(producer.get_freed.remote())) == expected)
    # Frees are batched for up to a second; wait past that for a second run.
    time.sleep(2)
    assert sorted(ray.get(producer.get_freed.remote())) == expected


def test_hook_runs_when_a_task_return_goes_out_of_scope(shutdown_only):
    ray.init(num_cpus=1)
    producer = Producer.remote()
    ref = producer.produce.remote(MiB)
    ray.wait([ref])
    object_id = ref.hex()
    del ref
    assert_freed_once(producer, [object_id])


def test_hook_runs_when_a_put_goes_out_of_scope(shutdown_only):
    ray.init(num_cpus=1)
    producer = Producer.remote()
    [ref] = ray.get(producer.put.remote(MiB))
    object_id = ref.hex()
    del ref
    assert_freed_once(producer, [object_id])


@pytest.mark.parametrize("read_after_spill", [False, True])
def test_hook_runs_once_for_spilled_objects(
    fs_only_object_spilling_config, shutdown_only, read_after_spill
):
    object_spilling_config, _ = fs_only_object_spilling_config
    ray.init(
        num_cpus=1,
        object_store_memory=75 * MiB,
        _system_config={
            "automatic_object_spilling_enabled": True,
            "object_spilling_config": object_spilling_config,
            "min_spilling_size": 0,
        },
    )
    producer = Producer.remote()
    # 200 MiB in a 75 MiB store: most of them are spilled and evicted.
    refs = [producer.produce.remote(20 * MiB) for _ in range(10)]
    # Without fetch_local=False, the wait restores the evicted ones.
    ray.wait(refs, num_returns=len(refs), fetch_local=False)
    if read_after_spill:
        # Restores the evicted ones, so the node has a copy in memory and on disk.
        for ref in refs:
            ray.get(ref)
        del ref
    object_ids = [ref.hex() for ref in refs]
    del refs
    assert_freed_once(producer, object_ids)


def test_hook_runs_once_for_an_object_copied_to_another_node(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"producer": 1})
    producer = Producer.options(resources={"producer": 1}).remote()
    ref = producer.produce.remote(MiB)
    # Copies the object to the driver's node.
    ray.get(ref)
    object_id = ref.hex()
    del ref
    assert_freed_once(producer, [object_id])


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
