import time
import unittest

import numpy as np
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


def test_large_e2e_backpressure_no_spilling(shutdown_only, restore_data_context):
    """Test backpressure can prevent object spilling on a synthetic large-scale workload."""
    # The cluster has 10 CPUs and 200MB object store memory.
    #
    # Each produce task generates 10 blocks, each of which has 10MB data.
    # In total, there will be 10 * 10 * 10MB = 1000MB intermediate data.
    #
    # `ReservationOpResourceAllocator` should dynamically allocate resources to each
    # operator and prevent object spilling.
    NUM_CPUS = 10
    NUM_ROWS_PER_TASK = 10
    NUM_TASKS = 20
    NUM_ROWS_TOTAL = NUM_ROWS_PER_TASK * NUM_TASKS
    BLOCK_SIZE = 10 * 1024 * 1024
    object_store_memory = 200 * 1024**2
    print(f"object_store_memory: {object_store_memory/1024/1024}MB")
    ray.init(num_cpus=NUM_CPUS, object_store_memory=object_store_memory)

    def produce(batch):
        print("Produce task started", batch["id"])
        time.sleep(0.1)
        for id in batch["id"]:
            print("Producing", id)
            yield {
                "id": [id],
                "image": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
            }

    def consume(batch):
        print("Consume task started", batch["id"])
        time.sleep(0.01)
        return {"id": batch["id"], "result": [0 for _ in batch["id"]]}

    data_context = ray.data.DataContext.get_current()
    data_context.execution_options.verbose_progress = True
    data_context.target_max_block_size = BLOCK_SIZE

    last_snapshot = get_initial_core_execution_metrics_snapshot()

    ds = ray.data.range(NUM_ROWS_TOTAL, parallelism=NUM_TASKS)
    ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK)
    ds = ds.map_batches(consume, batch_size=None, num_cpus=0.9)
    # Check core execution metrics every 10 rows, because it's expensive.
    for _ in ds.iter_batches(batch_size=NUM_ROWS_PER_TASK):
        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                object_store_stats={
                    "spilled_bytes_total": 0,
                    "restored_bytes_total": 0,
                },
            ),
            last_snapshot,
        )


def _build_dataset(
    obj_store_limit,
    producer_num_cpus,
    consumer_num_cpus,
    num_blocks,
    block_size,
):
    # Create a dataset with 2 operators:
    # - The producer op has only 1 task, which produces `num_blocks` blocks, each
    #   of which has `block_size` data.
    # - The consumer op has `num_blocks` tasks, each of which consumes 1 block.
    ctx = ray.data.DataContext.get_current()
    ctx.target_max_block_size = block_size
    ctx.execution_options.resource_limits.object_store_memory = obj_store_limit

    def producer(batch):
        for i in range(num_blocks):
            print("Producing block", i, time.time())
            yield {
                "id": [i],
                "data": [np.zeros(block_size, dtype=np.uint8)],
            }

    def consumer(batch):
        assert len(batch["id"]) == 1
        print("Consuming block", batch["id"][0], time.time())
        time.sleep(0.01)
        del batch["data"]
        return batch

    ds = ray.data.range(1, parallelism=1).materialize()
    ds = ds.map_batches(producer, batch_size=None, num_cpus=producer_num_cpus)
    ds = ds.map_batches(consumer, batch_size=None, num_cpus=consumer_num_cpus)
    return ds


@pytest.mark.parametrize(
    "cluster_cpus, cluster_obj_store_mem_mb",
    [
        (3, 500),  # CPU not enough
        (4, 100),  # Object store memory not enough
        (3, 100),  # Both not enough
    ],
)
def test_no_deadlock_on_small_cluster_resources(
    cluster_cpus,
    cluster_obj_store_mem_mb,
    shutdown_only,
    restore_data_context,
):
    """Test when cluster resources are not enough for launch one task per op,
    the execution can still proceed without deadlock.
    """
    cluster_obj_store_mem_mb *= 1024**2
    ray.init(num_cpus=cluster_cpus, object_store_memory=cluster_obj_store_mem_mb)
    num_blocks = 10
    block_size = 100 * 1024 * 1024
    ds = _build_dataset(
        obj_store_limit=cluster_obj_store_mem_mb // 2,
        producer_num_cpus=3,
        consumer_num_cpus=1,
        num_blocks=num_blocks,
        block_size=block_size,
    )
    assert len(ds.take_all()) == num_blocks


def test_no_deadlock_on_resource_contention(shutdown_only, restore_data_context):
    """Test when resources are preempted by non-Data code, the execution can
    still proceed without deadlock."""
    cluster_obj_store_mem = 1000 * 1024 * 1024
    ray.init(num_cpus=5, object_store_memory=cluster_obj_store_mem)
    # Create a non-Data actor that uses 4 CPUs, only 1 CPU
    # is left for Data. Currently Data StreamExecutor still
    # incorrectly assumes it has all the 5 CPUs.
    # Check that we don't deadlock in this case.

    @ray.remote(num_cpus=4)
    class DummyActor:
        def foo(self):
            return None

    dummy_actor = DummyActor.remote()
    ray.get(dummy_actor.foo.remote())

    num_blocks = 10
    block_size = 50 * 1024 * 1024
    ds = _build_dataset(
        obj_store_limit=cluster_obj_store_mem // 2,
        producer_num_cpus=1,
        consumer_num_cpus=0.9,
        num_blocks=num_blocks,
        block_size=block_size,
    )
    assert len(ds.take_all()) == num_blocks


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
