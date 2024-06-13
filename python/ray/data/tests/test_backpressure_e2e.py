import time
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
    get_initial_core_execution_metrics_snapshot,
)
from ray.tests.conftest import shutdown_only  # noqa: F401


def test_large_e2e_backpressure_no_spilling(
    shutdown_only, restore_data_context  # noqa: F811
):
    """Test backpressure can prevent object spilling on a synthetic large-scale
    workload."""
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

    ds = ray.data.range(NUM_ROWS_TOTAL, override_num_blocks=NUM_TASKS)
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
    insert_limit_op=False,
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

    ds = ray.data.range(1, override_num_blocks=1).materialize()
    ds = ds.map_batches(producer, batch_size=None, num_cpus=producer_num_cpus)
    # Add a limit op in the middle, to test that ReservationOpResourceAllocator
    # will account limit op's resource usage to the previous producer map op.
    if insert_limit_op:
        ds = ds.limit(num_blocks)
    ds = ds.map_batches(consumer, batch_size=None, num_cpus=consumer_num_cpus)
    if insert_limit_op:
        ds = ds.limit(num_blocks)
    return ds


@pytest.mark.parametrize(
    "cluster_cpus, cluster_obj_store_mem_mb",
    [
        (3, 500),  # CPU not enough
        (4, 100),  # Object store memory not enough
        (3, 100),  # Both not enough
    ],
)
@pytest.mark.parametrize("insert_limit_op", [False, True])
def test_no_deadlock_on_small_cluster_resources(
    cluster_cpus,
    cluster_obj_store_mem_mb,
    insert_limit_op,
    shutdown_only,  # noqa: F811
    restore_data_context,  # noqa: F811
):
    """Test when cluster resources are not enough for launching one task per op,
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
        insert_limit_op=insert_limit_op,
    )
    assert len(ds.take_all()) == num_blocks


@pytest.mark.parametrize("insert_limit_op", [False, True])
def test_no_deadlock_on_resource_contention(
    insert_limit_op, shutdown_only, restore_data_context  # noqa: F811
):
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
        insert_limit_op=insert_limit_op,
    )

    from ray.data._internal.execution.resource_manager import (
        ReservationOpResourceAllocator,
    )

    with patch.object(
        ReservationOpResourceAllocator.IdleDetector,
        "DETECTION_INTERVAL_S",
        0.1,
    ):
        assert len(ds.take_all()) == num_blocks


def test_no_deadlock_with_preserve_order(
    restore_data_context, shutdown_only  # noqa: F811
):
    """Test backpressure won't cause deadlocks when `preserve_order=True`."""
    num_blocks = 20
    block_size = 10 * 1024 * 1024
    ray.init(num_cpus=num_blocks)
    data_context = ray.data.DataContext.get_current()
    data_context.target_max_block_size = block_size
    data_context._max_num_blocks_in_streaming_gen_buffer = 1
    data_context.execution_options.preserve_order = True
    data_context.execution_options.resource_limits.object_store_memory = 5 * block_size

    # Some tasks are slower than others.
    # The faster tasks will finish first and occupy Map op's internal output buffer.
    # Test that we won't backpressure the operator in this case.
    def map_fn(batch):
        idx = batch["id"][0]
        print("map_fn", idx, time.time())
        if idx % 2 == 0:
            time.sleep(3)
        batch["data"] = [np.zeros(block_size, dtype=np.uint8)]
        return batch

    ds = ray.data.range(num_blocks, override_num_blocks=num_blocks)
    ds = ds.map_batches(map_fn, batch_size=None, num_cpus=1)
    assert len(ds.take_all()) == num_blocks


def test_input_backpressure_e2e(restore_data_context, shutdown_only):  # noqa: F811
    # Tests that backpressure applies even when reading directly from the input
    # datasource. This relies on datasource metadata size estimation.
    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1

        def get(self):
            return self.count

        def reset(self):
            self.count = 0

    class CountingRangeDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism, n):
            def range_(i):
                ray.get(self.counter.increment.remote())
                return [
                    pd.DataFrame({"data": np.ones((n // parallelism * 1024 * 1024,))})
                ]

            sz = (n // parallelism) * 1024 * 1024 * 8
            print("Block size", sz)

            return [
                ReadTask(
                    lambda i=i: range_(i),
                    BlockMetadata(
                        num_rows=n // parallelism,
                        size_bytes=sz,
                        schema=None,
                        input_files=None,
                        exec_stats=None,
                    ),
                )
                for i in range(parallelism)
            ]

    source = CountingRangeDatasource()
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.resource_limits.object_store_memory = 10e6

    # 10GiB dataset.
    ds = ray.data.read_datasource(source, n=10000, override_num_blocks=1000)
    it = iter(ds.iter_batches(batch_size=None, prefetch_batches=0))
    next(it)
    time.sleep(3)
    del it, ds
    launched = ray.get(source.counter.get.remote())

    # If backpressure is broken we'll launch 15+.
    assert launched <= 10, launched


def test_streaming_backpressure_e2e(restore_data_context):  # noqa: F811
    # This test case is particularly challenging since there is a large input->output
    # increase in data size: https://github.com/ray-project/ray/issues/34041
    class TestSlow:
        def __call__(self, df: np.ndarray):
            time.sleep(2)
            return {"id": np.random.randn(1, 20, 1024, 1024)}

    class TestFast:
        def __call__(self, df: np.ndarray):
            time.sleep(0.5)
            return {"id": np.random.randn(1, 20, 1024, 1024)}

    ctx = ray.init(object_store_memory=4e9)
    ds = ray.data.range_tensor(20, shape=(3, 1024, 1024), override_num_blocks=20)

    pipe = ds.map_batches(
        TestFast,
        batch_size=1,
        num_cpus=0.5,
        compute=ray.data.ActorPoolStrategy(size=2),
    ).map_batches(
        TestSlow,
        batch_size=1,
        compute=ray.data.ActorPoolStrategy(size=1),
    )

    for batch in pipe.iter_batches(batch_size=1, prefetch_batches=2):
        ...

    # If backpressure is not working right, we will spill.
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
