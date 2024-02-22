import time
import numpy as np
import ray
import pytest
import unittest

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

class TestDeadlockPrevention(unittest.TestCase):
    """Test backpressure can prevent deadlocks in edge cases."""

    @classmethod
    def setUpClass(cls):
        cls._cluster_cpus = 5
        cls._cluster_object_memory = 500 * 1024 * 1024
        ray.init(
            num_cpus=cls._cluster_cpus, object_store_memory=cls._cluster_object_memory
        )
        data_context = ray.data.DataContext.get_current()
        cls._num_blocks = 5
        cls._block_size = 100 * 1024 * 1024
        data_context.execution_options.preserve_order = True

    @classmethod
    def tearDownClass(cls):
        data_context = ray.data.DataContext.get_current()
        data_context.execution_options.preserve_order = False
        ray.shutdown()

    def _run_dataset(self, producer_num_cpus, consumer_num_cpus):
        # Create a dataset with 2 operators:
        # - The producer op has only 1 task, which produces 5 blocks, each of which
        #   has 100MB data.
        # - The consumer op has 5 slow tasks, each of which consumes 1 block.
        # Return the timestamps at the producer and consumer tasks for each block.
        num_blocks = self._num_blocks
        block_size = self._block_size
        ray.data.DataContext.get_current().target_max_block_size = block_size

        def producer(batch):
            for i in range(num_blocks):
                print("Producing block", i)
                yield {
                    "id": [i],
                    "data": [np.zeros(block_size, dtype=np.uint8)],
                    "producer_timestamp": [time.time()],
                }

        def consumer(batch):
            assert len(batch["id"]) == 1
            print("Consuming block", batch["id"][0])
            batch["consumer_timestamp"] = [time.time()]
            time.sleep(0.1)
            del batch["data"]
            return batch

        ds = ray.data.range(1, parallelism=1).materialize()
        ds = ds.map_batches(producer, batch_size=None, num_cpus=producer_num_cpus)
        ds = ds.map_batches(consumer, batch_size=None, num_cpus=consumer_num_cpus)

        res = ds.take_all()
        assert [row["id"] for row in res] == list(range(self._num_blocks))
        return (
            [row["producer_timestamp"] for row in res],
            [row["consumer_timestamp"] for row in res],
        )

    def test_no_deadlock(self):
        # The producer needs all 5 CPUs, and the consumer has no CPU to run.
        # In this case, we shouldn't backpressure the producer and let it run
        # until it finishes.
        producer_timestamps, consumer_timestamps = self._run_dataset(
            producer_num_cpus=5, consumer_num_cpus=1
        )
        assert producer_timestamps[-1] < consumer_timestamps[0], (
            producer_timestamps,
            consumer_timestamps,
        )

    def test_no_deadlock_for_resource_contention(self):
        """Test no deadlock in case of resource contention from
        non-Data code."""
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

        producer_timestamps, consumer_timestamps = self._run_dataset(
            producer_num_cpus=1, consumer_num_cpus=0.9
        )
        assert producer_timestamps[-1] < consumer_timestamps[0], (
            producer_timestamps,
            consumer_timestamps,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
