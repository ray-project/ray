import functools
import time
import unittest
from collections import defaultdict
from contextlib import contextmanager
from unittest.mock import MagicMock

import numpy as np

import ray
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    ConcurrencyCapBackpressurePolicy,
    StreamingOutputBackpressurePolicy,
)
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
    get_initial_core_execution_metrics_snapshot,
)
from ray.tests.conftest import shutdown_only  # noqa: F401


class TestConcurrencyCapBackpressurePolicy(unittest.TestCase):
    """Tests for ConcurrencyCapBackpressurePolicy."""

    @classmethod
    def setUpClass(cls):
        cls._cluster_cpus = 10
        ray.init(num_cpus=cls._cluster_cpus)
        data_context = ray.data.DataContext.get_current()
        data_context.set_config(
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
            [ConcurrencyCapBackpressurePolicy],
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
        data_context = ray.data.DataContext.get_current()
        data_context.remove_config(ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY)

    @contextmanager
    def _patch_config(self, init_cap, cap_multiply_threshold, cap_multiplier):
        data_context = ray.data.DataContext.get_current()
        data_context.set_config(
            ConcurrencyCapBackpressurePolicy.INIT_CAP_CONFIG_KEY,
            init_cap,
        )
        data_context.set_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLY_THRESHOLD_CONFIG_KEY,
            cap_multiply_threshold,
        )
        data_context.set_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLIER_CONFIG_KEY,
            cap_multiplier,
        )
        yield
        data_context.remove_config(ConcurrencyCapBackpressurePolicy.INIT_CAP_CONFIG_KEY)
        data_context.remove_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLY_THRESHOLD_CONFIG_KEY
        )
        data_context.remove_config(
            ConcurrencyCapBackpressurePolicy.CAP_MULTIPLIER_CONFIG_KEY
        )

    def test_basic(self):
        op = MagicMock()
        op.metrics = MagicMock(
            num_tasks_running=0,
            num_tasks_finished=0,
        )
        topology = {op: MagicMock()}

        init_cap = 4
        cap_multiply_threshold = 0.5
        cap_multiplier = 2.0

        with self._patch_config(init_cap, cap_multiply_threshold, cap_multiplier):
            policy = ConcurrencyCapBackpressurePolicy(topology)

        self.assertEqual(policy._concurrency_caps[op], 4)
        # Gradually increase num_tasks_running to the cap.
        for i in range(1, init_cap + 1):
            self.assertTrue(policy.can_add_input(op))
            op.metrics.num_tasks_running = i
        # Now num_tasks_running reaches the cap, so can_add_input should return False.
        self.assertFalse(policy.can_add_input(op))

        # If we increase num_task_finished to the threshold (4 * 0.5 = 2),
        # it should trigger the cap to increase.
        op.metrics.num_tasks_finished = init_cap * cap_multiply_threshold
        self.assertEqual(policy.can_add_input(op), True)
        self.assertEqual(policy._concurrency_caps[op], init_cap * cap_multiplier)

        # Now the cap is 8 (4 * 2).
        # If we increase num_tasks_finished directly to the next-level's threshold
        # (8 * 2 * 0.5 = 8), it should trigger the cap to increase twice.
        op.metrics.num_tasks_finished = (
            policy._concurrency_caps[op] * cap_multiplier * cap_multiply_threshold
        )
        op.metrics.num_tasks_running = 0
        self.assertEqual(policy.can_add_input(op), True)
        self.assertEqual(policy._concurrency_caps[op], init_cap * cap_multiplier**3)

    def test_config(self):
        topology = {}
        # Test good config.
        with self._patch_config(10, 0.3, 1.5):
            policy = ConcurrencyCapBackpressurePolicy(topology)
            self.assertEqual(policy._init_cap, 10)
            self.assertEqual(policy._cap_multiply_threshold, 0.3)
            self.assertEqual(policy._cap_multiplier, 1.5)

        with self._patch_config(10, 0.3, 1):
            policy = ConcurrencyCapBackpressurePolicy(topology)
            self.assertEqual(policy._init_cap, 10)
            self.assertEqual(policy._cap_multiply_threshold, 0.3)
            self.assertEqual(policy._cap_multiplier, 1)

        # Test bad configs.
        with self._patch_config(-1, 0.3, 1.5):
            with self.assertRaises(AssertionError):
                policy = ConcurrencyCapBackpressurePolicy(topology)
        with self._patch_config(10, 1.1, 1.5):
            with self.assertRaises(AssertionError):
                policy = ConcurrencyCapBackpressurePolicy(topology)
        with self._patch_config(10, 0.3, 0.5):
            with self.assertRaises(AssertionError):
                policy = ConcurrencyCapBackpressurePolicy(topology)

    def _create_record_time_actor(self):
        @ray.remote(num_cpus=0)
        class RecordTimeActor:
            def __init__(self):
                self._start_time = defaultdict(lambda: [])
                self._end_time = defaultdict(lambda: [])

            def record_start_time(self, index):
                self._start_time[index].append(time.time())

            def record_end_time(self, index):
                self._end_time[index].append(time.time())

            def get_start_and_end_time_for_op(self, index):
                return min(self._start_time[index]), max(self._end_time[index])

            def get_start_and_end_time_for_all_tasks_of_op(self, index):
                return self._start_time[index], self._end_time[index]

        actor = RecordTimeActor.remote()
        return actor

    def _get_map_func(self, actor, index):
        def map_func(data, actor, index):
            actor.record_start_time.remote(index)
            yield data
            actor.record_end_time.remote(index)

        return functools.partial(map_func, actor=actor, index=index)

    def test_e2e_normal(self):
        """A simple E2E test with ConcurrencyCapBackpressurePolicy enabled."""
        actor = self._create_record_time_actor()
        map_func1 = self._get_map_func(actor, 1)
        map_func2 = self._get_map_func(actor, 2)

        # Creat a dataset with 2 map ops. Each map op has N tasks, where N is
        # the number of cluster CPUs.
        N = self.__class__._cluster_cpus
        ds = ray.data.range(N, parallelism=N)
        # Use different `num_cpus` to make sure they don't fuse.
        ds = ds.map_batches(map_func1, batch_size=None, num_cpus=1)
        ds = ds.map_batches(map_func2, batch_size=None, num_cpus=1.1)
        res = ds.take_all()
        self.assertEqual(len(res), N)

        # We recorded the start and end time of each op,
        # check that these 2 ops are executed interleavingly.
        # This means that the executor didn't allocate all resources to the first
        # op in the beginning.
        start1, end1 = ray.get(actor.get_start_and_end_time_for_op.remote(1))
        start2, end2 = ray.get(actor.get_start_and_end_time_for_op.remote(2))
        assert start1 < start2 < end1 < end2, (start1, start2, end1, end2)

    def test_e2e_no_ramping_up(self):
        """Test setting the multiplier to 1.0, which means no ramping up of the
        concurrency cap."""
        with self._patch_config(1, 1, 1):
            actor = self._create_record_time_actor()
            map_func1 = self._get_map_func(actor, 1)
            N = self.__class__._cluster_cpus
            ds = ray.data.range(N, parallelism=N)
            ds = ds.map_batches(map_func1, batch_size=None, num_cpus=1)
            res = ds.take_all()
            self.assertEqual(len(res), N)

            start, end = ray.get(
                actor.get_start_and_end_time_for_all_tasks_of_op.remote(1)
            )
            for i in range(len(start) - 1):
                assert start[i] < end[i] < start[i + 1], (i, start, end)


class TestStreamOutputBackpressurePolicy(unittest.TestCase):
    """Tests for StreamOutputBackpressurePolicy."""

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
        policy_cls = StreamingOutputBackpressurePolicy
        cls._configs = {
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY: [policy_cls],
            policy_cls.MAX_BLOCKS_IN_OP_OUTPUT_QUEUE_CONFIG_KEY: 1,
            policy_cls.MAX_BLOCKS_IN_GENERATOR_BUFFER_CONFIG_KEY: 1,
        }
        for k, v in cls._configs.items():
            data_context.set_config(k, v)
        data_context.execution_options.preserve_order = True

    @classmethod
    def tearDownClass(cls):
        data_context = ray.data.DataContext.get_current()
        for k in cls._configs.keys():
            data_context.remove_config(k)
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
            time.sleep(0.1)
            print("Consuming block", batch["id"][0])
            del batch["data"]
            batch["consumer_timestamp"] = [time.time()]
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

    def test_basic_backpressure(self):
        producer_timestamps, consumer_timestamps = self._run_dataset(
            producer_num_cpus=1, consumer_num_cpus=2
        )
        # We can buffer at most 2 blocks.
        # Thus the producer should be backpressured after producing 2 blocks.
        # Note, although block 2 is backpressured, but producer_timestamps[2] has
        # been generated before the backpressure. Thus we assert
        # producer_timestamps[2] < consumer_timestamps[0] < producer_timestamps[3].
        assert (
            producer_timestamps[2] < consumer_timestamps[0] < producer_timestamps[3]
        ), (producer_timestamps, consumer_timestamps)

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


def test_large_e2e_backpressure(shutdown_only, restore_data_context):  # noqa: F811
    """Test backpressure on a synthetic large-scale workload."""
    # The cluster has 10 CPUs and 200MB object store memory.
    # The dataset will have 200MB * 25% = 50MB memory budget.
    #
    # Each produce task generates 10 blocks, each of which has 10MB data.
    #
    # Without any backpressure, the producer tasks will output at most
    # 10 * 10 * 10MB = 1000MB data.
    #
    # With StreamingOutputBackpressurePolicy and the following configuration,
    # the executor will still schedule 10 produce tasks, but only the first task is
    # allowed to output all blocks. The total size of pending blocks will be
    # (10 + 9 * 1 + 1) * 10MB = 200MB, where
    # - 10 is the number of blocks in the first task.
    # - 9 * 1 is the number of blocks pending at the streaming generator level of
    #   the other 15 tasks.
    # - 1 is the number of blocks pending at the output queue.

    NUM_CPUS = 10
    NUM_ROWS_PER_TASK = 10
    NUM_TASKS = 20
    NUM_ROWS_TOTAL = NUM_ROWS_PER_TASK * NUM_TASKS
    BLOCK_SIZE = 10 * 1024 * 1024
    STREMING_GEN_BUFFER_SIZE = 1
    OP_OUTPUT_QUEUE_SIZE = 1
    max_pending_block_bytes = (
        NUM_ROWS_PER_TASK
        + (NUM_CPUS - 1) * STREMING_GEN_BUFFER_SIZE
        + OP_OUTPUT_QUEUE_SIZE
    ) * BLOCK_SIZE
    print(f"max_pending_block_bytes: {max_pending_block_bytes/1024/1024}MB")

    ray.init(num_cpus=NUM_CPUS, object_store_memory=200 * 1024 * 1024)

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
    data_context.set_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
        [
            StreamingOutputBackpressurePolicy,
        ],
    )
    data_context.set_config(
        StreamingOutputBackpressurePolicy.MAX_BLOCKS_IN_OP_OUTPUT_QUEUE_CONFIG_KEY,
        OP_OUTPUT_QUEUE_SIZE,
    )
    data_context.set_config(
        StreamingOutputBackpressurePolicy.MAX_BLOCKS_IN_GENERATOR_BUFFER_CONFIG_KEY,
        STREMING_GEN_BUFFER_SIZE,
    )
    data_context.execution_options.verbose_progress = True
    data_context.target_max_block_size = BLOCK_SIZE

    last_snapshot = get_initial_core_execution_metrics_snapshot()

    ds = ray.data.range(NUM_ROWS_TOTAL, parallelism=NUM_TASKS)
    ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK)
    ds = ds.map_batches(consume, batch_size=None, num_cpus=0.9)
    # Check core execution metrics every 10 rows, because it's expensive.
    for _ in ds.iter_batches(batch_size=NUM_ROWS_PER_TASK):
        # The amount of generated data should be less than
        # max_pending_block_bytes (pending data) +
        # NUM_ROWS_PER_TASK * BLOCK_SIZE (consumed data)
        max_created_bytes_per_consumption = (
            max_pending_block_bytes + NUM_ROWS_PER_TASK * BLOCK_SIZE
        )

        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                object_store_stats={
                    "spilled_bytes_total": lambda x: x
                    <= 1.5 * max_created_bytes_per_consumption,
                    "restored_bytes_total": lambda x: x
                    <= 1.5 * max_created_bytes_per_consumption,
                    "cumulative_created_plasma_bytes": lambda x: x
                    <= 1.5 * max_created_bytes_per_consumption,
                    "cumulative_created_plasma_objects": lambda _: True,
                },
            ),
            last_snapshot,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
