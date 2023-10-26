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

    def test_e2e(self):
        """A simple E2E test with ConcurrencyCapBackpressurePolicy enabled."""

        @ray.remote(num_cpus=0)
        class RecordTimeActor:
            def __init__(self):
                self._start_time = defaultdict(lambda: float("inf"))
                self._end_time = defaultdict(lambda: 0.0)

            def record_start_time(self, index):
                self._start_time[index] = min(time.time(), self._start_time[index])

            def record_end_time(self, index):
                self._end_time[index] = max(time.time(), self._end_time[index])

            def get_start_and_end_time(self, index):
                return self._start_time[index], self._end_time[index]

        actor = RecordTimeActor.remote()

        def map_func(data, index):
            actor.record_start_time.remote(index)
            yield data
            actor.record_end_time.remote(index)

        # Creat a dataset with 2 map ops. Each map op has N tasks, where N is
        # the number of cluster CPUs.
        N = self.__class__._cluster_cpus
        ds = ray.data.range(N, parallelism=N)
        # Use different `num_cpus` to make sure they don't fuse.
        ds = ds.map_batches(
            functools.partial(map_func, index=1), batch_size=None, num_cpus=1
        )
        ds = ds.map_batches(
            functools.partial(map_func, index=2), batch_size=None, num_cpus=1.1
        )
        res = ds.take_all()
        self.assertEqual(len(res), N)

        # We recorded the start and end time of each op,
        # check that these 2 ops are executed interleavingly.
        # This means that the executor didn't allocate all resources to the first
        # op in the beginning.
        start1, end1 = ray.get(actor.get_start_and_end_time.remote(1))
        start2, end2 = ray.get(actor.get_start_and_end_time.remote(2))
        assert start1 < start2 < end1 < end2, (start1, start2, end1, end2)


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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
