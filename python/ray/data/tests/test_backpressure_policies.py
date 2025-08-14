import functools
import math
import time
import unittest
from collections import defaultdict
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    ConcurrencyCapBackpressurePolicy,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data.context import DataContext


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

    def test_basic(self):
        concurrency = 16
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op_no_concurrency = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            target_max_block_size=None,
        )
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=map_op_no_concurrency,
            target_max_block_size=None,
            concurrency=concurrency,
        )
        map_op.metrics.num_tasks_running = 0
        map_op.metrics.num_tasks_finished = 0
        topology = {
            map_op: MagicMock(),
            input_op: MagicMock(),
            map_op_no_concurrency: MagicMock(),
        }

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            topology,
            MagicMock(),
        )

        self.assertEqual(policy._concurrency_caps[map_op], concurrency)
        self.assertTrue(math.isinf(policy._concurrency_caps[input_op]))
        self.assertTrue(math.isinf(policy._concurrency_caps[map_op_no_concurrency]))

        # Gradually increase num_tasks_running to the cap.
        for i in range(1, concurrency + 1):
            self.assertTrue(policy.can_add_input(map_op))
            map_op.metrics.num_tasks_running = i
        # Now num_tasks_running reaches the cap, so can_add_input should return False.
        self.assertFalse(policy.can_add_input(map_op))

        map_op.metrics.num_tasks_running = concurrency / 2
        self.assertEqual(policy.can_add_input(map_op), True)

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

        # Create a dataset with 2 map ops. Each map op has N tasks, where N is
        # the number of cluster CPUs.
        N = self.__class__._cluster_cpus
        ds = ray.data.range(N, override_num_blocks=N)
        # Use different `num_cpus` to make sure they don't fuse.
        ds = ds.map_batches(map_func1, batch_size=None, num_cpus=1, concurrency=1)
        ds = ds.map_batches(map_func2, batch_size=None, num_cpus=1.1, concurrency=1)
        res = ds.take_all()
        self.assertEqual(len(res), N)

        # We recorded the start and end time of each op,
        # check that these 2 ops are executed interleavingly.
        # This means that the executor didn't allocate all resources to the first
        # op in the beginning.
        start1, end1 = ray.get(actor.get_start_and_end_time_for_op.remote(1))
        start2, end2 = ray.get(actor.get_start_and_end_time_for_op.remote(2))
        assert start1 < start2 < end1 < end2, (start1, start2, end1, end2)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
