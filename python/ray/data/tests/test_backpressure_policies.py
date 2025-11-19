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
        )
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=map_op_no_concurrency,
            max_concurrency=concurrency,
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

    def test_can_add_input_with_dynamic_output_queue_size_backpressure_disabled(self):
        """Test can_add_input when dynamic output queue size backpressure is disabled."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            max_concurrency=5,
        )
        map_op.metrics.num_tasks_running = 3

        topology = {map_op: MagicMock(), input_op: MagicMock()}

        # Create policy with dynamic output queue size backpressure disabled
        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            topology,
            MagicMock(),  # resource_manager
        )
        policy.enable_dynamic_output_queue_size_backpressure = False

        # Should only check against configured concurrency cap
        self.assertTrue(policy.can_add_input(map_op))  # 3 < 5

        map_op.metrics.num_tasks_running = 5
        self.assertFalse(policy.can_add_input(map_op))  # 5 >= 5

    def test_can_add_input_with_non_map_operator(self):
        """Test can_add_input with non-MapOperator (should use basic cap check)."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        input_op.metrics.num_tasks_running = 1

        topology = {input_op: MagicMock()}

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            topology,
            MagicMock(),  # resource_manager
        )

        # InputDataBuffer has infinite concurrency cap, so should always allow
        self.assertTrue(policy.can_add_input(input_op))

    def test_can_add_input_with_object_store_memory_usage_ratio_above_threshold(self):
        """Test can_add_input when object store memory usage ratio is above threshold."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            max_concurrency=5,
        )
        map_op.metrics.num_tasks_running = 3

        topology = {map_op: MagicMock(), input_op: MagicMock()}

        mock_resource_manager = MagicMock()

        # Mock object store memory usage ratio above threshold
        # Ratio = budget / (usage + budget) > OBJECT_STORE_BUDGET_RATIO
        threshold = ConcurrencyCapBackpressurePolicy.OBJECT_STORE_BUDGET_RATIO
        mock_usage = MagicMock()
        mock_usage.object_store_memory = 1000  # usage
        mock_budget = MagicMock()
        # Calculate budget so ratio > threshold
        # budget / (usage + budget) > threshold
        # budget > threshold * usage / (1 - threshold)
        mock_budget.object_store_memory = int(
            threshold * 1000 / (1 - threshold) + 1
        )  # budget above threshold

        mock_resource_manager.get_op_usage.return_value = mock_usage
        mock_resource_manager.get_budget.return_value = mock_budget

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            topology,
            mock_resource_manager,
        )
        policy.enable_dynamic_output_queue_size_backpressure = True

        # Initialize EWMA state to verify it's not updated when ratio > threshold
        initial_level = 100.0
        initial_dev = 20.0
        policy._q_level_nbytes[map_op] = initial_level
        policy._q_level_dev[map_op] = initial_dev

        # Should skip dynamic backpressure and use basic cap check
        # EWMA state should not be updated (early return)
        self.assertTrue(policy.can_add_input(map_op))  # 3 < 5
        self.assertEqual(policy._q_level_nbytes[map_op], initial_level)
        self.assertEqual(policy._q_level_dev[map_op], initial_dev)

        map_op.metrics.num_tasks_running = 5
        self.assertFalse(policy.can_add_input(map_op))  # 5 >= 5
        # EWMA state should still not be updated
        self.assertEqual(policy._q_level_nbytes[map_op], initial_level)
        self.assertEqual(policy._q_level_dev[map_op], initial_dev)

    def test_can_add_input_with_object_store_memory_usage_ratio_below_threshold(self):
        """Test can_add_input when object store memory usage ratio is below threshold."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            max_concurrency=5,
        )
        map_op.metrics.num_tasks_running = 3

        topology = {map_op: MagicMock(), input_op: MagicMock()}

        mock_resource_manager = MagicMock()

        # Mock object store memory usage ratio below threshold
        # Ratio = budget / (usage + budget) < OBJECT_STORE_BUDGET_RATIO
        threshold = ConcurrencyCapBackpressurePolicy.OBJECT_STORE_BUDGET_RATIO
        mock_usage = MagicMock()
        mock_usage.object_store_memory = 1000  # usage
        mock_budget = MagicMock()
        # Calculate budget so ratio < threshold
        # budget / (usage + budget) < threshold
        # budget < threshold * usage / (1 - threshold)
        mock_budget.object_store_memory = max(
            0, int(threshold * 1000 / (1 - threshold) - 1)
        )  # below threshold

        mock_resource_manager.get_op_usage.return_value = mock_usage
        mock_resource_manager.get_budget.return_value = mock_budget

        # Mock queue size methods
        mock_resource_manager.get_op_internal_object_store_usage.return_value = 100
        mock_resource_manager.get_op_outputs_object_store_usage_with_downstream.return_value = (
            200
        )

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            topology,
            mock_resource_manager,
        )
        policy.enable_dynamic_output_queue_size_backpressure = True

        # Should proceed with dynamic backpressure logic
        # Initialize EWMA state for the operator with a different level
        # so we can verify the update happens (queue size is 300)
        initial_level = 200.0
        initial_dev = 50.0
        policy._q_level_nbytes[map_op] = initial_level
        policy._q_level_dev[map_op] = initial_dev

        result = policy.can_add_input(map_op)
        # With queue size 300, initial level=200, dev=50, bounds=[100, 300]
        # Queue size 300 is at the upper bound, so should hold.
        # running=3 < effective_cap=3 should be False
        self.assertFalse(result)
        # EWMA state should be updated when ratio < threshold
        # Level should move toward 300 (queue size)
        self.assertNotEqual(policy._q_level_nbytes[map_op], initial_level)
        # Dev should also be updated
        self.assertNotEqual(policy._q_level_dev[map_op], initial_dev)

    def test_can_add_input_effective_cap_calculation(self):
        """Test that effective cap calculation works correctly with different queue sizes."""
        input_op = InputDataBuffer(DataContext.get_current(), input_data=[MagicMock()])
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            data_context=DataContext.get_current(),
            input_op=input_op,
            max_concurrency=8,
        )
        map_op.metrics.num_tasks_running = 4

        topology = {map_op: MagicMock(), input_op: MagicMock()}

        mock_resource_manager = MagicMock()
        threshold = ConcurrencyCapBackpressurePolicy.OBJECT_STORE_BUDGET_RATIO
        mock_usage = MagicMock()
        mock_usage.object_store_memory = 1000
        mock_budget = MagicMock()
        # Calculate budget so ratio < threshold
        # budget / (usage + budget) < threshold
        # budget < threshold * usage / (1 - threshold)
        mock_budget.object_store_memory = max(
            0, int(threshold * 1000 / (1 - threshold) - 1)
        )  # below threshold

        mock_resource_manager.get_op_usage.return_value = mock_usage
        mock_resource_manager.get_budget.return_value = mock_budget

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            topology,
            mock_resource_manager,
        )
        policy.enable_dynamic_output_queue_size_backpressure = True

        # Test different queue sizes using policy constants
        test_cases = [
            # (internal_usage, downstream_usage, level, dev, expected_result, description)
            (
                50,
                50,
                5000.0,
                200.0,
                True,
                "low_queue_below_lower_bound",
            ),  # 100 < 5000 - 2*200 = 4600, ramp up
            (
                200,
                200,
                400.0,
                50.0,
                False,
                "medium_queue_in_hold_region",
            ),  # 400 in [300, 500], hold
            (
                300,
                300,
                200.0,
                50.0,
                False,
                "high_queue_above_upper_bound",
            ),  # 600 > 200 + 2*50 = 300, backoff
        ]

        for (
            internal_usage,
            downstream_usage,
            level,
            dev,
            expected_result,
            description,
        ) in test_cases:
            with self.subTest(description=description):
                mock_resource_manager.get_op_internal_object_store_usage.return_value = (
                    internal_usage
                )
                mock_resource_manager.get_op_outputs_object_store_usage_with_downstream.return_value = (
                    downstream_usage
                )

                # Initialize EWMA state
                policy._q_level_nbytes[map_op] = level
                policy._q_level_dev[map_op] = dev

                result = policy.can_add_input(map_op)
                assert (
                    result == expected_result
                ), f"Expected {expected_result} for {description}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
