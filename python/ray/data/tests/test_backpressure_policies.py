import functools
import math
import time
import unittest
from collections import defaultdict, deque
from unittest.mock import MagicMock, patch

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

    def test_can_add_input_with_normal_concurrency_cap(self):
        """Test can_add_input when using normal concurrency cap (queue size disabled)."""
        mock_op = MagicMock()
        mock_op.name = "TestOperator"
        mock_op.metrics.num_tasks_running = 3
        mock_op.throttling_disabled.return_value = False
        mock_op.execution_finished.return_value = False
        mock_op.output_dependencies = []

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Disable queue size based backpressure
        policy.enable_dynamic_output_queue_size_backpressure = False
        policy._concurrency_caps[mock_op] = 5

        # Should allow input when running < cap
        result = policy.can_add_input(mock_op)
        self.assertTrue(result)

        # Should deny input when running >= cap
        mock_op.metrics.num_tasks_running = 5
        result = policy.can_add_input(mock_op)
        self.assertFalse(result)

    def test_update_queue_threshold_bootstrap(self):
        """Test threshold update for first sample (bootstrap)."""
        mock_op = MagicMock()
        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Add sample to history first (required for threshold calculation)
        policy._queue_history[mock_op].append(1000)

        # First sample should bootstrap threshold
        # The threshold will be calculated as max(level + K_DEV * dev, q_now)
        # where level=q_now=1000, dev=0 (first sample), so threshold = max(1000 + 4*0, 1000) = 1000
        threshold = policy._update_queue_threshold(mock_op, 1000)
        self.assertEqual(threshold, 1000)
        self.assertEqual(policy._queue_thresholds[mock_op], 1000)

        # Test bootstrap with zero queue (should set threshold to 1 due to rounding)
        fresh_mock_op = MagicMock()
        fresh_policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {fresh_mock_op: MagicMock()},
            MagicMock(),
        )
        fresh_policy._queue_thresholds[fresh_mock_op] = 0  # Reset to idle state
        fresh_policy._queue_history[fresh_mock_op] = deque([0])
        # Fresh policy starts with clean EWMA state

        threshold_zero = fresh_policy._update_queue_threshold(fresh_mock_op, 0)
        # When q_now=0, level=0, dev=0, threshold = max(1, max(0 + 4*0, 0)) = 1
        self.assertEqual(threshold_zero, 1)
        self.assertEqual(fresh_policy._queue_thresholds[fresh_mock_op], 1)

    def test_update_queue_threshold_asymmetric_ewma(self):
        """Test threshold update with asymmetric EWMA behavior."""
        mock_op = MagicMock()
        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Set up initial state
        policy._q_level_nbytes[mock_op] = 100.0
        policy._q_level_dev[mock_op] = 20.0
        policy._queue_history[mock_op] = deque([100, 120, 140, 160, 180, 200])

        # Test with growing queue (should use faster alpha_up)
        threshold = policy._update_queue_threshold(mock_op, 300)

        # Threshold should be at least as high as current queue
        self.assertGreaterEqual(threshold, 300)

        # Level should have moved toward the new sample using alpha_up
        self.assertGreater(policy._q_level_nbytes[mock_op], 100.0)

        # Test with declining queue (should use slower EWMA_ALPHA)
        policy._q_level_nbytes[mock_op] = 200.0
        policy._q_level_dev[mock_op] = 30.0
        policy._update_queue_threshold(mock_op, 150)

        # Level should have moved less aggressively downward
        self.assertGreater(policy._q_level_nbytes[mock_op], 150.0)
        self.assertLess(policy._q_level_nbytes[mock_op], 200.0)

    def test_update_queue_threshold_no_decrease(self):
        """Test that thresholds are never decreased, only maintained or increased."""
        mock_op = MagicMock()
        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Set up initial state with high threshold
        policy._queue_thresholds[mock_op] = 200
        policy._q_level_nbytes[mock_op] = 10.0  # Very low level
        policy._q_level_dev[mock_op] = 1.0  # Very low deviation
        policy._queue_history[mock_op] = deque([10, 11, 12, 13, 14, 15])

        # Test that threshold is maintained when calculated threshold is lower
        threshold = policy._update_queue_threshold(mock_op, 150)

        # Should maintain the existing threshold (no decrease)
        self.assertEqual(threshold, 200)
        self.assertEqual(policy._queue_thresholds[mock_op], 200)

        # Test with even lower queue size
        threshold_small = policy._update_queue_threshold(mock_op, 50)
        self.assertEqual(threshold_small, 200)  # Still maintained
        self.assertEqual(policy._queue_thresholds[mock_op], 200)

    def test_update_queue_threshold_increase(self):
        """Test that thresholds are increased when calculated threshold is higher."""
        mock_op = MagicMock()
        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Set up initial state with moderate threshold
        policy._queue_thresholds[mock_op] = 100
        policy._q_level_nbytes[mock_op] = 50.0
        policy._q_level_dev[mock_op] = 20.0
        policy._queue_history[mock_op] = deque([50, 60, 70, 80, 90, 100])

        # Test that threshold is increased when calculated threshold is higher
        threshold = policy._update_queue_threshold(mock_op, 200)

        # Should increase the threshold
        self.assertGreaterEqual(threshold, 200)
        self.assertGreaterEqual(policy._queue_thresholds[mock_op], 200)

    def test_effective_cap_calculation_with_trend(self):
        """Test effective cap calculation with different trend scenarios."""
        mock_op = MagicMock()
        mock_op.metrics.num_tasks_running = 5

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Set up queue history for trend calculation
        policy._queue_history[mock_op] = deque(
            [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
        )
        policy._q_level_dev[mock_op] = 100.0
        policy._queue_thresholds[mock_op] = 500
        policy._concurrency_caps[mock_op] = 10

        # Test with high pressure (queue > threshold)
        with patch.object(
            policy._resource_manager,
            "get_op_internal_object_store_usage",
            return_value=1000,
        ), patch.object(
            policy._resource_manager,
            "get_op_outputs_object_store_usage_with_downstream",
            return_value=1000,
        ):
            effective_cap = policy._effective_cap(mock_op)
            # Should be reduced due to high pressure
            self.assertLess(effective_cap, 10)
            self.assertGreaterEqual(effective_cap, 1)  # Should be at least 1

    def test_effective_cap_insufficient_history(self):
        """Test effective cap when there's insufficient history for trend calculation."""
        mock_op = MagicMock()
        mock_op.metrics.num_tasks_running = 5

        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Set up insufficient history (less than 6 samples)
        policy._queue_history[mock_op] = deque([100, 200, 300])
        policy._concurrency_caps[mock_op] = 10

        effective_cap = policy._effective_cap(mock_op)
        # Should return max(1, running) when insufficient history
        self.assertEqual(effective_cap, 5)

    def test_signal_calculation_formulas(self):
        """Test pressure_signal and trend_signal calculation formulas."""
        # Test pressure_signal formula: (q_now - threshold) / max(1.0, dev)
        pressure_cases = [
            (1000, 500, 100, 5.0, "High pressure"),
            (500, 500, 100, 0.0, "Neutral pressure"),
            (200, 500, 100, -3.0, "Low pressure"),
            (500, 500, 0, 0.0, "Zero deviation uses scale=1.0"),
        ]

        for q_now, threshold, dev, expected, description in pressure_cases:
            with self.subTest(signal="pressure", description=description):
                scale = max(1.0, float(dev))
                pressure_signal = (q_now - threshold) / scale
                self.assertAlmostEqual(pressure_signal, expected, places=5)

        # Test trend_signal formula: (recent_avg - older_avg) / max(1.0, dev)
        trend_cases = [
            (1000, 500, 100, 5.0, "Strong growth"),
            (500, 500, 100, 0.0, "No trend"),
            (200, 500, 100, -3.0, "Strong decline"),
            (500, 500, 0, 0.0, "Zero deviation uses scale=1.0"),
        ]

        for recent_avg, older_avg, dev, expected, description in trend_cases:
            with self.subTest(signal="trend", description=description):
                scale = max(1.0, float(dev))
                trend_signal = (recent_avg - older_avg) / scale
                self.assertAlmostEqual(trend_signal, expected, places=5)

    def test_decision_rules_table_comprehensive(self):
        """Test all decision rules from the table comprehensively."""
        test_cases = [
            # (pressure_signal, trend_signal, expected_step, description)
            # High pressure scenarios
            (2.5, 1.5, -1, "High pressure + growing trend -> backoff"),
            (2.0, 1.0, -1, "High pressure + growing trend (boundary) -> backoff"),
            (2.5, 0.5, 0, "High pressure + mild growth -> hold"),
            (2.5, 0.0, 0, "High pressure + no trend -> hold"),
            (2.5, -0.5, 0, "High pressure + mild decline -> hold"),
            (2.5, -1.0, 0, "High pressure + declining trend -> hold"),
            # Moderate pressure scenarios
            (1.5, 1.5, 0, "Moderate pressure + growing trend -> hold"),
            (1.0, 1.0, 0, "Moderate pressure + growing trend (boundary) -> hold"),
            (1.5, 0.5, 0, "Moderate pressure + mild growth -> hold"),
            (1.5, 0.0, 0, "Moderate pressure + no trend -> hold"),
            (1.5, -0.5, 0, "Moderate pressure + mild decline -> hold"),
            (1.5, -1.0, 0, "Moderate pressure + declining trend -> hold"),
            # Low pressure scenarios
            (-1.5, -1.5, 1, "Low pressure + declining trend -> increase"),
            (-1.0, -1.0, 1, "Low pressure + declining trend (boundary) -> increase"),
            (-1.5, -0.5, 0, "Low pressure + mild decline -> hold"),
            (-1.5, 0.0, 0, "Low pressure + no trend -> hold"),
            (-1.5, 0.5, 0, "Low pressure + mild growth -> hold"),
            (-1.5, 1.0, 0, "Low pressure + growing trend -> hold"),
            # Very low pressure scenarios
            (-2.5, -2.5, 2, "Very low pressure + declining trend -> increase by 2"),
            (
                -2.0,
                -2.0,
                2,
                "Very low pressure + declining trend (boundary) -> increase by 2",
            ),
            (-2.5, -1.5, 1, "Very low pressure + mild decline -> increase by 1"),
            (-2.5, -1.0, 1, "Very low pressure + mild decline -> increase by 1"),
            (-2.5, 0.0, 0, "Very low pressure + no trend -> hold"),
            (-2.5, 0.5, 0, "Very low pressure + mild growth -> hold"),
            (-2.5, 1.0, 0, "Very low pressure + growing trend -> hold"),
            # Neutral scenarios
            (0.5, 0.5, 0, "Low pressure + mild growth -> hold"),
            (0.0, 0.0, 0, "Neutral pressure + no trend -> hold"),
            (-0.5, -0.5, 0, "Low pressure + mild decline -> hold"),
            # Edge cases
            (1.0, 0.0, 0, "Moderate pressure + no trend (boundary) -> hold"),
            (0.0, 1.0, 0, "Neutral pressure + growing trend -> hold"),
            (0.0, -1.0, 0, "Neutral pressure + declining trend -> hold"),
        ]

        # Create a policy instance to access the helper method
        mock_op = MagicMock()
        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        for pressure_signal, trend_signal, expected_step, description in test_cases:
            with self.subTest(description=description):
                # Use the actual helper method from the policy
                step = policy._quantized_controller_step(pressure_signal, trend_signal)

                self.assertEqual(
                    step,
                    expected_step,
                    f"Failed for pressure={pressure_signal}, trend={trend_signal}",
                )

    def test_ewma_calculation_formulas(self):
        """Test EWMA level, deviation, and alpha calculation formulas."""
        # Test EWMA level formula: (1 - alpha) * prev + alpha * sample
        level_cases = [
            (100.0, 120.0, 0.2, 104.0, "Normal alpha"),
            (100.0, 80.0, 0.2, 96.0, "Normal alpha down"),
            (100.0, 100.0, 0.2, 100.0, "Stable"),
            (0.0, 100.0, 0.2, 20.0, "Bootstrap"),
        ]

        for prev_level, sample, alpha, expected, description in level_cases:
            with self.subTest(formula="level", description=description):
                new_level = (1 - alpha) * prev_level + alpha * sample
                self.assertAlmostEqual(new_level, expected, places=5)

        # Test EWMA deviation formula: (1 - alpha) * prev_dev + alpha * abs(sample - prev_level)
        dev_cases = [
            (20.0, 120.0, 100.0, 0.2, 20.0, "Growing"),
            (20.0, 100.0, 100.0, 0.2, 16.0, "Stable"),
            (0.0, 100.0, 0.0, 0.2, 20.0, "Bootstrap"),
        ]

        for prev_dev, sample, prev_level, alpha, expected, description in dev_cases:
            with self.subTest(formula="deviation", description=description):
                new_dev = (1 - alpha) * prev_dev + alpha * abs(sample - prev_level)
                self.assertAlmostEqual(new_dev, expected, places=5)

        # Test alpha_up calculation: 1.0 - (1.0 - EWMA_ALPHA) ** 2
        alpha_cases = [
            (0.2, 0.36, "Normal EWMA_ALPHA"),
            (0.1, 0.19, "Low EWMA_ALPHA"),
            (0.5, 0.75, "High EWMA_ALPHA"),
        ]

        for EWMA_ALPHA, expected, description in alpha_cases:
            with self.subTest(formula="alpha_up", description=description):
                alpha_up = 1.0 - (1.0 - EWMA_ALPHA) ** 2
                self.assertAlmostEqual(alpha_up, expected, places=5)

    def test_threshold_calculation_formula(self):
        """Test threshold calculation: max(level + K_DEV * dev, q_now)."""
        test_cases = [
            (100.0, 20.0, 150.0, 4.0, 180.0, "Normal case"),
            (100.0, 20.0, 200.0, 4.0, 200.0, "High queue"),
            (100.0, 0.0, 150.0, 4.0, 150.0, "Zero deviation"),
            (100.0, 20.0, 150.0, 2.0, 150.0, "Lower K_DEV"),
        ]

        for level, dev, q_now, K_DEV, expected, description in test_cases:
            with self.subTest(description=description):
                threshold = max(level + K_DEV * dev, q_now)
                self.assertAlmostEqual(threshold, expected, places=5)

    def test_threshold_update_logic_comprehensive(self):
        """Test comprehensive threshold update logic including bootstrap, upward, and no-decrease cases."""
        mock_op = MagicMock()
        policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {mock_op: MagicMock()},
            MagicMock(),
        )

        # Test 1: Bootstrap case (prev_threshold = 0)
        policy._queue_thresholds[mock_op] = 0
        policy._queue_history[mock_op] = deque([100])
        threshold1 = policy._update_queue_threshold(mock_op, 100)
        # Bootstrap: threshold = max(level + K_DEV * dev, q_now) = max(100 + 4*0, 100) = 100
        self.assertEqual(threshold1, 100)

        # Test 2: Upward adjustment (threshold > prev_threshold)
        policy._queue_thresholds[mock_op] = 100
        policy._q_level_nbytes[mock_op] = 50.0
        policy._q_level_dev[mock_op] = 10.0
        policy._queue_history[mock_op] = deque([50, 60, 70, 80, 90, 100])
        threshold2 = policy._update_queue_threshold(mock_op, 200)
        # The EWMA will update level and dev, so we can't predict exact value
        # Just verify it's >= 200 (upward adjustment)
        self.assertGreaterEqual(threshold2, 200)

        # Test 3: No decrease (threshold < prev_threshold, should maintain existing)
        policy._queue_thresholds[mock_op] = 200
        policy._q_level_nbytes[mock_op] = 10.0  # Very low level
        policy._q_level_dev[mock_op] = 1.0  # Very low deviation
        policy._queue_history[mock_op] = deque([10, 11, 12, 13, 14, 15])
        threshold3 = policy._update_queue_threshold(mock_op, 150)
        self.assertEqual(threshold3, 200)

        # Test 4: Zero threshold case
        fresh_mock_op = MagicMock()
        fresh_policy = ConcurrencyCapBackpressurePolicy(
            DataContext.get_current(),
            {fresh_mock_op: MagicMock()},
            MagicMock(),
        )
        fresh_policy._queue_thresholds[fresh_mock_op] = 0
        fresh_policy._queue_history[fresh_mock_op] = deque([0])
        # Fresh policy starts with clean EWMA state
        threshold4 = fresh_policy._update_queue_threshold(fresh_mock_op, 0)
        self.assertEqual(threshold4, 1)  # Should round up to 1

    def test_trend_and_effective_cap_formulas(self):
        """Test trend calculation and effective cap formulas."""
        # Test trend calculation: recent_avg - older_avg
        trend_cases = [
            ([100, 200, 300, 400, 500, 600], 500.0, 200.0, 300.0, "6 samples"),
            ([100, 200, 300, 400, 500, 600, 700], 600.0, 300.0, 300.0, "7 samples"),
        ]

        for (
            history,
            expected_recent,
            expected_older,
            expected_trend,
            description,
        ) in trend_cases:
            with self.subTest(formula="trend", description=description):
                h = list(history)
                recent_window = len(h) // 2
                older_window = len(h) // 2

                recent_avg = sum(h[-recent_window:]) / float(recent_window)
                older_avg = sum(
                    h[-(recent_window + older_window) : -recent_window]
                ) / float(older_window)
                trend = recent_avg - older_avg

                self.assertAlmostEqual(recent_avg, expected_recent, places=5)
                self.assertAlmostEqual(older_avg, expected_older, places=5)
                self.assertAlmostEqual(trend, expected_trend, places=5)

        # Test effective cap formula: max(1, running + step)
        cap_cases = [
            (5, -1, 4, "Reduce by 1"),
            (5, 0, 5, "No change"),
            (5, 1, 6, "Increase by 1"),
            (1, -1, 1, "Min cap"),
        ]

        for running, step, expected, description in cap_cases:
            with self.subTest(formula="effective_cap", description=description):
                effective_cap = max(1, running + step)
                self.assertEqual(effective_cap, expected)

    def test_ewma_asymmetric_behavior(self):
        """Test EWMA asymmetric behavior and level calculation."""
        # Test alpha selection: alpha_up if sample > prev else EWMA_ALPHA
        alpha_cases = [
            (100.0, 150.0, 0.2, 0.36, "Rising uses alpha_up"),
            (100.0, 50.0, 0.2, 0.2, "Falling uses EWMA_ALPHA"),
            (100.0, 100.0, 0.2, 0.2, "Stable uses EWMA_ALPHA"),
        ]

        for prev_level, sample, EWMA_ALPHA, expected, description in alpha_cases:
            with self.subTest(behavior="alpha_selection", description=description):
                alpha_up = 1.0 - (1.0 - EWMA_ALPHA) ** 2
                alpha = alpha_up if sample > prev_level else EWMA_ALPHA
                self.assertAlmostEqual(alpha, expected, places=5)

        # Test level calculation with asymmetric alpha
        level_cases = [
            (100.0, 150.0, 0.2, 118.0, "Rising with alpha_up"),
            (100.0, 50.0, 0.2, 90.0, "Falling with EWMA_ALPHA"),
            (0.0, 100.0, 0.2, 100.0, "Bootstrap uses sample"),
        ]

        for prev_level, sample, EWMA_ALPHA, expected, description in level_cases:
            with self.subTest(behavior="level_calculation", description=description):
                if prev_level <= 0:
                    level = sample
                else:
                    alpha_up = 1.0 - (1.0 - EWMA_ALPHA) ** 2
                    alpha = alpha_up if sample > prev_level else EWMA_ALPHA
                    level = (1 - alpha) * prev_level + alpha * sample
                self.assertAlmostEqual(level, expected, places=5)

    def test_simple_calculation_formulas(self):
        """Test simple calculation formulas: scale, min_samples, and windows."""
        # Test scale calculation: max(1.0, float(dev))
        scale_cases = [
            (100.0, 100.0, "Normal deviation"),
            (0.0, 1.0, "Zero deviation"),
            (0.5, 1.0, "Small deviation"),
            (1.1, 1.1, "Just above unit"),
        ]

        for dev, expected, description in scale_cases:
            with self.subTest(formula="scale", description=description):
                scale = max(1.0, float(dev))
                self.assertAlmostEqual(scale, expected, places=5)

        # Test min_samples calculation: recent_window + older_window
        min_samples_cases = [
            (10, 10, "HISTORY_LEN=10"),
            (6, 6, "HISTORY_LEN=6"),
            (12, 12, "HISTORY_LEN=12"),
        ]

        for HISTORY_LEN, expected, description in min_samples_cases:
            with self.subTest(formula="min_samples", description=description):
                recent_window = HISTORY_LEN // 2
                older_window = HISTORY_LEN // 2
                min_samples = recent_window + older_window
                self.assertEqual(min_samples, expected)

        # Test window calculation: recent_window = older_window = HISTORY_LEN // 2
        window_cases = [
            (10, 5, 5, "HISTORY_LEN=10"),
            (6, 3, 3, "HISTORY_LEN=6"),
            (9, 4, 4, "HISTORY_LEN=9 (integer division)"),
        ]

        for HISTORY_LEN, expected_recent, expected_older, description in window_cases:
            with self.subTest(formula="windows", description=description):
                recent_window = HISTORY_LEN // 2
                older_window = HISTORY_LEN // 2
                self.assertEqual(recent_window, expected_recent)
                self.assertEqual(older_window, expected_older)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
