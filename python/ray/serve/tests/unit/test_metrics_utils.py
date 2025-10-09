import asyncio
import sys

import pytest

from ray._common.test_utils import async_wait_for_condition
from ray.serve._private.metrics_utils import (
    InMemoryMetricsStore,
    MetricsPusher,
    TimeStampedValue,
    merge_instantaneous_total,
    merge_timeseries_dicts,
    time_weighted_average,
)
from ray.serve._private.test_utils import MockAsyncTimer


class TestMetricsPusher:
    @pytest.mark.asyncio
    async def test_no_tasks(self):
        """Test that a metrics pusher can be started with zero tasks.

        After a task is registered, it should work.
        """
        val = 0

        def inc():
            nonlocal val
            val += 1

        metrics_pusher = MetricsPusher()
        metrics_pusher.start()
        assert len(metrics_pusher._tasks) == 0

        metrics_pusher.register_or_update_task("inc", inc, 0.01)

        async_wait_for_condition(lambda: val > 0, timeout=10)

    @pytest.mark.asyncio
    async def test_basic(self):
        timer = MockAsyncTimer(0)
        state = {"val": 0}

        def task(s):
            s["val"] += 1

        metrics_pusher = MetricsPusher(async_sleep=timer.sleep)
        metrics_pusher.start()

        metrics_pusher.register_or_update_task("basic", lambda: task(state), 0.5)
        for i in range(20):
            await async_wait_for_condition(
                lambda: timer.num_sleepers() == 1, retry_interval_ms=1
            )
            timer.advance(0.5)
            await asyncio.sleep(0)
            assert state["val"] == i + 1

        await metrics_pusher.graceful_shutdown()

    @pytest.mark.asyncio
    async def test_multiple_tasks(self):
        timer = MockAsyncTimer(0)

        state = {"A": 0, "B": 0, "C": 0}

        def task(key, s):
            s[key] += 1

        metrics_pusher = MetricsPusher(async_sleep=timer.sleep)
        metrics_pusher.start()

        # Each task interval is different, and they don't divide each other.
        metrics_pusher.register_or_update_task("A", lambda: task("A", state), 0.2)
        metrics_pusher.register_or_update_task("B", lambda: task("B", state), 0.3)
        metrics_pusher.register_or_update_task("C", lambda: task("C", state), 0.5)

        times = sorted(
            [(0, None, None)]
            + [(0.2 * (i + 1), "A", i + 2) for i in range(15)]
            + [(0.3 * (i + 1), "B", i + 2) for i in range(10)]
            + [(0.5 * (i + 1), "C", i + 2) for i in range(6)]
        )
        advances = [(j[0] - i[0], j[1], j[2]) for i, j in zip(times[:-1], times[1:])]

        for t, key, expected in advances:
            await async_wait_for_condition(
                lambda: timer.num_sleepers() == 3, retry_interval_ms=1, timeout=1
            )
            timer.advance(t + 0.001)
            await async_wait_for_condition(
                lambda: state[key] == expected, retry_interval_ms=1, timeout=1
            )

        # At 7 seconds, tasks A, B, C should have executed 16, 11, and 7
        # times respectively.
        assert state["A"] == 16
        assert state["B"] == 11
        assert state["C"] == 7
        await metrics_pusher.graceful_shutdown()

    @pytest.mark.asyncio
    async def test_update_task(self):
        _start = {"A": 0}
        timer = MockAsyncTimer(_start["A"])
        state = {"A": 0, "B": 0}

        def f(s):
            s["A"] += 1

        # Start metrics pusher and register task() with interval 1s.
        # After (fake) 10s, the task should have executed 10 times
        metrics_pusher = MetricsPusher(async_sleep=timer.sleep)
        metrics_pusher.start()

        # Give the metrics pusher thread opportunity to execute task
        # The only thing that should be moving the timer forward is
        # the metrics pusher thread. So if the timer has reached 11,
        # the task should have at least executed 10 times.
        metrics_pusher.register_or_update_task("my_task", lambda: f(state), 1)
        for i in range(20):
            await async_wait_for_condition(
                lambda: timer.num_sleepers() == 1, retry_interval_ms=1
            )
            timer.advance(1)
            await asyncio.sleep(0)
            assert state["A"] == i + 1

        def new_f(s):
            s["B"] += 1

        # Re-register new_f() with interval 50s.
        metrics_pusher.register_or_update_task("my_task", lambda: new_f(state), 50)
        for i in range(20):
            await async_wait_for_condition(
                lambda: timer.num_sleepers() == 1, retry_interval_ms=1, timeout=1
            )
            timer.advance(50)

            await asyncio.sleep(0)
            assert state["B"] == i + 1

        await metrics_pusher.graceful_shutdown()


def assert_timeseries_equal(actual, expected):
    assert len(actual) == len(
        expected
    ), f"Length mismatch: {len(actual)} vs {len(expected)}"
    for i, (a, e) in enumerate(zip(actual, expected)):
        assert (
            a.timestamp == e.timestamp
        ), f"Timestamp mismatch at {i}: {a.timestamp} vs {e.timestamp}"
        assert a.value == e.value, f"Value mismatch at {i}: {a.value} vs {e.value}"


class TestInMemoryMetricsStore:
    def test_basics(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 2}, timestamp=2)
        assert s.aggregate_avg(["m1"]) == (1.5, 1)
        assert s.aggregate_max(["m1"]) == (2, 1)
        assert s.aggregate_min(["m1"]) == (1, 1)
        assert s.get_latest("m1") == 2

    def test_out_of_order_insert(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 5}, timestamp=5)
        s.add_metrics_point({"m1": 3}, timestamp=3)
        s.add_metrics_point({"m1": 2}, timestamp=2)
        s.add_metrics_point({"m1": 4}, timestamp=4)
        assert s.aggregate_avg(["m1"]) == (3, 1)
        assert s.aggregate_max(["m1"]) == (5, 1)
        assert s.aggregate_min(["m1"]) == (1, 1)

    def test_window_start_timestamp(self):
        s = InMemoryMetricsStore()
        assert s.aggregate_avg(["m1"]) == (None, 0)
        assert s.aggregate_max(["m1"]) == (None, 0)
        assert s.aggregate_min(["m1"]) == (None, 0)

        s.add_metrics_point({"m1": 1}, timestamp=2)
        assert s.aggregate_avg(["m1"]) == (1, 1)
        s.prune_keys_and_compact_data(10)
        assert s.aggregate_avg(["m1"]) == (None, 0)

    def test_multiple_metrics(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1, "m2": -1}, timestamp=1)
        s.add_metrics_point({"m1": 2, "m2": -2}, timestamp=2)
        assert s.aggregate_avg(["m1"]) == (1.5, 1)
        assert s.aggregate_avg(["m2"]) == (-1.5, 1)
        assert s.aggregate_avg(["m1", "m2"]) == (0, 2)
        assert s.aggregate_max(["m1"]) == (2, 1)
        assert s.aggregate_max(["m2"]) == (-1, 1)
        assert s.aggregate_max(["m1", "m2"]) == (2, 2)
        assert s.aggregate_min(["m1"]) == (1, 1)
        assert s.aggregate_min(["m2"]) == (-2, 1)
        assert s.aggregate_min(["m1", "m2"]) == (-2, 2)

    def test_empty_key_mix(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1}, timestamp=1)
        assert s.aggregate_avg(["m1", "m2"]) == (1, 1)
        assert s.aggregate_max(["m1", "m2"]) == (1, 1)
        assert s.aggregate_min(["m1", "m2"]) == (1, 1)
        assert s.aggregate_avg(["m2"]) == (None, 0)

    def test_prune_keys_and_compact_data(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1, "m2": 2, "m3": 8, "m4": 5}, timestamp=1)
        s.add_metrics_point({"m1": 2, "m2": 3, "m3": 8}, timestamp=2)
        s.add_metrics_point({"m1": 2, "m2": 5}, timestamp=3)
        s.prune_keys_and_compact_data(1.1)
        assert set(s.data) == {"m1", "m2", "m3"}
        assert len(s.data["m1"]) == 2 and s.data["m1"] == s._get_datapoints("m1", 1.1)
        assert len(s.data["m2"]) == 2 and s.data["m2"] == s._get_datapoints("m2", 1.1)
        assert len(s.data["m3"]) == 1 and s.data["m3"] == s._get_datapoints("m3", 1.1)


class TestInstantaneousMerge:
    """Test the new instantaneous merge functionality."""

    def test_merge_instantaneous_total_empty(self):
        """Test merge_instantaneous_total with empty input."""
        result = merge_instantaneous_total([])
        assert result == []

        result = merge_instantaneous_total([[], []])
        assert result == []

    def test_merge_instantaneous_total_single_replica(self):
        """Test merge_instantaneous_total with single replica."""
        series = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 7.0),
            TimeStampedValue(3.0, 3.0),
        ]
        result = merge_instantaneous_total([series])

        expected = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 7.0),
            TimeStampedValue(3.0, 3.0),
        ]
        assert_timeseries_equal(result, expected)

    def test_merge_instantaneous_total_two_replicas(self):
        """Test merge_instantaneous_total with two replicas."""
        series1 = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(3.0, 7.0),
        ]
        series2 = [
            TimeStampedValue(2.0, 3.0),
            TimeStampedValue(4.0, 1.0),
        ]
        result = merge_instantaneous_total([series1, series2])

        # Expected: t=1.0: +5 (total=5), t=2.0: +3 (total=8), t=3.0: +2 (total=10), t=4.0: -2 (total=8)
        expected = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 8.0),
            TimeStampedValue(3.0, 10.0),
            TimeStampedValue(4.0, 8.0),
        ]
        assert_timeseries_equal(result, expected)

    def test_merge_instantaneous_total_complex_scenario(self):
        """Test complex scenario matching the autoscaling example."""
        # r1: starts at 5 (t=0.2), changes to 7 (t=0.8), then 6 (t=1.5)
        series1 = [
            TimeStampedValue(0.2, 5.0),
            TimeStampedValue(0.8, 7.0),
            TimeStampedValue(1.5, 6.0),
        ]
        # r2: starts at 3 (t=0.1), changes to 4 (t=0.9), then 8 (t=1.2)
        series2 = [
            TimeStampedValue(0.1, 3.0),
            TimeStampedValue(0.9, 4.0),
            TimeStampedValue(1.2, 8.0),
        ]
        result = merge_instantaneous_total([series1, series2])

        expected = [
            TimeStampedValue(0.1, 3.0),  # r2 starts
            TimeStampedValue(0.2, 8.0),  # r1 starts: 3+5=8
            TimeStampedValue(0.8, 10.0),  # r1 changes: 8+(7-5)=10
            TimeStampedValue(0.9, 11.0),  # r2 changes: 10+(4-3)=11
            TimeStampedValue(1.2, 15.0),  # r2 changes: 11+(8-4)=15
            TimeStampedValue(1.5, 14.0),  # r1 changes: 15+(6-7)=14
        ]
        assert_timeseries_equal(result, expected)

    def test_time_weighted_average_empty(self):
        """Test time_weighted_average with empty series."""
        result = time_weighted_average([], 0.0, 1.0)
        assert result is None

    def test_time_weighted_average_no_overlap(self):
        """Test time_weighted_average with no data overlap."""
        series = [TimeStampedValue(2.0, 5.0)]
        result = time_weighted_average(series, 0.0, 1.0)
        assert result == 0.0  # Default value before first point

    def test_time_weighted_average_constant_value(self):
        """Test time_weighted_average with constant value."""
        series = [TimeStampedValue(0.5, 10.0)]
        result = time_weighted_average(series, 1.0, 2.0)
        assert result == 10.0

    def test_time_weighted_average_step_function(self):
        """Test time_weighted_average with step function."""
        series = [
            TimeStampedValue(0.0, 5.0),
            TimeStampedValue(1.0, 10.0),
            TimeStampedValue(2.0, 15.0),
        ]
        # Average over [0.5, 1.5): 0.5s at value 5, 0.5s at value 10
        result = time_weighted_average(series, 0.5, 1.5)
        expected = (5.0 * 0.5 + 10.0 * 0.5) / 1.0
        assert abs(result - expected) < 1e-10

    def test_time_weighted_average_none_window_start(self):
        """Test time_weighted_average with None window_start."""
        series = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 10.0),
            TimeStampedValue(3.0, 15.0),
        ]
        # Should use full series from start (t=1.0) to window_end (t=2.5)
        result = time_weighted_average(series, None, 2.5)
        # 1.0s at value 5 (from 1.0 to 2.0), 0.5s at value 10 (from 2.0 to 2.5)
        expected = (5.0 * 1.0 + 10.0 * 0.5) / 1.5
        assert abs(result - expected) < 1e-10

    def test_time_weighted_average_none_window_end(self):
        """Test time_weighted_average with None window_end."""
        series = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 10.0),
            TimeStampedValue(3.0, 15.0),
        ]
        # Should use from window_start (t=1.5) to end of series (t=3.0+1.0=4.0)
        result = time_weighted_average(series, 1.5, None)
        # 0.5s at value 5 (from 1.5 to 2.0), 1.0s at value 10 (from 2.0 to 3.0), 1.0s at value 15 (from 3.0 to 4.0)
        expected = (5.0 * 0.5 + 10.0 * 1.0 + 15.0 * 1.0) / 2.5
        assert abs(result - expected) < 1e-10

    def test_time_weighted_average_both_none(self):
        """Test time_weighted_average with both window_start and window_end None."""
        series = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 10.0),
            TimeStampedValue(3.0, 15.0),
        ]
        # Should use full series from t=1.0 to t=3.0+1.0=4.0
        result = time_weighted_average(series, None, None)
        # 1.0s at value 5, 1.0s at value 10, 1.0s at value 15
        expected = (5.0 * 1.0 + 10.0 * 1.0 + 15.0 * 1.0) / 3.0
        assert abs(result - expected) < 1e-10

    def test_time_weighted_average_single_point_none_bounds(self):
        """Test time_weighted_average with single point and None bounds."""
        series = [TimeStampedValue(2.0, 10.0)]
        result = time_weighted_average(series, None, None)
        # Single point with 1.0s duration (from 2.0 to 3.0)
        assert result == 10.0

    def test_time_weighted_average_custom_last_window_s(self):
        """Test time_weighted_average with custom last_window_s parameter."""
        series = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 10.0),
            TimeStampedValue(3.0, 15.0),
        ]

        # Test with last_window_s=2.0 (double the default)
        result_2s = time_weighted_average(series, None, None, last_window_s=2.0)
        # Should use from t=1.0 to t=3.0+2.0=5.0
        # 1.0s at value 5 (from 1.0 to 2.0), 1.0s at value 10 (from 2.0 to 3.0), 2.0s at value 15 (from 3.0 to 5.0)
        expected_2s = (5.0 * 1.0 + 10.0 * 1.0 + 15.0 * 2.0) / 4.0
        assert abs(result_2s - expected_2s) < 1e-10

        # Test with last_window_s=0.5 (half the default)
        result_0_5s = time_weighted_average(series, None, None, last_window_s=0.5)
        # Should use from t=1.0 to t=3.0+0.5=3.5
        # 1.0s at value 5 (from 1.0 to 2.0), 1.0s at value 10 (from 2.0 to 3.0), 0.5s at value 15 (from 3.0 to 3.5)
        expected_0_5s = (5.0 * 1.0 + 10.0 * 1.0 + 15.0 * 0.5) / 2.5
        assert abs(result_0_5s - expected_0_5s) < 1e-10

        # Test with window_start specified but window_end None - should still use last_window_s
        result_with_start = time_weighted_average(series, 1.5, None, last_window_s=3.0)
        # Should use from t=1.5 to t=3.0+3.0=6.0
        # 0.5s at value 5 (from 1.5 to 2.0), 1.0s at value 10 (from 2.0 to 3.0), 3.0s at value 15 (from 3.0 to 6.0)
        expected_with_start = (5.0 * 0.5 + 10.0 * 1.0 + 15.0 * 3.0) / 4.5
        assert abs(result_with_start - expected_with_start) < 1e-10

        # Test that last_window_s is ignored when window_end is explicitly provided
        result_explicit_end = time_weighted_average(
            series, None, 4.0, last_window_s=10.0
        )
        # Should use from t=1.0 to t=4.0 (ignoring last_window_s=10.0)
        # 1.0s at value 5 (from 1.0 to 2.0), 1.0s at value 10 (from 2.0 to 3.0), 1.0s at value 15 (from 3.0 to 4.0)
        expected_explicit_end = (5.0 * 1.0 + 10.0 * 1.0 + 15.0 * 1.0) / 3.0
        assert abs(result_explicit_end - expected_explicit_end) < 1e-10

    def test_merge_timeseries_dicts_instantaneous_basic(self):
        """Test merge_timeseries_dicts basic functionality with instantaneous approach."""
        s1 = InMemoryMetricsStore()
        s2 = InMemoryMetricsStore()

        s1.add_metrics_point({"metric1": 5, "metric2": 10}, timestamp=1.0)
        s1.add_metrics_point({"metric1": 7}, timestamp=2.0)

        s2.add_metrics_point({"metric1": 3, "metric3": 20}, timestamp=1.5)

        result = merge_timeseries_dicts(s1.data, s2.data)

        # metric1: s1 starts at 5 (t=1.0), s2 starts at 3 (t=1.5), s1 changes to 7 (t=2.0)
        expected_metric1 = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(1.5, 8.0),  # 5+3=8
            TimeStampedValue(2.0, 10.0),  # 3+(7-5)=10
        ]
        assert_timeseries_equal(result["metric1"], expected_metric1)

        # metric2: only from s1
        expected_metric2 = [TimeStampedValue(1.0, 10.0)]
        assert_timeseries_equal(result["metric2"], expected_metric2)

        # metric3: only from s2
        expected_metric3 = [TimeStampedValue(1.5, 20.0)]
        assert_timeseries_equal(result["metric3"], expected_metric3)

    def test_merge_instantaneous_vs_windowed_comparison(self):
        """Compare instantaneous merge vs windowed approach."""
        # Create test data that highlights the difference
        s1 = InMemoryMetricsStore()
        s2 = InMemoryMetricsStore()

        # Replica 1: 10 requests at t=0.1, then 5 at t=0.9
        s1.add_metrics_point({"requests": 10}, timestamp=0.1)
        s1.add_metrics_point({"requests": 5}, timestamp=0.9)

        # Replica 2: 3 requests at t=0.5, then 8 at t=1.1
        s2.add_metrics_point({"requests": 3}, timestamp=0.5)
        s2.add_metrics_point({"requests": 8}, timestamp=1.1)

        # Instantaneous approach
        instantaneous = merge_timeseries_dicts(s1.data, s2.data)

        # Instantaneous should have: t=0.1: 10, t=0.5: 13, t=0.9: 8, t=1.1: 13
        expected_instantaneous = [
            TimeStampedValue(0.1, 10.0),
            TimeStampedValue(0.5, 13.0),  # 10+3=13
            TimeStampedValue(0.9, 8.0),  # 3+(5-10)=8
            TimeStampedValue(1.1, 13.0),  # 5+(8-3)=13
        ]
        assert_timeseries_equal(instantaneous["requests"], expected_instantaneous)

    def test_instantaneous_merge_handles_zero_deltas(self):
        """Test that zero deltas are properly filtered out."""
        series1 = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(2.0, 5.0),  # No change
            TimeStampedValue(3.0, 7.0),
        ]
        series2 = [
            TimeStampedValue(1.5, 3.0),
            TimeStampedValue(2.5, 3.0),  # No change
        ]

        result = merge_instantaneous_total([series1, series2])

        # Should skip zero deltas
        expected = [
            TimeStampedValue(1.0, 5.0),
            TimeStampedValue(1.5, 8.0),  # 5+3=8
            TimeStampedValue(3.0, 10.0),  # 8+(7-5)=10
        ]
        assert_timeseries_equal(result, expected)

    def test_instantaneous_merge_with_epoch_times(self):
        """Test instantaneous merge with realistic epoch timestamps."""

        # Use realistic epoch times (around current time)
        base_time = 1703980800.0  # December 30, 2023 16:00:00 UTC

        # Simulate 3 replicas reporting metrics over a 30-second period
        replica1_series = [
            TimeStampedValue(base_time + 0.0, 12.0),  # t=0s: 12 running requests
            TimeStampedValue(base_time + 5.2, 15.0),  # t=5.2s: increased to 15
            TimeStampedValue(base_time + 18.7, 8.0),  # t=18.7s: dropped to 8
            TimeStampedValue(base_time + 25.1, 11.0),  # t=25.1s: back up to 11
        ]

        replica2_series = [
            TimeStampedValue(base_time + 1.3, 7.0),  # t=1.3s: 7 running requests
            TimeStampedValue(base_time + 8.9, 9.0),  # t=8.9s: increased to 9
            TimeStampedValue(base_time + 22.4, 4.0),  # t=22.4s: dropped to 4
        ]

        replica3_series = [
            TimeStampedValue(base_time + 3.1, 5.0),  # t=3.1s: 5 running requests
            TimeStampedValue(base_time + 12.6, 8.0),  # t=12.6s: increased to 8
            TimeStampedValue(base_time + 20.8, 6.0),  # t=20.8s: dropped to 6
            TimeStampedValue(base_time + 28.3, 9.0),  # t=28.3s: increased to 9
        ]

        # Merge all replicas
        result = merge_instantaneous_total(
            [replica1_series, replica2_series, replica3_series]
        )

        # Expected timeline of instantaneous totals:
        expected = [
            TimeStampedValue(base_time + 0.0, 12.0),  # r1 starts: 12
            TimeStampedValue(base_time + 1.3, 19.0),  # r2 starts: 12+7=19
            TimeStampedValue(base_time + 3.1, 24.0),  # r3 starts: 19+5=24
            TimeStampedValue(base_time + 5.2, 27.0),  # r1 changes: 24+(15-12)=27
            TimeStampedValue(base_time + 8.9, 29.0),  # r2 changes: 27+(9-7)=29
            TimeStampedValue(base_time + 12.6, 32.0),  # r3 changes: 29+(8-5)=32
            TimeStampedValue(base_time + 18.7, 25.0),  # r1 changes: 32+(8-15)=25
            TimeStampedValue(base_time + 20.8, 23.0),  # r3 changes: 25+(6-8)=23
            TimeStampedValue(base_time + 22.4, 18.0),  # r2 changes: 23+(4-9)=18
            TimeStampedValue(base_time + 25.1, 21.0),  # r1 changes: 18+(11-8)=21
            TimeStampedValue(base_time + 28.3, 24.0),  # r3 changes: 21+(9-6)=24
        ]

        assert_timeseries_equal(result, expected)

        # Test time-weighted average over different intervals
        # Full series average
        full_avg = time_weighted_average(result, None, None)
        assert full_avg is not None
        assert full_avg > 0

        # Average over first 10 seconds
        early_avg = time_weighted_average(result, base_time, base_time + 10.0)
        assert early_avg is not None

        # Average over last 10 seconds
        late_avg = time_weighted_average(result, base_time + 20.0, base_time + 30.0)
        assert late_avg is not None

        # Verify the averages make sense relative to each other
        # (early period has higher values, so early_avg should be > late_avg)
        assert early_avg > late_avg

        print(f"Full series average: {full_avg:.2f}")
        print(f"Early period average (0-10s): {early_avg:.2f}")
        print(f"Late period average (20-30s): {late_avg:.2f}")

    def test_merge_instantaneous_total_timestamp_rounding(self):
        """Test that timestamps are rounded to 10ms precision."""
        series1 = [
            TimeStampedValue(1.001234, 5.0),  # Should round to 1.00
            TimeStampedValue(2.005678, 7.0),  # Should round to 2.01
            TimeStampedValue(3.009999, 3.0),  # Should round to 3.01
        ]
        series2 = [
            TimeStampedValue(1.504321, 2.0),  # Should round to 1.50
            TimeStampedValue(2.008765, 4.0),  # Should round to 2.01
        ]

        result = merge_instantaneous_total([series1, series2])

        # Verify timestamps are rounded to 2 decimal places (10ms precision)
        expected_timestamps = [1.00, 1.50, 2.01, 3.01]
        actual_timestamps = [point.timestamp for point in result]

        assert len(actual_timestamps) == len(expected_timestamps)
        for actual, expected in zip(actual_timestamps, expected_timestamps):
            assert actual == expected, f"Expected {expected}, got {actual}"

        # Verify values are correct with rounded timestamps
        expected = [
            TimeStampedValue(1.00, 5.0),  # series1 starts
            TimeStampedValue(1.50, 7.0),  # series2 starts: 5+2=7
            TimeStampedValue(
                2.01, 11.0
            ),  # s1 becomes 7, s2 becomes 4. Total: 7 + 4 = 11.0
            TimeStampedValue(3.01, 7.0),  # series1 changes: 11+(3-7)=7
        ]
        assert_timeseries_equal(result, expected)

    def test_merge_instantaneous_total_combine_same_timestamp(self):
        """Test that datapoints with same rounded timestamp are combined."""
        # Create series where multiple events round to the same timestamp
        series1 = [
            TimeStampedValue(1.001, 5.0),  # Rounds to 1.00
            TimeStampedValue(1.004, 7.0),  # Also rounds to 1.00
            TimeStampedValue(2.000, 10.0),  # Rounds to 2.00
        ]
        series2 = [
            TimeStampedValue(1.002, 3.0),  # Rounds to 1.00
            TimeStampedValue(1.005, 4.0),  # Also rounds to 1.00
        ]

        result = merge_instantaneous_total([series1, series2])

        # Should only have unique rounded timestamps
        timestamps = [point.timestamp for point in result]
        assert timestamps == [
            1.00,
            2.00,
        ], f"Expected [1.00, 2.00], got {timestamps}"

        # The value at 1.00 should be the final state after all changes at that rounded time
        # Order of events at rounded timestamp 1.00:
        # - series1: 0->5 (t=1.001)
        # - series2: 0->3 (t=1.002)
        # - series1: 5->7 (t=1.004)
        # - series2: 3->4 (t=1.005)
        # Final state: series1=7, series2=4, total=11
        expected = [
            TimeStampedValue(1.00, 11.0),  # Final combined state at rounded timestamp
            TimeStampedValue(2.00, 14.0),  # series1 changes: 11+(10-7)=14
        ]
        assert_timeseries_equal(result, expected)

    def test_merge_instantaneous_total_edge_cases_rounding(self):
        """Test edge cases for timestamp rounding and combination."""
        # Test rounding edge cases
        series1 = [
            TimeStampedValue(1.004999, 5.0),  # Should round to 1.0
            TimeStampedValue(1.005000, 7.0),  # Should round to 1.0 (round half to even)
            TimeStampedValue(1.005001, 9.0),  # Should round to 1.01
        ]

        result = merge_instantaneous_total([series1])

        # Should have two distinct rounded timestamps
        expected_timestamps = [1.0, 1.01]
        actual_timestamps = [point.timestamp for point in result]
        assert actual_timestamps == expected_timestamps

        # Values should reflect the changes
        # Both 1.004999 and 1.005000 round to 1.0, so they get combined
        # Order: 1.004999 (0->5), then 1.005000 (5->7) - final value at 1.0 is 7.0
        # Then 1.005001 (7->9) rounds to 1.01 - value at 1.01 is 9.0
        expected = [
            TimeStampedValue(
                1.0, 7.0
            ),  # Final state after all changes that round to 1.0 (1.004999: 0->5, 1.005000: 5->7)
            TimeStampedValue(1.01, 9.0),  # State after change at 1.005001 (7->9)
        ]
        assert_timeseries_equal(result, expected)

    def test_merge_instantaneous_total_no_changes_filtered(self):
        """Test that zero-change events are filtered even with rounding."""
        series1 = [
            TimeStampedValue(1.001, 5.0),  # Rounds to 1.00
            TimeStampedValue(1.004, 5.0),  # Also rounds to 1.00, no change
            TimeStampedValue(2.000, 7.0),  # Rounds to 2.00, change
        ]

        result = merge_instantaneous_total([series1])

        # Should only include points where value actually changed
        expected = [
            TimeStampedValue(1.00, 5.0),  # Initial value
            TimeStampedValue(2.00, 7.0),  # Value change
        ]
        assert_timeseries_equal(result, expected)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
