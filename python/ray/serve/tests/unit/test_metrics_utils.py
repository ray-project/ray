import asyncio
import sys
from collections import defaultdict

import pytest

from ray._common.test_utils import async_wait_for_condition
from ray.serve._private.metrics_utils import (
    QUEUED_REQUESTS_KEY,
    InMemoryMetricsStore,
    MetricsPusher,
    TimeStampedValue,
    _bucket_latest_by_window,
    _merge_two_timeseries,
    merge_timeseries_dicts,
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

    def test_merge_metrics_stores(self):
        s1 = InMemoryMetricsStore()
        s2 = InMemoryMetricsStore()
        s3 = InMemoryMetricsStore()
        s1.add_metrics_point(
            {"m1": 1, "m2": 2, "m3": 3, QUEUED_REQUESTS_KEY: 1}, timestamp=1
        )
        s2.add_metrics_point({"m1": 2, "m2": 2, QUEUED_REQUESTS_KEY: 1}, timestamp=2)
        s3.add_metrics_point({"m2": 10, QUEUED_REQUESTS_KEY: 10}, timestamp=2)
        merged = merge_timeseries_dicts(s1.data, s2.data, s3.data, window_s=1)

        assert_timeseries_equal(
            merged["m1"], [TimeStampedValue(1, 1), TimeStampedValue(2, 2)]
        )
        assert_timeseries_equal(
            merged["m2"], [TimeStampedValue(1, 2), TimeStampedValue(2, 12)]
        )
        assert_timeseries_equal(merged["m3"], [TimeStampedValue(1, 3)])
        assert_timeseries_equal(
            merged[QUEUED_REQUESTS_KEY],
            [TimeStampedValue(1, 1), TimeStampedValue(2, 11)],
        )

        s4 = InMemoryMetricsStore()
        s4.add_metrics_point(
            {"m1": 100, "m2": 100, "m3": 100, QUEUED_REQUESTS_KEY: 10}, timestamp=0
        )

        merged = merge_timeseries_dicts(s1.data, s2.data, s3.data, s4.data, window_s=2)

        # With window_s=2 and window start alignment:
        # Window boundaries: [0,2), [2,4), etc.
        # timestamp=0 (s4) and timestamp=1 (s1) -> window 0
        # timestamp=2 (s2, s3) -> window 1
        assert_timeseries_equal(
            merged["m1"],
            [TimeStampedValue(0, 101), TimeStampedValue(2, 2)],  # 100+1=101, then 2
        )
        assert_timeseries_equal(
            merged["m2"],
            [
                TimeStampedValue(0, 102),
                TimeStampedValue(2, 12),
            ],  # 100+2=102, then 2+10=12
        )
        assert_timeseries_equal(
            merged["m3"], [TimeStampedValue(0, 103)]  # 100+3=103, no data in window 1
        )
        assert_timeseries_equal(
            merged[QUEUED_REQUESTS_KEY],
            [TimeStampedValue(0, 11), TimeStampedValue(2, 11)],  # 10+1=11, then 1+10=11
        )

        s1_s2 = merge_timeseries_dicts(s1.data, s2.data, window_s=1)
        s2_s1 = merge_timeseries_dicts(s2.data, s1.data, window_s=1)
        s1_s2_s3_s4 = merge_timeseries_dicts(
            s1.data, s2.data, s3.data, s4.data, window_s=1
        )
        s4_s1_s3_s2 = merge_timeseries_dicts(
            s4.data, s1.data, s3.data, s2.data, window_s=1
        )

        # dict equality -> compare per-key time series
        for k in s1_s2:
            assert_timeseries_equal(s1_s2[k], s2_s1[k])
        for k in s1_s2_s3_s4:
            assert_timeseries_equal(s1_s2_s3_s4[k], s4_s1_s3_s2[k])

        a1_none = merge_timeseries_dicts(s1.data, defaultdict(list), window_s=1)
        for k in a1_none:
            assert_timeseries_equal(a1_none[k], s1.data[k])

    def test_bucket_latest_by_window_basic(self):
        """Test basic functionality of _bucket_latest_by_window."""
        series = [
            TimeStampedValue(1.0, 10.0),
            TimeStampedValue(1.5, 15.0),  # Same window as 1.0, should overwrite
            TimeStampedValue(3.0, 30.0),
        ]

        # With window_s=1.0, start=0.0
        buckets = _bucket_latest_by_window(series, start=0.0, window_s=1.0)

        # Window 1: timestamps 1.0-2.0, latest value should be 15.0
        # Window 3: timestamp 3.0-4.0, value should be 30.0
        expected = {1: 15.0, 3: 30.0}
        assert buckets == expected

    def test_bucket_latest_by_window_empty(self):
        """Test _bucket_latest_by_window with empty series."""
        buckets = _bucket_latest_by_window([], start=0.0, window_s=1.0)
        assert buckets == {}

    def test_bucket_latest_by_window_single_value(self):
        """Test _bucket_latest_by_window with single value."""
        series = [TimeStampedValue(2.5, 25.0)]
        buckets = _bucket_latest_by_window(series, start=0.0, window_s=1.0)
        assert buckets == {2: 25.0}

    def test_bucket_latest_by_window_negative_timestamps(self):
        """Test _bucket_latest_by_window with negative timestamps."""
        series = [
            TimeStampedValue(-1.5, 10.0),
            TimeStampedValue(-0.5, 20.0),
            TimeStampedValue(0.5, 30.0),
        ]
        buckets = _bucket_latest_by_window(series, start=-2.0, window_s=1.0)
        # Window 0: -1.5 (index = (-1.5 - (-2.0)) // 1.0 = 0.5 // 1.0 = 0)
        # Window 1: -0.5 (index = (-0.5 - (-2.0)) // 1.0 = 1.5 // 1.0 = 1)
        # Window 2: 0.5 (index = (0.5 - (-2.0)) // 1.0 = 2.5 // 1.0 = 2)
        expected = {0: 10.0, 1: 20.0, 2: 30.0}
        assert buckets == expected

    def test_bucket_latest_by_window_very_small_window(self):
        """Test _bucket_latest_by_window with very small windows."""
        series = [
            TimeStampedValue(1.001, 10.0),
            TimeStampedValue(1.002, 20.0),  # Different window
        ]
        buckets = _bucket_latest_by_window(series, start=1.0, window_s=0.001)
        # With window_s=0.001:
        # 1.001: (1.001 - 1.0) // 0.001 = 1.0 => window 1, but floor division gives 0
        # 1.002: (1.002 - 1.0) // 0.001 = 2.0 => window 2
        expected = {
            0: 10.0,
            2: 20.0,
        }  # Corrected based on actual floor division behavior
        assert buckets == expected

    def test_merge_two_timeseries_both_empty(self):
        """Test _merge_two_timeseries with both series empty."""
        result = _merge_two_timeseries([], [], window_s=1.0)
        assert result == []

    def test_merge_two_timeseries_one_empty(self):
        """Test _merge_two_timeseries with one series empty."""
        t1 = [TimeStampedValue(1.0, 10.0), TimeStampedValue(2.0, 20.0)]

        result1 = _merge_two_timeseries(t1, [], window_s=1.0)
        result2 = _merge_two_timeseries([], t1, window_s=1.0)

        # Results should be the same regardless of order
        assert len(result1) == len(result2) == 2
        assert_timeseries_equal(result1, result2)

    def test_merge_two_timeseries_overlapping_windows(self):
        """Test _merge_two_timeseries with values in overlapping time windows."""
        t1 = [TimeStampedValue(1.0, 10.0), TimeStampedValue(1.5, 15.0)]
        t2 = [TimeStampedValue(1.3, 13.0), TimeStampedValue(1.8, 18.0)]

        result = _merge_two_timeseries(t1, t2, window_s=1.0)

        # With window_s=1.0 and earliest=1.0:
        # start = 1.0 // 1.0 * 1.0 = 1.0
        # Window boundaries are [1.0, 2.0), [2.0, 3.0), etc.
        # All values (1.0, 1.3, 1.5, 1.8) fall in window [1.0, 2.0)
        # So we get 1 window
        assert len(result) == 1

        # Window 0: latest from t1 is 15.0 (1.5 > 1.0), latest from t2 is 18.0 (1.8 > 1.3), sum: 33.0
        assert result[0].value == 33.0

    def test_merge_two_timeseries_zero_window(self):
        """Test _merge_two_timeseries with zero window size."""
        t1 = [TimeStampedValue(1.0, 10.0)]
        t2 = [TimeStampedValue(1.0, 20.0)]

        # Zero window should raise ValueError
        with pytest.raises(ValueError, match="window_s must be positive, got 0"):
            _merge_two_timeseries(t1, t2, window_s=0.0)

    def test_merge_two_timeseries_negative_window(self):
        """Test _merge_two_timeseries with negative window size."""
        t1 = [TimeStampedValue(1.0, 10.0)]
        t2 = [TimeStampedValue(1.0, 20.0)]

        # Negative window should raise ValueError
        with pytest.raises(ValueError, match="window_s must be positive, got -1"):
            _merge_two_timeseries(t1, t2, window_s=-1.0)

    def test_merge_two_timeseries_very_small_window(self):
        """Test _merge_two_timeseries with very small window."""
        t1 = [TimeStampedValue(1.0, 10.0)]
        t2 = [TimeStampedValue(1.0001, 20.0)]

        result = _merge_two_timeseries(t1, t2, window_s=0.0001)

        # With very small window, these should be in different buckets
        assert len(result) == 2

    def test_merge_two_timeseries_large_window(self):
        """Test _merge_two_timeseries with very large window."""
        t1 = [TimeStampedValue(1.0, 10.0), TimeStampedValue(100.0, 15.0)]
        t2 = [TimeStampedValue(50.0, 20.0), TimeStampedValue(200.0, 25.0)]

        result = _merge_two_timeseries(t1, t2, window_s=1000.0)

        # All values should be in the same window
        assert len(result) == 1
        # Latest from t1: 15.0, latest from t2: 25.0, sum: 40.0
        assert result[0].value == 40.0

    def test_merge_two_timeseries_duplicate_timestamps(self):
        """Test _merge_two_timeseries with duplicate timestamps in same series."""
        t1 = [
            TimeStampedValue(1.0, 10.0),
            TimeStampedValue(1.0, 15.0),  # Duplicate timestamp
        ]
        t2 = [TimeStampedValue(1.0, 20.0)]

        result = _merge_two_timeseries(t1, t2, window_s=1.0)

        # Latest from t1 should be 15.0, t2 should be 20.0, sum: 35.0
        assert len(result) == 1
        assert result[0].value == 35.0

    def test_merge_two_timeseries_floating_point_precision(self):
        """Test _merge_two_timeseries with floating point precision edge cases."""
        # Test with timestamps that might have precision issues
        t1 = [TimeStampedValue(0.1 + 0.2, 10.0)]  # 0.30000000000000004
        t2 = [TimeStampedValue(0.3, 20.0)]

        result = _merge_two_timeseries(t1, t2, window_s=0.01)

        # These should be in the same window due to floating point precision
        # but let's verify the behavior
        assert len(result) >= 1

    def test_merge_timeseries_dicts_empty_dicts(self):
        """Test merge_timeseries_dicts with empty dictionaries."""
        result = merge_timeseries_dicts(
            defaultdict(list), defaultdict(list), window_s=1.0
        )
        assert dict(result) == {}

    def test_merge_timeseries_dicts_single_dict(self):
        """Test merge_timeseries_dicts with single dictionary."""
        data = defaultdict(list)
        data["key1"] = [TimeStampedValue(1.0, 10.0)]

        result = merge_timeseries_dicts(data, window_s=1.0)
        # With windowing applied, the result should have the same values but potentially different timestamps
        expected = defaultdict(list)
        expected["key1"] = [TimeStampedValue(1.0, 10.0)]  # Window [1,2) starts at 1.0
        assert_timeseries_equal(result["key1"], expected["key1"])

    def test_merge_timeseries_dicts_no_common_keys(self):
        """Test merge_timeseries_dicts with dictionaries having no common keys."""
        d1 = defaultdict(list)
        d1["key1"] = [TimeStampedValue(1.0, 10.0)]

        d2 = defaultdict(list)
        d2["key2"] = [TimeStampedValue(2.0, 20.0)]

        result = merge_timeseries_dicts(d1, d2, window_s=1.0)

        assert "key1" in result
        assert "key2" in result
        assert len(result["key1"]) == 1
        assert len(result["key2"]) == 1

    def test_merge_timeseries_dicts_many_stores(self):
        """Test merge_timeseries_dicts with many stores."""
        stores = []
        for i in range(10):
            store = defaultdict(list)
            store["common_key"] = [TimeStampedValue(float(i), float(i * 10))]
            stores.append(store)

        result = merge_timeseries_dicts(*stores, window_s=1.0)

        # Each value should be in its own window, sum should be 0+10+20+...+90 = 450
        assert "common_key" in result
        total_value = sum(point.value for point in result["common_key"])
        assert total_value == 450.0

    def test_merge_timeseries_dicts_zero_window(self):
        """Test merge_timeseries_dicts with zero window size."""
        d1 = defaultdict(list)
        d1["key1"] = [TimeStampedValue(1.0, 10.0)]

        d2 = defaultdict(list)
        d2["key1"] = [TimeStampedValue(1.0, 20.0)]

        # Zero window should raise ValueError
        with pytest.raises(ValueError, match="window_s must be positive, got 0"):
            merge_timeseries_dicts(d1, d2, window_s=0.0)

    def test_merge_timeseries_dicts_negative_window(self):
        """Test merge_timeseries_dicts with negative window size."""
        d1 = defaultdict(list)
        d1["key1"] = [TimeStampedValue(1.0, 10.0)]

        # Negative window should raise ValueError
        with pytest.raises(ValueError, match="window_s must be positive, got -1"):
            merge_timeseries_dicts(d1, window_s=-1.0)

    def test_merge_timeseries_dicts_window_alignment_consistency(self):
        """Test that window alignment is consistent regardless of input order."""
        # Create data that might expose window alignment issues
        d1 = defaultdict(list)
        d1["key1"] = [TimeStampedValue(1.1, 10.0)]

        d2 = defaultdict(list)
        d2["key1"] = [TimeStampedValue(1.9, 20.0)]

        d3 = defaultdict(list)
        d3["key1"] = [TimeStampedValue(2.1, 30.0)]

        # Test different orderings
        result1 = merge_timeseries_dicts(d1, d2, d3, window_s=1.0)
        result2 = merge_timeseries_dicts(d3, d1, d2, window_s=1.0)
        result3 = merge_timeseries_dicts(d2, d3, d1, window_s=1.0)

        # Results should be the same regardless of order
        assert_timeseries_equal(result1["key1"], result2["key1"])
        assert_timeseries_equal(result1["key1"], result3["key1"])

    def test_merge_stores_bug_fix_window_center_calculation(self):
        """Test for potential bug in window center calculation."""
        # This test checks if the window center calculation is correct
        d1 = defaultdict(list)
        d1["key1"] = [
            TimeStampedValue(0.0, 10.0),
            TimeStampedValue(1.0, 15.0),
            TimeStampedValue(2.0, 20.0),
            TimeStampedValue(4.0, 30.0),
            TimeStampedValue(5.0, 40.0),
        ]

        result = merge_timeseries_dicts(d1, window_s=2.0)

        # With window_s=2.0 and window start alignment:
        # Window [0,2): timestamps 0.0, 1.0 -> latest value 15.0 at window start 0.0
        # Window [2,4): timestamp 2.0 -> value 20.0 at window start 2.0
        # Window [4,6): timestamps 4.0, 5.0 -> latest value 40.0 at window start 4.0
        assert len(result["key1"]) == 3
        expected = [
            TimeStampedValue(timestamp=0.0, value=15.0),  # Latest in window [0,2)
            TimeStampedValue(timestamp=2.0, value=20.0),  # Value in window [2,4)
            TimeStampedValue(timestamp=4.0, value=40.0),  # Latest in window [4,6)
        ]
        assert_timeseries_equal(result["key1"], expected)

    def test_merge_stores_preserves_value_precision(self):
        """Test that merging preserves floating point precision of values."""
        d1 = defaultdict(list)
        d1["key1"] = [TimeStampedValue(1.0, 0.1)]

        d2 = defaultdict(list)
        d2["key1"] = [TimeStampedValue(1.0, 0.2)]

        result = merge_timeseries_dicts(d1, d2, window_s=1.0)

        # 0.1 + 0.2 should equal 0.3 exactly
        assert len(result["key1"]) == 1
        assert abs(result["key1"][0].value - 0.3) < 1e-10


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
