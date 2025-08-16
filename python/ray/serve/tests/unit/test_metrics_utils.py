import asyncio
import sys

import pytest

from ray._common.test_utils import async_wait_for_condition
from ray.serve._private.metrics_utils import (
    QUEUED_REQUESTS_KEY,
    InMemoryMetricsStore,
    MetricsPusher,
    consolidate_metrics_stores,
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

    def test_consolidate_metrics_stores(self):
        s1 = InMemoryMetricsStore()
        s2 = InMemoryMetricsStore()
        s3 = InMemoryMetricsStore()
        s1.add_metrics_point({"m1": 1, "m3": 3, QUEUED_REQUESTS_KEY: 1}, timestamp=1)
        s2.add_metrics_point({"m1": 2, "m2": 2, QUEUED_REQUESTS_KEY: 1}, timestamp=2)
        # Earliest timestamps are ignored be later stores.
        s3.add_metrics_point(
            {"m1": 100, "m2": 100, "m3": 100, QUEUED_REQUESTS_KEY: 100}, timestamp=0
        )
        consolidated = consolidate_metrics_stores(s1, s2, s3)

        # Earliest store overrides the latest store for each key.
        assert consolidated.aggregate_avg(["m1"]) == (2, 1)
        # New keys are added from later stores.
        assert consolidated.aggregate_avg(["m2"]) == (2, 1)
        # New keys are added from earlier stores.
        assert consolidated.aggregate_avg(["m3"]) == (3, 1)
        # QUEUED_REQUESTS_KEY is summed across stores.
        assert consolidated.get_latest(QUEUED_REQUESTS_KEY) == 102


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
