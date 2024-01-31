import sys
import time
from unittest.mock import patch

import pytest

from ray._private.test_utils import wait_for_condition
from ray.serve._private.metrics_utils import InMemoryMetricsStore, MetricsPusher
from ray.serve._private.test_utils import MockTimer


class TestMetricsPusher:
    def test_no_tasks(self):
        """Test that a metrics pusher can't be started with zero tasks."""

        metrics_pusher = MetricsPusher()
        with pytest.raises(ValueError):
            metrics_pusher.start()

    def test_basic(self):
        start = 0
        timer = MockTimer(start)

        with patch("time.time", new=timer.time), patch(
            "time.sleep", new=timer.realistic_sleep
        ):
            counter = {"val": 0}
            result = {}
            expected_result = 20

            def task(c, res):
                timer.realistic_sleep(0.001)
                c["val"] += 1
                # At 10 seconds, this task should have been called 20 times
                if timer.time() >= 10 and "val" not in res:
                    res["val"] = c["val"]

            metrics_pusher = MetricsPusher()
            metrics_pusher.register_or_update_task(
                "basic_task", lambda: task(counter, result), 0.5
            )

            metrics_pusher.start()
            # This busy wait loop should run for at most a few hundred milliseconds
            # The test should finish by then, and if the test fails this prevents
            # an infinite loop
            for _ in range(10000000):
                if "val" in result:
                    assert result["val"] == expected_result
                    break

            assert result["val"] == expected_result

    def test_multiple_tasks(self):
        start = 0
        timer = MockTimer(start)

        with patch("time.time", new=timer.time), patch(
            "time.sleep", new=timer.realistic_sleep
        ):
            counter = {"A": 0, "B": 0, "C": 0}
            result = {}
            expected_results = {"A": 35, "B": 14, "C": 10}

            def task(key, c, res):
                time.sleep(0.001)
                c[key] += 1
                # Check for how many times this task has been called
                # At 7 seconds, tasks A, B, C should have executed 35, 14, and 10
                # times respectively.
                if timer.time() >= 7 and key not in res:
                    res[key] = c[key]

            metrics_pusher = MetricsPusher()
            # Each task interval is different, and they don't divide each other.
            metrics_pusher.register_or_update_task(
                "increment_A", lambda: task("A", counter, result), 0.2
            )
            metrics_pusher.register_or_update_task(
                "increment_B", lambda: task("B", counter, result), 0.5
            )
            metrics_pusher.register_or_update_task(
                "increment_C", lambda: task("C", counter, result), 0.7
            )
            metrics_pusher.start()

            # Check there are three results set and all are expected.
            def check_results():
                for key in result.keys():
                    assert result[key] == expected_results[key]
                assert len(result) == 3
                return True

            wait_for_condition(check_results, timeout=20)

    def test_update_task(self):
        _start = {"A": 0}
        timer = MockTimer(_start["A"])

        with patch("time.time", new=timer.time), patch(
            "time.sleep", new=timer.realistic_sleep
        ):
            counter = {"A": 0, "B": 0}
            result = {}

            # Task that:
            # - increments c["A"] on each iteration
            # - writes to res["B"] when time has reached 10
            def task(c, res):
                c["A"] += 1
                if timer.time() >= 10 and "A" not in res:
                    res["A"] = c["A"]

                time.sleep(0.001)

            # Start metrics pusher and register task() with interval 1s.
            # After (fake) 10s, the task should have executed 10 times
            metrics_pusher = MetricsPusher()
            metrics_pusher.register_or_update_task(
                "my_task", lambda: task(counter, result), 1
            )
            metrics_pusher.start()

            # Give the metrics pusher thread opportunity to execute task
            # The only thing that should be moving the timer forward is
            # the metrics pusher thread. So if the timer has reached 11,
            # the task should have at least executed 10 times.
            while timer.time() < 11:
                time.sleep(0.001)
            assert result["A"] == 10

            # New task that:
            # - increments c["B"] on each iteration
            # - writes to res["B"] when 450 seconds have passed since first executed
            def new_task(c, res, start):
                if "B" not in start:
                    start["B"] = timer.time()

                c["B"] += 1
                if timer.time() >= start["B"] + 450 and "B" not in res:
                    res["B"] = c["B"]

                time.sleep(0.001)

            _start = {}
            # Re-register new_task() with interval 50s. After (fake)
            # 500s, the task should have executed 10 times.
            metrics_pusher.register_or_update_task(
                "my_task", lambda: new_task(counter, result, _start), 50
            )

            # Wait for the metrics pusher thread to execute the new task
            # at least once, and fetch the time of first execution.
            while "B" not in _start:
                time.sleep(0.001)

            # When the timer has advanced at least 500 past the time of
            # first execution, the new task should have at least
            # executed 10 times.
            while timer.time() < _start["B"] + 500:
                time.sleep(0.001)
            assert result["B"] == 10


class TestInMemoryMetricsStore:
    def test_basics(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 2}, timestamp=2)
        assert s.window_average("m1", window_start_timestamp_s=0) == 1.5
        assert s.max("m1", window_start_timestamp_s=0) == 2

    def test_out_of_order_insert(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 5}, timestamp=5)
        s.add_metrics_point({"m1": 3}, timestamp=3)
        s.add_metrics_point({"m1": 2}, timestamp=2)
        s.add_metrics_point({"m1": 4}, timestamp=4)
        assert s.window_average("m1", window_start_timestamp_s=0) == 3
        assert s.max("m1", window_start_timestamp_s=0) == 5

    def test_window_start_timestamp(self):
        s = InMemoryMetricsStore()
        assert s.window_average("m1", window_start_timestamp_s=0) is None
        assert s.max("m1", window_start_timestamp_s=0) is None

        s.add_metrics_point({"m1": 1}, timestamp=2)
        assert s.window_average("m1", window_start_timestamp_s=0) == 1
        assert (
            s.window_average("m1", window_start_timestamp_s=10, do_compact=False)
            is None
        )

    def test_compaction_window(self):
        s = InMemoryMetricsStore()

        s.add_metrics_point({"m1": 1}, timestamp=1)
        s.add_metrics_point({"m1": 2}, timestamp=2)

        assert (
            s.window_average("m1", window_start_timestamp_s=0, do_compact=False) == 1.5
        )
        s.window_average("m1", window_start_timestamp_s=1.1, do_compact=True)
        # First record should be removed.
        assert s.window_average("m1", window_start_timestamp_s=0, do_compact=False) == 2

    def test_compaction_max(self):
        s = InMemoryMetricsStore()

        s.add_metrics_point({"m1": 1}, timestamp=2)
        s.add_metrics_point({"m1": 2}, timestamp=1)

        assert s.max("m1", window_start_timestamp_s=0, do_compact=False) == 2

        s.window_average("m1", window_start_timestamp_s=1.1, do_compact=True)

        assert s.window_average("m1", window_start_timestamp_s=0, do_compact=False) == 1

    def test_multiple_metrics(self):
        s = InMemoryMetricsStore()
        s.add_metrics_point({"m1": 1, "m2": -1}, timestamp=1)
        s.add_metrics_point({"m1": 2, "m2": -2}, timestamp=2)
        assert s.window_average("m1", window_start_timestamp_s=0) == 1.5
        assert s.max("m1", window_start_timestamp_s=0) == 2
        assert s.max("m2", window_start_timestamp_s=0) == -1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
