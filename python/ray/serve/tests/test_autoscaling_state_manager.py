import asyncio
import time
import unittest

from ray.serve._private.autoscaling_state import AutoscalingStateManager
from ray.serve._private.common import DeploymentID


class TestAutoscalingStateManagerQPS(unittest.TestCase):
    def setUp(self):
        self.manager = AutoscalingStateManager()
        # Allow time for async tasks in InMemoryMetricsStore if any were started
        # (though current implementation is synchronous for add/window_average)

    def test_record_and_get_qps_single_deployment(self):
        dep_id = DeploymentID(name="test-dep")

        current_ts = time.time()
        # Record points: (value, timestamp_offset_from_current)
        points = [
            (10.0, -9.5),  # t-9.5s
            (12.0, -8.5),  # t-8.5s
            (14.0, -7.5),  # t-7.5s
            (16.0, -0.5),  # t-0.5s (out of 5s window from t-10s)
        ]

        for val, ts_offset in points:
            self.manager.record_qps_metric(dep_id, val, current_ts + ts_offset)

        # Window from t-10s to t (current_ts)
        # Expected points in window: (10, t-9.5), (12, t-8.5), (14, t-7.5)
        # (16, t-0.5) is NOT in a window that starts 5 seconds ago from current_ts

        # Test window covering first 3 points
        # Window: [current_ts - 10s, current_ts] for get_average_qps
        # get_average_qps calculates window as [window_start_timestamp_s, now]
        # So, window_start_timestamp_s should be current_ts - 10
        avg_qps = self.manager.get_average_qps(dep_id, current_ts - 10.0)
        self.assertIsNotNone(avg_qps)
        # Expected average: (10+12+14+16)/4 if all points were in the window from InMemoryMetricsStore.
        # However, get_average_qps in AutoscalingStateManager calls
        # self.qps_store.window_average(deployment_id, window_start_timestamp_s, time.time())
        # So the window is [window_start_timestamp_s, current_time_of_call]
        # For this test, let's assume current_time_of_call is very close to current_ts

        # If window is [current_ts - 10.0, current_ts], all 4 points are in.
        # (10+12+14+16)/4 = 52/4 = 13.0
        # This depends on InMemoryMetricsStore's windowing logic.
        # Let's assume window_average is inclusive [start, end].
        # If window_start_timestamp_s = current_ts - 10.0, and end = current_ts (approx)
        # Points included: all four. Average = 13.0
        self.assertAlmostEqual(avg_qps, 13.0, places=5)

        # Test window covering only the last point
        avg_qps_tight_window = self.manager.get_average_qps(dep_id, current_ts - 1.0)
        self.assertIsNotNone(avg_qps_tight_window)
        # Point included: (16, t-0.5). Average = 16.0
        self.assertAlmostEqual(avg_qps_tight_window, 16.0, places=5)

        # Test window covering middle points
        avg_qps_middle_window = self.manager.get_average_qps(dep_id, current_ts - 9.0)
        # Window [current_ts - 9.0, current_ts]
        # Includes (12, t-8.5), (14, t-7.5), (16, t-0.5)
        # Average = (12+14+16)/3 = 42/3 = 14.0
        self.assertIsNotNone(avg_qps_middle_window)
        self.assertAlmostEqual(avg_qps_middle_window, 14.0, places=5)

    def test_record_and_get_qps_multiple_deployments(self):
        dep_id1 = DeploymentID(name="test-dep1")
        dep_id2 = DeploymentID(name="test-dep2")
        current_ts = time.time()

        self.manager.record_qps_metric(dep_id1, 10.0, current_ts - 5.0)
        self.manager.record_qps_metric(dep_id1, 20.0, current_ts - 4.0)

        self.manager.record_qps_metric(dep_id2, 100.0, current_ts - 5.0)
        self.manager.record_qps_metric(dep_id2, 200.0, current_ts - 4.0)

        avg_qps1 = self.manager.get_average_qps(dep_id1, current_ts - 10.0)
        self.assertAlmostEqual(avg_qps1, 15.0, places=5)  # (10+20)/2

        avg_qps2 = self.manager.get_average_qps(dep_id2, current_ts - 10.0)
        self.assertAlmostEqual(avg_qps2, 150.0, places=5)  # (100+200)/2

    def test_get_average_qps_no_data(self):
        dep_id = DeploymentID(name="no-data-dep")
        current_ts = time.time()
        avg_qps = self.manager.get_average_qps(dep_id, current_ts - 10.0)
        self.assertIsNone(avg_qps)

    def test_get_average_qps_data_outside_window(self):
        dep_id = DeploymentID(name="outside-window-dep")
        current_ts = time.time()
        self.manager.record_qps_metric(
            dep_id, 10.0, current_ts - 20.0
        )  # Data from 20s ago

        # Window is last 10s
        avg_qps = self.manager.get_average_qps(dep_id, current_ts - 10.0)
        self.assertIsNone(avg_qps)  # Should be None as data is too old

    def test_get_average_qps_key_error_if_dep_not_in_store(self):
        # This tests if get_average_qps correctly handles when a dep_id was never recorded
        # (InMemoryMetricsStore might raise KeyError if dep_id never had data)
        dep_id_never_seen = DeploymentID(name="never-seen-dep")
        current_ts = time.time()
        avg_qps = self.manager.get_average_qps(dep_id_never_seen, current_ts - 10.0)
        self.assertIsNone(
            avg_qps
        )  # AutoscalingStateManager catches KeyError and returns None

    def test_get_average_qps_value_error_if_no_data_in_window_from_store(self):
        # This tests if get_average_qps correctly handles ValueError from InMemoryMetricsStore
        # (e.g., when deployment exists but has no data points within the specific window)
        dep_id = DeploymentID(name="value-error-dep")
        current_ts = time.time()
        # Record data far in the past
        self.manager.record_qps_metric(dep_id, 100.0, current_ts - 1000.0)

        # Now query a recent window where there's no data
        avg_qps = self.manager.get_average_qps(dep_id, current_ts - 10.0)
        self.assertIsNone(
            avg_qps
        )  # AutoscalingStateManager catches ValueError and returns None

    def test_record_qps_multiple_points_same_timestamp(self):
        dep_id = DeploymentID(name="same-ts-dep")
        ts = time.time() - 5.0  # 5 seconds ago

        # Simulating multiple proxies reporting QPS at roughly the same time
        self.manager.record_qps_metric(dep_id, 10.0, ts)
        self.manager.record_qps_metric(dep_id, 12.0, ts)
        self.manager.record_qps_metric(
            dep_id, 14.0, ts + 0.001
        )  # Slightly different ts

        # Window covers these points
        avg_qps = self.manager.get_average_qps(dep_id, ts - 1.0)
        self.assertIsNotNone(avg_qps)
        # InMemoryMetricsStore stores these as separate points.
        # The average should be (10 + 12 + 14) / 3 = 36 / 3 = 12.0
        self.assertAlmostEqual(avg_qps, 12.0, places=5)


if __name__ == "__main__":
    unittest.main()
