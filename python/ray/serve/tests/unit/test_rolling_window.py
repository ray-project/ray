import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from ray.serve._private.rolling_window_accumulator import (
    RollingWindowAccumulator,
    RollingWindowMax,
    _RollingWindowBase,
)


class TestRollingWindowBaseInit:
    def test_basic_initialization(self):
        """Test basic initialization with default parameters."""
        base = _RollingWindowBase(window_duration_s=10.0)
        assert base.window_duration_s == 10.0
        assert base.num_buckets == 60  # default
        assert base.bucket_duration_s == 10.0 / 60

    def test_custom_num_buckets(self):
        """Test initialization with custom number of buckets."""
        base = _RollingWindowBase(
            window_duration_s=100.0,
            num_buckets=10,
        )
        assert base.window_duration_s == 100.0
        assert base.num_buckets == 10
        assert base.bucket_duration_s == 10.0

    def test_invalid_window_duration(self):
        """Test that invalid window duration raises ValueError."""
        with pytest.raises(ValueError, match="window_duration_s must be positive"):
            _RollingWindowBase(window_duration_s=0)

        with pytest.raises(ValueError, match="window_duration_s must be positive"):
            _RollingWindowBase(window_duration_s=-1.0)

    def test_invalid_num_buckets(self):
        """Test that invalid num_buckets raises ValueError."""
        with pytest.raises(ValueError, match="num_buckets must be positive"):
            _RollingWindowBase(window_duration_s=10.0, num_buckets=0)

        with pytest.raises(ValueError, match="num_buckets must be positive"):
            _RollingWindowBase(window_duration_s=10.0, num_buckets=-1)


class TestRollingWindowAccumulatorSingleThread:
    def test_basic_add_and_get_total(self):
        """Test basic add and get_total functionality."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        # Add some values
        accumulator.add(100.0)
        accumulator.add(50.0)
        accumulator.add(25.0)

        # Total should be sum of all added values
        assert accumulator.get_total() == 175.0

    def test_add_zero(self):
        """Test that zero values are handled correctly."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        accumulator.add(0.0)
        accumulator.add(0.0)
        assert accumulator.get_total() == 0.0

    def test_add_negative(self):
        """Test that negative values are handled correctly."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        accumulator.add(100.0)
        accumulator.add(-50.0)
        assert accumulator.get_total() == 50.0

    def test_add_float_precision(self):
        """Test float precision with small values."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        for _ in range(1000):
            accumulator.add(0.001)

        # Allow for small floating point errors
        assert abs(accumulator.get_total() - 1.0) < 0.0001

    def test_empty_accumulator(self):
        """Test get_total on empty accumulator."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        assert accumulator.get_total() == 0.0

    def test_bucket_rotation(self):
        """Test that buckets rotate correctly as time passes."""
        with patch("time.time") as mock_time:
            # Start at time 0
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=10.0,  # 10 second window
                num_buckets=10,  # Each bucket is 1 second
            )

            # Add 100 to first bucket
            accumulator.add(100.0)
            assert accumulator.get_total() == 100.0

            # Advance time by 1 second (move to next bucket)
            mock_time.return_value = 1.0
            accumulator.add(200.0)
            assert accumulator.get_total() == 300.0

            # Advance time by 9 more seconds (10 total - first bucket should expire)
            mock_time.return_value = 10.0
            accumulator.add(50.0)
            # First bucket (100) should be cleared, only 200 + 50 remain
            assert accumulator.get_total() == 250.0

    def test_full_window_idle(self):
        """Test that all buckets are cleared after being idle for full window."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=10.0,
                num_buckets=10,
            )

            # Add value at time 0
            accumulator.add(100.0)
            assert accumulator.get_total() == 100.0

            # Advance past the full window (all data should be stale)
            mock_time.return_value = 15.0
            assert accumulator.get_total() == 0.0

    def test_concurrent_adds_same_bucket(self):
        """Test that multiple adds within same bucket period are accumulated."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=10.0,
                num_buckets=10,
            )

            # Add multiple values without time advancing
            for _ in range(100):
                accumulator.add(1.0)

            assert accumulator.get_total() == 100.0

    def test_gradual_data_expiry(self):
        """Test that data gradually expires as the window slides."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=10.0,
                num_buckets=10,  # 1 second per bucket
            )

            # Add 10 values, one per second
            for i in range(10):
                mock_time.return_value = float(i)
                accumulator.add(10.0)

            # Total should be 100
            mock_time.return_value = 9.0
            assert accumulator.get_total() == 100.0

            # After 1 more second, the first bucket expires
            mock_time.return_value = 10.0
            # Need to trigger rotation by calling get_total or add
            total = accumulator.get_total()
            assert total == 90.0

            # After 5 more seconds, 6 buckets have expired
            mock_time.return_value = 15.0
            total = accumulator.get_total()
            assert total == 40.0

    def test_large_time_jump(self):
        """Test handling of large time jumps (e.g., system clock changes)."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=10.0,
                num_buckets=10,
            )

            accumulator.add(100.0)
            assert accumulator.get_total() == 100.0

            # Jump forward by a very large amount
            mock_time.return_value = 1000000.0
            assert accumulator.get_total() == 0.0

            # Should still work after the jump
            accumulator.add(50.0)
            assert accumulator.get_total() == 50.0


class TestRollingWindowAccumulatorMultiThread:
    def test_thread_registration(self):
        """Test that threads are correctly registered."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        # Initially no threads registered
        assert accumulator.get_num_registered_threads() == 0

        # After first add, one thread registered
        accumulator.add(100.0)
        assert accumulator.get_num_registered_threads() == 1

        # Multiple adds from same thread don't increase count
        accumulator.add(100.0)
        assert accumulator.get_num_registered_threads() == 1

    def test_multiple_threads_registration(self):
        """Test that multiple threads are correctly registered."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        num_threads = 8
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()  # Synchronize all threads
            accumulator.add(1.0)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert accumulator.get_num_registered_threads() == num_threads
        assert accumulator.get_total() == num_threads

    def test_concurrent_adds_correctness(self):
        """Test that concurrent adds produce correct total."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=600.0,  # Large window to avoid expiry
            num_buckets=60,
        )

        num_threads = 8
        adds_per_thread = 1000
        value_per_add = 1.0

        def worker():
            for _ in range(adds_per_thread):
                accumulator.add(value_per_add)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected = num_threads * adds_per_thread * value_per_add
        assert accumulator.get_total() == expected

    def test_concurrent_adds_with_threadpool(self):
        """Test concurrent adds using ThreadPoolExecutor."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=600.0,
            num_buckets=60,
        )

        num_workers = 16
        adds_per_worker = 500

        def worker():
            for _ in range(adds_per_worker):
                accumulator.add(1.0)
            return adds_per_worker

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker) for _ in range(num_workers)]
            total_adds = sum(f.result() for f in futures)

        assert accumulator.get_total() == total_adds

    def test_add_and_get_total_concurrent(self):
        """Test concurrent add() and get_total() calls."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=600.0,
            num_buckets=60,
        )

        num_threads = 4
        iterations = 1000
        results = []
        lock = threading.Lock()

        def adder():
            for _ in range(iterations):
                accumulator.add(1.0)

        def reader():
            totals = []
            for _ in range(iterations):
                totals.append(accumulator.get_total())
            with lock:
                results.extend(totals)

        threads = []
        for _ in range(num_threads):
            threads.append(threading.Thread(target=adder))
            threads.append(threading.Thread(target=reader))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Final total should be exactly num_threads * iterations
        expected = num_threads * iterations
        assert accumulator.get_total() == expected

        # All read totals should be non-negative and <= expected
        assert all(0 <= r <= expected for r in results)

    def test_thread_isolation_independent_values(self):
        """Test that each thread has its own independent bucket storage.

        This verifies that values added by one thread don't overwrite
        values added by another thread.
        """
        accumulator = RollingWindowAccumulator(
            window_duration_s=600.0,
            num_buckets=60,
        )

        num_threads = 4
        value_per_thread = [100.0, 200.0, 300.0, 400.0]
        barrier = threading.Barrier(num_threads)

        def worker(thread_idx):
            barrier.wait()  # Synchronize start
            # Each thread adds its unique value multiple times
            for _ in range(10):
                accumulator.add(value_per_thread[thread_idx])

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Total should be sum of all threads' contributions
        expected = sum(v * 10 for v in value_per_thread)
        assert accumulator.get_total() == expected
        assert accumulator.get_num_registered_threads() == num_threads

    def test_thread_isolation_bucket_rotation(self):
        """Test that bucket rotation in one thread doesn't affect other threads.

        Each thread should maintain its own bucket index and rotation time.
        """
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=10.0,
                num_buckets=10,  # 1 second per bucket
            )

            results = {}
            lock = threading.Lock()

            def thread_a():
                # Thread A adds at time 0
                accumulator.add(100.0)
                with lock:
                    results["a_added"] = True

            def thread_b():
                # Wait for thread A to finish
                while "a_added" not in results:
                    pass
                # Thread B adds at time 5 (different bucket)
                mock_time.return_value = 5.0
                accumulator.add(200.0)
                with lock:
                    results["b_added"] = True

            t_a = threading.Thread(target=thread_a)
            t_b = threading.Thread(target=thread_b)

            t_a.start()
            t_a.join()
            t_b.start()
            t_b.join()

            # Both threads should be registered
            assert accumulator.get_num_registered_threads() == 2

            # At time 5, thread A's value (added at time 0) should still be valid
            # because the window is 10 seconds
            mock_time.return_value = 5.0
            assert accumulator.get_total() == 300.0

            # At time 12, thread A's value should have expired (added at time 0,
            # window is 10s), but thread B's value (added at time 5) should still
            # be valid
            mock_time.return_value = 12.0
            total = accumulator.get_total()
            assert total == 200.0

    def test_thread_local_data_not_shared(self):
        """Test that thread-local data objects are truly separate.

        Verifies that accessing _local.data from different threads returns
        different objects.
        """
        accumulator = RollingWindowAccumulator(
            window_duration_s=600.0,
            num_buckets=60,
        )

        data_ids = []
        lock = threading.Lock()

        def worker():
            accumulator.add(1.0)
            # Get the id of this thread's data object
            data = accumulator._local.data
            with lock:
                data_ids.append(id(data))

        num_threads = 4
        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All threads should have different data object IDs
        assert len(set(data_ids)) == num_threads, (
            f"Expected {num_threads} unique data objects, "
            f"got {len(set(data_ids))}: {data_ids}"
        )


class TestUtilizationCalculation:
    def test_utilization_formula(self):
        """Test that utilization is calculated correctly.

        Utilization = user_code_time / (window_duration * max_ongoing_requests)
        """
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=600.0,  # 10 minutes
                num_buckets=60,
            )

            # Simulate 1 request taking 60 seconds (60000 ms) over a 10 minute window
            # with max_ongoing_requests=1
            # Utilization = 60000 / (600 * 1000 * 1) = 10%
            accumulator.add(60000.0)  # 60 seconds in ms

            total_user_code_time_ms = accumulator.get_total()
            window_duration_ms = 600 * 1000
            max_ongoing_requests = 1
            max_capacity_ms = window_duration_ms * max_ongoing_requests

            utilization_percent = (total_user_code_time_ms / max_capacity_ms) * 100
            assert utilization_percent == 10.0

    def test_utilization_with_multiple_concurrent_requests(self):
        """Test utilization calculation with multiple concurrent requests."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=600.0,  # 10 minutes
                num_buckets=60,
            )

            # If max_ongoing_requests=4 and we use 240 seconds of user code time
            # Utilization = 240000 / (600000 * 4) = 10%
            accumulator.add(240000.0)  # 240 seconds in ms

            total_user_code_time_ms = accumulator.get_total()
            window_duration_ms = 600 * 1000
            max_ongoing_requests = 4
            max_capacity_ms = window_duration_ms * max_ongoing_requests

            utilization_percent = (total_user_code_time_ms / max_capacity_ms) * 100
            assert utilization_percent == 10.0

    def test_full_utilization(self):
        """Test 100% utilization scenario."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=600.0,
                num_buckets=60,
            )

            # Full utilization: 600 seconds * 2 concurrent requests = 1200 seconds
            # = 1200000 ms of user code time
            max_ongoing_requests = 2
            accumulator.add(1200000.0)

            total_user_code_time_ms = accumulator.get_total()
            window_duration_ms = 600 * 1000
            max_capacity_ms = window_duration_ms * max_ongoing_requests

            utilization_percent = (total_user_code_time_ms / max_capacity_ms) * 100
            assert utilization_percent == 100.0

    def test_utilization_capped_at_100(self):
        """Test that utilization is capped at 100% even if calculation exceeds it."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=600.0,
                num_buckets=60,
            )

            # Over-utilization scenario (shouldn't happen in practice but test the cap)
            max_ongoing_requests = 1
            accumulator.add(700000.0)  # More than 600 seconds

            total_user_code_time_ms = accumulator.get_total()
            window_duration_ms = 600 * 1000
            max_capacity_ms = window_duration_ms * max_ongoing_requests

            utilization_percent = (total_user_code_time_ms / max_capacity_ms) * 100
            utilization_percent = min(utilization_percent, 100.0)  # Cap at 100%
            assert utilization_percent == 100.0


class TestEdgeCases:
    def test_very_small_window(self):
        """Test with a very small window duration."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=0.001,  # 1 millisecond
            num_buckets=10,
        )

        accumulator.add(1.0)
        # Value should be present immediately
        assert accumulator.get_total() >= 0.0

    def test_very_large_window(self):
        """Test with a very large window duration."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=86400.0,  # 24 hours
            num_buckets=1440,  # 1 minute per bucket
        )

        accumulator.add(1000.0)
        assert accumulator.get_total() == 1000.0

    def test_single_bucket(self):
        """Test with a single bucket."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            accumulator = RollingWindowAccumulator(
                window_duration_s=10.0,
                num_buckets=1,
            )

            accumulator.add(100.0)
            assert accumulator.get_total() == 100.0

            # After window expires, everything is cleared
            mock_time.return_value = 15.0
            assert accumulator.get_total() == 0.0

    def test_many_buckets(self):
        """Test with many buckets."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=1000,
        )

        for _ in range(100):
            accumulator.add(1.0)

        assert accumulator.get_total() == 100.0

    def test_very_large_values(self):
        """Test with very large values."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        large_value = 1e15
        accumulator.add(large_value)
        accumulator.add(large_value)

        assert accumulator.get_total() == 2 * large_value

    def test_very_small_values(self):
        """Test with very small values."""
        accumulator = RollingWindowAccumulator(
            window_duration_s=10.0,
            num_buckets=10,
        )

        small_value = 1e-15
        for _ in range(1000):
            accumulator.add(small_value)

        # Should be approximately 1000 * 1e-15 = 1e-12
        assert abs(accumulator.get_total() - 1e-12) < 1e-14


class TestRollingWindowMaxSingleThread:
    def test_basic_add_and_get_max(self):
        """Test that get_max returns the largest value added."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        tracker.add(100.0)
        tracker.add(500.0)
        tracker.add(50.0)

        assert tracker.get_max() == 500.0

    def test_single_value(self):
        """Test get_max with a single value."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        tracker.add(42.0)
        assert tracker.get_max() == 42.0

    def test_empty_tracker(self):
        """Test get_max on empty tracker returns 0."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)
        assert tracker.get_max() == 0.0

    def test_all_same_values(self):
        """Test get_max when all values are identical."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        for _ in range(100):
            tracker.add(42.0)

        assert tracker.get_max() == 42.0

    def test_increasing_values(self):
        """Test that max tracks the latest highest value."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        for i in range(1, 11):
            tracker.add(float(i * 100))

        assert tracker.get_max() == 1000.0

    def test_decreasing_values(self):
        """Test that max retains the first (highest) value."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        for i in range(10, 0, -1):
            tracker.add(float(i * 100))

        assert tracker.get_max() == 1000.0

    def test_max_across_buckets(self):
        """Test that get_max returns max across different time buckets."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=10.0,
                num_buckets=10,
            )

            # Bucket 0: max 100
            tracker.add(100.0)
            tracker.add(50.0)

            # Bucket 1: max 500
            mock_time.return_value = 1.0
            tracker.add(500.0)
            tracker.add(200.0)

            # Bucket 2: max 30
            mock_time.return_value = 2.0
            tracker.add(30.0)

            assert tracker.get_max() == 500.0

    def test_max_expires_with_window(self):
        """Test that the max value expires when its bucket rotates out."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=10.0,
                num_buckets=10,
            )

            # Add a high value in bucket 0
            tracker.add(999.0)
            assert tracker.get_max() == 999.0

            # Add a lower value in bucket 1
            mock_time.return_value = 1.0
            tracker.add(50.0)
            assert tracker.get_max() == 999.0

            # Advance past the window so bucket 0 expires
            mock_time.return_value = 10.0
            tracker.add(50.0)
            # The 999 from bucket 0 should be gone
            assert tracker.get_max() == 50.0

    def test_full_window_idle(self):
        """Test that get_max returns 0 after being idle for full window."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=10.0,
                num_buckets=10,
            )

            tracker.add(999.0)
            assert tracker.get_max() == 999.0

            # Advance past the full window
            mock_time.return_value = 15.0
            assert tracker.get_max() == 0.0

    def test_gradual_max_expiry(self):
        """Test that the max shifts as old buckets expire."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=10.0,
                num_buckets=10,
            )

            # Bucket 0 (t=0): max=1000
            tracker.add(1000.0)

            # Bucket 3 (t=3): max=500
            mock_time.return_value = 3.0
            tracker.add(500.0)

            # Bucket 7 (t=7): max=200
            mock_time.return_value = 7.0
            tracker.add(200.0)

            # At t=7, all buckets are valid
            assert tracker.get_max() == 1000.0

            # At t=10, bucket 0 (with 1000) expires
            mock_time.return_value = 10.0
            tracker.add(10.0)
            assert tracker.get_max() == 500.0

            # At t=13, bucket 3 (with 500) expires
            mock_time.return_value = 13.0
            tracker.add(10.0)
            assert tracker.get_max() == 200.0

            # At t=17, bucket 7 (with 200) expires
            mock_time.return_value = 17.0
            tracker.add(10.0)
            assert tracker.get_max() == 10.0

    def test_large_time_jump(self):
        """Test handling of large time jumps."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=10.0,
                num_buckets=10,
            )

            tracker.add(999.0)
            assert tracker.get_max() == 999.0

            # Jump forward by a very large amount
            mock_time.return_value = 1000000.0
            assert tracker.get_max() == 0.0

            # Should still work after the jump
            tracker.add(42.0)
            assert tracker.get_max() == 42.0

    def test_max_does_not_accumulate(self):
        """Test that add() takes max, not sum, within a bucket."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        tracker.add(100.0)
        tracker.add(100.0)
        tracker.add(100.0)

        # Should be 100 (max), not 300 (sum)
        assert tracker.get_max() == 100.0


class TestRollingWindowMaxMultiThread:
    def test_thread_registration(self):
        """Test that threads are correctly registered."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        assert tracker.get_num_registered_threads() == 0

        tracker.add(100.0)
        assert tracker.get_num_registered_threads() == 1

        tracker.add(200.0)
        assert tracker.get_num_registered_threads() == 1

    def test_multiple_threads_registration(self):
        """Test that multiple threads are correctly registered."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        num_threads = 8
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            tracker.add(1.0)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert tracker.get_num_registered_threads() == num_threads

    def test_max_across_threads(self):
        """Test that get_max returns the global max across all threads."""
        tracker = RollingWindowMax(
            window_duration_s=600.0,
            num_buckets=60,
        )

        num_threads = 8
        barrier = threading.Barrier(num_threads)

        def worker(thread_idx):
            barrier.wait()
            tracker.add(float((thread_idx + 1) * 100))

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert tracker.get_max() == float(num_threads * 100)

    def test_concurrent_adds_with_threadpool(self):
        """Test concurrent adds using ThreadPoolExecutor."""
        tracker = RollingWindowMax(
            window_duration_s=600.0,
            num_buckets=60,
        )

        num_workers = 16
        adds_per_worker = 500

        def worker(worker_idx):
            for i in range(adds_per_worker):
                tracker.add(float(worker_idx * adds_per_worker + i))

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(worker, i) for i in range(num_workers)]
            for f in futures:
                f.result()

        expected_max = float(num_workers * adds_per_worker - 1)
        assert tracker.get_max() == expected_max

    def test_add_and_get_max_concurrent(self):
        """Test concurrent add() and get_max() calls."""
        tracker = RollingWindowMax(
            window_duration_s=600.0,
            num_buckets=60,
        )

        num_threads = 4
        iterations = 1000
        max_value = float(num_threads * iterations)
        results = []
        lock = threading.Lock()

        def adder(thread_idx):
            for i in range(iterations):
                tracker.add(float(thread_idx * iterations + i))

        def reader():
            maxes = []
            for _ in range(iterations):
                maxes.append(tracker.get_max())
            with lock:
                results.extend(maxes)

        threads = []
        for i in range(num_threads):
            threads.append(threading.Thread(target=adder, args=(i,)))
            threads.append(threading.Thread(target=reader))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert tracker.get_max() == max_value - 1

        # All read values should be non-negative and <= max_value
        assert all(0 <= r <= max_value for r in results)

    def test_thread_isolation_with_expiry(self):
        """Test that bucket expiry works correctly across threads."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=10.0,
                num_buckets=10,
            )

            results = {}
            lock = threading.Lock()

            def thread_a():
                # Thread A adds high value at time 0
                tracker.add(1000.0)
                with lock:
                    results["a_added"] = True

            def thread_b():
                while "a_added" not in results:
                    pass
                # Thread B adds lower value at time 5
                mock_time.return_value = 5.0
                tracker.add(200.0)
                with lock:
                    results["b_added"] = True

            t_a = threading.Thread(target=thread_a)
            t_b = threading.Thread(target=thread_b)

            t_a.start()
            t_a.join()
            t_b.start()
            t_b.join()

            assert tracker.get_num_registered_threads() == 2

            # At time 5, thread A's 1000 is still valid
            mock_time.return_value = 5.0
            assert tracker.get_max() == 1000.0

            # At time 12, thread A's data (added at time 0) has expired
            mock_time.return_value = 12.0
            assert tracker.get_max() == 200.0


class TestRollingWindowMaxEdgeCases:
    def test_single_bucket(self):
        """Test with a single bucket."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=10.0,
                num_buckets=1,
            )

            tracker.add(100.0)
            tracker.add(500.0)
            assert tracker.get_max() == 500.0

            # After window expires, everything is cleared
            mock_time.return_value = 15.0
            assert tracker.get_max() == 0.0

    def test_many_buckets(self):
        """Test with many buckets."""
        tracker = RollingWindowMax(
            window_duration_s=10.0,
            num_buckets=1000,
        )

        for i in range(100):
            tracker.add(float(i))

        assert tracker.get_max() == 99.0

    def test_very_large_values(self):
        """Test with very large values."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        large_value = 1e15
        tracker.add(large_value)
        tracker.add(large_value - 1)

        assert tracker.get_max() == large_value

    def test_very_small_values(self):
        """Test with very small positive values."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        tracker.add(1e-15)
        tracker.add(2e-15)
        tracker.add(1e-15)

        assert tracker.get_max() == 2e-15

    def test_zero_values(self):
        """Test that zero values are handled correctly."""
        tracker = RollingWindowMax(window_duration_s=10.0, num_buckets=10)

        tracker.add(0.0)
        tracker.add(0.0)

        assert tracker.get_max() == 0.0

    def test_new_max_replaces_old_after_expiry(self):
        """Test the typical latency tracking scenario: old spike expires,
        new lower steady-state values become the max."""
        with patch("time.time") as mock_time:
            mock_time.return_value = 0.0

            tracker = RollingWindowMax(
                window_duration_s=60.0,
                num_buckets=6,
            )

            # Latency spike at t=0
            tracker.add(5000.0)

            # Normal latencies at t=10, t=20, t=30
            for t in [10.0, 20.0, 30.0]:
                mock_time.return_value = t
                tracker.add(50.0)

            # Spike is still in window
            assert tracker.get_max() == 5000.0

            # At t=60, the spike bucket (t=0) has expired
            mock_time.return_value = 60.0
            tracker.add(50.0)
            assert tracker.get_max() == 50.0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
