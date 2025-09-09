"""Stress tests for Ray Data stage caching functionality."""

import gc
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

import ray
from ray.data._internal.cache.stage_cache import (
    clear_stage_cache,
    get_stage_cache_stats,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class TestStageCacheStress:
    """Stress tests for stage cache functionality."""

    def setup_method(self):
        """Set up each test with a clean cache."""
        clear_stage_cache()
        self.original_setting = getattr(
            DataContext.get_current(), "enable_stage_cache", False
        )

    def teardown_method(self):
        """Clean up after each test."""
        DataContext.get_current().enable_stage_cache = self.original_setting
        clear_stage_cache()
        gc.collect()  # Force garbage collection

    def test_many_identical_operations(self, ray_start_regular_shared):
        """Test cache with many identical operations."""
        DataContext.get_current().enable_stage_cache = True

        # Create many identical datasets
        datasets = []
        for _ in range(20):
            ds = ray.data.range(50).map(lambda x: {"value": x["id"] * 3})
            datasets.append(ds)

        # Materialize all datasets
        results = []
        for ds in datasets:
            mat = ds.materialize()
            results.append(mat.count())

        # All should have same count
        assert all(count == 50 for count in results)

        # Should have many cache hits
        stats = get_stage_cache_stats()
        assert stats["hits"] >= 15  # Most should be cache hits
        print(
            f"Cache performance: {stats['hits']} hits, {stats['misses']} misses, "
            f"{stats['hit_rate']:.2%} hit rate"
        )

    def test_many_different_operations(self, ray_start_regular_shared):
        """Test cache with many different operations (should not interfere)."""
        DataContext.get_current().enable_stage_cache = True

        # Create many different datasets
        results = []
        for i in range(20):
            # Each dataset is different (different range or multiplier)
            if i % 2 == 0:
                ds = ray.data.range(10 + i).map(lambda x: {"value": x["id"] * 2})
            else:
                ds = ray.data.range(50).map(lambda x: {"value": x["id"] * (i + 1)})

            mat = ds.materialize()
            results.append(mat.count())

        # Should have executed all independently
        stats = get_stage_cache_stats()
        assert stats["hits"] == 0  # No cache hits expected for different operations
        assert stats["misses"] >= 20
        print(f"Different operations: {stats['misses']} misses (expected)")

    def test_concurrent_identical_operations(self, ray_start_regular_shared):
        """Test concurrent access to cache with identical operations."""
        DataContext.get_current().enable_stage_cache = True

        def create_and_materialize(thread_id):
            """Create and materialize identical dataset."""
            try:
                ds = ray.data.range(30).map(lambda x: {"thread_safe": x["id"] * 5})
                mat = ds.materialize()
                return thread_id, mat.count(), None
            except Exception as e:
                return thread_id, None, str(e)

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(create_and_materialize, i) for i in range(15)]
            results = [future.result() for future in as_completed(futures)]

        # Check results
        errors = [error for _, _, error in results if error]
        counts = [count for _, count, error in results if not error]

        assert len(errors) == 0, f"Errors in concurrent execution: {errors}"
        assert all(count == 30 for count in counts), f"Inconsistent counts: {counts}"

        # Should have good cache performance
        stats = get_stage_cache_stats()
        assert stats["hits"] >= 10  # Most should be cache hits
        print(f"Concurrent performance: {stats['hits']} hits, {stats['misses']} misses")

    def test_concurrent_mixed_operations(self, ray_start_regular_shared):
        """Test concurrent access with mix of identical and different operations."""
        DataContext.get_current().enable_stage_cache = True

        def create_and_materialize(operation_type, thread_id):
            """Create and materialize dataset based on operation type."""
            try:
                if operation_type == "identical":
                    ds = ray.data.range(25).map(lambda x: {"shared": x["id"] * 4})
                elif operation_type == "unique":
                    ds = ray.data.range(25).map(
                        lambda x: {"unique": x["id"] * thread_id}
                    )
                else:  # "similar"
                    ds = ray.data.range(25).map(lambda x: {"similar": x["id"] * 4})

                mat = ds.materialize()
                return thread_id, operation_type, mat.count(), None
            except Exception as e:
                return thread_id, operation_type, None, str(e)

        # Mix of operation types
        operations = (
            [("identical", i) for i in range(8)]  # 8 identical operations
            + [("unique", i) for i in range(8, 12)]  # 4 unique operations
            + [("similar", i) for i in range(12, 16)]  # 4 similar operations
        )

        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(create_and_materialize, op_type, tid)
                for op_type, tid in operations
            ]
            results = [future.result() for future in as_completed(futures)]

        # Analyze results
        errors = [error for _, _, _, error in results if error]
        identical_results = [
            (tid, count)
            for tid, op_type, count, error in results
            if op_type == "identical" and not error
        ]
        unique_results = [
            (tid, count)
            for tid, op_type, count, error in results
            if op_type == "unique" and not error
        ]

        assert len(errors) == 0, f"Errors in mixed concurrent execution: {errors}"
        assert all(count == 25 for _, count in identical_results + unique_results)

        # Should have some cache hits from identical operations
        stats = get_stage_cache_stats()
        assert stats["hits"] >= 4  # At least some cache hits from identical operations
        print(f"Mixed operations: {stats['hits']} hits, {stats['misses']} misses")

    def test_cache_memory_pressure(self, ray_start_regular_shared):
        """Test cache behavior under memory pressure."""
        DataContext.get_current().enable_stage_cache = True

        # Create many different datasets to fill cache
        datasets_and_results = []

        for i in range(50):  # Create 50 different cached entries
            ds = ray.data.range(20).map(lambda x, i=i: {"batch": i, "value": x["id"]})
            mat = ds.materialize()
            datasets_and_results.append((ds, mat))

        # Cache should contain many entries
        stats = get_stage_cache_stats()
        initial_cache_size = stats["cache_size"]
        assert initial_cache_size >= 40  # Most should be cached

        # Create more datasets to potentially trigger cleanup
        for i in range(50, 100):
            ds = ray.data.range(20).map(lambda x, i=i: {"batch": i, "value": x["id"]})
            mat = ds.materialize()

        # Cache should still be functional
        final_stats = get_stage_cache_stats()
        print(
            f"Memory pressure test: initial cache size {initial_cache_size}, "
            f"final cache size {final_stats['cache_size']}"
        )

    def test_rapid_cache_operations(self, ray_start_regular_shared):
        """Test rapid cache operations."""
        DataContext.get_current().enable_stage_cache = True

        # Rapidly create and access cache
        start_time = time.time()

        for i in range(100):
            if i % 10 == 0:  # Every 10th operation is identical (should hit cache)
                ds = ray.data.range(15).map(lambda x: {"rapid": x["id"] * 7})
            else:  # Others are unique
                ds = ray.data.range(15).map(lambda x, i=i: {"rapid": x["id"] * i})

            mat = ds.materialize()
            assert mat.count() == 15

        duration = time.time() - start_time
        stats = get_stage_cache_stats()

        print(
            f"Rapid operations: {100} operations in {duration:.2f}s, "
            f"{stats['hits']} hits, {stats['misses']} misses"
        )

        # Should have some cache hits from identical operations
        assert stats["hits"] >= 5

    def test_cache_with_failures(self, ray_start_regular_shared):
        """Test cache behavior when some operations fail."""
        DataContext.get_current().enable_stage_cache = True

        successful_operations = 0
        failed_operations = 0

        for i in range(20):
            try:
                if i % 5 == 4:  # Every 5th operation fails
                    # Create an operation that will fail
                    ds = ray.data.range(10).map(lambda x: 1 / 0)  # Division by zero
                    mat = ds.materialize()
                    mat.count()  # This should trigger the error
                else:
                    # Normal successful operation
                    ds = ray.data.range(10).map(lambda x: {"safe": x["id"] * 2})
                    mat = ds.materialize()
                    assert mat.count() == 10
                    successful_operations += 1
            except Exception:
                failed_operations += 1

        # Should have some successful operations and some failures
        assert successful_operations >= 15
        assert failed_operations >= 3

        # Cache should still work for successful operations
        stats = get_stage_cache_stats()
        print(
            f"Failure test: {successful_operations} successful, {failed_operations} failed, "
            f"{stats['hits']} cache hits"
        )

    def test_cache_clear_under_load(self, ray_start_regular_shared):
        """Test cache clearing while operations are running."""
        DataContext.get_current().enable_stage_cache = True

        results = []
        errors = []

        def background_operations():
            """Run background cache operations."""
            for i in range(30):
                try:
                    ds = ray.data.range(10).map(lambda x, i=i: {"bg": x["id"] + i})
                    mat = ds.materialize()
                    results.append(mat.count())
                    time.sleep(0.01)  # Small delay
                except Exception as e:
                    errors.append(str(e))

        # Start background operations
        thread = threading.Thread(target=background_operations)
        thread.start()

        # Clear cache multiple times while operations are running
        for _ in range(5):
            time.sleep(0.05)
            clear_stage_cache()

        # Wait for background operations to complete
        thread.join()

        # Should have no errors despite cache clearing
        assert len(errors) == 0, f"Errors during cache clearing: {errors}"
        assert len(results) == 30
        assert all(count == 10 for count in results)

    def test_large_number_of_cache_entries(self, ray_start_regular_shared):
        """Test cache with large number of entries."""
        DataContext.get_current().enable_stage_cache = True

        # Create many unique cache entries
        num_entries = 200
        results = []

        start_time = time.time()
        for i in range(num_entries):
            # Each dataset is unique
            ds = ray.data.range(5).map(lambda x, i=i: {"entry": i, "value": x["id"]})
            mat = ds.materialize()
            results.append(mat.count())

            # Occasionally test cache retrieval
            if i % 50 == 0 and i > 0:
                # Create identical dataset to test cache hit
                ds_identical = ray.data.range(5).map(
                    lambda x, i=i: {"entry": i, "value": x["id"]}
                )
                mat_identical = ds_identical.materialize()
                assert mat_identical.count() == 5

        duration = time.time() - start_time
        stats = get_stage_cache_stats()

        print(
            f"Large cache test: {num_entries} entries in {duration:.2f}s, "
            f"cache size: {stats['cache_size']}, hits: {stats['hits']}"
        )

        # All operations should have succeeded
        assert all(count == 5 for count in results)

        # Should have reasonable cache size (may not be exactly num_entries due to GC)
        assert stats["cache_size"] >= num_entries // 2


def run_stress_test_suite():
    """Run all stress tests manually (for development/debugging)."""
    print("🔥 Running Stage Cache Stress Tests...")

    test_instance = TestStageCacheStress()
    test_methods = [
        method
        for method in dir(test_instance)
        if method.startswith("test_") and callable(getattr(test_instance, method))
    ]

    passed = 0
    failed = 0

    for method_name in test_methods:
        print(f"\n📋 Running {method_name}...")
        try:
            # Mock ray_start_regular_shared fixture
            class MockFixture:
                pass

            test_instance.setup_method()
            method = getattr(test_instance, method_name)
            method(MockFixture())
            test_instance.teardown_method()

            print(f"✅ {method_name} PASSED")
            passed += 1

        except Exception as e:
            print(f"❌ {method_name} FAILED: {e}")
            failed += 1
            try:
                test_instance.teardown_method()
            except:
                pass

    print(f"\n🏁 Stress Test Results: {passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    # Run stress tests if executed directly
    import sys

    print("Running stage cache stress test suite...")
    try:
        success = run_stress_test_suite()
        if success:
            print("✅ All stress tests passed!")
        else:
            print("❌ Some stress tests failed!")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Stress test suite failed: {e}")
        sys.exit(1)
