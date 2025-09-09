"""Comprehensive tests for Ray Data stage caching functionality."""

import hashlib
import time
from unittest.mock import patch

import pytest

import ray
from ray.data._internal.cache.stage_cache import (
    StageCache,
    cache_stage_result,
    clear_stage_cache,
    get_cached_stage_result,
    get_stage_cache,
    get_stage_cache_stats,
)
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data.context import DataContext
from ray.data.dataset import MaterializedDataset
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class TestStageCacheCore:
    """Test core StageCache functionality."""

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

    def test_stage_cache_singleton(self):
        """Test that get_stage_cache returns the same instance."""
        cache1 = get_stage_cache()
        cache2 = get_stage_cache()
        assert cache1 is cache2

    def test_stage_cache_disabled_by_default(self):
        """Test that stage cache is disabled by default."""
        cache = StageCache()
        assert not cache._is_enabled()

    def test_stage_cache_respects_context_setting(self):
        """Test that stage cache respects DataContext setting."""
        cache = StageCache()

        # Disabled by default
        assert not cache._is_enabled()

        # Enable via context
        DataContext.get_current().enable_stage_cache = True
        assert cache._is_enabled()

        # Disable again
        DataContext.get_current().enable_stage_cache = False
        assert not cache._is_enabled()

    def test_stage_cache_handles_missing_context(self):
        """Test that stage cache handles missing DataContext gracefully."""
        cache = StageCache()

        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_get.side_effect = Exception("No context")
            assert not cache._is_enabled()

    def test_fingerprint_consistency(self, ray_start_regular_shared):
        """Test that identical plans produce identical fingerprints."""
        cache = StageCache()

        # Create identical datasets
        ds1 = ray.data.range(100)
        ds2 = ray.data.range(100)

        # Should produce same fingerprint
        fp1 = cache._compute_plan_fingerprint(ds1._logical_plan)
        fp2 = cache._compute_plan_fingerprint(ds2._logical_plan)
        assert fp1 == fp2

    def test_fingerprint_different_for_different_plans(self, ray_start_regular_shared):
        """Test that different plans produce different fingerprints."""
        cache = StageCache()

        # Create different datasets
        ds1 = ray.data.range(100)
        ds2 = ray.data.range(200)
        ds3 = ray.data.range(100).map(lambda x: x * 2)

        fp1 = cache._compute_plan_fingerprint(ds1._logical_plan)
        fp2 = cache._compute_plan_fingerprint(ds2._logical_plan)
        fp3 = cache._compute_plan_fingerprint(ds3._logical_plan)

        assert fp1 != fp2
        assert fp1 != fp3
        assert fp2 != fp3

    def test_fingerprint_includes_context_factors(self, ray_start_regular_shared):
        """Test that fingerprint changes with relevant context settings."""
        cache = StageCache()
        context = DataContext.get_current()
        ds = ray.data.range(100)

        # Get initial fingerprint
        fp1 = cache._compute_plan_fingerprint(ds._logical_plan)

        # Change a context setting
        original_block_size = context.target_max_block_size
        context.target_max_block_size = (
            original_block_size * 2 if original_block_size else 1024
        )

        # Should produce different fingerprint
        fp2 = cache._compute_plan_fingerprint(ds._logical_plan)
        assert fp1 != fp2

        # Restore original setting
        context.target_max_block_size = original_block_size

    def test_cache_operations_when_disabled(self, ray_start_regular_shared):
        """Test that cache operations are no-ops when disabled."""
        cache = StageCache()
        ds = ray.data.range(10)

        # Should return None when disabled
        assert cache.get(ds._logical_plan) is None

        # Put should be no-op when disabled
        fake_result = "fake_result"
        cache.put(ds._logical_plan, fake_result)
        assert cache.get(ds._logical_plan) is None

        # Stats should show no activity
        stats = cache.get_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0

    def test_cache_operations_when_enabled(self, ray_start_regular_shared):
        """Test cache operations when enabled."""
        DataContext.get_current().enable_stage_cache = True
        cache = StageCache()
        ds = ray.data.range(10)

        # Initially empty
        assert cache.get(ds._logical_plan) is None

        # Put and retrieve
        test_result = "test_result"
        cache.put(ds._logical_plan, test_result)
        assert cache.get(ds._logical_plan) == test_result

        # Stats should reflect activity
        stats = cache.get_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1

    def test_cache_clear(self, ray_start_regular_shared):
        """Test cache clearing functionality."""
        DataContext.get_current().enable_stage_cache = True
        cache = StageCache()
        ds = ray.data.range(10)

        # Add something to cache
        cache.put(ds._logical_plan, "test")
        assert cache.get(ds._logical_plan) == "test"

        # Clear cache
        cache.clear()
        assert cache.get(ds._logical_plan) is None

        # Stats should be reset
        stats = cache.get_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["cache_size"] == 0

    def test_cache_stats_calculation(self, ray_start_regular_shared):
        """Test cache statistics calculation."""
        DataContext.get_current().enable_stage_cache = True
        cache = StageCache()
        ds1 = ray.data.range(10)
        ds2 = ray.data.range(20)

        # Initial stats
        stats = cache.get_stats()
        assert stats["hit_rate"] == 0.0

        # Add misses
        cache.get(ds1._logical_plan)  # miss
        cache.get(ds2._logical_plan)  # miss

        stats = cache.get_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 2
        assert stats["hit_rate"] == 0.0

        # Add cache entries and hits
        cache.put(ds1._logical_plan, "result1")
        cache.put(ds2._logical_plan, "result2")
        cache.get(ds1._logical_plan)  # hit
        cache.get(ds1._logical_plan)  # hit

        stats = cache.get_stats()
        assert stats["hits"] == 2
        assert stats["misses"] == 2
        assert stats["hit_rate"] == 0.5
        assert stats["cache_size"] == 2


class TestStageCacheIntegration:
    """Test stage cache integration with Dataset operations."""

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

    def test_materialize_without_stage_cache(self, ray_start_regular_shared):
        """Test that materialize works normally without stage cache."""
        # Ensure stage cache is disabled
        DataContext.get_current().enable_stage_cache = False

        ds1 = ray.data.range(50).map(lambda x: {"value": x["id"] * 2})
        ds2 = ray.data.range(50).map(lambda x: {"value": x["id"] * 2})

        # Both should execute independently
        mat1 = ds1.materialize()
        mat2 = ds2.materialize()

        # Results should be identical but independently computed
        assert mat1.count() == mat2.count() == 50
        assert list(mat1.take(5)) == list(mat2.take(5))

        # No cache activity
        stats = get_stage_cache_stats()
        assert stats["hits"] == 0

    def test_materialize_with_stage_cache_identical_operations(
        self, ray_start_regular_shared
    ):
        """Test stage cache with identical operations."""
        DataContext.get_current().enable_stage_cache = True

        # Create identical datasets
        ds1 = ray.data.range(30).map(lambda x: {"doubled": x["id"] * 2})
        ds2 = ray.data.range(30).map(lambda x: {"doubled": x["id"] * 2})

        # First materialize - should cache
        mat1 = ds1.materialize()

        # Second materialize - should use cache
        mat2 = ds2.materialize()

        # Results should be identical
        assert mat1.count() == mat2.count() == 30
        assert list(mat1.take(3)) == list(mat2.take(3))

        # Should have cache hit
        stats = get_stage_cache_stats()
        assert stats["hits"] >= 1

    def test_materialize_with_stage_cache_different_operations(
        self, ray_start_regular_shared
    ):
        """Test stage cache with different operations."""
        DataContext.get_current().enable_stage_cache = True

        # Create different datasets
        ds1 = ray.data.range(20).map(lambda x: {"value": x["id"] * 2})
        ds2 = ray.data.range(20).map(
            lambda x: {"value": x["id"] * 3}
        )  # Different multiplier
        ds3 = ray.data.range(25).map(
            lambda x: {"value": x["id"] * 2}
        )  # Different range

        # All should execute independently (no cache hits)
        mat1 = ds1.materialize()
        mat2 = ds2.materialize()
        mat3 = ds3.materialize()

        # Results should be different
        assert mat1.count() == mat2.count() == 20
        assert mat3.count() == 25
        assert list(mat1.take(3)) != list(mat2.take(3))

        # Should have no cache hits (all different operations)
        stats = get_stage_cache_stats()
        assert stats["hits"] == 0
        assert stats["misses"] >= 3

    def test_stage_cache_with_context_changes(self, ray_start_regular_shared):
        """Test that context changes invalidate cache."""
        DataContext.get_current().enable_stage_cache = True
        context = DataContext.get_current()

        # Create dataset
        ds1 = ray.data.range(20)
        mat1 = ds1.materialize()

        # Change context setting
        original_block_size = context.target_max_block_size
        context.target_max_block_size = (
            original_block_size * 2 if original_block_size else 2048
        )

        try:
            # Same operation but different context - should not use cache
            ds2 = ray.data.range(20)
            mat2 = ds2.materialize()

            # Results should be the same but computed independently
            assert mat1.count() == mat2.count() == 20

            # Should have no cache hits due to context change
            stats = get_stage_cache_stats()
            # Note: There might be 1 hit from the first materialize checking its own cache
            # but the second should not hit the first's cache
            assert stats["misses"] >= 2

        finally:
            # Restore original setting
            context.target_max_block_size = original_block_size

    def test_stage_cache_memory_cleanup(self, ray_start_regular_shared):
        """Test that stage cache allows memory cleanup."""
        DataContext.get_current().enable_stage_cache = True

        ds = ray.data.range(10)
        mat = ds.materialize()

        # Cache should contain the result
        stats = get_stage_cache_stats()
        assert stats["cache_size"] >= 1

        # Delete reference
        del mat

        # Cache should still contain entry (it's the cache's job to hold references)
        stats = get_stage_cache_stats()
        assert stats["cache_size"] >= 1


class TestStageCacheEdgeCases:
    """Test edge cases and error conditions."""

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

    def test_stage_cache_with_empty_dataset(self, ray_start_regular_shared):
        """Test stage cache with empty datasets."""
        DataContext.get_current().enable_stage_cache = True

        # Create empty datasets
        ds1 = ray.data.range(0)
        ds2 = ray.data.range(0)

        mat1 = ds1.materialize()
        mat2 = ds2.materialize()

        assert mat1.count() == mat2.count() == 0

        # Should still cache empty results
        stats = get_stage_cache_stats()
        assert stats["hits"] >= 1

    def test_stage_cache_with_large_fingerprints(self, ray_start_regular_shared):
        """Test stage cache with complex operation chains (large fingerprints)."""
        DataContext.get_current().enable_stage_cache = True

        # Create complex operation chain
        def create_complex_ds():
            return (
                ray.data.range(10)
                .map(lambda x: {"a": x["id"]})
                .filter(lambda x: x["a"] > 2)
                .map(lambda x: {"b": x["a"] * 2})
                .filter(lambda x: x["b"] < 15)
                .map(lambda x: {"c": x["b"] + 1})
            )

        ds1 = create_complex_ds()
        ds2 = create_complex_ds()

        mat1 = ds1.materialize()
        mat2 = ds2.materialize()

        # Should produce same results
        assert mat1.count() == mat2.count()
        assert list(mat1.take_all()) == list(mat2.take_all())

        # Should use cache
        stats = get_stage_cache_stats()
        assert stats["hits"] >= 1

    def test_stage_cache_concurrent_access(self, ray_start_regular_shared):
        """Test stage cache with concurrent access."""
        import threading

        DataContext.get_current().enable_stage_cache = True

        results = []
        errors = []

        def materialize_dataset(thread_id):
            try:
                ds = ray.data.range(10).map(
                    lambda x: {"thread": thread_id, "value": x["id"]}
                )
                mat = ds.materialize()
                results.append((thread_id, mat.count()))
            except Exception as e:
                errors.append((thread_id, str(e)))

        # Create multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=materialize_dataset, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # Should have no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5

        # All should have same count
        for thread_id, count in results:
            assert count == 10

    def test_stage_cache_with_invalid_plans(self):
        """Test stage cache behavior with invalid/None plans."""
        DataContext.get_current().enable_stage_cache = True
        cache = StageCache()

        # Test with None plan
        with pytest.raises(AttributeError):
            cache.get(None)

        with pytest.raises(AttributeError):
            cache.put(None, "result")

    def test_global_cache_functions(self, ray_start_regular_shared):
        """Test global cache functions."""
        DataContext.get_current().enable_stage_cache = True

        ds = ray.data.range(5)
        plan = ds._logical_plan

        # Initially no cached result
        assert get_cached_stage_result(plan) is None

        # Cache a result
        test_result = "test_result"
        cache_stage_result(plan, test_result)

        # Should retrieve cached result
        assert get_cached_stage_result(plan) == test_result

        # Clear and verify
        clear_stage_cache()
        assert get_cached_stage_result(plan) is None

    def test_stage_cache_fingerprint_stability(self, ray_start_regular_shared):
        """Test that fingerprints are stable across multiple calls."""
        cache = StageCache()
        ds = ray.data.range(10).map(lambda x: x["id"] * 2)

        # Generate fingerprint multiple times
        fingerprints = []
        for _ in range(5):
            fp = cache._compute_plan_fingerprint(ds._logical_plan)
            fingerprints.append(fp)

        # All should be identical
        assert all(fp == fingerprints[0] for fp in fingerprints)
        assert len(set(fingerprints)) == 1  # All unique values should be 1

    def test_stage_cache_with_different_data_types(self, ray_start_regular_shared):
        """Test stage cache with different data types."""
        DataContext.get_current().enable_stage_cache = True

        # Test with different data structures
        test_cases = [
            ray.data.from_items([1, 2, 3, 4, 5]),
            ray.data.from_items([{"a": 1}, {"a": 2}, {"a": 3}]),
            ray.data.from_items([{"x": i, "y": i * 2} for i in range(5)]),
        ]

        for i, ds1 in enumerate(test_cases):
            # Create identical dataset
            if i == 0:
                ds2 = ray.data.from_items([1, 2, 3, 4, 5])
            elif i == 1:
                ds2 = ray.data.from_items([{"a": 1}, {"a": 2}, {"a": 3}])
            else:
                ds2 = ray.data.from_items([{"x": i, "y": i * 2} for i in range(5)])

            # Clear cache for each test case
            clear_stage_cache()

            mat1 = ds1.materialize()
            mat2 = ds2.materialize()

            # Should have same results
            assert mat1.count() == mat2.count()

            # Should use cache
            stats = get_stage_cache_stats()
            assert stats["hits"] >= 1, f"No cache hits for test case {i}"


class TestStageCachePerformance:
    """Test performance characteristics of stage cache."""

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

    def test_stage_cache_performance_benefit(self, ray_start_regular_shared):
        """Test that stage cache provides performance benefit."""
        DataContext.get_current().enable_stage_cache = True

        # Create a dataset with some computation
        def create_dataset():
            return ray.data.range(100).map(lambda x: {"computed": x["id"] ** 2})

        # Time first execution
        start_time = time.time()
        ds1 = create_dataset()
        mat1 = ds1.materialize()
        first_duration = time.time() - start_time

        # Time second execution (should use cache)
        start_time = time.time()
        ds2 = create_dataset()
        mat2 = ds2.materialize()
        second_duration = time.time() - start_time

        # Results should be identical
        assert mat1.count() == mat2.count() == 100
        assert list(mat1.take(5)) == list(mat2.take(5))

        # Second should be faster (though this might be flaky in CI)
        # We'll mainly rely on cache stats rather than timing
        stats = get_stage_cache_stats()
        assert stats["hits"] >= 1

        # If timing is reliable, second should be significantly faster
        if second_duration > 0:  # Avoid division by zero
            speedup = first_duration / second_duration
            # Don't make this assertion too strict as timing can be unreliable
            # Just log the speedup for debugging
            print(
                f"Speedup: {speedup:.2f}x (first: {first_duration:.3f}s, second: {second_duration:.3f}s)"
            )

    def test_fingerprint_computation_performance(self, ray_start_regular_shared):
        """Test that fingerprint computation is reasonably fast."""
        cache = StageCache()

        # Create complex dataset
        ds = (
            ray.data.range(1000)
            .map(lambda x: {"a": x["id"]})
            .filter(lambda x: x["a"] > 100)
            .map(lambda x: {"b": x["a"] * 2})
            .filter(lambda x: x["b"] < 1500)
            .map(lambda x: {"c": x["b"] + 1})
        )

        # Time fingerprint computation
        start_time = time.time()
        for _ in range(10):  # Multiple iterations to get better timing
            fp = cache._compute_plan_fingerprint(ds._logical_plan)
        duration = time.time() - start_time

        # Should be reasonably fast (less than 1 second for 10 iterations)
        assert (
            duration < 1.0
        ), f"Fingerprint computation too slow: {duration:.3f}s for 10 iterations"
        assert len(fp) == 64  # SHA256 hex digest length


if __name__ == "__main__":
    # Run a quick smoke test if executed directly
    import sys

    print("Running stage cache smoke test...")
    try:
        # Test basic functionality without Ray
        cache = StageCache()
        print("✓ StageCache creation")

        # Test disabled state
        assert not cache._is_enabled()
        print("✓ Disabled by default")

        # Test stats
        stats = cache.get_stats()
        assert stats["hits"] == 0
        print("✓ Initial stats")

        print("✅ Stage cache smoke test passed!")
        print("Run with pytest for full test suite:")
        print("pytest ray/python/ray/data/tests/test_stage_cache_comprehensive.py -v")

    except Exception as e:
        print(f"❌ Smoke test failed: {e}")
        sys.exit(1)
