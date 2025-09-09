"""Unit tests for stage cache core functionality without Ray dependencies."""

import sys
import threading
import unittest
from unittest.mock import Mock, patch


class TestStageCacheUnit(unittest.TestCase):
    """Unit tests for StageCache class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the Ray imports to avoid dependency issues
        self.ray_patches = []

        # Mock ray.data.context
        context_patch = patch("ray.data._internal.cache.stage_cache.DataContext")
        self.mock_context_class = context_patch.start()
        self.ray_patches.append(context_patch)

        # Mock LogicalPlan
        plan_patch = patch("ray.data._internal.cache.stage_cache.LogicalPlan")
        self.mock_plan_class = plan_patch.start()
        self.ray_patches.append(plan_patch)

        # Now we can import the stage cache
        from ray.data._internal.cache.stage_cache import StageCache

        self.StageCache = StageCache

    def tearDown(self):
        """Clean up test fixtures."""
        for patch_obj in self.ray_patches:
            patch_obj.stop()

    def test_stage_cache_initialization(self):
        """Test StageCache initialization."""
        cache = self.StageCache()

        # Check that instance variables are initialized
        self.assertIsInstance(cache._cache, dict)
        self.assertIsNotNone(cache._lock)
        self.assertEqual(cache._cache_hits, 0)
        self.assertEqual(cache._cache_misses, 0)

    def test_is_enabled_disabled_by_default(self):
        """Test that cache is disabled by default."""
        cache = self.StageCache()

        # Mock context without enable_stage_cache attribute
        mock_context = Mock()
        self.mock_context_class.get_current.return_value = mock_context

        self.assertFalse(cache._is_enabled())

    def test_is_enabled_respects_context_setting(self):
        """Test that _is_enabled respects context setting."""
        cache = self.StageCache()

        # Test enabled
        mock_context = Mock()
        mock_context.enable_stage_cache = True
        self.mock_context_class.get_current.return_value = mock_context

        self.assertTrue(cache._is_enabled())

        # Test disabled
        mock_context.enable_stage_cache = False
        self.assertFalse(cache._is_enabled())

    def test_is_enabled_handles_exceptions(self):
        """Test that _is_enabled handles exceptions gracefully."""
        cache = self.StageCache()

        # Mock exception when getting context
        self.mock_context_class.get_current.side_effect = Exception("No context")

        self.assertFalse(cache._is_enabled())

    def test_fingerprint_computation(self):
        """Test fingerprint computation."""
        cache = self.StageCache()

        # Mock plan
        mock_plan = Mock()
        mock_plan.__str__ = Mock(return_value="test_plan")

        # Mock context
        mock_context = Mock()
        mock_context.target_max_block_size = 1024
        mock_context.target_min_block_size = 512
        mock_context.use_push_based_shuffle = True
        mock_context.actor_prefetcher_enabled = False
        mock_context.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context

        # Compute fingerprint
        fingerprint = cache._compute_plan_fingerprint(mock_plan)

        # Should be a hex string of length 64 (SHA256)
        self.assertIsInstance(fingerprint, str)
        self.assertEqual(len(fingerprint), 64)
        self.assertTrue(all(c in "0123456789abcdef" for c in fingerprint))

        # Same plan should produce same fingerprint
        fingerprint2 = cache._compute_plan_fingerprint(mock_plan)
        self.assertEqual(fingerprint, fingerprint2)

    def test_fingerprint_different_for_different_plans(self):
        """Test that different plans produce different fingerprints."""
        cache = self.StageCache()

        # Mock different plans
        mock_plan1 = Mock()
        mock_plan1.__str__ = Mock(return_value="plan_1")

        mock_plan2 = Mock()
        mock_plan2.__str__ = Mock(return_value="plan_2")

        # Mock context
        mock_context = Mock()
        mock_context.target_max_block_size = 1024
        mock_context.target_min_block_size = 512
        mock_context.use_push_based_shuffle = True
        mock_context.actor_prefetcher_enabled = False
        mock_context.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context

        fp1 = cache._compute_plan_fingerprint(mock_plan1)
        fp2 = cache._compute_plan_fingerprint(mock_plan2)

        self.assertNotEqual(fp1, fp2)

    def test_fingerprint_changes_with_context(self):
        """Test that fingerprint changes when context changes."""
        cache = self.StageCache()

        mock_plan = Mock()
        mock_plan.__str__ = Mock(return_value="test_plan")

        # First context
        mock_context1 = Mock()
        mock_context1.target_max_block_size = 1024
        mock_context1.target_min_block_size = 512
        mock_context1.use_push_based_shuffle = True
        mock_context1.actor_prefetcher_enabled = False
        mock_context1.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context1

        fp1 = cache._compute_plan_fingerprint(mock_plan)

        # Second context with different settings
        mock_context2 = Mock()
        mock_context2.target_max_block_size = 2048  # Different
        mock_context2.target_min_block_size = 512
        mock_context2.use_push_based_shuffle = True
        mock_context2.actor_prefetcher_enabled = False
        mock_context2.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context2

        fp2 = cache._compute_plan_fingerprint(mock_plan)

        self.assertNotEqual(fp1, fp2)

    def test_cache_operations_when_disabled(self):
        """Test cache operations when disabled."""
        cache = self.StageCache()

        # Mock disabled context
        mock_context = Mock()
        mock_context.enable_stage_cache = False
        self.mock_context_class.get_current.return_value = mock_context

        mock_plan = Mock()

        # Get should return None
        result = cache.get(mock_plan)
        self.assertIsNone(result)

        # Put should be no-op
        cache.put(mock_plan, "test_result")
        result = cache.get(mock_plan)
        self.assertIsNone(result)

    def test_cache_operations_when_enabled(self):
        """Test cache operations when enabled."""
        cache = self.StageCache()

        # Mock enabled context
        mock_context = Mock()
        mock_context.enable_stage_cache = True
        mock_context.target_max_block_size = 1024
        mock_context.target_min_block_size = 512
        mock_context.use_push_based_shuffle = True
        mock_context.actor_prefetcher_enabled = False
        mock_context.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context

        mock_plan = Mock()
        mock_plan.__str__ = Mock(return_value="test_plan")

        # Initially should be None (miss)
        result = cache.get(mock_plan)
        self.assertIsNone(result)

        # Put something
        cache.put(mock_plan, "cached_result")

        # Should now return cached result (hit)
        result = cache.get(mock_plan)
        self.assertEqual(result, "cached_result")

    def test_cache_stats(self):
        """Test cache statistics."""
        cache = self.StageCache()

        # Mock enabled context
        mock_context = Mock()
        mock_context.enable_stage_cache = True
        mock_context.target_max_block_size = 1024
        mock_context.target_min_block_size = 512
        mock_context.use_push_based_shuffle = True
        mock_context.actor_prefetcher_enabled = False
        mock_context.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context

        mock_plan = Mock()
        mock_plan.__str__ = Mock(return_value="test_plan")

        # Initial stats
        stats = cache.get_stats()
        self.assertEqual(stats["hits"], 0)
        self.assertEqual(stats["misses"], 0)
        self.assertEqual(stats["hit_rate"], 0.0)
        self.assertEqual(stats["cache_size"], 0)

        # Generate a miss
        cache.get(mock_plan)

        stats = cache.get_stats()
        self.assertEqual(stats["hits"], 0)
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hit_rate"], 0.0)

        # Add to cache and generate a hit
        cache.put(mock_plan, "result")
        cache.get(mock_plan)

        stats = cache.get_stats()
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hit_rate"], 0.5)
        self.assertEqual(stats["cache_size"], 1)

    def test_cache_clear(self):
        """Test cache clearing."""
        cache = self.StageCache()

        # Mock enabled context
        mock_context = Mock()
        mock_context.enable_stage_cache = True
        mock_context.target_max_block_size = 1024
        mock_context.target_min_block_size = 512
        mock_context.use_push_based_shuffle = True
        mock_context.actor_prefetcher_enabled = False
        mock_context.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context

        mock_plan = Mock()
        mock_plan.__str__ = Mock(return_value="test_plan")

        # Add to cache
        cache.put(mock_plan, "result")
        self.assertEqual(cache.get(mock_plan), "result")

        # Clear cache
        cache.clear()

        # Should be empty
        self.assertIsNone(cache.get(mock_plan))

        # Stats should be reset
        stats = cache.get_stats()
        self.assertEqual(stats["hits"], 0)
        self.assertEqual(stats["misses"], 0)
        self.assertEqual(stats["cache_size"], 0)

    def test_thread_safety(self):
        """Test basic thread safety."""
        cache = self.StageCache()

        # Mock enabled context
        mock_context = Mock()
        mock_context.enable_stage_cache = True
        mock_context.target_max_block_size = 1024
        mock_context.target_min_block_size = 512
        mock_context.use_push_based_shuffle = True
        mock_context.actor_prefetcher_enabled = False
        mock_context.max_num_blocks_in_streaming_gen_buffer = 10
        self.mock_context_class.get_current.return_value = mock_context

        results = []
        errors = []

        def worker(thread_id):
            try:
                mock_plan = Mock()
                mock_plan.__str__ = Mock(return_value=f"plan_{thread_id}")

                cache.put(mock_plan, f"result_{thread_id}")
                result = cache.get(mock_plan)
                results.append((thread_id, result))
            except Exception as e:
                errors.append((thread_id, str(e)))

        # Run concurrent operations
        threads = []
        for i in range(10):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should have no errors
        self.assertEqual(len(errors), 0, f"Thread safety errors: {errors}")
        self.assertEqual(len(results), 10)

        # Each thread should get its own result
        for thread_id, result in results:
            self.assertEqual(result, f"result_{thread_id}")


class TestGlobalFunctions(unittest.TestCase):
    """Test global cache functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the Ray imports
        self.ray_patches = []

        context_patch = patch("ray.data._internal.cache.stage_cache.DataContext")
        self.mock_context_class = context_patch.start()
        self.ray_patches.append(context_patch)

        plan_patch = patch("ray.data._internal.cache.stage_cache.LogicalPlan")
        self.mock_plan_class = plan_patch.start()
        self.ray_patches.append(plan_patch)

    def tearDown(self):
        """Clean up test fixtures."""
        for patch_obj in self.ray_patches:
            patch_obj.stop()

    def test_global_functions_exist(self):
        """Test that global functions exist and are callable."""
        from ray.data._internal.cache.stage_cache import (
            cache_stage_result,
            clear_stage_cache,
            get_cached_stage_result,
            get_stage_cache,
            get_stage_cache_stats,
        )

        # Test functions exist
        self.assertTrue(callable(cache_stage_result))
        self.assertTrue(callable(clear_stage_cache))
        self.assertTrue(callable(get_cached_stage_result))
        self.assertTrue(callable(get_stage_cache))
        self.assertTrue(callable(get_stage_cache_stats))

    def test_singleton_behavior(self):
        """Test that get_stage_cache returns the same instance."""
        from ray.data._internal.cache.stage_cache import get_stage_cache

        cache1 = get_stage_cache()
        cache2 = get_stage_cache()

        self.assertIs(cache1, cache2)

    def test_global_stats(self):
        """Test global stats function."""
        from ray.data._internal.cache.stage_cache import get_stage_cache_stats

        stats = get_stage_cache_stats()

        self.assertIsInstance(stats, dict)
        self.assertIn("hits", stats)
        self.assertIn("misses", stats)
        self.assertIn("hit_rate", stats)
        self.assertIn("cache_size", stats)

    def test_global_clear(self):
        """Test global clear function."""
        from ray.data._internal.cache.stage_cache import clear_stage_cache

        # Should not raise exception
        clear_stage_cache()


def run_unit_tests():
    """Run all unit tests."""
    print("🧪 Running Stage Cache Unit Tests")
    print("=" * 40)

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestStageCacheUnit))
    suite.addTests(loader.loadTestsFromTestCase(TestGlobalFunctions))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 40)
    if result.wasSuccessful():
        print("✅ All unit tests passed!")
        return True
    else:
        print(f"❌ {len(result.failures)} failures, {len(result.errors)} errors")
        return False


if __name__ == "__main__":
    success = run_unit_tests()
    sys.exit(0 if success else 1)
