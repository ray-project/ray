"""Validation script for Ray Data stage cache functionality.

This script performs basic validation that can run without a full Ray installation,
focusing on the core logic and structure of the stage cache implementation.
"""

import hashlib
import sys
import threading
from unittest.mock import Mock, patch


def test_stage_cache_imports():
    """Test that all stage cache modules can be imported."""
    print("🔍 Testing imports...")

    try:
        from ray.data._internal.cache.stage_cache import (
            StageCache,
            cache_stage_result,
            clear_stage_cache,
            get_cached_stage_result,
            get_stage_cache,
            get_stage_cache_stats,
        )

        print("✅ All stage cache imports successful")
        return True
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False


def test_stage_cache_basic_structure():
    """Test basic StageCache structure and methods."""
    print("🔍 Testing StageCache basic structure...")

    try:
        from ray.data._internal.cache.stage_cache import StageCache

        # Test instantiation
        cache = StageCache()

        # Test basic attributes exist
        assert hasattr(cache, "_cache")
        assert hasattr(cache, "_lock")
        assert hasattr(cache, "_cache_hits")
        assert hasattr(cache, "_cache_misses")

        # Test methods exist
        assert hasattr(cache, "_compute_plan_fingerprint")
        assert hasattr(cache, "get")
        assert hasattr(cache, "put")
        assert hasattr(cache, "clear")
        assert hasattr(cache, "get_stats")
        assert hasattr(cache, "_is_enabled")

        print("✅ StageCache structure validation passed")
        return True

    except Exception as e:
        print(f"❌ Structure validation failed: {e}")
        return False


def test_stage_cache_disabled_state():
    """Test that stage cache is disabled by default."""
    print("🔍 Testing disabled state...")

    try:
        from ray.data._internal.cache.stage_cache import StageCache

        cache = StageCache()

        # Mock DataContext to return disabled state
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            mock_context.enable_stage_cache = False
            mock_get.return_value = mock_context

            assert not cache._is_enabled()

        # Test with enabled state
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            mock_context.enable_stage_cache = True
            mock_get.return_value = mock_context

            assert cache._is_enabled()

        # Test with missing attribute (should default to False)
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            del mock_context.enable_stage_cache  # Remove attribute
            mock_get.return_value = mock_context

            assert not cache._is_enabled()

        print("✅ Disabled state validation passed")
        return True

    except Exception as e:
        print(f"❌ Disabled state validation failed: {e}")
        return False


def test_fingerprint_computation():
    """Test fingerprint computation logic."""
    print("🔍 Testing fingerprint computation...")

    try:
        from ray.data._internal.cache.stage_cache import StageCache

        cache = StageCache()

        # Mock a logical plan
        mock_plan1 = Mock()
        mock_plan1.__str__ = Mock(return_value="test_plan_1")

        mock_plan2 = Mock()
        mock_plan2.__str__ = Mock(return_value="test_plan_2")

        # Mock DataContext
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            mock_context.target_max_block_size = 1024
            mock_context.target_min_block_size = 512
            mock_context.use_push_based_shuffle = True
            mock_context.actor_prefetcher_enabled = False
            mock_context.max_num_blocks_in_streaming_gen_buffer = 10
            mock_get.return_value = mock_context

            # Test fingerprint computation
            fp1 = cache._compute_plan_fingerprint(mock_plan1)
            fp2 = cache._compute_plan_fingerprint(mock_plan2)
            fp1_again = cache._compute_plan_fingerprint(mock_plan1)

            # Fingerprints should be hex strings
            assert isinstance(fp1, str)
            assert len(fp1) == 64  # SHA256 hex digest
            assert all(c in "0123456789abcdef" for c in fp1)

            # Same plan should produce same fingerprint
            assert fp1 == fp1_again

            # Different plans should produce different fingerprints
            assert fp1 != fp2

        print("✅ Fingerprint computation validation passed")
        return True

    except Exception as e:
        print(f"❌ Fingerprint computation validation failed: {e}")
        return False


def test_cache_operations_disabled():
    """Test cache operations when disabled."""
    print("🔍 Testing cache operations when disabled...")

    try:
        from ray.data._internal.cache.stage_cache import StageCache

        cache = StageCache()

        # Mock disabled context
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            mock_context.enable_stage_cache = False
            mock_get.return_value = mock_context

            mock_plan = Mock()
            mock_plan.__str__ = Mock(return_value="test_plan")

            # Get should return None when disabled
            result = cache.get(mock_plan)
            assert result is None

            # Put should be no-op when disabled
            cache.put(mock_plan, "test_result")
            result = cache.get(mock_plan)
            assert result is None

            # Stats should show no activity
            stats = cache.get_stats()
            assert stats["hits"] == 0
            assert stats["misses"] == 0
            assert stats["hit_rate"] == 0.0
            assert stats["cache_size"] == 0

        print("✅ Disabled cache operations validation passed")
        return True

    except Exception as e:
        print(f"❌ Disabled cache operations validation failed: {e}")
        return False


def test_cache_operations_enabled():
    """Test cache operations when enabled."""
    print("🔍 Testing cache operations when enabled...")

    try:
        from ray.data._internal.cache.stage_cache import StageCache

        cache = StageCache()

        # Mock enabled context
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            mock_context.enable_stage_cache = True
            mock_context.target_max_block_size = 1024
            mock_context.target_min_block_size = 512
            mock_context.use_push_based_shuffle = True
            mock_context.actor_prefetcher_enabled = False
            mock_context.max_num_blocks_in_streaming_gen_buffer = 10
            mock_get.return_value = mock_context

            mock_plan = Mock()
            mock_plan.__str__ = Mock(return_value="test_plan")

            # Initially should return None (cache miss)
            result = cache.get(mock_plan)
            assert result is None

            # Put something in cache
            test_result = "cached_result"
            cache.put(mock_plan, test_result)

            # Should now retrieve from cache (cache hit)
            result = cache.get(mock_plan)
            assert result == test_result

            # Stats should reflect activity
            stats = cache.get_stats()
            assert stats["hits"] == 1
            assert stats["misses"] == 1
            assert stats["hit_rate"] == 0.5
            assert stats["cache_size"] == 1

        print("✅ Enabled cache operations validation passed")
        return True

    except Exception as e:
        print(f"❌ Enabled cache operations validation failed: {e}")
        return False


def test_cache_clear():
    """Test cache clearing functionality."""
    print("🔍 Testing cache clearing...")

    try:
        from ray.data._internal.cache.stage_cache import StageCache

        cache = StageCache()

        # Mock enabled context
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            mock_context.enable_stage_cache = True
            mock_context.target_max_block_size = 1024
            mock_context.target_min_block_size = 512
            mock_context.use_push_based_shuffle = True
            mock_context.actor_prefetcher_enabled = False
            mock_context.max_num_blocks_in_streaming_gen_buffer = 10
            mock_get.return_value = mock_context

            mock_plan = Mock()
            mock_plan.__str__ = Mock(return_value="test_plan")

            # Add something to cache
            cache.put(mock_plan, "test_result")
            assert cache.get(mock_plan) == "test_result"

            # Clear cache
            cache.clear()

            # Should be empty now
            assert cache.get(mock_plan) is None

            # Stats should be reset
            stats = cache.get_stats()
            assert stats["hits"] == 0
            assert stats["misses"] == 0
            assert stats["cache_size"] == 0

        print("✅ Cache clearing validation passed")
        return True

    except Exception as e:
        print(f"❌ Cache clearing validation failed: {e}")
        return False


def test_thread_safety():
    """Test basic thread safety of cache operations."""
    print("🔍 Testing thread safety...")

    try:
        from ray.data._internal.cache.stage_cache import StageCache

        cache = StageCache()
        results = []
        errors = []

        # Mock enabled context
        with patch(
            "ray.data._internal.cache.stage_cache.DataContext.get_current"
        ) as mock_get:
            mock_context = Mock()
            mock_context.enable_stage_cache = True
            mock_context.target_max_block_size = 1024
            mock_context.target_min_block_size = 512
            mock_context.use_push_based_shuffle = True
            mock_context.actor_prefetcher_enabled = False
            mock_context.max_num_blocks_in_streaming_gen_buffer = 10
            mock_get.return_value = mock_context

            def worker(thread_id):
                """Worker function for concurrent testing."""
                try:
                    mock_plan = Mock()
                    mock_plan.__str__ = Mock(return_value=f"plan_{thread_id}")

                    # Each thread works with its own plan
                    cache.put(mock_plan, f"result_{thread_id}")
                    result = cache.get(mock_plan)
                    results.append((thread_id, result))

                except Exception as e:
                    errors.append((thread_id, str(e)))

            # Run multiple threads
            threads = []
            for i in range(10):
                t = threading.Thread(target=worker, args=(i,))
                threads.append(t)
                t.start()

            # Wait for all threads
            for t in threads:
                t.join()

            # Check results
            assert len(errors) == 0, f"Thread safety errors: {errors}"
            assert len(results) == 10

            # Each thread should get its own result
            for thread_id, result in results:
                assert result == f"result_{thread_id}"

        print("✅ Thread safety validation passed")
        return True

    except Exception as e:
        print(f"❌ Thread safety validation failed: {e}")
        return False


def test_global_functions():
    """Test global cache functions."""
    print("🔍 Testing global functions...")

    try:
        from ray.data._internal.cache.stage_cache import (
            cache_stage_result,
            clear_stage_cache,
            get_cached_stage_result,
            get_stage_cache,
            get_stage_cache_stats,
        )

        # Test singleton
        cache1 = get_stage_cache()
        cache2 = get_stage_cache()
        assert cache1 is cache2

        # Test global functions exist and are callable
        assert callable(cache_stage_result)
        assert callable(clear_stage_cache)
        assert callable(get_cached_stage_result)
        assert callable(get_stage_cache_stats)

        # Test clear function
        clear_stage_cache()  # Should not raise exception

        # Test stats function
        stats = get_stage_cache_stats()
        assert isinstance(stats, dict)
        assert "hits" in stats
        assert "misses" in stats
        assert "hit_rate" in stats
        assert "cache_size" in stats

        print("✅ Global functions validation passed")
        return True

    except Exception as e:
        print(f"❌ Global functions validation failed: {e}")
        return False


def run_all_validations():
    """Run all validation tests."""
    print("🚀 Starting Stage Cache Validation Suite")
    print("=" * 50)

    tests = [
        test_stage_cache_imports,
        test_stage_cache_basic_structure,
        test_stage_cache_disabled_state,
        test_fingerprint_computation,
        test_cache_operations_disabled,
        test_cache_operations_enabled,
        test_cache_clear,
        test_thread_safety,
        test_global_functions,
    ]

    passed = 0
    failed = 0

    for test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ {test_func.__name__} failed with exception: {e}")
            failed += 1
        print()  # Add blank line between tests

    print("=" * 50)
    print(f"🏁 Validation Results: {passed} passed, {failed} failed")

    if failed == 0:
        print("✅ All validations passed! Stage cache implementation looks good.")
    else:
        print("❌ Some validations failed. Please check the implementation.")

    return failed == 0


if __name__ == "__main__":
    success = run_all_validations()
    sys.exit(0 if success else 1)
