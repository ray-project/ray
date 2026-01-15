"""Utility tests for cloud functionality."""

import asyncio
import sys

import pytest

from ray.llm._internal.common.utils.cloud_utils import (
    CloudObjectCache,
    is_remote_path,
    remote_object_cache,
)


class MockSyncFetcher:
    def __init__(self):
        self.call_count = 0
        self.calls = []

    def __call__(self, key: str):
        self.call_count += 1
        self.calls.append(key)
        if key == "missing":
            return -1
        return f"value-{key}"


class MockAsyncFetcher:
    def __init__(self):
        self.call_count = 0
        self.calls = []

    async def __call__(self, key: str):
        self.call_count += 1
        self.calls.append(key)
        if key == "missing":
            return -1
        return f"value-{key}"


class TestCloudObjectCache:
    """Tests for the CloudObjectCache class."""

    def test_sync_cache_basic(self):
        """Test basic synchronous cache functionality."""
        fetcher = MockSyncFetcher()
        cache = CloudObjectCache(max_size=2, fetch_fn=fetcher)

        # Test fetching a value (should be a miss)
        assert cache.get("key1") == "value-key1"
        assert fetcher.call_count == 1
        assert fetcher.calls == ["key1"]

        # Test cache hit (should not call fetcher)
        assert cache.get("key1") == "value-key1"
        assert fetcher.call_count == 1  # Count should not increase
        assert fetcher.calls == ["key1"]  # Calls should not change

        # Test cache size limit
        assert cache.get("key2") == "value-key2"  # Miss, should call fetcher
        assert fetcher.call_count == 2
        assert fetcher.calls == ["key1", "key2"]

        assert (
            cache.get("key3") == "value-key3"
        )  # Miss, should call fetcher and evict key1
        assert fetcher.call_count == 3
        assert fetcher.calls == ["key1", "key2", "key3"]

        assert len(cache) == 2

        # Verify key1 was evicted by checking if it's fetched again
        assert cache.get("key1") == "value-key1"  # Miss, should call fetcher
        assert fetcher.call_count == 4
        assert fetcher.calls == ["key1", "key2", "key3", "key1"]

        # Verify final cache state
        assert len(cache) == 2
        assert "key3" in cache._cache  # key3 should still be in cache
        assert "key1" in cache._cache  # key1 should be back in cache
        assert "key2" not in cache._cache  # key2 should have been evicted

    @pytest.mark.asyncio
    async def test_async_cache_missing_object_expiration(self):
        """Test cache expiration for missing objects in async mode."""
        fetcher = MockAsyncFetcher()
        cache = CloudObjectCache(
            max_size=2,
            fetch_fn=fetcher,
            missing_expire_seconds=1,  # 1 second to expire missing object
            exists_expire_seconds=3,  # 3 seconds to expire existing object
            missing_object_value=-1,
        )

        # Test missing object expiration
        assert await cache.aget("missing") is -1  # First fetch
        assert fetcher.call_count == 1
        assert fetcher.calls == ["missing"]

        # Should still be cached
        assert await cache.aget("missing") is -1  # Cache hit
        assert fetcher.call_count == 1  # No new fetch
        assert fetcher.calls == ["missing"]

        await asyncio.sleep(1.5)  # Wait for missing object to expire
        assert await cache.aget("missing") is -1  # Should fetch again after expiration
        assert fetcher.call_count == 2  # New fetch
        assert fetcher.calls == ["missing", "missing"]

    @pytest.mark.asyncio
    async def test_async_cache_existing_object_expiration(self):
        """Test expiration of existing objects in async mode."""
        fetcher = MockAsyncFetcher()
        cache = CloudObjectCache(
            max_size=2,
            fetch_fn=fetcher,
            missing_expire_seconds=1,  # 1 second to expire missing object
            exists_expire_seconds=3,  # 3 seconds to expire existing object
            missing_object_value=-1,
        )

        # Test existing object expiration
        assert await cache.aget("key1") == "value-key1"  # First fetch
        assert fetcher.call_count == 1
        assert fetcher.calls == ["key1"]

        # Should still be cached (not expired)
        assert await cache.aget("key1") == "value-key1"  # Cache hit
        assert fetcher.call_count == 1  # No new fetch
        assert fetcher.calls == ["key1"]

        await asyncio.sleep(1.5)  # Not expired yet (exists_expire_seconds=3)
        assert await cache.aget("key1") == "value-key1"  # Should still hit cache
        assert fetcher.call_count == 1  # No new fetch
        assert fetcher.calls == ["key1"]  # No change in calls

        await asyncio.sleep(2)  # Now expired (total > 2 seconds)
        assert await cache.aget("key1") == "value-key1"  # Should fetch again
        assert fetcher.call_count == 2  # New fetch
        assert fetcher.calls == ["key1", "key1"]

        # Verify final cache state
        assert len(cache) == 1


class TestRemoteObjectCacheDecorator:
    """Tests for the remote_object_cache decorator."""

    @pytest.mark.asyncio
    async def test_basic_functionality(self):
        """Test basic remote_object_cache decorator functionality."""
        call_count = 0
        MISSING = object()

        @remote_object_cache(
            max_size=2,
            missing_expire_seconds=1,
            exists_expire_seconds=3,
            missing_object_value=MISSING,
        )
        async def fetch(key: str):
            nonlocal call_count
            call_count += 1
            if key == "missing":
                return MISSING
            return f"value-{key}"

        # Test cache hit
        assert await fetch("key1") == "value-key1"
        assert call_count == 1
        assert await fetch("key1") == "value-key1"  # Should hit cache
        assert call_count == 1  # Count should not increase

        # Test cache size limit
        assert await fetch("key2") == "value-key2"
        assert call_count == 2
        assert await fetch("key3") == "value-key3"  # Should evict key1
        assert call_count == 3

        # Verify key1 was evicted
        assert await fetch("key1") == "value-key1"
        assert call_count == 4

    @pytest.mark.asyncio
    async def test_expiration(self):
        """Test cache expiration for both missing and existing objects."""
        call_count = 0
        MISSING = object()

        @remote_object_cache(
            max_size=2,
            missing_expire_seconds=1,  # 1 second to expire missing object
            exists_expire_seconds=3,  # 3 seconds to expire existing object
            missing_object_value=MISSING,
        )
        async def fetch(key: str):
            nonlocal call_count
            call_count += 1
            if key == "missing":
                return MISSING
            return f"value-{key}"

        # Test missing object expiration
        assert await fetch("missing") is MISSING
        assert call_count == 1
        assert await fetch("missing") is MISSING  # Should hit cache
        assert call_count == 1

        await asyncio.sleep(1.5)  # Wait for missing object to expire
        assert await fetch("missing") is MISSING  # Should fetch again
        assert call_count == 2

        # Test existing object expiration
        assert await fetch("key1") == "value-key1"
        assert call_count == 3
        assert await fetch("key1") == "value-key1"  # Should hit cache
        assert call_count == 3

        await asyncio.sleep(1.5)  # Not expired yet
        assert await fetch("key1") == "value-key1"  # Should still hit cache
        assert call_count == 3

        await asyncio.sleep(2)  # Now expired (total > 3 seconds)
        assert await fetch("key1") == "value-key1"  # Should fetch again
        assert call_count == 4

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in remote_object_cache decorator."""
        call_count = 0

        @remote_object_cache(max_size=2)
        async def fetch(key: str):
            nonlocal call_count
            call_count += 1
            if key == "error":
                raise ValueError("Test error")
            return f"value-{key}"

        # Test successful case
        assert await fetch("key1") == "value-key1"
        assert call_count == 1

        # Test error case
        with pytest.raises(ValueError, match="Test error"):
            await fetch("error")
        assert call_count == 2

        # Verify error wasn't cached
        with pytest.raises(ValueError, match="Test error"):
            await fetch("error")
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_concurrent_access(self):
        """Test concurrent access to cached function."""
        call_count = 0
        DELAY = 0.1

        @remote_object_cache(max_size=2)
        async def slow_fetch(key: str):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(DELAY)  # Simulate slow operation
            return f"value-{key}"

        # Launch multiple concurrent calls
        tasks = [slow_fetch("key1") for _ in range(5)]
        results = await asyncio.gather(*tasks)

        # All results should be the same
        assert all(r == "value-key1" for r in results)
        # Should only call once despite multiple concurrent requests
        assert call_count == 1


class TestIsRemotePath:
    """Tests for the is_remote_path utility function."""

    def test_s3_paths(self):
        """Test S3 path detection."""
        assert is_remote_path("s3://bucket/path") is True
        assert is_remote_path("s3://bucket") is True
        assert is_remote_path("s3://anonymous@bucket/path") is True

    def test_gcs_paths(self):
        """Test GCS path detection."""
        assert is_remote_path("gs://bucket/path") is True
        assert is_remote_path("gs://bucket") is True
        assert is_remote_path("gs://anonymous@bucket/path") is True

    def test_abfss_paths(self):
        """Test ABFSS path detection."""
        assert (
            is_remote_path("abfss://container@account.dfs.core.windows.net/path")
            is True
        )
        assert is_remote_path("abfss://container@account.dfs.core.windows.net") is True

    def test_azure_paths(self):
        """Test Azure path detection."""
        assert (
            is_remote_path("azure://container@account.blob.core.windows.net/path")
            is True
        )
        assert (
            is_remote_path("azure://container@account.dfs.core.windows.net/path")
            is True
        )

    def test_local_paths(self):
        """Test local path detection."""
        assert is_remote_path("/local/path") is False
        assert is_remote_path("./relative/path") is False
        assert is_remote_path("file:///local/path") is False
        assert is_remote_path("http://example.com") is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
