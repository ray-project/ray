import sys
import pytest
import time
import asyncio
from ray.llm._internal.serve.deployments.utils.cloud_utils import CloudObjectCache

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

def test_sync_cache_basic():
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

    assert cache.get("key3") == "value-key3"  # Miss, should call fetcher and evict key1
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


def test_sync_cache_missing_object_expiration():
    """Test cache expiration for both missing and existing objects."""
    fetcher = MockSyncFetcher()
    cache = CloudObjectCache(
        max_size=2,
        fetch_fn=fetcher,
        missing_expire_seconds=1, # 1 second to expire missing object
        exists_expire_seconds=3, # 3 seconds to expire existing object
        missing_object_value=-1,
    )

    # Test missing object expiration
    assert cache.get("missing") is -1  # First fetch
    assert fetcher.call_count == 1
    assert fetcher.calls == ["missing"]
    
    # Should still be cached
    assert cache.get("missing") is -1  # Cache hit
    assert fetcher.call_count == 1  # No new fetch
    assert fetcher.calls == ["missing"]
    
    time.sleep(1.5)  # Wait for missing object to expire
    assert cache.get("missing") is -1  # Should fetch again after expiration
    assert fetcher.call_count == 2  # New fetch
    assert fetcher.calls == ["missing", "missing"]


def test_sync_cache_existing_object_expiration():
    """Test expiration of existing objects in sync mode."""
    fetcher = MockSyncFetcher()
    cache = CloudObjectCache(
        max_size=2,
        fetch_fn=fetcher,
        missing_expire_seconds=1, # 1 second to expire missing object
        exists_expire_seconds=3, # 3 seconds to expire existing object
        missing_object_value=-1,
    )
    
    # Test existing object expiration
    assert cache.get("key1") == "value-key1"  # First fetch
    assert fetcher.call_count == 1
    assert fetcher.calls == ["key1"]
    
    # Should still be cached (not expired)
    assert cache.get("key1") == "value-key1"  # Cache hit
    assert fetcher.call_count == 1  # No new fetch
    
    time.sleep(1.5)  # Not expired yet (exists_expire_seconds=3)
    assert cache.get("key1") == "value-key1"  # Should still hit cache
    assert fetcher.call_count == 1  # No new fetch
    
    time.sleep(2)  # Now expired (total > 3 seconds)
    assert cache.get("key1") == "value-key1"  # Should fetch again
    assert fetcher.call_count == 2  # New fetch
    assert fetcher.calls == ["key1", "key1"]
    
    # Verify final cache state
    assert len(cache) == 1


@pytest.mark.asyncio
async def test_async_cache_missing_object_expiration():
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
async def test_async_cache_existing_object_expiration():
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
